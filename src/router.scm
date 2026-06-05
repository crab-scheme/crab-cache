; router.scm — map a decoded command to where it should run.
;
; A command is (name-bv . operand-bvs). We classify it into a route:
;   'any        — keyless command, run on any shard (we use shard 0)
;   'all        — fan out to every shard and aggregate (DBSIZE/FLUSHALL/KEYS)
;   'cluster    — CLUSTER subcommands, answered from topology (conn-level)
;   'crossslot  — multi-key command whose keys span >1 slot (Redis -CROSSSLOT)
;   <integer>   — the shard index that owns the (single) slot of all its keys
;
; Depends on: slotmap.scm (key-slot, slot->shard), encoding.scm.

(define (cmd-upcase name-bv) (string-upcase (utf8->string name-bv)))

(define (member-str s lst)
  (cond ((null? lst) #f) ((string=? s (car lst)) #t) (else (member-str s (cdr lst)))))

; indices 0..n-1
(define (index-list n)
  (let loop ((i (- n 1)) (acc '())) (if (< i 0) acc (loop (- i 1) (cons i acc)))))
; even indices 0,2,4,... below n  (MSET key val key val …)
(define (even-index-list n)
  (let loop ((i 0) (acc '())) (if (>= i n) (reverse acc) (loop (+ i 2) (cons i acc)))))

; Which operand indices are keys (or a symbolic route). argc = #operands.
(define (key-positions name argc)
  (cond
    ((member-str name '("PING" "ECHO" "SELECT" "COMMAND" "INFO" "QUIT" "TICK")) 'any)
    ((member-str name '("DBSIZE" "FLUSHALL" "FLUSHDB" "KEYS")) 'all)
    ((string=? name "CLUSTER") 'cluster)
    ((member-str name '("DEL" "EXISTS" "UNLINK" "MGET" "SMISMEMBER")) (index-list argc))
    ((string=? name "MSET") (even-index-list argc))
    (else (list 0))))                       ; single key at operand 0

(define (all-equal? xs)
  (cond ((null? xs) #t) ((null? (cdr xs)) #t)
        ((= (car xs) (cadr xs)) (all-equal? (cdr xs))) (else #f)))

; classify-route : name-string operand-bvs nshards -> route
(define (classify-route name operands nshards)
  (let ((spec (key-positions name (length operands))))
    (cond
      ((symbol? spec) spec)                  ; 'any / 'all / 'cluster
      (else
       (let* ((n (length operands))
              (keys (let loop ((ps spec) (acc '()))
                      (cond ((null? ps) (reverse acc))
                            ((< (car ps) n) (loop (cdr ps) (cons (list-ref operands (car ps)) acc)))
                            (else (loop (cdr ps) acc))))))
         (if (null? keys)
             0                               ; keyed command given no key -> shard 0 arity-errs
             (let ((slots (map key-slot keys)))
               (if (all-equal? slots)
                   (slot->shard (car slots) nshards)
                   'crossslot))))))))

; ---- aggregation for 'all (fan-out) routes ----
; Combine per-shard replies (a list of reply.scm values) for one command.
(define (aggregate-replies name replies)
  (let ((u (string-upcase name)))
    (cond
      ((string=? u "DBSIZE")
       (r-int (let loop ((rs replies) (s 0))
                (if (null? rs) s
                    (loop (cdr rs) (+ s (if (eq? (reply-tag (car rs)) 'int) (reply-val (car rs)) 0)))))))
      ((string=? u "KEYS")
       (r-array (let loop ((rs replies) (acc '()))
                  (if (null? rs) acc
                      (loop (cdr rs)
                            (append acc (if (eq? (reply-tag (car rs)) 'array) (reply-val (car rs)) '())))))))
      (else (r-ok)))))                        ; FLUSHALL/FLUSHDB -> OK
