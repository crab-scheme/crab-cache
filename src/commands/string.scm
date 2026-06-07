; commands/string.scm — the string type + counters.
;
; Handler contract (every command module follows it):
;   (handler ctx operands) -> reply
;     ctx      : a shard-ctx
;     operands : list of bytevector arguments, command NAME excluded
;     reply    : a reply.scm value
; Each handler validates its own arity and types and registers itself with
; (register-command! "NAME" handler) at load time.
;
; Depends on: encoding.scm, store-ctx.scm, reply.scm, shard.scm.

; Fetch a string key's (value . deadline), or 'wrong if another type, or
; #f if absent. Applies lazy expiry via key-entry.
(define (str-get ctx key)
  (let ((e (key-entry ctx key)))
    (cond ((not e) #f)
          ((char=? (car e) #\s)
           (let ((v (kv-get ctx (str-key key))))
             ; perf #4: warm the in-memory serving map on a shard read so a cold
             ; map (after recovery/failover) repopulates here. Persistent only —
             ; TTL'd keys stay out so their reads keep routing to lazy-expiry.
             (if (= (cdr e) 0) (table-insert! 'cc-str key v))
             (cons v (cdr e))))
          (else 'wrong))))

; Write a string value with an explicit deadline (purging any prior
; non-string key first, since SET/INCR/APPEND replace any type).
(define (str-write! ctx key valbv deadline)
  (let ((t (key-type ctx key)))
    (if (and t (not (char=? t #\s))) (purge-key! ctx key)))
  (kv-put! ctx (str-key key) valbv)
  (dir-set! ctx key #\s deadline)
  ; perf #2/#4: keep the in-memory serving map coherent. A persistent string is
  ; inserted (value held WITH its key, so reads need no separate dir lookup); a
  ; TTL'd write is evicted so reads route to the shard's lazy-expiry path.
  (if (= deadline 0)
      (table-insert! 'cc-str key valbv)
      (table-delete! 'cc-str key)))

; ---- SET key value [NX|XX] [EX s|PX ms|KEEPTTL] ----

(define (parse-set-opts opts)
  ; -> (list nx? xx? ttl-mode ttl-val) | 'err
  ;    ttl-mode ∈ 'none 'keep 'ex   (ex value already in ticks)
  (let loop ((o opts) (nx #f) (xx #f) (mode 'none) (val 0))
    (if (null? o)
        (list nx xx mode val)
        (let ((w (string-upcase (bv->s (car o)))))
          (cond
            ((string=? w "NX") (loop (cdr o) #t xx mode val))
            ((string=? w "XX") (loop (cdr o) nx #t mode val))
            ((string=? w "KEEPTTL") (loop (cdr o) nx xx 'keep val))
            ((or (string=? w "EX") (string=? w "PX"))
             (if (null? (cdr o)) 'err
                 (let ((n (bytes->int (cadr o))))
                   (if (and n (> n 0)) (loop (cddr o) nx xx 'ex n) 'err))))
            (else 'err))))))

(define (cmd-set ctx operands)
  (if (< (length operands) 2)
      (r-wrong-args "set")
      (let ((key (car operands)) (val (cadr operands))
            (parsed (parse-set-opts (cddr operands))))
        (if (eq? parsed 'err)
            (r-syntax)
            (let* ((nx (car parsed)) (xx (cadr parsed))
                   (mode (caddr parsed)) (tval (cadddr parsed))
                   (e (key-entry ctx key)) (exists (and e #t)))
              (cond
                ((and nx exists) (r-nil))
                ((and xx (not exists)) (r-nil))
                (else
                 (let ((deadline
                        (cond ((eq? mode 'ex) (+ (clock ctx) tval))
                              ((eq? mode 'keep) (if e (cdr e) 0))
                              (else 0))))
                   (str-write! ctx key val deadline)
                   (r-ok)))))))))

(define (cmd-setnx ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "setnx")
      (if (key-exists? ctx (car operands))
          (r-int 0)
          (begin (str-write! ctx (car operands) (cadr operands) 0) (r-int 1)))))

; ---- GET / GETSET / STRLEN / APPEND ----

(define (cmd-get ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "get")
      (let ((g (str-get ctx (car operands))))
        (cond ((eq? g 'wrong) (r-wrongtype))
              ((not g) (r-nil))
              (else (r-bulk (car g)))))))

(define (cmd-getset ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "getset")
      (let ((g (str-get ctx (car operands))))
        (if (eq? g 'wrong) (r-wrongtype)
            (begin (str-write! ctx (car operands) (cadr operands) 0)
                   (if g (r-bulk (car g)) (r-nil)))))))

(define (cmd-strlen ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "strlen")
      (let ((g (str-get ctx (car operands))))
        (cond ((eq? g 'wrong) (r-wrongtype))
              ((not g) (r-int 0))
              (else (r-int (bytevector-length (car g))))))))

(define (cmd-append ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "append")
      (let ((g (str-get ctx (car operands))))
        (if (eq? g 'wrong) (r-wrongtype)
            (let* ((old (if g (car g) (make-bytevector 0 0)))
                   (new (bytevector-append old (cadr operands)))
                   (deadline (if g (cdr g) 0)))
              (str-write! ctx (car operands) new deadline)
              (r-int (bytevector-length new)))))))

; ---- INCR / DECR / INCRBY / DECRBY ----

(define (incr-by ctx key delta)
  (let ((g (str-get ctx key)))
    (if (eq? g 'wrong) (r-wrongtype)
        (let ((cur (if g (bytes->int (car g)) 0)))
          (cond ((and g (not cur)) (r-not-int))
                (else
                 (let ((n (+ (if cur cur 0) delta))
                       (deadline (if g (cdr g) 0)))
                   (str-write! ctx key (int->bytes n) deadline)
                   (r-int n))))))))

(define (cmd-incr ctx o)
  (if (= (length o) 1) (incr-by ctx (car o) 1) (r-wrong-args "incr")))
(define (cmd-decr ctx o)
  (if (= (length o) 1) (incr-by ctx (car o) -1) (r-wrong-args "decr")))
(define (cmd-incrby ctx o)
  (if (= (length o) 2)
      (let ((n (bytes->int (cadr o))))
        (if n (incr-by ctx (car o) n) (r-not-int)))
      (r-wrong-args "incrby")))
(define (cmd-decrby ctx o)
  (if (= (length o) 2)
      (let ((n (bytes->int (cadr o))))
        (if n (incr-by ctx (car o) (- n)) (r-not-int)))
      (r-wrong-args "decrby")))

; ---- MGET / MSET ----

(define (cmd-mget ctx operands)
  (if (null? operands)
      (r-wrong-args "mget")
      (r-array
       (map (lambda (key)
              (let ((g (str-get ctx key)))
                (if (or (eq? g 'wrong) (not g)) (r-nil) (r-bulk (car g)))))
            operands))))

(define (cmd-mset ctx operands)
  (if (or (null? operands) (not (even? (length operands))))
      (r-wrong-args "mset")
      (let loop ((o operands))
        (if (null? o)
            (r-ok)
            (begin (str-write! ctx (car o) (cadr o) 0)
                   (loop (cddr o)))))))

(register-command! "SET" cmd-set)
(register-command! "SETNX" cmd-setnx)
(register-command! "GET" cmd-get)
(register-command! "GETSET" cmd-getset)
(register-command! "STRLEN" cmd-strlen)
(register-command! "APPEND" cmd-append)
(register-command! "INCR" cmd-incr)
(register-command! "DECR" cmd-decr)
(register-command! "INCRBY" cmd-incrby)
(register-command! "DECRBY" cmd-decrby)
(register-command! "MGET" cmd-mget)
(register-command! "MSET" cmd-mset)
