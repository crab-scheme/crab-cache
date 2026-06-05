; commands/list.scm — the list type.
;
; Handler contract: (handler ctx operands) -> reply
; Each command validates arity, type-guards, and registers itself.
;
; Depends on: encoding.scm, store-ctx.scm, reply.scm, shard.scm.
;
; List encoding:
;   - type char: #\l
;   - Data: L:<lp ukey><seq:8 order> -> element bytes (scan sorted = head->tail)
;   - Meta: L#:<ukey> -> [head:s64][tail:s64][len:u64] (24 bytes)
;   LPUSH decrements head; RPUSH increments tail.
;   Empty: no meta + no elements; purge when len=0.
;
; IMPORTANT: After LREM there may be gaps in seq space. All index-based ops
; (LINDEX, LSET, LRANGE, LTRIM) MUST index by scanning sorted keys, not by
; seq arithmetic (head + i).

; ---- meta helpers ----

(define (lm-get ctx ukey)
  (kv-get ctx (list-meta-key ukey)))

(define (lm-put! ctx ukey m)
  (kv-put! ctx (list-meta-key ukey) m))

; Return (head tail len) triple from stored meta, or default (0 -1 0).
(define (lm-triple ctx ukey)
  (let ((m (lm-get ctx ukey)))
    (if m
        (list (list-meta-head m) (list-meta-tail m) (list-meta-len m))
        (list 0 -1 0))))

; All elements as (seq . bv) in head->tail order (sorted by key bytes).
(define (list-all-seqs ctx ukey)
  (map (lambda (kv) (cons (list-seq-of (car kv) ukey) (cdr kv)))
       (kv-scan ctx (list-prefix ukey))))

; Rebuild and store meta from live keys (after LREM/LTRIM which create gaps).
; Purges the key if now empty.
(define (rebuild-meta! ctx ukey)
  (let ((live (list-all-seqs ctx ukey)))
    (let ((n (length live)))
      (if (= n 0)
          (begin
            (kv-del! ctx (list-meta-key ukey))
            (purge-key! ctx ukey))
          (let ((new-head (caar live))
                (new-tail (caar (last-pair live))))
            (lm-put! ctx ukey (list-meta new-head new-tail n)))))))

; Scan-indexed access: returns (seq . bv) at list index idx (0-based, neg ok).
; Returns #f if out of range.
(define (list-get-by-idx ctx ukey idx)
  (let ((elems (list-all-seqs ctx ukey)))
    (let* ((n (length elems))
           (ri (if (< idx 0) (+ n idx) idx)))
      (if (or (< ri 0) (>= ri n))
          #f
          (list-ref elems ri)))))

; ---- LPUSH / RPUSH internals ----

(define (do-lpush ctx ukey vals)
  (let ((triple (lm-triple ctx ukey)))
    (ctype-touch! ctx ukey #\l)
    (let loop ((vs vals) (h (car triple)) (t (cadr triple)) (l (caddr triple)))
      (if (null? vs)
          (begin (lm-put! ctx ukey (list-meta h t l)) (r-int l))
          (let ((nh (- h 1)))
            (kv-put! ctx (list-elem-key ukey nh) (car vs))
            (loop (cdr vs) nh t (+ l 1)))))))

(define (do-rpush ctx ukey vals)
  (let ((triple (lm-triple ctx ukey)))
    (ctype-touch! ctx ukey #\l)
    (let loop ((vs vals) (h (car triple)) (t (cadr triple)) (l (caddr triple)))
      (if (null? vs)
          (begin (lm-put! ctx ukey (list-meta h t l)) (r-int l))
          (let ((nt (+ t 1)))
            (kv-put! ctx (list-elem-key ukey nt) (car vs))
            (loop (cdr vs) h nt (+ l 1)))))))

; ---- LPUSH key value [value ...] ----

(define (cmd-lpush ctx operands)
  (if (< (length operands) 2)
      (r-wrong-args "lpush")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          (else (do-lpush ctx ukey (cdr operands)))))))

; ---- RPUSH key value [value ...] ----

(define (cmd-rpush ctx operands)
  (if (< (length operands) 2)
      (r-wrong-args "rpush")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          (else (do-rpush ctx ukey (cdr operands)))))))

; ---- LPUSHX key value [value ...] ----

(define (cmd-lpushx ctx operands)
  (if (< (length operands) 2)
      (r-wrong-args "lpushx")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          ((not (key-exists? ctx ukey)) (r-int 0))
          (else (do-lpush ctx ukey (cdr operands)))))))

; ---- RPUSHX key value [value ...] ----

(define (cmd-rpushx ctx operands)
  (if (< (length operands) 2)
      (r-wrong-args "rpushx")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          ((not (key-exists? ctx ukey)) (r-int 0))
          (else (do-rpush ctx ukey (cdr operands)))))))

; ---- LPOP / RPOP internals ----

(define (do-pop ctx ukey from-head)
  (let ((triple (lm-triple ctx ukey)))
    (let ((head (car triple)) (tail (cadr triple)) (len (caddr triple)))
      (if (= len 0)
          (r-nil)
          (let ((seq (if from-head head tail)))
            (let ((val (kv-get ctx (list-elem-key ukey seq))))
              (kv-del! ctx (list-elem-key ukey seq))
              (let ((new-len (- len 1)))
                (if (= new-len 0)
                    (begin
                      (kv-del! ctx (list-meta-key ukey))
                      (purge-key! ctx ukey))
                    (if from-head
                        (lm-put! ctx ukey (list-meta (+ head 1) tail new-len))
                        (lm-put! ctx ukey (list-meta head (- tail 1) new-len)))))
              (if val (r-bulk val) (r-nil))))))))

; ---- LPOP key ----

(define (cmd-lpop ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "lpop")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          ((not (key-exists? ctx ukey)) (r-nil))
          (else (do-pop ctx ukey #t))))))

; ---- RPOP key ----

(define (cmd-rpop ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "rpop")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          ((not (key-exists? ctx ukey)) (r-nil))
          (else (do-pop ctx ukey #f))))))

; ---- LLEN key ----

(define (cmd-llen ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "llen")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          ((not (key-exists? ctx ukey)) (r-int 0))
          (else
           (let ((m (lm-get ctx ukey)))
             (r-int (if m (list-meta-len m) 0))))))))

; ---- LINDEX key index ----
; Scan-based so gaps from LREM are handled correctly.

(define (cmd-lindex ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "lindex")
      (let ((ukey (car operands)) (idxbv (cadr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          (else
           (let ((idx (bytes->int idxbv)))
             (if (not idx)
                 (r-not-int)
                 (if (not (key-exists? ctx ukey))
                     (r-nil)
                     (let ((pair (list-get-by-idx ctx ukey idx)))
                       (if pair (r-bulk (cdr pair)) (r-nil)))))))))))

; ---- LSET key index value ----

(define (cmd-lset ctx operands)
  (if (not (= (length operands) 3))
      (r-wrong-args "lset")
      (let ((ukey (car operands)) (idxbv (cadr operands)) (val (caddr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          ((not (key-exists? ctx ukey)) (r-err "ERR no such key"))
          (else
           (let ((idx (bytes->int idxbv)))
             (if (not idx)
                 (r-not-int)
                 (let ((pair (list-get-by-idx ctx ukey idx)))
                   (if (not pair)
                       (r-err "ERR index out of range")
                       (begin
                         (kv-put! ctx (list-elem-key ukey (car pair)) val)
                         (r-ok)))))))))))

; ---- LRANGE key start stop ----
; Scan-based to handle gaps from LREM.

(define (cmd-lrange ctx operands)
  (if (not (= (length operands) 3))
      (r-wrong-args "lrange")
      (let ((ukey (car operands)) (startbv (cadr operands)) (stopbv (caddr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          ((not (key-exists? ctx ukey)) (r-array '()))
          (else
           (let ((si (bytes->int startbv)) (ei (bytes->int stopbv)))
             (if (or (not si) (not ei))
                 (r-not-int)
                 (let* ((elems (list-all-seqs ctx ukey))
                        (n (length elems))
                        (rstart (if (< si 0) (max 0 (+ n si)) si))
                        (rstop (if (< ei 0) (+ n ei) (min ei (- n 1)))))
                   (if (> rstart rstop)
                       (r-array '())
                       (let loop ((i rstart) (acc '()))
                         (if (> i rstop)
                             (r-array (reverse acc))
                             (loop (+ i 1) (cons (r-bulk (cdr (list-ref elems i))) acc)))))))))))))

; ---- LREM key count value ----
; count > 0: remove up to count occurrences head->tail.
; count < 0: remove up to |count| occurrences tail->head.
; count = 0: remove all occurrences.
; Returns number removed.

(define (cmd-lrem ctx operands)
  (if (not (= (length operands) 3))
      (r-wrong-args "lrem")
      (let ((ukey (car operands)) (countbv (cadr operands)) (valbv (caddr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          ((not (key-exists? ctx ukey)) (r-int 0))
          (else
           (let ((count (bytes->int countbv)))
             (if (not count)
                 (r-not-int)
                 (let* ((elems (list-all-seqs ctx ukey))
                        (candidates (if (< count 0) (reverse elems) elems))
                        (limit (if (= count 0) (length candidates) (abs count))))
                   (let loop ((cs candidates) (removed 0))
                     (if (or (null? cs) (= removed limit))
                         (begin
                           (rebuild-meta! ctx ukey)
                           (r-int removed))
                         (if (bytevector=? (cdar cs) valbv)
                             (begin
                               (kv-del! ctx (list-elem-key ukey (caar cs)))
                               (loop (cdr cs) (+ removed 1)))
                             (loop (cdr cs) removed))))))))))))

; ---- LTRIM key start stop ----
; Scan-based to handle gaps and correct index semantics.

(define (cmd-ltrim ctx operands)
  (if (not (= (length operands) 3))
      (r-wrong-args "ltrim")
      (let ((ukey (car operands)) (startbv (cadr operands)) (stopbv (caddr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\l) 'wrong) (r-wrongtype))
          ((not (key-exists? ctx ukey)) (r-ok))
          (else
           (let ((si (bytes->int startbv)) (ei (bytes->int stopbv)))
             (if (or (not si) (not ei))
                 (r-not-int)
                 (let* ((elems (list-all-seqs ctx ukey))
                        (n (length elems))
                        (rstart (if (< si 0) (max 0 (+ n si)) si))
                        (rstop (if (< ei 0) (+ n ei) (min ei (- n 1)))))
                   (if (> rstart rstop)
                       (begin
                         (for-each (lambda (kv) (kv-del! ctx (car kv)))
                                   (kv-scan ctx (list-prefix ukey)))
                         (kv-del! ctx (list-meta-key ukey))
                         (purge-key! ctx ukey)
                         (r-ok))
                       (begin
                         (let loop ((i 0) (es elems))
                           (if (not (null? es))
                               (begin
                                 (if (or (< i rstart) (> i rstop))
                                     (kv-del! ctx (list-elem-key ukey (caar es))))
                                 (loop (+ i 1) (cdr es)))))
                         (rebuild-meta! ctx ukey)
                         (r-ok)))))))))))

; ---- registrations ----

(register-command! "LPUSH"  cmd-lpush)
(register-command! "RPUSH"  cmd-rpush)
(register-command! "LPUSHX" cmd-lpushx)
(register-command! "RPUSHX" cmd-rpushx)
(register-command! "LPOP"   cmd-lpop)
(register-command! "RPOP"   cmd-rpop)
(register-command! "LLEN"   cmd-llen)
(register-command! "LINDEX" cmd-lindex)
(register-command! "LSET"   cmd-lset)
(register-command! "LRANGE" cmd-lrange)
(register-command! "LREM"   cmd-lrem)
(register-command! "LTRIM"  cmd-ltrim)
