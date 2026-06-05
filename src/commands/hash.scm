; commands/hash.scm — the hash type.
;
; Handler contract: (handler ctx operands) -> reply
;   ctx      : a shard-ctx
;   operands : list of bytevector arguments, command NAME excluded
; Registers at load via (register-command! ...).
;
; Depends on: encoding.scm, store-ctx.scm, reply.scm, shard.scm.
;
; Hash lifecycle:
;   - type char: #\h
;   - Data: H:<lp ukey><field> -> field-value-bytes
;   - Cardinality via kv-scan-count (correctness-first, no counter)
;   - ctype-touch! on first write; purge-if-empty! after each delete

; ---- internal helpers ----

; Get a field value from a hash, or #f if absent. Does NOT type-check.
(define (hash-field-get ctx ukey field)
  (kv-get ctx (hash-field-key ukey field)))

; Write a field value. Returns #t if the field is new, #f if overwritten.
(define (hash-field-set! ctx ukey field val)
  (let* ((fk (hash-field-key ukey field))
         (is-new (not (kv-get ctx fk))))
    (ctype-touch! ctx ukey #\h)
    (kv-put! ctx fk val)
    is-new))

; Delete a field. Returns #t if it existed.
(define (hash-field-del! ctx ukey field)
  (let* ((fk (hash-field-key ukey field))
         (existed (kv-get ctx fk)))
    (if existed
        (begin (kv-del! ctx fk) #t)
        #f)))

; ---- HSET key field value [field value ...] ----
; Returns number of NEW fields added (not updated).
(define (cmd-hset ctx operands)
  (if (or (< (length operands) 3) (not (even? (- (length operands) 1))))
      (r-wrong-args "hset")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          (else
           (let loop ((pairs (cdr operands)) (added 0))
             (if (null? pairs)
                 (r-int added)
                 (let ((field (car pairs)) (val (cadr pairs)))
                   (let ((is-new (hash-field-set! ctx ukey field val)))
                     (loop (cddr pairs) (if is-new (+ added 1) added)))))))))))

; ---- HSETNX key field value ----
; Set only if field does not exist. Returns 1 if set, 0 if already existed.
(define (cmd-hsetnx ctx operands)
  (if (not (= (length operands) 3))
      (r-wrong-args "hsetnx")
      (let ((ukey (car operands)) (field (cadr operands)) (val (caddr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          ((hash-field-get ctx ukey field) (r-int 0))
          (else
           (hash-field-set! ctx ukey field val)
           (r-int 1))))))

; ---- HGET key field ----
(define (cmd-hget ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "hget")
      (let ((ukey (car operands)) (field (cadr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          (else
           (let ((v (hash-field-get ctx ukey field)))
             (if v (r-bulk v) (r-nil))))))))

; ---- HMGET key field [field ...] ----
; Returns array of bulk/nil replies.
(define (cmd-hmget ctx operands)
  (if (< (length operands) 2)
      (r-wrong-args "hmget")
      (let ((ukey (car operands)) (fields (cdr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          (else
           (r-array (map (lambda (field)
                           (let ((v (hash-field-get ctx ukey field)))
                             (if v (r-bulk v) (r-nil))))
                         fields)))))))

; ---- HMSET key field value [field value ...] ----
; Old multi-set alias; always returns OK.
(define (cmd-hmset ctx operands)
  (if (or (< (length operands) 3) (not (even? (- (length operands) 1))))
      (r-wrong-args "hmset")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          (else
           (let loop ((pairs (cdr operands)))
             (if (null? pairs)
                 (r-ok)
                 (begin
                   (hash-field-set! ctx ukey (car pairs) (cadr pairs))
                   (loop (cddr pairs))))))))))

; ---- HDEL key field [field ...] ----
; Returns number of fields removed.
(define (cmd-hdel ctx operands)
  (if (< (length operands) 2)
      (r-wrong-args "hdel")
      (let ((ukey (car operands)) (fields (cdr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          ((not (key-exists? ctx ukey)) (r-int 0))
          (else
           (let loop ((fs fields) (removed 0))
             (if (null? fs)
                 (begin
                   (purge-if-empty! ctx ukey (kv-scan-count ctx (hash-prefix ukey)))
                   (r-int removed))
                 (let ((deleted (hash-field-del! ctx ukey (car fs))))
                   (loop (cdr fs) (if deleted (+ removed 1) removed))))))))))

; ---- HGETALL key ----
; Returns flat array: field, val, field, val, ...
(define (cmd-hgetall ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "hgetall")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          (else
           (let ((pairs (kv-scan ctx (hash-prefix ukey))))
             (r-array
              (let loop ((ps pairs) (acc '()))
                (if (null? ps)
                    (reverse acc)
                    (let ((field (hash-field-of (caar ps) ukey))
                          (val   (cdar ps)))
                      (loop (cdr ps) (cons (r-bulk val)
                                           (cons (r-bulk field) acc)))))))))))))

; ---- HKEYS key ----
(define (cmd-hkeys ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "hkeys")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          (else
           (r-array (map (lambda (kv) (r-bulk (hash-field-of (car kv) ukey)))
                         (kv-scan ctx (hash-prefix ukey)))))))))

; ---- HVALS key ----
(define (cmd-hvals ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "hvals")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          (else
           (r-array (map (lambda (kv) (r-bulk (cdr kv)))
                         (kv-scan ctx (hash-prefix ukey)))))))))

; ---- HLEN key ----
(define (cmd-hlen ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "hlen")
      (let ((ukey (car operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          (else (r-int (kv-scan-count ctx (hash-prefix ukey))))))))

; ---- HEXISTS key field ----
(define (cmd-hexists ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "hexists")
      (let ((ukey (car operands)) (field (cadr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          ((hash-field-get ctx ukey field) (r-int 1))
          (else (r-int 0))))))

; ---- HSTRLEN key field ----
(define (cmd-hstrlen ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "hstrlen")
      (let ((ukey (car operands)) (field (cadr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          (else
           (let ((v (hash-field-get ctx ukey field)))
             (if v (r-int (bytevector-length v)) (r-int 0))))))))

; ---- HINCRBY key field increment ----
; Field must be an integer string (or absent, treated as 0).
(define (cmd-hincrby ctx operands)
  (if (not (= (length operands) 3))
      (r-wrong-args "hincrby")
      (let ((ukey (car operands)) (field (cadr operands)) (incrbv (caddr operands)))
        (cond
          ((eq? (type-guard ctx ukey #\h) 'wrong) (r-wrongtype))
          (else
           (let ((delta (bytes->int incrbv)))
             (if (not delta)
                 (r-not-int)
                 (let* ((cur-bv (hash-field-get ctx ukey field))
                        (cur (if cur-bv (bytes->int cur-bv) 0)))
                   (if (and cur-bv (not cur))
                       (r-not-int)
                       (let ((n (+ (if cur cur 0) delta)))
                         (hash-field-set! ctx ukey field (int->bytes n))
                         (r-int n)))))))))))

; ---- registrations ----

(register-command! "HSET"    cmd-hset)
(register-command! "HSETNX"  cmd-hsetnx)
(register-command! "HGET"    cmd-hget)
(register-command! "HMGET"   cmd-hmget)
(register-command! "HMSET"   cmd-hmset)
(register-command! "HDEL"    cmd-hdel)
(register-command! "HGETALL" cmd-hgetall)
(register-command! "HKEYS"   cmd-hkeys)
(register-command! "HVALS"   cmd-hvals)
(register-command! "HLEN"    cmd-hlen)
(register-command! "HEXISTS"  cmd-hexists)
(register-command! "HSTRLEN" cmd-hstrlen)
(register-command! "HINCRBY" cmd-hincrby)
