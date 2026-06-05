; commands/keys.scm — key-space commands cutting across all types.
;
; DEL EXISTS TYPE EXPIRE PEXPIRE TTL PTTL PERSIST KEYS RENAME
;
; TTL is a logical-clock model (DD-3): EXPIRE/PEXPIRE set a deadline of
; (clock + n) ticks; TTL/PTTL report (deadline - clock). Phase 3 advances
; the clock locally; Phase 5 advances it through Raft so every replica
; expires a key at the same tick.
;
; Depends on: encoding.scm, store-ctx.scm, reply.scm, shard.scm.

(define (type-char->name c)
  (cond ((char=? c #\s) "string") ((char=? c #\h) "hash")
        ((char=? c #\l) "list")   ((char=? c #\e) "set")
        ((char=? c #\z) "zset")   (else "none")))

(define (cmd-del ctx operands)
  (if (null? operands)
      (r-wrong-args "del")
      (r-int (let loop ((o operands) (n 0))
               (if (null? o) n
                   (loop (cdr o) (if (purge-key! ctx (car o)) (+ n 1) n)))))))
; UNLINK is DEL semantics here (no separate async reclaim).
(define (cmd-exists ctx operands)
  (if (null? operands)
      (r-wrong-args "exists")
      (r-int (let loop ((o operands) (n 0))
               (if (null? o) n
                   (loop (cdr o) (if (key-exists? ctx (car o)) (+ n 1) n)))))))

(define (cmd-type ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "type")
      (let ((t (key-type ctx (car operands))))
        (r-simple (if t (type-char->name t) "none")))))

(define (expire-generic ctx operands name)
  (if (not (= (length operands) 2))
      (r-wrong-args name)
      (let ((n (bytes->int (cadr operands))))
        (if (not n)
            (r-not-int)
            ; A non-positive expire deletes the key immediately (Redis).
            (if (<= n 0)
                (r-int (if (purge-key! ctx (car operands)) 1 0))
                (if (set-deadline! ctx (car operands) (+ (clock ctx) n))
                    (r-int 1) (r-int 0)))))))
(define (cmd-expire ctx o)  (expire-generic ctx o "expire"))
(define (cmd-pexpire ctx o) (expire-generic ctx o "pexpire"))

(define (ttl-generic ctx operands name)
  (if (not (= (length operands) 1))
      (r-wrong-args name)
      (let ((e (key-entry ctx (car operands))))
        (cond ((not e) (r-int -2))            ; no such key
              ((= (cdr e) 0) (r-int -1))      ; exists, no expiry
              (else (r-int (- (cdr e) (clock ctx))))))))
(define (cmd-ttl ctx o)  (ttl-generic ctx o "ttl"))
(define (cmd-pttl ctx o) (ttl-generic ctx o "pttl"))

(define (cmd-persist ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "persist")
      (let ((e (key-entry ctx (car operands))))
        (if (and e (not (= (cdr e) 0)))
            (begin (dir-set! ctx (car operands) (car e) 0) (r-int 1))
            (r-int 0)))))

; ---- KEYS pattern (glob: * ? and literals; binary-safe over bytes) ----

(define (glob-match? pat str pi si)
  ; pat, str : bytevectors; pi, si : indices
  (let ((pn (bytevector-length pat)) (sn (bytevector-length str)))
    (cond
      ((= pi pn) (= si sn))
      (else
       (let ((pc (bytevector-u8-ref pat pi)))
         (cond
           ((= pc 42)  ; '*' — match zero+ chars
            (or (glob-match? pat str (+ pi 1) si)
                (and (< si sn) (glob-match? pat str pi (+ si 1)))))
           ((= pc 63)  ; '?' — match exactly one
            (and (< si sn) (glob-match? pat str (+ pi 1) (+ si 1))))
           ((= pc 92)  ; '\' — escape next pattern byte (literal)
            (and (< (+ pi 1) pn) (< si sn)
                 (= (bytevector-u8-ref pat (+ pi 1)) (bytevector-u8-ref str si))
                 (glob-match? pat str (+ pi 2) (+ si 1))))
           (else
            (and (< si sn)
                 (= pc (bytevector-u8-ref str si))
                 (glob-match? pat str (+ pi 1) (+ si 1))))))))))

(define (cmd-keys ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "keys")
      (let ((pat (car operands)))
        (r-array
         (let loop ((rows (kv-scan ctx TAG-DIR)) (acc '()))
           (if (null? rows)
               (reverse acc)
               (let* ((fullkey (caar rows))
                      (ukey (subbv fullkey (bytevector-length TAG-DIR)
                                   (bytevector-length fullkey))))
                 ; honor lazy expiry: only report live keys
                 (if (and (key-exists? ctx ukey) (glob-match? pat ukey 0 0))
                     (loop (cdr rows) (cons (r-bulk ukey) acc))
                     (loop (cdr rows) acc)))))))))

(define (cmd-rename ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "rename")
      (let ((src (car operands)) (dst (cadr operands)))
        (if (not (key-exists? ctx src))
            (r-err "ERR no such key")
            ; Move by copy: read src's full materialization is type-specific;
            ; simplest correct path — only strings need a fast path here, the
            ; general copy is a Phase-5 concern. For now support string rename
            ; and fall back to error for composite (documented Phase-3 limit).
            (let ((t (key-type ctx src)))
              (if (char=? t #\s)
                  (let* ((g (kv-get ctx (str-key src)))
                         (dl (key-deadline ctx src)))
                    (purge-key! ctx dst)
                    (str-write! ctx dst g dl)
                    (purge-key! ctx src)
                    (r-ok))
                  (r-err "ERR RENAME of composite types is a Phase-5 feature")))))))

; TICK [n] — internal: advance the shard's logical clock by n (default 1)
; and return the new value. The active-expiry actor (Phase 4) drives this;
; Phase 5 routes it through Raft so replicas tick in lockstep.
(define (cmd-tick ctx operands)
  (let ((n (if (null? operands) 1 (let ((v (bytes->int (car operands)))) (if v v 1)))))
    (ctx-clock-advance! ctx n)
    (r-int (clock ctx))))

(register-command! "TICK" cmd-tick)
(register-command! "DEL" cmd-del)
(register-command! "UNLINK" cmd-del)
(register-command! "EXISTS" cmd-exists)
(register-command! "TYPE" cmd-type)
(register-command! "EXPIRE" cmd-expire)
(register-command! "PEXPIRE" cmd-pexpire)
(register-command! "TTL" cmd-ttl)
(register-command! "PTTL" cmd-pttl)
(register-command! "PERSIST" cmd-persist)
(register-command! "KEYS" cmd-keys)
(register-command! "RENAME" cmd-rename)
