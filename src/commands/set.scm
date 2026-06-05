; commands/set.scm — the Redis Set type (unordered collection of unique members).
;
; Type char: #\e  (mnemonic: sEt, since #\s is taken by string)
; Encoding:  E:<lp ukey><member> -> #vu8(1)
;
; DETERMINISM NOTE: SPOP and SRANDMEMBER are random in Redis but this
; implementation picks the FIRST member(s) in scan order (lexicographic by
; member bytes). This is intentional for future Raft replication — every
; replica must produce the same result for the same log entry, so random
; selection is forbidden in Phase 3+.
;
; Commands: SADD SREM SISMEMBER SMEMBERS SCARD SPOP SRANDMEMBER SMISMEMBER
;
; Depends on: encoding.scm, store-ctx.scm, reply.scm, shard.scm.

; ---- internal helpers ----

; Return the cardinality of a set key (0 if absent or wrong type — callers
; have already type-guarded before calling this).
(define (set-card ctx ukey)
  (kv-scan-count ctx (set-prefix ukey)))

; ---- SADD key member [member ...] ----
; Returns the number of NEW members added (duplicates don't count).

(define (cmd-sadd ctx operands)
  (if (< (length operands) 2)
      (r-wrong-args "sadd")
      (let ((ukey (car operands)) (members (cdr operands)))
        (if (eq? (type-guard ctx ukey #\e) 'wrong)
            (r-wrongtype)
            (let loop ((ms members) (added 0))
              (if (null? ms)
                  (r-int added)
                  (let ((mk (set-member-key ukey (car ms))))
                    (if (kv-get ctx mk)
                        (loop (cdr ms) added)
                        (begin
                          (ctype-touch! ctx ukey #\e)
                          (kv-put! ctx mk enc:one)
                          (loop (cdr ms) (+ added 1)))))))))))

; ---- SREM key member [member ...] ----
; Returns the number of members actually removed.

(define (cmd-srem ctx operands)
  (if (< (length operands) 2)
      (r-wrong-args "srem")
      (let ((ukey (car operands)) (members (cdr operands)))
        (if (eq? (type-guard ctx ukey #\e) 'wrong)
            (r-wrongtype)
            (let loop ((ms members) (removed 0))
              (if (null? ms)
                  (begin
                    (purge-if-empty! ctx ukey (set-card ctx ukey))
                    (r-int removed))
                  (let ((mk (set-member-key ukey (car ms))))
                    (if (kv-get ctx mk)
                        (begin
                          (kv-del! ctx mk)
                          (loop (cdr ms) (+ removed 1)))
                        (loop (cdr ms) removed)))))))))

; ---- SISMEMBER key member ----
; Returns 1 if member is in the set, 0 otherwise (missing key -> 0).

(define (cmd-sismember ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "sismember")
      (let ((ukey (car operands)) (member (cadr operands)))
        (if (eq? (type-guard ctx ukey #\e) 'wrong)
            (r-wrongtype)
            (if (kv-get ctx (set-member-key ukey member))
                (r-int 1)
                (r-int 0))))))

; ---- SMISMEMBER key member [member ...] ----
; Returns array of 1/0 per member.

(define (cmd-smismember ctx operands)
  (if (< (length operands) 2)
      (r-wrong-args "smismember")
      (let ((ukey (car operands)) (members (cdr operands)))
        (if (eq? (type-guard ctx ukey #\e) 'wrong)
            (r-wrongtype)
            (r-array
             (map (lambda (m)
                    (if (kv-get ctx (set-member-key ukey m))
                        (r-int 1)
                        (r-int 0)))
                  members))))))

; ---- SMEMBERS key ----
; Returns array of all members (in scan/lexicographic order).

(define (cmd-smembers ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "smembers")
      (let ((ukey (car operands)))
        (if (eq? (type-guard ctx ukey #\e) 'wrong)
            (r-wrongtype)
            (let ((rows (kv-scan ctx (set-prefix ukey))))
              (r-array
               (map (lambda (kv)
                      (r-bulk (set-member-of (car kv) ukey)))
                    rows)))))))

; ---- SCARD key ----
; Returns the cardinality (0 for missing key).

(define (cmd-scard ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "scard")
      (let ((ukey (car operands)))
        (if (eq? (type-guard ctx ukey #\e) 'wrong)
            (r-wrongtype)
            (r-int (set-card ctx ukey))))))

; ---- SPOP key [count] ----
; Remove and return count members. Deterministic: picks first in scan order.
; Without count returns a single bulk (nil if empty); with count returns array.

(define (spop-n ctx ukey n)
  ; Take up to n items from the front of the scan, remove them, return members.
  (let loop ((rows (kv-scan ctx (set-prefix ukey))) (acc '()) (rem n))
    (if (or (null? rows) (= rem 0))
        (reverse acc)
        (let ((member (set-member-of (caar rows) ukey)))
          (kv-del! ctx (caar rows))
          (loop (cdr rows) (cons member acc) (- rem 1))))))

(define (cmd-spop ctx operands)
  (cond
    ((null? operands) (r-wrong-args "spop"))
    ((not (eq? (type-guard ctx (car operands) #\e) 'ok)) (r-wrongtype))
    (else
     (let ((ukey (car operands)))
       (if (null? (cdr operands))
           ; no count: return single bulk or nil
           (let ((rows (kv-scan ctx (set-prefix ukey))))
             (if (null? rows)
                 (r-nil)
                 (let ((member (set-member-of (caar rows) ukey)))
                   (kv-del! ctx (caar rows))
                   (purge-if-empty! ctx ukey (set-card ctx ukey))
                   (r-bulk member))))
           ; with count
           (let ((n (bytes->int (cadr operands))))
             (if (not n)
                 (r-not-int)
                 (let ((members (spop-n ctx ukey n)))
                   (purge-if-empty! ctx ukey (set-card ctx ukey))
                   (r-array (map r-bulk members))))))))))

; ---- SRANDMEMBER key [count] ----
; Return (without removing) count members in scan order (deterministic).
; Positive count: up to count distinct members.
; Negative count: abs(count) members, may repeat (but we return scan-order
;   distinct capped at cardinality — repeats only matter for true random;
;   since we are deterministic we just cycle the scan).

(define (srand-positive ctx ukey n)
  (let loop ((rows (kv-scan ctx (set-prefix ukey))) (acc '()) (rem n))
    (if (or (null? rows) (= rem 0))
        (reverse acc)
        (loop (cdr rows)
              (cons (set-member-of (caar rows) ukey) acc)
              (- rem 1)))))

(define (srand-negative ctx ukey n)
  ; |n| members, cycling through scan order if needed.
  (let ((all (map (lambda (kv) (set-member-of (car kv) ukey))
                  (kv-scan ctx (set-prefix ukey))))
        (cnt (abs n)))
    (if (null? all)
        '()
        (let loop ((rem cnt) (lst all) (acc '()))
          (if (= rem 0)
              (reverse acc)
              (let ((cur (if (null? lst) all lst)))
                (loop (- rem 1) (cdr cur) (cons (car cur) acc))))))))

(define (cmd-srandmember ctx operands)
  (cond
    ((null? operands) (r-wrong-args "srandmember"))
    ((not (eq? (type-guard ctx (car operands) #\e) 'ok)) (r-wrongtype))
    (else
     (let ((ukey (car operands)))
       (if (null? (cdr operands))
           ; no count: single element or nil
           (let ((rows (kv-scan ctx (set-prefix ukey))))
             (if (null? rows)
                 (r-nil)
                 (r-bulk (set-member-of (caar rows) ukey))))
           ; with count
           (let ((n (bytes->int (cadr operands))))
             (if (not n)
                 (r-not-int)
                 (if (>= n 0)
                     (r-array (map r-bulk (srand-positive ctx ukey n)))
                     (r-array (map r-bulk (srand-negative ctx ukey n)))))))))))

; ---- registrations ----

(register-command! "SADD"        cmd-sadd)
(register-command! "SREM"        cmd-srem)
(register-command! "SISMEMBER"   cmd-sismember)
(register-command! "SMISMEMBER"  cmd-smismember)
(register-command! "SMEMBERS"    cmd-smembers)
(register-command! "SCARD"       cmd-scard)
(register-command! "SPOP"        cmd-spop)
(register-command! "SRANDMEMBER" cmd-srandmember)
