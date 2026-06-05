; commands/zset.scm — the Redis Sorted Set type (zset).
;
; Type char: #\z
; Dual index:
;   by-member:  Z:<lp ukey><member>                -> raw f64 score (8 bytes)
;   by-score:   Zs:<lp ukey><score:8 order><member> -> #vu8(1)
;
; The by-score index keys sort in ascending score order (then by member bytes)
; because f64->order-bytes is a total-order-preserving transform.
;
; ZADD flags supported: NX, XX, GT, LT, CH.
;
; Commands: ZADD ZSCORE ZREM ZCARD ZRANK ZREVRANK ZRANGE ZREVRANGE
;           ZRANGEBYSCORE ZINCRBY ZCOUNT
;
; Depends on: encoding.scm, store-ctx.scm, reply.scm, shard.scm.

; ---- internal helpers ----

; Write a member into the zset, maintaining BOTH indices.
; ZADD must delete the old by-score key before writing a new one, otherwise
; stale by-score entries accumulate and corrupt ZRANGE order.
(define (zset-put! ctx ukey member score)
  (let ((mk (zset-member-key ukey member)))
    (let ((old-val (kv-get ctx mk)))
      (if old-val
          (let ((old-score (bytes->f64 old-val 0)))
            (kv-del! ctx (zset-score-key ukey old-score member)))))
    (kv-put! ctx mk (f64->bytes score))
    (kv-put! ctx (zset-score-key ukey score member) enc:one)))

; Remove a member from both indices. Returns #t if the member existed.
(define (zset-remove-member! ctx ukey member)
  (let ((mk (zset-member-key ukey member)))
    (let ((val (kv-get ctx mk)))
      (if val
          (begin
            (kv-del! ctx (zset-score-key ukey (bytes->f64 val 0) member))
            (kv-del! ctx mk)
            #t)
          #f))))

; Cardinality via by-member prefix scan.
(define (zset-card ctx ukey)
  (kv-scan-count ctx (zset-member-prefix ukey)))

; Scan the by-score index; return list of (score . member-bv) in asc order.
(define (zset-score-scan ctx ukey)
  (map (lambda (kv)
         (cons (zset-score-of (car kv) ukey)
               (zset-member-of-score-key (car kv) ukey)))
       (kv-scan ctx (zset-score-prefix ukey))))

; Build the flat member-score (member first, score second) reply list from
; a list of (score . member) pairs.
(define (pairs->ws-replies pairs)
  (let loop ((ps pairs) (acc '()))
    (if (null? ps)
        (reverse acc)
        (loop (cdr ps)
              (cons (r-bulk (float->bytes (caar ps)))
                    (cons (r-bulk (cdar ps)) acc))))))

; Slice a list to indices [s, e] inclusive (0-based).
(define (list-slice lst s e)
  (let loop ((ps lst) (i 0) (acc '()))
    (if (null? ps)
        (reverse acc)
        (if (and (>= i s) (<= i e))
            (loop (cdr ps) (+ i 1) (cons (car ps) acc))
            (loop (cdr ps) (+ i 1) acc)))))

; Normalise Redis range start: negative counts from end, positive as-is (may be >= n).
(define (norm-start i n)
  (if (< i 0) (max 0 (+ n i)) i))

; Normalise Redis range stop: negative counts from end, positive capped at n-1.
(define (norm-stop i n)
  (if (< i 0) (max 0 (+ n i)) (min i (- n 1))))

; Check whether WITHSCORES appears in the options list.
(define (has-withscores? opts)
  (let loop ((o opts))
    (if (null? o)
        #f
        (if (string=? (string-upcase (bv->s (car o))) "WITHSCORES")
            #t
            (loop (cdr o))))))

; Parse a ZRANGEBYSCORE bound.  Returns (exclusive? . val) or #f on error.
(define (parse-score-bound b)
  (let ((s (bv->s b)))
    (cond
      ((string=? s "+inf")  (cons #f +inf.0))
      ((string=? s "-inf")  (cons #f -inf.0))
      ((and (> (string-length s) 0) (char=? (string-ref s 0) #\())
       (let ((rest (substring s 1 (string-length s))))
         (cond
           ((string=? rest "+inf") (cons #t +inf.0))
           ((string=? rest "-inf") (cons #t -inf.0))
           (else
            (let ((n (string->number rest)))
              (if (and n (real? n)) (cons #t (exact->inexact n)) #f))))))
      (else
       (let ((n (string->number s)))
         (if (and n (real? n)) (cons #f (exact->inexact n)) #f))))))

(define (score-in-range? score lo-pair hi-pair)
  (let ((lo-exc (car lo-pair)) (lo-val (cdr lo-pair))
        (hi-exc (car hi-pair)) (hi-val (cdr hi-pair)))
    (and (if lo-exc (> score lo-val) (>= score lo-val))
         (if hi-exc (< score hi-val) (<= score hi-val)))))

; ---- ZADD key [NX|XX|GT|LT|CH ...] score member [score member ...] ----

(define (parse-zadd-flags operands)
  ; Returns (nx? xx? gt? lt? ch? remaining-pairs) — stops when a token is not a flag.
  (let loop ((o operands) (nx #f) (xx #f) (gt #f) (lt #f) (ch #f))
    (if (null? o)
        (list nx xx gt lt ch '())
        (let ((w (string-upcase (bv->s (car o)))))
          (cond
            ((string=? w "NX") (loop (cdr o) #t xx gt lt ch))
            ((string=? w "XX") (loop (cdr o) nx #t gt lt ch))
            ((string=? w "GT") (loop (cdr o) nx xx #t lt ch))
            ((string=? w "LT") (loop (cdr o) nx xx gt #t ch))
            ((string=? w "CH") (loop (cdr o) nx xx gt lt #t))
            (else (list nx xx gt lt ch o)))))))

(define (zadd-skip? nx xx gt lt score old-score exists)
  ; Returns #t if this score/member pair should be skipped given flags.
  (cond
    ((and nx exists) #t)
    ((and xx (not exists)) #t)
    ((and gt exists (not (> score old-score))) #t)
    ((and lt exists (not (< score old-score))) #t)
    (else #f)))

(define (cmd-zadd ctx operands)
  (if (< (length operands) 3)
      (r-wrong-args "zadd")
      (let ((ukey (car operands)))
        (if (eq? (type-guard ctx ukey #\z) 'wrong)
            (r-wrongtype)
            (let* ((parsed (parse-zadd-flags (cdr operands)))
                   (nx (list-ref parsed 0))
                   (xx (list-ref parsed 1))
                   (gt (list-ref parsed 2))
                   (lt (list-ref parsed 3))
                   (ch (list-ref parsed 4))
                   (pairs (list-ref parsed 5)))
              (if (or (null? pairs) (odd? (length pairs)))
                  (r-wrong-args "zadd")
                  (let loop ((ps pairs) (added 0) (changed 0))
                    (if (null? ps)
                        (r-int (if ch (+ added changed) added))
                        (let ((score-bv (car ps)) (member (cadr ps)))
                          (let ((score (bytes->float score-bv)))
                            (if (not score)
                                (r-not-float)
                                (let* ((mk (zset-member-key ukey member))
                                       (old-val (kv-get ctx mk))
                                       (old-score (if old-val (bytes->f64 old-val 0) #f))
                                       (exists (and old-score #t)))
                                  (if (zadd-skip? nx xx gt lt score old-score exists)
                                      (loop (cddr ps) added changed)
                                      (let ((is-new (not exists))
                                            (is-ch (and exists (not (= score old-score)))))
                                        (ctype-touch! ctx ukey #\z)
                                        (zset-put! ctx ukey member score)
                                        (loop (cddr ps)
                                              (if is-new (+ added 1) added)
                                              (if is-ch (+ changed 1) changed))))))))))))))))

; ---- ZSCORE key member ----

(define (cmd-zscore ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "zscore")
      (let ((ukey (car operands)) (member (cadr operands)))
        (if (eq? (type-guard ctx ukey #\z) 'wrong)
            (r-wrongtype)
            (let ((val (kv-get ctx (zset-member-key ukey member))))
              (if val
                  (r-bulk (float->bytes (bytes->f64 val 0)))
                  (r-nil)))))))

; ---- ZREM key member [member ...] ----

(define (cmd-zrem ctx operands)
  (if (< (length operands) 2)
      (r-wrong-args "zrem")
      (let ((ukey (car operands)) (members (cdr operands)))
        (if (eq? (type-guard ctx ukey #\z) 'wrong)
            (r-wrongtype)
            (let loop ((ms members) (removed 0))
              (if (null? ms)
                  (begin
                    (purge-if-empty! ctx ukey (zset-card ctx ukey))
                    (r-int removed))
                  (if (zset-remove-member! ctx ukey (car ms))
                      (loop (cdr ms) (+ removed 1))
                      (loop (cdr ms) removed))))))))

; ---- ZCARD key ----

(define (cmd-zcard ctx operands)
  (if (not (= (length operands) 1))
      (r-wrong-args "zcard")
      (let ((ukey (car operands)))
        (if (eq? (type-guard ctx ukey #\z) 'wrong)
            (r-wrongtype)
            (r-int (zset-card ctx ukey))))))

; ---- ZRANK key member / ZREVRANK key member ----
; 0-based rank (nil if absent).

(define (zrank-generic ctx ukey member rev?)
  (let ((pairs (zset-score-scan ctx ukey)))
    (if (null? pairs)
        (r-nil)
        (let ((n (length pairs)))
          (let loop ((ps pairs) (i 0))
            (if (null? ps)
                (r-nil)
                (if (bytevector=? (cdar ps) member)
                    (r-int (if rev? (- n 1 i) i))
                    (loop (cdr ps) (+ i 1)))))))))

(define (cmd-zrank ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "zrank")
      (let ((ukey (car operands)) (member (cadr operands)))
        (if (eq? (type-guard ctx ukey #\z) 'wrong)
            (r-wrongtype)
            (zrank-generic ctx ukey member #f)))))

(define (cmd-zrevrank ctx operands)
  (if (not (= (length operands) 2))
      (r-wrong-args "zrevrank")
      (let ((ukey (car operands)) (member (cadr operands)))
        (if (eq? (type-guard ctx ukey #\z) 'wrong)
            (r-wrongtype)
            (zrank-generic ctx ukey member #t)))))

; ---- ZRANGE key start stop [WITHSCORES] ----
; Ascending order; negative indices count from end.

(define (cmd-zrange ctx operands)
  (if (< (length operands) 3)
      (r-wrong-args "zrange")
      (let ((ukey (car operands))
            (start-bv (cadr operands))
            (stop-bv (caddr operands))
            (opts (cdddr operands)))
        (if (eq? (type-guard ctx ukey #\z) 'wrong)
            (r-wrongtype)
            (let ((start (bytes->int start-bv)) (stop (bytes->int stop-bv)))
              (if (not (and start stop))
                  (r-not-int)
                  (let* ((pairs (zset-score-scan ctx ukey))
                         (ws (has-withscores? opts)))
                    (if (null? pairs)
                        (r-array '())
                        (let* ((n (length pairs))
                               (s (norm-start start n))
                               (e (norm-stop stop n)))
                          (if (> s e)
                              (r-array '())
                              (let ((slice (list-slice pairs s e)))
                                (if ws
                                    (r-array (pairs->ws-replies slice))
                                    (r-array (map (lambda (p) (r-bulk (cdr p))) slice))))))))))))))

; ---- ZREVRANGE key start stop [WITHSCORES] ----
; Descending order; indices apply after reversing the sorted list.

(define (cmd-zrevrange ctx operands)
  (if (< (length operands) 3)
      (r-wrong-args "zrevrange")
      (let ((ukey (car operands))
            (start-bv (cadr operands))
            (stop-bv (caddr operands))
            (opts (cdddr operands)))
        (if (eq? (type-guard ctx ukey #\z) 'wrong)
            (r-wrongtype)
            (let ((start (bytes->int start-bv)) (stop (bytes->int stop-bv)))
              (if (not (and start stop))
                  (r-not-int)
                  (let* ((pairs (reverse (zset-score-scan ctx ukey)))
                         (ws (has-withscores? opts)))
                    (if (null? pairs)
                        (r-array '())
                        (let* ((n (length pairs))
                               (s (norm-start start n))
                               (e (norm-stop stop n)))
                          (if (> s e)
                              (r-array '())
                              (let ((slice (list-slice pairs s e)))
                                (if ws
                                    (r-array (pairs->ws-replies slice))
                                    (r-array (map (lambda (p) (r-bulk (cdr p))) slice))))))))))))))

; ---- ZRANGEBYSCORE key min max [WITHSCORES] ----
; Supports -inf +inf and exclusive ( prefix. LIMIT not implemented (Phase 3).

(define (cmd-zrangebyscore ctx operands)
  (if (< (length operands) 3)
      (r-wrong-args "zrangebyscore")
      (let ((ukey (car operands))
            (min-bv (cadr operands))
            (max-bv (caddr operands))
            (opts (cdddr operands)))
        (if (eq? (type-guard ctx ukey #\z) 'wrong)
            (r-wrongtype)
            (let ((lo (parse-score-bound min-bv)) (hi (parse-score-bound max-bv)))
              (if (not (and lo hi))
                  (r-not-float)
                  (let* ((pairs (zset-score-scan ctx ukey))
                         (filtered (filter (lambda (p) (score-in-range? (car p) lo hi)) pairs))
                         (ws (has-withscores? opts)))
                    (if ws
                        (r-array (pairs->ws-replies filtered))
                        (r-array (map (lambda (p) (r-bulk (cdr p))) filtered))))))))))

; ---- ZINCRBY key increment member ----

(define (cmd-zincrby ctx operands)
  (if (not (= (length operands) 3))
      (r-wrong-args "zincrby")
      (let ((ukey (car operands)) (delta-bv (cadr operands)) (member (caddr operands)))
        (if (eq? (type-guard ctx ukey #\z) 'wrong)
            (r-wrongtype)
            (let ((delta (bytes->float delta-bv)))
              (if (not delta)
                  (r-not-float)
                  (let* ((mk (zset-member-key ukey member))
                         (old-val (kv-get ctx mk))
                         (old-score (if old-val (bytes->f64 old-val 0) 0.0))
                         (new-score (+ old-score delta)))
                    (ctype-touch! ctx ukey #\z)
                    (zset-put! ctx ukey member new-score)
                    (r-bulk (float->bytes new-score)))))))))

; ---- ZCOUNT key min max ----

(define (cmd-zcount ctx operands)
  (if (not (= (length operands) 3))
      (r-wrong-args "zcount")
      (let ((ukey (car operands)) (min-bv (cadr operands)) (max-bv (caddr operands)))
        (if (eq? (type-guard ctx ukey #\z) 'wrong)
            (r-wrongtype)
            (let ((lo (parse-score-bound min-bv)) (hi (parse-score-bound max-bv)))
              (if (not (and lo hi))
                  (r-not-float)
                  (r-int (length (filter (lambda (p) (score-in-range? (car p) lo hi))
                                        (zset-score-scan ctx ukey))))))))))

; ---- registrations ----

(register-command! "ZADD"          cmd-zadd)
(register-command! "ZSCORE"        cmd-zscore)
(register-command! "ZREM"          cmd-zrem)
(register-command! "ZCARD"         cmd-zcard)
(register-command! "ZRANK"         cmd-zrank)
(register-command! "ZREVRANK"      cmd-zrevrank)
(register-command! "ZRANGE"        cmd-zrange)
(register-command! "ZREVRANGE"     cmd-zrevrange)
(register-command! "ZRANGEBYSCORE" cmd-zrangebyscore)
(register-command! "ZINCRBY"       cmd-zincrby)
(register-command! "ZCOUNT"        cmd-zcount)
