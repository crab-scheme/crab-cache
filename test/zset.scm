; test/zset.scm — semantics tests for the Sorted Set (zset) type commands.
;
; Float score replies come back as bulk strings (via float->bytes):
;   whole numbers: "1" "2" "3" (no decimal point)
;   fractional:    "1.5" "2.5" etc.
;
; Uses the check-cmd / section / rc helpers from test/harness.scm.

(define (test-zset ctx)
  (section "ZADD / ZSCORE / ZCARD")

  ; Basic add
  (check-cmd "zadd-new"        2   ctx "ZADD" "z1" "1" "a" "2" "b")
  (check-cmd "zadd-update"     0   ctx "ZADD" "z1" "3" "a")        ; update a's score
  (check-cmd "zcard"           2   ctx "ZCARD" "z1")
  (check-cmd "zcard-missing"   0   ctx "ZCARD" "nokey")
  (check-cmd "zscore-a"        "3" ctx "ZSCORE" "z1" "a")          ; updated
  (check-cmd "zscore-b"        "2" ctx "ZSCORE" "z1" "b")
  (check-cmd "zscore-miss"    'nil  ctx "ZSCORE" "z1" "absent")
  (check-cmd "zscore-nokey"   'nil  ctx "ZSCORE" "nokey" "x")

  ; ZADD with float scores
  (section "ZADD float scores")
  (check-cmd "zadd-float"      1   ctx "ZADD" "zf" "1.5" "x")
  (check-cmd "zscore-float"    "1.5" ctx "ZSCORE" "zf" "x")
  (check-cmd "zadd-float2"     1   ctx "ZADD" "zf" "2.5" "y")

  ; ZRANGE / ZREVRANGE
  (section "ZRANGE / ZREVRANGE")
  ; z1 after updates: b=2, a=3 (ascending by score)
  (check-cmd "zrange-all"      '("b" "a")  ctx "ZRANGE" "z1" "0" "-1")
  (check-cmd "zrange-first"    '("b")      ctx "ZRANGE" "z1" "0" "0")
  (check-cmd "zrange-last"     '("a")      ctx "ZRANGE" "z1" "-1" "-1")
  (check-cmd "zrange-empty-range" '()      ctx "ZRANGE" "z1" "2" "3")
  (check-cmd "zrange-missing"  '()          ctx "ZRANGE" "nokey" "0" "-1")

  ; WITHSCORES: interleaved member, score pairs (member first, score second)
  (check-cmd "zrange-ws"  '("b" "2" "a" "3")  ctx "ZRANGE" "z1" "0" "-1" "WITHSCORES")

  ; ZREVRANGE
  (check-cmd "zrevrange-all"   '("a" "b")  ctx "ZREVRANGE" "z1" "0" "-1")
  (check-cmd "zrevrange-ws"  '("a" "3" "b" "2")  ctx "ZREVRANGE" "z1" "0" "-1" "WITHSCORES")

  ; Score update consistency: add new member, re-check order
  (section "score-update consistency")
  (check-cmd "zadd-c"          1   ctx "ZADD" "z1" "1" "c")       ; c=1, so order: c b a
  (check-cmd "zrange-after-c"  '("c" "b" "a")  ctx "ZRANGE" "z1" "0" "-1")
  ; Update c to score 5: now order is b a c
  (check-cmd "zadd-update-c"   0   ctx "ZADD" "z1" "5" "c")
  (check-cmd "zrange-after-update-c"  '("b" "a" "c")  ctx "ZRANGE" "z1" "0" "-1")
  ; Verify WITHSCORES reflects updated scores
  (check-cmd "zrange-ws-updated"  '("b" "2" "a" "3" "c" "5")
             ctx "ZRANGE" "z1" "0" "-1" "WITHSCORES")

  ; ZRANK / ZREVRANK
  (section "ZRANK / ZREVRANK")
  ; z1: b=2(rank0) a=3(rank1) c=5(rank2)
  (check-cmd "zrank-b"         0   ctx "ZRANK" "z1" "b")
  (check-cmd "zrank-a"         1   ctx "ZRANK" "z1" "a")
  (check-cmd "zrank-c"         2   ctx "ZRANK" "z1" "c")
  (check-cmd "zrank-miss"     'nil  ctx "ZRANK" "z1" "absent")
  (check-cmd "zrank-nokey"    'nil  ctx "ZRANK" "nokey" "x")
  (check-cmd "zrevrank-b"      2   ctx "ZREVRANK" "z1" "b")
  (check-cmd "zrevrank-c"      0   ctx "ZREVRANK" "z1" "c")

  ; ZRANGEBYSCORE
  (section "ZRANGEBYSCORE")
  ; z1: b=2 a=3 c=5
  (check-cmd "zrbs-all"        '("b" "a" "c")  ctx "ZRANGEBYSCORE" "z1" "-inf" "+inf")
  (check-cmd "zrbs-range"      '("a")           ctx "ZRANGEBYSCORE" "z1" "3" "4")
  (check-cmd "zrbs-excl-lo"    '("a" "c")       ctx "ZRANGEBYSCORE" "z1" "(2" "+inf")
  (check-cmd "zrbs-excl-hi"    '("b" "a")       ctx "ZRANGEBYSCORE" "z1" "-inf" "(5")
  (check-cmd "zrbs-excl-both"  '("a")           ctx "ZRANGEBYSCORE" "z1" "(2" "(5")
  (check-cmd "zrbs-none"       '()              ctx "ZRANGEBYSCORE" "z1" "10" "+inf")
  (check-cmd "zrbs-ws"  '("b" "2" "a" "3")  ctx "ZRANGEBYSCORE" "z1" "2" "3" "WITHSCORES")
  (check-cmd "zrbs-missing"    '()              ctx "ZRANGEBYSCORE" "nokey" "-inf" "+inf")

  ; ZCOUNT
  (section "ZCOUNT")
  (check-cmd "zcount-all"      3   ctx "ZCOUNT" "z1" "-inf" "+inf")
  (check-cmd "zcount-range"    2   ctx "ZCOUNT" "z1" "2" "3")
  (check-cmd "zcount-excl"     1   ctx "ZCOUNT" "z1" "(2" "(5")
  (check-cmd "zcount-none"     0   ctx "ZCOUNT" "z1" "10" "+inf")
  (check-cmd "zcount-missing"  0   ctx "ZCOUNT" "nokey" "-inf" "+inf")

  ; ZINCRBY
  (section "ZINCRBY")
  ; b's current score = 2
  (check-cmd "zincrby-hit"     "4.5"  ctx "ZINCRBY" "z1" "2.5" "b")
  (check-cmd "zscore-b-incr"   "4.5"  ctx "ZSCORE" "z1" "b")
  ; New member via ZINCRBY
  (check-cmd "zincrby-new"     "7"    ctx "ZINCRBY" "z1" "7" "new")
  (check-cmd "zcard-after-incr" 4     ctx "ZCARD" "z1")
  ; Verify order after incrby: a=3 b=4.5 c=5 new=7
  (check-cmd "zrange-after-incr"  '("a" "b" "c" "new")
             ctx "ZRANGE" "z1" "0" "-1")

  ; ZADD NX / XX / GT / LT / CH flags
  (section "ZADD flags")
  ; NX: only add if absent
  (check-cmd "zadd-nx-new"     1   ctx "ZADD" "zn" "NX" "10" "x")
  (check-cmd "zadd-nx-dup"     0   ctx "ZADD" "zn" "NX" "20" "x")  ; x exists, skip
  (check-cmd "zscore-nx-dup"   "10"  ctx "ZSCORE" "zn" "x")         ; score unchanged
  ; XX: only update if present
  (check-cmd "zadd-xx-hit"     0   ctx "ZADD" "zn" "XX" "15" "x")  ; exists: update
  (check-cmd "zscore-xx-hit"   "15"  ctx "ZSCORE" "zn" "x")
  (check-cmd "zadd-xx-miss"    0   ctx "ZADD" "zn" "XX" "99" "y")  ; absent: no-op
  (check-cmd "zscore-xx-miss" 'nil  ctx "ZSCORE" "zn" "y")
  ; GT: update only if new > old
  (check-cmd "zadd-gt-higher"  0   ctx "ZADD" "zn" "GT" "20" "x") ; 20>15: update
  (check-cmd "zscore-gt-hi"    "20"  ctx "ZSCORE" "zn" "x")
  (check-cmd "zadd-gt-lower"   0   ctx "ZADD" "zn" "GT" "5" "x")  ; 5<20: skip
  (check-cmd "zscore-gt-lo"    "20"  ctx "ZSCORE" "zn" "x")        ; unchanged
  ; LT: update only if new < old
  (check-cmd "zadd-lt-lower"   0   ctx "ZADD" "zn" "LT" "10" "x") ; 10<20: update
  (check-cmd "zscore-lt-lo"    "10"  ctx "ZSCORE" "zn" "x")
  (check-cmd "zadd-lt-higher"  0   ctx "ZADD" "zn" "LT" "50" "x") ; 50>10: skip
  (check-cmd "zscore-lt-hi"    "10"  ctx "ZSCORE" "zn" "x")        ; unchanged
  ; CH: return added+changed
  (check-cmd "zadd-ch"         2   ctx "ZADD" "zn" "CH" "99" "x" "99" "znew") ; x updated + znew added

  ; ZREM
  (section "ZREM")
  (check-cmd "zrem-hit"        1   ctx "ZREM" "z1" "new")
  (check-cmd "zrem-miss"       0   ctx "ZREM" "z1" "absent")
  (check-cmd "zcard-post-rem"  3   ctx "ZCARD" "z1")
  ; Remove all members -> key purged
  (check-cmd "zadd-for-purge"  2   ctx "ZADD" "zdel" "1" "p" "2" "q")
  (check-cmd "zrem-all"        2   ctx "ZREM" "zdel" "p" "q")
  (check-cmd "zcard-purged"    0   ctx "ZCARD" "zdel")
  (check-cmd "zscore-purged"  'nil  ctx "ZSCORE" "zdel" "p")

  ; WRONGTYPE
  (section "ZSET WRONGTYPE")
  (check-cmd "set-str2"        'OK  ctx "SET" "zwt" "hello")
  (check-cmd "zadd-wrongtype"
    '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
    ctx "ZADD" "zwt" "1" "m")
  (check-cmd "zscore-wrongtype"
    '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
    ctx "ZSCORE" "zwt" "m")
  (check-cmd "zrange-wrongtype"
    '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
    ctx "ZRANGE" "zwt" "0" "-1")
  (check-cmd "zrangebyscore-wrongtype"
    '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
    ctx "ZRANGEBYSCORE" "zwt" "-inf" "+inf"))
