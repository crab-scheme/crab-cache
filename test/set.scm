; test/set.scm — semantics tests for the Set type commands.
;
; Uses the check-cmd / section / rc helpers from test/harness.scm.

(define (test-set ctx)
  (section "SADD / SCARD / SISMEMBER")

  ; Basic add
  (check-cmd "sadd-new-1"      1  ctx "SADD" "s1" "a")
  (check-cmd "sadd-new-2"      2  ctx "SADD" "s1" "b" "c")
  (check-cmd "sadd-dup-0"      0  ctx "SADD" "s1" "a")
  (check-cmd "sadd-dup+new"    1  ctx "SADD" "s1" "a" "d")
  (check-cmd "scard"           4  ctx "SCARD" "s1")
  (check-cmd "scard-missing"   0  ctx "SCARD" "nokey")

  ; SISMEMBER
  (check-cmd "sismember-yes"   1  ctx "SISMEMBER" "s1" "a")
  (check-cmd "sismember-no"    0  ctx "SISMEMBER" "s1" "z")
  (check-cmd "sismember-miss"  0  ctx "SISMEMBER" "nokey" "x")

  ; SMEMBERS
  (section "SMEMBERS")
  ; scan order is lexicographic by member bytes: a b c d
  (check-cmd "smembers"  '("a" "b" "c" "d")  ctx "SMEMBERS" "s1")
  (check-cmd "smembers-missing"  '()  ctx "SMEMBERS" "nokey")

  ; SMISMEMBER
  (section "SMISMEMBER")
  (check-cmd "smismember-mix"  '(1 0 1)  ctx "SMISMEMBER" "s1" "a" "z" "c")

  ; SREM
  (section "SREM")
  (check-cmd "srem-hit"        1  ctx "SREM" "s1" "b")
  (check-cmd "srem-miss"       0  ctx "SREM" "s1" "b")     ; already removed
  (check-cmd "srem-multi"      2  ctx "SREM" "s1" "a" "d")
  (check-cmd "scard-after-rem" 1  ctx "SCARD" "s1")
  ; Remove last member — key should be purged
  (check-cmd "srem-last"       1  ctx "SREM" "s1" "c")
  (check-cmd "scard-purged"    0  ctx "SCARD" "s1")
  (check-cmd "sismember-purged" 0 ctx "SISMEMBER" "s1" "c")

  ; SPOP (deterministic: first in scan order)
  (section "SPOP")
  (check-cmd "sadd-for-spop"   3  ctx "SADD" "sp" "x" "y" "z")
  (check-cmd "spop-one"        "x"  ctx "SPOP" "sp")       ; "x" is first lex
  (check-cmd "scard-after-spop" 2  ctx "SCARD" "sp")
  (check-cmd "spop-count"      '("y" "z")  ctx "SPOP" "sp" "2")
  (check-cmd "scard-all-popped" 0  ctx "SCARD" "sp")
  ; SPOP on missing key
  (check-cmd "spop-missing"    'nil  ctx "SPOP" "nokey")
  ; SPOP with count on empty/missing
  (check-cmd "spop-count-miss" '()  ctx "SPOP" "nokey" "3")

  ; SRANDMEMBER (deterministic: scan order, no removal)
  (section "SRANDMEMBER")
  (check-cmd "sadd-for-srand"  3  ctx "SADD" "sr" "a" "b" "c")
  ; no count: single element (first in scan order = "a")
  (check-cmd "srandmember-1"   "a"  ctx "SRANDMEMBER" "sr")
  ; positive count <= card
  (check-cmd "srandmember-pos" '("a" "b")  ctx "SRANDMEMBER" "sr" "2")
  ; positive count > card: return all
  (check-cmd "srandmember-over" '("a" "b" "c")  ctx "SRANDMEMBER" "sr" "10")
  ; negative count: abs(n) items, may repeat (we return cycling scan order)
  (check-cmd "srandmember-neg" '("a" "b" "c" "a")  ctx "SRANDMEMBER" "sr" "-4")
  ; count 0 returns empty array
  (check-cmd "srandmember-zero" '()  ctx "SRANDMEMBER" "sr" "0")
  ; srandmember missing key with no count
  (check-cmd "srandmember-miss" 'nil  ctx "SRANDMEMBER" "nokey")
  ; card unchanged — no removal
  (check-cmd "scard-after-srand" 3  ctx "SCARD" "sr")

  ; WRONGTYPE
  (section "SET WRONGTYPE")
  (check-cmd "set-str"         'OK  ctx "SET" "wt" "hello")
  (check-cmd "sadd-wrongtype"
    '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
    ctx "SADD" "wt" "member")
  (check-cmd "srem-wrongtype"
    '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
    ctx "SREM" "wt" "member")
  (check-cmd "smembers-wrongtype"
    '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
    ctx "SMEMBERS" "wt")
  (check-cmd "scard-wrongtype"
    '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
    ctx "SCARD" "wt"))
