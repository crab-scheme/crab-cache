; test/string.scm — string + counter command semantics vs Redis.

(define (test-strings ctx)
  (section "string")
  (check-cmd "set basic"      'OK    ctx "SET" "k" "v")
  (check-cmd "get basic"      "v"    ctx "GET" "k")
  (check-cmd "get missing"    'nil   ctx "GET" "absent")
  (check-cmd "set overwrite"  'OK    ctx "SET" "k" "v2")
  (check-cmd "get overwrite"  "v2"   ctx "GET" "k")
  (check-cmd "strlen"         2      ctx "STRLEN" "k")
  (check-cmd "strlen missing" 0      ctx "STRLEN" "absent")
  (check-cmd "append new"     3      ctx "APPEND" "ap" "abc")
  (check-cmd "append more"    6      ctx "APPEND" "ap" "def")
  (check-cmd "append val"     "abcdef" ctx "GET" "ap")

  ; NX / XX
  (check-cmd "setnx-on-new"   'OK    ctx "SET" "nk" "1" "NX")
  (check-cmd "setnx-on-exist" 'nil   ctx "SET" "nk" "2" "NX")
  (check-cmd "nk unchanged"   "1"    ctx "GET" "nk")
  (check-cmd "setxx-on-exist" 'OK    ctx "SET" "nk" "3" "XX")
  (check-cmd "setxx-on-miss"  'nil   ctx "SET" "absent2" "x" "XX")
  (check-cmd "SETNX cmd new"  1      ctx "SETNX" "snx" "a")
  (check-cmd "SETNX cmd dup"  0      ctx "SETNX" "snx" "b")

  ; counters
  (check-cmd "incr fresh"     1      ctx "INCR" "c")
  (check-cmd "incr again"     2      ctx "INCR" "c")
  (check-cmd "incrby"         12     ctx "INCRBY" "c" "10")
  (check-cmd "decr"           11     ctx "DECR" "c")
  (check-cmd "decrby"         1      ctx "DECRBY" "c" "10")
  (check-cmd "incr nonint"    '(err "ERR value is not an integer or out of range")
             ctx "INCR" "k")

  ; GETSET
  (check-cmd "getset old"     "1"    ctx "GETSET" "c" "100")
  (check-cmd "getset new"     "100"  ctx "GET" "c")
  (check-cmd "getset fresh"   'nil   ctx "GETSET" "gsnew" "z")

  ; MGET / MSET
  (check-cmd "mset"           'OK    ctx "MSET" "m1" "a" "m2" "b" "m3" "c")
  (check-cmd "mget"           '("a" "b" "c" nil)
             ctx "MGET" "m1" "m2" "m3" "absent")

  ; TTL via SET EX + clock (logical ticks)
  (check-cmd "set ex"         'OK    ctx "SET" "tk" "x" "EX" "10")
  (check-cmd "ttl after set"  10     ctx "TTL" "tk")
  (ctx-clock-advance! ctx 4)
  (check-cmd "ttl advanced"   6      ctx "TTL" "tk")
  (check-cmd "still live"     "x"    ctx "GET" "tk")
  (ctx-clock-advance! ctx 6)            ; now clock=10, deadline=10 -> dead
  (check-cmd "expired get"    'nil   ctx "GET" "tk")
  (check-cmd "expired ttl"    -2     ctx "TTL" "tk")
  (check-cmd "keepttl keeps"  'OK    ctx "SET" "kt" "1" "EX" "5")
  (check-cmd "kt ttl"         5      ctx "TTL" "kt")
  (check-cmd "kt set keepttl" 'OK    ctx "SET" "kt" "2" "KEEPTTL")
  (check-cmd "kt ttl kept"    5      ctx "TTL" "kt")
  (check-cmd "kt set drops"   'OK    ctx "SET" "kt" "3")
  (check-cmd "kt ttl gone"    -1     ctx "TTL" "kt"))
