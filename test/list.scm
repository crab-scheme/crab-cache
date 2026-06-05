; test/list.scm — list command semantics tests.
; Uses check-cmd from test/harness.scm.

(define (test-list ctx)
  (section "list: RPUSH / LPUSH basics")
  (check-cmd "rpush new"         3    ctx "RPUSH" "l1" "a" "b" "c")
  (check-cmd "llen"              3    ctx "LLEN" "l1")
  (check-cmd "llen missing"      0    ctx "LLEN" "nokey")
  (check-cmd "lrange all"        '("a" "b" "c") ctx "LRANGE" "l1" "0" "-1")
  (check-cmd "rpush more"        5    ctx "RPUSH" "l1" "d" "e")
  (check-cmd "lrange after rpush" '("a" "b" "c" "d" "e") ctx "LRANGE" "l1" "0" "-1")

  (check-cmd "lpush new key"     3    ctx "LPUSH" "l2" "x" "y" "z")
  ; lpush pushes left-to-right so x first, y next (head), z last (head)
  ; result head->tail: z y x
  (check-cmd "lrange lpush"      '("z" "y" "x") ctx "LRANGE" "l2" "0" "-1")
  (check-cmd "lpush more"        5    ctx "LPUSH" "l2" "a" "b")
  ; lpush a then b: b ends up at head
  (check-cmd "lrange after lpush" '("b" "a" "z" "y" "x") ctx "LRANGE" "l2" "0" "-1")

  (section "list: LRANGE negative indices + clamping")
  ; l1: a b c d e
  (check-cmd "lrange tail-1"     '("e")          ctx "LRANGE" "l1" "-1" "-1")
  (check-cmd "lrange last-2"     '("d" "e")      ctx "LRANGE" "l1" "-2" "-1")
  (check-cmd "lrange 1 to -2"    '("b" "c" "d")  ctx "LRANGE" "l1" "1" "-2")
  (check-cmd "lrange empty inv"  '()             ctx "LRANGE" "l1" "3" "1")
  (check-cmd "lrange clamp past" '("a" "b" "c" "d" "e") ctx "LRANGE" "l1" "-100" "100")
  (check-cmd "lrange missing"    '()             ctx "LRANGE" "nokey" "0" "-1")

  (section "list: LINDEX")
  ; l1: a b c d e
  (check-cmd "lindex 0"          "a"  ctx "LINDEX" "l1" "0")
  (check-cmd "lindex 4"          "e"  ctx "LINDEX" "l1" "4")
  (check-cmd "lindex -1"         "e"  ctx "LINDEX" "l1" "-1")
  (check-cmd "lindex -5"         "a"  ctx "LINDEX" "l1" "-5")
  (check-cmd "lindex oob"        'nil ctx "LINDEX" "l1" "5")
  (check-cmd "lindex neg oob"    'nil ctx "LINDEX" "l1" "-6")
  (check-cmd "lindex missing"    'nil ctx "LINDEX" "nokey" "0")

  (section "list: LSET")
  ; l1: a b c d e
  (check-cmd "lset 0"            'OK  ctx "LSET" "l1" "0" "A")
  (check-cmd "lset -1"           'OK  ctx "LSET" "l1" "-1" "E")
  (check-cmd "lrange after lset" '("A" "b" "c" "d" "E") ctx "LRANGE" "l1" "0" "-1")
  (check-cmd "lset oob"          '(err "ERR index out of range")
             ctx "LSET" "l1" "10" "x")
  (check-cmd "lset missing key"  '(err "ERR no such key")
             ctx "LSET" "nokey" "0" "x")

  (section "list: LPOP / RPOP")
  ; l1: A b c d E
  (check-cmd "lpop"              "A"  ctx "LPOP" "l1")
  (check-cmd "rpop"              "E"  ctx "RPOP" "l1")
  (check-cmd "lrange after pops" '("b" "c" "d") ctx "LRANGE" "l1" "0" "-1")
  (check-cmd "lpop down to 1"    "b"  ctx "LPOP" "l1")
  (check-cmd "rpop down to 1"    "d"  ctx "RPOP" "l1")
  ; one element left: c
  (check-cmd "llen 1"            1    ctx "LLEN" "l1")
  (check-cmd "lpop last"         "c"  ctx "LPOP" "l1")
  ; now empty -> purged
  (check-cmd "l1 len after empty" 0   ctx "LLEN" "l1")
  (check-cmd "l1 exists after purge" 0 ctx "EXISTS" "l1")
  (check-cmd "lpop missing"      'nil ctx "LPOP" "nokey")
  (check-cmd "rpop missing"      'nil ctx "RPOP" "nokey")

  (section "list: LPUSHX / RPUSHX")
  (check-cmd "lpushx missing"    0    ctx "LPUSHX" "ghost" "v")
  (check-cmd "rpushx missing"    0    ctx "RPUSHX" "ghost" "v")
  (check-cmd "ghost not created" 0    ctx "EXISTS" "ghost")
  (check-cmd "rpush base"        2    ctx "RPUSH" "lx" "p" "q")
  (check-cmd "lpushx existing"   3    ctx "LPUSHX" "lx" "o")
  (check-cmd "rpushx existing"   4    ctx "RPUSHX" "lx" "r")
  (check-cmd "lrange lpushx"     '("o" "p" "q" "r") ctx "LRANGE" "lx" "0" "-1")

  (section "list: LREM")
  ; build: a b a c a d
  (check-cmd "rpush lrem"        6    ctx "RPUSH" "lr" "a" "b" "a" "c" "a" "d")
  (check-cmd "lrem 0 all a"      3    ctx "LREM" "lr" "0" "a")
  (check-cmd "lrange after lrem 0" '("b" "c" "d") ctx "LRANGE" "lr" "0" "-1")
  ; reset: a a a b
  (check-cmd "rpush lr2"         4    ctx "RPUSH" "lr2" "a" "a" "a" "b")
  (check-cmd "lrem 2 head"       2    ctx "LREM" "lr2" "2" "a")
  (check-cmd "lrange lr2"        '("a" "b") ctx "LRANGE" "lr2" "0" "-1")
  ; reset: b a a a
  (check-cmd "rpush lr3"         4    ctx "RPUSH" "lr3" "b" "a" "a" "a")
  (check-cmd "lrem -2 tail"      2    ctx "LREM" "lr3" "-2" "a")
  (check-cmd "lrange lr3"        '("b" "a") ctx "LRANGE" "lr3" "0" "-1")
  (check-cmd "lrem missing key"  0    ctx "LREM" "nokey" "1" "x")
  ; lrem removes all -> purge
  (check-cmd "rpush lr4"         2    ctx "RPUSH" "lr4" "x" "x")
  (check-cmd "lrem all"          2    ctx "LREM" "lr4" "0" "x")
  (check-cmd "lr4 purged"        0    ctx "EXISTS" "lr4")

  (section "list: LTRIM")
  ; build: a b c d e
  (check-cmd "rpush ltrim"       5    ctx "RPUSH" "lt" "a" "b" "c" "d" "e")
  (check-cmd "ltrim 1 3"         'OK  ctx "LTRIM" "lt" "1" "3")
  (check-cmd "lrange after ltrim" '("b" "c" "d") ctx "LRANGE" "lt" "0" "-1")
  (check-cmd "ltrim neg"         'OK  ctx "LTRIM" "lt" "0" "-2")
  (check-cmd "lrange after neg trim" '("b" "c") ctx "LRANGE" "lt" "0" "-1")
  ; trim to empty
  (check-cmd "ltrim to empty"    'OK  ctx "LTRIM" "lt" "5" "10")
  (check-cmd "lt purged"         0    ctx "EXISTS" "lt")
  (check-cmd "ltrim missing"     'OK  ctx "LTRIM" "nokey" "0" "1")

  (section "list: WRONGTYPE")
  ; set a string key, then try list ops
  (check-cmd "set strkey2"       'OK  ctx "SET" "strkey2" "hello")
  (check-cmd "rpush wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "RPUSH" "strkey2" "v")
  (check-cmd "lpush wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "LPUSH" "strkey2" "v")
  (check-cmd "lpop wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "LPOP" "strkey2")
  (check-cmd "lrange wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "LRANGE" "strkey2" "0" "-1")
  (check-cmd "llen wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "LLEN" "strkey2")
  (check-cmd "lindex wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "LINDEX" "strkey2" "0")
  (check-cmd "lset wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "LSET" "strkey2" "0" "v"))
