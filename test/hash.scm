; test/hash.scm — hash command semantics tests.
; Uses check-cmd from test/harness.scm.

(define (test-hash ctx)
  (section "hash: HSET / HGET basics")
  (check-cmd "hset new fields"   2    ctx "HSET" "h1" "f1" "v1" "f2" "v2")
  (check-cmd "hget f1"           "v1" ctx "HGET" "h1" "f1")
  (check-cmd "hget f2"           "v2" ctx "HGET" "h1" "f2")
  (check-cmd "hget missing field" 'nil ctx "HGET" "h1" "absent")
  (check-cmd "hget missing key"  'nil ctx "HGET" "nokey" "f1")
  (check-cmd "hset update field" 0    ctx "HSET" "h1" "f1" "updated")
  (check-cmd "hget updated"      "updated" ctx "HGET" "h1" "f1")
  (check-cmd "hset mixed new+upd" 1   ctx "HSET" "h1" "f1" "v1" "f3" "v3")

  (section "hash: HSETNX")
  (check-cmd "hsetnx new"        1    ctx "HSETNX" "h2" "fa" "va")
  (check-cmd "hsetnx existing"   0    ctx "HSETNX" "h2" "fa" "vb")
  (check-cmd "hsetnx val unchanged" "va" ctx "HGET" "h2" "fa")

  (section "hash: HMGET / HMSET")
  (check-cmd "hmset"             'OK  ctx "HMSET" "hm" "a" "1" "b" "2" "c" "3")
  (check-cmd "hmget all"         '("1" "2" "3")
             ctx "HMGET" "hm" "a" "b" "c")
  (check-cmd "hmget with missing" '("1" nil "3")
             ctx "HMGET" "hm" "a" "gone" "c")
  (check-cmd "hmget missing key" '(nil nil)
             ctx "HMGET" "nokey" "x" "y")

  (section "hash: HLEN / HEXISTS / HSTRLEN")
  (check-cmd "hlen"              3    ctx "HLEN" "hm")
  (check-cmd "hlen missing key"  0    ctx "HLEN" "nokey")
  (check-cmd "hexists yes"       1    ctx "HEXISTS" "hm" "a")
  (check-cmd "hexists no"        0    ctx "HEXISTS" "hm" "z")
  (check-cmd "hexists missing"   0    ctx "HEXISTS" "nokey" "a")
  (check-cmd "hstrlen"           1    ctx "HSTRLEN" "hm" "a")
  (check-cmd "hstrlen missing f" 0    ctx "HSTRLEN" "hm" "z")
  (check-cmd "hstrlen missing k" 0    ctx "HSTRLEN" "nokey" "a")

  (section "hash: HKEYS / HVALS / HGETALL ordering")
  ; hm has fields a=1, b=2, c=3 (scanned in key-byte order)
  (check-cmd "hkeys"             '("a" "b" "c") ctx "HKEYS" "hm")
  (check-cmd "hvals"             '("1" "2" "3") ctx "HVALS" "hm")
  (check-cmd "hgetall"           '("a" "1" "b" "2" "c" "3") ctx "HGETALL" "hm")
  (check-cmd "hkeys missing"     '()  ctx "HKEYS" "nokey")
  (check-cmd "hvals missing"     '()  ctx "HVALS" "nokey")
  (check-cmd "hgetall missing"   '()  ctx "HGETALL" "nokey")

  (section "hash: HINCRBY")
  (check-cmd "hincrby new"       10   ctx "HINCRBY" "hincr" "n" "10")
  (check-cmd "hincrby again"     15   ctx "HINCRBY" "hincr" "n" "5")
  (check-cmd "hincrby negative"  10   ctx "HINCRBY" "hincr" "n" "-5")
  ; set a string field that cannot parse as int, then test hincrby on it
  (check-cmd "hmset nonint"      'OK  ctx "HMSET" "hm" "s" "hello")
  (check-cmd "hincrby nonint field"
             '(err "ERR value is not an integer or out of range")
             ctx "HINCRBY" "hm" "s" "1")
  (check-cmd "hincrby bad field val"
             '(err "ERR value is not an integer or out of range")
             ctx "HINCRBY" "hm" "s" "1")
  (check-cmd "hincrby bad delta"
             '(err "ERR value is not an integer or out of range")
             ctx "HINCRBY" "hm" "a" "notanum")

  (section "hash: HDEL + removal to empty")
  (check-cmd "hdel existing"     1    ctx "HDEL" "h2" "fa")
  (check-cmd "hdel gone"         0    ctx "HEXISTS" "h2" "fa")
  ; h2 should now be empty -> purged
  (check-cmd "h2 exists after purge" 0 ctx "EXISTS" "h2")
  (check-cmd "h2 type after purge" "none" ctx "TYPE" "h2")
  ; multi-field delete
  (check-cmd "hset multi"        3    ctx "HSET" "hdel2" "x" "1" "y" "2" "z" "3")
  (check-cmd "hdel multi"        2    ctx "HDEL" "hdel2" "x" "y")
  (check-cmd "hdel remaining"    1    ctx "HLEN" "hdel2")
  (check-cmd "hdel missing field" 0   ctx "HDEL" "hdel2" "gone")
  (check-cmd "hdel missing key"  0    ctx "HDEL" "nokey" "f")
  ; delete last field -> purge
  (check-cmd "hdel last"         1    ctx "HDEL" "hdel2" "z")
  (check-cmd "hdel2 purged"      0    ctx "EXISTS" "hdel2")

  (section "hash: WRONGTYPE")
  ; set a string key then try hash ops on it
  (check-cmd "set string key"    'OK  ctx "SET" "strkey" "hello")
  (check-cmd "hset wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "HSET" "strkey" "f" "v")
  (check-cmd "hget wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "HGET" "strkey" "f")
  (check-cmd "hdel wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "HDEL" "strkey" "f")
  (check-cmd "hgetall wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "HGETALL" "strkey")
  (check-cmd "hincrby wrongtype"
             '(err "WRONGTYPE Operation against a key holding the wrong kind of value")
             ctx "HINCRBY" "strkey" "n" "1"))
