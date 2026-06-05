; test/keys.scm — key-space command semantics vs Redis.

(define (test-keys ctx)
  (section "keys")
  (rc ctx "SET" "a" "1")
  (rc ctx "SET" "b" "2")
  (check-cmd "exists one"    1   ctx "EXISTS" "a")
  (check-cmd "exists multi"  2   ctx "EXISTS" "a" "b" "a-missing")
  (check-cmd "exists dupes"  2   ctx "EXISTS" "a" "a")     ; counts repeats
  (check-cmd "type string"   "string" ctx "TYPE" "a")
  (check-cmd "type none"     "none"   ctx "TYPE" "nope")
  (check-cmd "del"           2   ctx "DEL" "a" "b" "nope") ; 2 existed
  (check-cmd "del gone"      0   ctx "EXISTS" "a")

  ; EXPIRE / TTL / PERSIST on logical clock
  (rc ctx "SET" "e" "v")
  (check-cmd "expire set"    1   ctx "EXPIRE" "e" "20")
  (check-cmd "ttl"           20  ctx "TTL" "e")
  (check-cmd "expire missing" 0  ctx "EXPIRE" "missing" "10")
  (check-cmd "persist"       1   ctx "PERSIST" "e")
  (check-cmd "ttl after persist" -1 ctx "TTL" "e")
  (check-cmd "persist noop"  0   ctx "PERSIST" "e")
  (check-cmd "ttl no key"    -2  ctx "TTL" "ghost")
  ; negative expire deletes
  (rc ctx "SET" "neg" "v")
  (check-cmd "expire neg deletes" 1 ctx "EXPIRE" "neg" "-1")
  (check-cmd "neg gone"      0   ctx "EXISTS" "neg")

  ; KEYS glob
  (rc ctx "FLUSHDB-LIKE")            ; (no-op; just spacing — ignore unknown)
  (rc ctx "SET" "user:1" "a")
  (rc ctx "SET" "user:2" "b")
  (rc ctx "SET" "admin:1" "c")
  (let ((all (rcx ctx "KEYS" "*")))
    (check "keys * count>=3" #t (>= (length all) 3)))
  (let ((us (rcx ctx "KEYS" "user:*")))
    (check "keys user:* = 2" 2 (length us)))
  (let ((q (rcx ctx "KEYS" "user:?")))
    (check "keys user:? = 2" 2 (length q)))

  ; RENAME (string)
  (rc ctx "SET" "rn" "hello")
  (check-cmd "rename"        'OK   ctx "RENAME" "rn" "rn2")
  (check-cmd "rename gone"   0     ctx "EXISTS" "rn")
  (check-cmd "rename dst"    "hello" ctx "GET" "rn2"))
