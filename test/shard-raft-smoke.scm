; shard-raft-smoke.scm — drive commands through a 1-voter Raft shard-replica
; and confirm propose -> commit -> apply(RocksDB) -> reply works.

(make-table 'cc-shard-pid "set")
(make-table 'cc-shard-role "set")
(make-table 'cc-test "set")

(spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main "0" '(n) 'n "/tmp/cc-shard0-db" #f)

(define (await tbl key)
  (let loop ((i 0))
    (let ((v (table-lookup tbl key)))
      (cond (v v) ((> i 80000000) (error "timeout" key)) (else (loop (+ i 1)))))))

(await 'cc-shard-pid "0")

(define client-src "
  (define (b s) (string->utf8 s))
  (define (ask cmd)
    (let ((o (table-lookup 'cc-shard-pid \"0\")))
      (send o (cons (self) cmd))
      (raw-receive)))
  (define (client)
    (ask (list (b \"SET\") (b \"k\") (b \"v\")))
    (let ((g (ask (list (b \"GET\") (b \"k\")))))
      (ask (list (b \"INCR\") (b \"ctr\")))
      (let ((i2 (ask (list (b \"INCR\") (b \"ctr\"))))
            (h  (ask (list (b \"HSET\") (b \"hh\") (b \"f\") (b \"1\")))))
        (table-insert! 'cc-test \"get\" (cdr g))
        (table-insert! 'cc-test \"incr\" (cdr i2))
        (table-insert! 'cc-test \"hset\" (cdr h)))))")
(spawn-source client-src 'client)

(define got (await 'cc-test "get"))
(define inc (await 'cc-test "incr"))
(define hs  (await 'cc-test "hset"))
(define role (table-lookup 'cc-shard-role "0"))
(display "shard role: ") (display role) (newline)
(display "GET  => ") (display (utf8->string got)) (newline)
(display "INCR => ") (display inc) (newline)
(display "HSET => ") (display hs) (newline)
(if (and (string=? (utf8->string got) "v") (= inc 2) (= hs 1) (eq? role 'leader))
    (display "PHASE 5 shard-raft smoke OK\n")
    (error "smoke FAILED" got inc hs role))
