; sim-cluster-smoke.scm — Phase 6 logic in ONE process over the in-memory sim
; transport (node-link!): 3 logical nodes a/b/c, one shard "0" replicated on
; all three (a 3-voter Raft group). Proves leader election, cross-"node"
; replication, the commit->ack bridge over node-send, and RocksDB convergence
; — the exact code that runs over real TCP, only the transport wiring differs.

(make-table 'cc-shard-pid "set")
(make-table 'cc-shard-role "set")
(make-table 'cc-shard-leader "set")
(make-table 'cc-shard-commit "set")
(make-table 'cc-test "set")

(for-each node-make (list "a" "b" "c"))
(node-link! "a" "b") (node-link! "a" "c") (node-link! "b" "c")

(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                 "0" '(a b c) nd (string-append "/tmp/cc-sim-" (symbol->string nd) "-s0")))
 '(a b c))
(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller nd '("0") 150))
 '(a b c))

(define (role nd) (table-lookup 'cc-shard-role (string-append nd ":0")))
(define (commit nd) (let ((c (table-lookup 'cc-shard-commit (string-append nd ":0")))) (if c c 0)))
(define (leader-pid nd) (table-lookup 'cc-shard-pid (string-append nd ":0")))

(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t)
          ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))

(define (leader-node)
  (cond ((eq? (role "a") 'leader) "a")
        ((eq? (role "b") 'leader) "b")
        ((eq? (role "c") 'leader) "c")
        (else #f)))

(spin (lambda () (leader-node)) "leader election")
(define ldr (leader-node))
(display "leader elected: ") (display ldr) (newline)

; drive writes at the leader via a client actor (real PID reply path)
(table-insert! 'cc-test "ldr" ldr)
(define client-src "
  (define (b s) (string->utf8 s))
  (define (ask pid cmd) (send pid (cons (self) cmd)) (raw-receive))
  (define (client)
    (let ((o (table-lookup 'cc-shard-pid (string-append (table-lookup 'cc-test \"ldr\") \":0\"))))
      (ask o (list (b \"SET\") (b \"city\") (b \"oslo\")))
      (ask o (list (b \"INCR\") (b \"n\")))
      (let ((g (ask o (list (b \"GET\") (b \"city\")))))
        (table-insert! 'cc-test \"get\" (cdr g))
        (table-insert! 'cc-test \"done\" #t))))")
(spawn-source client-src 'client)

(spin (lambda () (table-lookup 'cc-test "done")) "writes acked by quorum")
(display "GET city via leader => ") (display (utf8->string (table-lookup 'cc-test "get"))) (newline)

; every replica must converge: commit index catches up on all three nodes
(spin (lambda () (and (>= (commit "a") 3) (>= (commit "b") 3) (>= (commit "c") 3)))
      "all replicas commit")
(display "commit a/b/c = ")
(display (list (commit "a") (commit "b") (commit "c"))) (newline)

(if (and (string=? (utf8->string (table-lookup 'cc-test "get")) "oslo")
         (>= (commit "a") 3) (>= (commit "b") 3) (>= (commit "c") 3))
    (display "PHASE 6 sim-cluster replication OK\n")
    (error "sim-cluster FAILED"))
