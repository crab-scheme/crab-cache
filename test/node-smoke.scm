; node-smoke.scm — start a real TCP server (reduced owner: no zset, which is
; still being written) to validate the whole network path against redis-cli
; on the current binary. A single connection doesn't need the net
; concurrency fix, so this runs pre-rebuild.

(define H (store-open "/tmp/cc-node-smoke-db"))
(make-table 'crabcache "set")

(define owner-src "
  (include \"src/reply.scm\")
  (include \"src/encoding.scm\")
  (include \"src/store-ctx.scm\")
  (include \"src/shard.scm\")
  (include \"src/commands/string.scm\")
  (include \"src/commands/keys.scm\")
  (include \"src/commands/hash.scm\")
  (include \"src/commands/list.scm\")
  (include \"src/commands/set.scm\")
  (include \"src/commands/server.scm\")
  (define (owner handle)
    (let ((ctx (make-ctx handle \"default\")))
      (let loop ()
        (let ((m (raw-receive)))
          (if (pair? m) (send (car m) (shard-dispatch ctx (cdr m))))
          (loop)))))")

(define owner-pid (spawn-source owner-src 'owner H))
(table-insert! 'crabcache "owner" owner-pid)

(define listener (tcp-listen "127.0.0.1" 6399))
(display "node-smoke listening on 6399") (newline)

(let loop ()
  (let ((s (tcp-accept listener)))
    (spawn-source "(include \"src/server/conn.scm\")" 'conn s)
    (loop)))
