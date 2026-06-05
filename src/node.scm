; node.scm — single-node crab-cache server bootstrap (Phase 4).
;
;   crabscheme run src/node.scm -- --port 6400 --db /tmp/crab-cache-db
;
; The main thread opens the store, spawns the shard-owner actor (which owns
; the store + the whole cache on its own thread), publishes its PID in a
; process-global table (the only way to hand a PID to a freshly-spawned
; actor — PIDs can't be spawn-source arguments), then BECOMES the accept
; loop: each accepted socket gets its own conn-actor (own thread). Phase 5
; adds sharding/Raft; Phase 6 adds peers.

; --- tiny CLI arg parser over (command-line) ---
(define (arg-after flag default)
  (let loop ((a (command-line)))
    (cond ((or (null? a) (null? (cdr a))) default)
          ((string=? (car a) flag) (cadr a))
          (else (loop (cdr a))))))

(define port  (string->number (arg-after "--port" "6400")))
(define dbpath (arg-after "--db" "/tmp/crab-cache-db"))
(define host  (arg-after "--host" "127.0.0.1"))

; --- store + shard-owner ---
(define H (store-open dbpath))
(make-table 'crabcache "set")
(define owner-pid (spawn-source "(include \"src/server/owner.scm\")" 'owner H))
(table-insert! 'crabcache "owner" owner-pid)

; --- accept loop (main thread = listener) ---
(define listener (tcp-listen host port))
(display "crab-cache: listening on ") (display host) (display ":") (display port)
(display "  db=") (display dbpath) (newline)

(let accept-loop ()
  (let ((sock (tcp-accept listener)))
    (spawn-source "(include \"src/server/conn.scm\")" 'conn sock)
    (accept-loop)))
