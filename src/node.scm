; node.scm — crab-cache node bootstrap (Phase 5: single host, N shards).
;
;   crabscheme run src/node.scm -- --port 6400 --db /tmp/cc --shards 3
;
; Spawns one shard-replica actor per shard (each its own RocksDB, a 1-voter
; Raft group on this host), publishes the topology, waits for the shards to
; come up, then becomes the accept loop spawning a conn-actor per client.
; Phase 6 turns the 1-voter groups into R-replica groups across nodes.

(include "src/encoding.scm")
(include "src/slotmap.scm")

(define (arg-after flag default)
  (let loop ((a (command-line)))
    (cond ((or (null? a) (null? (cdr a))) default)
          ((string=? (car a) flag) (cadr a))
          (else (loop (cdr a))))))

(define port    (string->number (arg-after "--port" "6400")))
(define dbbase  (arg-after "--db" "/tmp/crab-cache"))
(define host    (arg-after "--host" "127.0.0.1"))
(define nshards (string->number (arg-after "--shards" "3")))
(define durable (string=? (arg-after "--durable" "no") "yes"))  ; fsync each write
(define node-name 'n)

; a 40-hex-char node id derived from the node name (Redis-cluster shaped)
(define (node-id-of name)
  (let loop ((cs (string->list (symbol->string name))) (acc ""))
    (if (null? cs)
        (let ((pad (- 40 (string-length acc))))
          (if (>= pad 0) (string-append acc (make-string pad #\0)) (substring acc 0 40)))
        (let ((h (number->string (char->integer (car cs)) 16)))
          (loop (cdr cs) (string-append acc (if (= (string-length h) 1) (string-append "0" h) h)))))))
(define node-id (node-id-of node-name))

(make-table 'cc-shard-pid "set")
(make-table 'cc-shard-role "set")
(make-table 'cc-shard-leader "set")
(make-table 'cc-shard-commit "set")
(make-table 'cc-broker "set")
(make-table 'cc-config "set")

; pub/sub broker (single node => no peers to fan out to)
(spawn-source "(include \"src/server/pubsub.scm\")" 'broker-main node-name '())

; topology ranges: (start end host port node-id) per shard (all this node)
(define ranges
  (let loop ((i 0) (acc '()))
    (if (>= i nshards) (reverse acc)
        (let ((rng (shard-slot-range i nshards)))
          (loop (+ i 1) (cons (list (car rng) (cdr rng) host port node-id) acc))))))
; single node => it leads every shard; nothing is ever MOVED.
(define addrs (list (cons node-name (string-append host ":" (number->string port)))))
(table-insert! 'cc-config "cfg" (list host port nshards node-id ranges node-name addrs))

; spawn one shard-replica per shard (own DB dir, 1-voter group = this node)
(let loop ((i 0))
  (if (< i nshards)
      (begin
        (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                      (number->string i) (list node-name) node-name
                      (string-append dbbase "-shard" (number->string i)) durable)
        (loop (+ i 1)))))

; wait until every shard has published its pid (so routing always resolves)
(define (shard-qk i) (string-append (symbol->string node-name) ":" (number->string i)))
(let spin ((i 0))
  (cond ((>= i nshards) #t)
        ((table-lookup 'cc-shard-pid (shard-qk i)) (spin (+ i 1)))
        (else (spin 0))))

(define listener (tcp-listen host port))
(display "crab-cache: ") (display nshards) (display " shards on ")
(display host) (display ":") (display port) (newline)

(let accept-loop ()
  (let ((sock (tcp-accept listener)))
    (spawn-source "(include \"src/server/conn.scm\")" 'conn sock)
    (accept-loop)))
