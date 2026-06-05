; node-cluster.scm — a crab-cache CLUSTER node (Phase 6). One process per node.
;
;   crabscheme run src/node-cluster.scm -- \
;       --node a --shards 3 --db /tmp/cc-a \
;       --cluster a:127.0.0.1:7001:6001,b:127.0.0.1:7002:6002,c:127.0.0.1:7003:6003
;
; Every node replicates every shard, so each shard is an R-voter Raft group
; (R = #nodes) spread across the cluster. The node:
;   1. node-make + node-listen on its raft addr; dials higher-named peers
;      (one TCP connection per pair) and waits for the full mesh;
;   2. spawns a shard-replica per shard (voters = all node names);
;   3. spawns the peer-poller (drains node-poll -> local replicas; ticks);
;   4. serves RESP on its client port, MOVED-ing keyed commands whose shard
;      this node doesn't currently lead to the leader's client address.

(include "src/encoding.scm")
(include "src/slotmap.scm")

(define (arg-after flag default)
  (let loop ((a (command-line)))
    (cond ((or (null? a) (null? (cdr a))) default)
          ((string=? (car a) flag) (cadr a))
          (else (loop (cdr a))))))

(define (split-on s ch)
  (let loop ((i 0) (start 0) (acc '()))
    (cond ((= i (string-length s)) (reverse (cons (substring s start i) acc)))
          ((char=? (string-ref s i) ch) (loop (+ i 1) (+ i 1) (cons (substring s start i) acc)))
          (else (loop (+ i 1) start acc)))))

(define me      (string->symbol (arg-after "--node" "a")))
(define nshards (string->number (arg-after "--shards" "3")))
(define dbbase  (arg-after "--db" "/tmp/cc-node"))
(define durable (string=? (arg-after "--durable" "no") "yes"))
(define cluster-spec (arg-after "--cluster" "a:127.0.0.1:7001:6001"))

; parse "name:host:raftport:clientport,..." -> list of (name host raftport clientport)
(define nodes
  (map (lambda (e)
         (let ((p (split-on e #\:)))
           (list (string->symbol (car p)) (cadr p)
                 (string->number (caddr p)) (string->number (cadddr p)))))
       (split-on cluster-spec #\,)))

(define (node-field nm i)
  (let loop ((ns nodes)) (cond ((null? ns) #f) ((eqv? (caar ns) nm) (list-ref (car ns) i)) (else (loop (cdr ns))))))
(define (raft-addr nm)   (string-append (node-field nm 1) ":" (number->string (node-field nm 2))))
(define (client-addr nm) (string-append (node-field nm 1) ":" (number->string (node-field nm 3))))

(define all-names (map car nodes))
(define my-host (node-field me 1))
(define my-cport (node-field me 3))

(make-table 'cc-shard-pid "set")
(make-table 'cc-shard-role "set")
(make-table 'cc-shard-leader "set")
(make-table 'cc-shard-commit "set")
(make-table 'cc-broker "set")
(make-table 'cc-config "set")

; ---- bring up the inter-node mesh ----
(node-make (symbol->string me))
(node-listen (symbol->string me) (raft-addr me))

; dial only higher-named peers (one connection per pair); retry until up.
(define (sym>? a b) (string>? (symbol->string a) (symbol->string b)))
(define dial-peers (let loop ((ns all-names) (acc '()))
                     (cond ((null? ns) (reverse acc))
                           ((sym>? (car ns) me) (loop (cdr ns) (cons (car ns) acc)))
                           (else (loop (cdr ns) acc)))))
(define (try-connect addr) (guard (e (#t #f)) (node-connect (symbol->string me) addr) #t))
; On a fresh start the dial-higher-named handshakes form the mesh; on a
; RESTART, peers re-dial us via their peer-poller heal, so we just wait for the
; full peer count (be patient — the healing peer may be a tick or two away).
(let mesh ((tries 0))
  (for-each (lambda (nm) (try-connect (raft-addr nm))) dial-peers)
  (cond ((>= (node-peer-count (symbol->string me)) (- (length nodes) 1)) #t)
        ((> tries 200000000) (error "cluster: mesh did not form"))
        (else (mesh (+ tries 1)))))
(display "node ") (display me) (display ": mesh up (")
(display (node-peer-count (symbol->string me))) (display " peers)") (newline)

; ---- shards: one replica per shard, voters = all nodes ----
(let loop ((i 0))
  (if (< i nshards)
      (begin
        (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                      (number->string i) all-names me
                      (string-append dbbase "-shard" (number->string i)) durable)
        (loop (+ i 1)))))

; shard-key list for the poller
(define shard-keys (let loop ((i 0) (acc '()))
                     (if (>= i nshards) (reverse acc) (loop (+ i 1) (cons (number->string i) acc)))))
(define dial-addrs (map raft-addr dial-peers))
(spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
              me shard-keys 120 dial-addrs (- (length nodes) 1))

; pub/sub broker: fans PUBLISH out to peer brokers over node-send (the
; peer-poller delivers inbound broker-publish frames to it).
(define peer-names
  (let loop ((ns all-names) (acc '()))
    (cond ((null? ns) (reverse acc))
          ((eqv? (car ns) me) (loop (cdr ns) acc))
          (else (loop (cdr ns) (cons (car ns) acc))))))
(spawn-source "(include \"src/server/pubsub.scm\")" 'broker-main me peer-names)

; ---- config for conn-actors (MOVED uses live cc-shard-leader + these addrs) ----
(define node-id "0000000000000000000000000000000000000000")
(define ranges
  (let loop ((i 0) (acc '()))
    (if (>= i nshards) (reverse acc)
        (let ((rng (shard-slot-range i nshards)))
          (loop (+ i 1) (cons (list (car rng) (cdr rng) my-host my-cport node-id) acc))))))
(define addrs (map (lambda (nm) (cons nm (client-addr nm))) all-names))
(table-insert! 'cc-config "cfg" (list my-host my-cport nshards node-id ranges me addrs))

; wait until this node has elected/learned a leader for every shard, so the
; first client never hits a "no leader yet" window.
(define (qk i) (string-append (symbol->string me) ":" (number->string i)))
(let spin ((i 0))
  (cond ((>= i nshards) #t)
        ((table-lookup 'cc-shard-leader (qk i)) (spin (+ i 1)))
        (else (spin 0))))

(define listener (tcp-listen my-host my-cport))
(display "node ") (display me) (display ": serving RESP on ")
(display my-host) (display ":") (display my-cport) (newline)
(let accept-loop ()
  (let ((sock (tcp-accept listener)))
    (spawn-source "(include \"src/server/conn.scm\")" 'conn sock)
    (accept-loop)))
