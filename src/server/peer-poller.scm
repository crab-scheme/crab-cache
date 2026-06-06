; server/peer-poller.scm — one per node. The ONLY actor that calls node-poll
; for this node: it drains inbound frames and fans each Raft RPC to the right
; LOCAL shard-replica's mailbox, and emits periodic ticks (heartbeat for a
; leader, election clock for a follower). Replicas never poll the network —
; they're mailbox-driven — so frames are never split between pollers.
;
;   (spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
;                 NODE-NAME SHARD-KEYS TICK-EVERY)
;   SHARD-KEYS : list of shard-key strings whose replicas live on this node
;   TICK-EVERY : emit a tick after this many idle poll iterations. NOTE: the
;                idle branch must stay (yield), NOT (sleep-ms). This loop is
;                two things at once: the Raft tick clock (ticks pace
;                heartbeats/elections, counted in idle iterations) AND this
;                node's sole inbound-frame drainer. Any real sleep slows tick
;                emission + frame delivery, stretching the propose->replicate
;                window — measured ~2/3 failover runs then lose an acked write.
;                (This actor is a dedicated-thread spawn-source actor, so it's
;                a protocol-timing issue, not worker starvation; and sleep-ms
;                now hard-errors on a shared LocalSet worker anyway.)

; dial-addrs : raft addresses of higher-named peers to (re)dial
; target     : expected peer count (= #nodes - 1); when we have fewer, a peer
;              has gone (or a restarted peer is coming back) so we re-dial,
;              healing the mesh. node-connect is keyed by peer name, so
;              re-dialing a live peer is a harmless replace.
(define (peer-poller node-name shard-keys tick-every dial-addrs target)
  (define (local-pid sk)
    (table-lookup 'cc-shard-pid (string-append (symbol->string node-name) ":" sk)))
  (define (heal!)
    (node-detect-disconnects (symbol->string node-name))   ; prune dead peers first
    (if (< (node-peer-count (symbol->string node-name)) target)
        (for-each (lambda (a) (guard (e (#t #f)) (node-connect (symbol->string node-name) a)))
                  dial-addrs)))
  ; route an inbound frame: a Raft RPC to its local shard replica, or a
  ; cross-node PUBLISH to the local broker.
  (define (route! frame)
    (cond
      ((not (pair? frame)) #f)
      ((eq? (car frame) 'shard-engine)
       (let ((pid (local-pid (cadr frame))))
         (if pid (send pid (list 'engine (caddr frame) (cadddr frame))))))
      ((eq? (car frame) 'broker-publish)
       (let ((b (table-lookup 'cc-broker (symbol->string node-name))))
         (if b (send b (list 'rpublish (cadr frame) (caddr frame))))))))
  (define (tick-all!)
    (for-each (lambda (sk) (let ((p (local-pid sk))) (if p (send p (list 'tick)))))
              shard-keys))
  (let loop ((i 0))
    (let ((msgs (node-poll (symbol->string node-name))))
      (for-each route! msgs)
      (cond
        ((>= i tick-every) (tick-all!) (heal!) (loop 0))
        ((null? msgs) (yield) (loop (+ i 1)))
        (else (loop (+ i 1)))))))
