; server/peer-poller.scm — one per node. The ONLY actor that calls node-poll
; for this node: it drains inbound frames and fans each Raft RPC to the right
; LOCAL shard-replica's mailbox, and emits periodic ticks (heartbeat for a
; leader, election clock for a follower). Replicas never poll the network —
; they're mailbox-driven — so frames are never split between pollers.
;
;   (spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
;                 NODE-NAME SHARD-KEYS TICK-EVERY)
;   SHARD-KEYS : list of shard-key strings whose replicas live on this node
;   TICK-EVERY : emit a tick after this many idle poll iterations

(define (peer-poller node-name shard-keys tick-every)
  (define (local-pid sk)
    (table-lookup 'cc-shard-pid (string-append (symbol->string node-name) ":" sk)))
  ; frame = (shard-engine SHARD-KEY FROM RPC) -> deliver (engine FROM RPC)
  (define (route! frame)
    (if (and (pair? frame) (eq? (car frame) 'shard-engine))
        (let ((pid (local-pid (cadr frame))))
          (if pid (send pid (list 'engine (caddr frame) (cadddr frame)))))))
  (define (tick-all!)
    (for-each (lambda (sk) (let ((p (local-pid sk))) (if p (send p (list 'tick)))))
              shard-keys))
  (let loop ((i 0))
    (let ((msgs (node-poll (symbol->string node-name))))
      (for-each route! msgs)
      (cond
        ((>= i tick-every) (tick-all!) (loop 0))
        ((null? msgs) (yield) (loop (+ i 1)))
        (else (loop (+ i 1)))))))
