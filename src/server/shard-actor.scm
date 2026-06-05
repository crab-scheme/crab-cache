; server/shard-actor.scm — the shard-replica actor body (solo Phase 5 AND
; multi-voter cross-node Phase 6).
;
; Loaded by spawn-source into its own runtime/thread:
;   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
;                 SHARD-KEY VOTERS NODE-NAME DB-PATH)
;   SHARD-KEY : string routing/table key ("0".."N-1")
;   VOTERS    : list of node-name symbols in this shard's Raft group
;   NODE-NAME : this replica's Raft id = this node's name (symbol)
;   DB-PATH   : this shard's own RocksDB directory
;
; It is ENTIRELY MAILBOX-DRIVEN. Three message shapes arrive on the mailbox:
;   (conn-pid . cmd-list)   a local client proposal (conn-pid is a PID)
;   (engine FROM RPC)       a Raft RPC, forwarded by the node's peer-poller
;   (tick)                  a heartbeat/election tick from the peer-poller
; Raft OUTPUTS (AppendEntries/RequestVote to peers) are shipped by node name
; via (node-send self peer (list 'shard-engine SHARD-KEY self rpc)); the peer
; node's peer-poller delivers them to the right local replica's mailbox.
;
; raft.scm has no timers, so leadership is driven here: the leader heartbeats
; on every tick (raft-tick -> AE), and a follower that misses `timeout` ticks
; (staggered by voter index, so the lowest-index live voter wins and split
; votes are unlikely) campaigns. The commit->ack bridge is async: pending[
; log-index]=conn at propose, replies sent when that index commits+applies
; (which may be a later engine message once a quorum of AERs arrives).

(include "src/reply.scm")
(include "src/encoding.scm")
(include "src/store-ctx.scm")
(include "src/shard.scm")
(include "src/commands/string.scm")
(include "src/commands/keys.scm")
(include "src/commands/hash.scm")
(include "src/commands/list.scm")
(include "src/commands/set.scm")
(include "src/commands/zset.scm")
(include "src/commands/server.scm")
(include "src/raft.scm")

(define (raft-applied st) (aget st 'applied))

(define (index-of x lst)
  (let loop ((i 0) (l lst))
    (cond ((null? l) 0) ((eqv? (car l) x) i) (else (loop (+ i 1) (cdr l))))))

(define (member-str? s lst)
  (cond ((null? lst) #f) ((string=? s (car lst)) #t) (else (member-str? s (cdr lst)))))

; Commands that MUTATE state must go through the Raft log; everything else is
; a read the leader can serve directly from its applied RocksDB (linearizable
; on the leader, and keeps reads out of the log).
(define (write-cmd? name)
  (member-str? name
   '("SET" "SETNX" "GETSET" "APPEND" "INCR" "DECR" "INCRBY" "DECRBY" "MSET"
     "DEL" "UNLINK" "EXPIRE" "PEXPIRE" "PERSIST" "RENAME" "TICK"
     "HSET" "HSETNX" "HMSET" "HDEL" "HINCRBY"
     "LPUSH" "RPUSH" "LPUSHX" "RPUSHX" "LPOP" "RPOP" "LSET" "LREM" "LTRIM"
     "SADD" "SREM" "SPOP" "ZADD" "ZREM" "ZINCRBY" "FLUSHALL" "FLUSHDB")))

(define (shard-main shard-key voters node-name db-path sync?)
  (let* ((handle  (store-open db-path))
         (ctx     (make-ctx handle "default" sync?))
         (pending (make-eqv-hashtable))            ; log-index -> conn-pid
         (acc     '())                             ; replies, newest-first (apply order)
         (snap-n  0)                               ; checkpoint counter (unique dirs)
         (solo    (null? (cdr voters)))            ; 1-voter group?
         ; Staggered election timeout, ROTATED by shard so leadership spreads:
         ; for shard S the voter at index S has the shortest timeout and tends
         ; to win it. Deterministic => no split votes, predictable failover.
         (timeout (+ 4 (* (modulo (- (index-of node-name voters)
                                     (let ((n (string->number shard-key))) (if n n 0)))
                                  (length voters))
                          3))))
    (define (apply-fn sm cmd)
      (set! acc (cons (shard-dispatch ctx cmd) acc))
      (+ sm 1))
    ; ship engine outputs (target-node . rpc) to peers over the node transport.
    ; A send to a DOWN peer must not crash us — Raft is lossy-tolerant and
    ; recovers the entry on the next heartbeat/AE, so swallow transport errors.
    (define (emit! outs)
      (for-each
       (lambda (o)
         (guard (e (#t #f))
           (node-send (symbol->string node-name) (symbol->string (car o))
                      (list 'shard-engine shard-key node-name (cdr o)))))
       outs))
    ; match in-order applied replies to the indices that produced them
    (define (drain! old-applied)
      (let loop ((k 0) (rs (reverse acc)))
        (if (pair? rs)
            (let* ((idx (+ old-applied 1 k)) (conn (hashtable-ref pending idx #f)))
              (if conn (begin (send conn (car rs)) (hashtable-delete! pending idx)))
              (loop (+ k 1) (cdr rs)))))
      (set! acc '()))
    ; solo log compaction (RocksDB is the snapshot); no-op for multi-voter
    (define (compact st)
      (if (and solo (= (raft-applied st) (log-len st)))
          (aset* st (list 'log '() 'commit 0 'applied 0))
          st))
    ; node-qualified table keys ("node:shard") so the in-process sim (all
    ; replicas in one process) doesn't collide; in production each node has its
    ; own process-global table and the node prefix is simply constant.
    (define (qk) (string-append (symbol->string node-name) ":" shard-key))
    (define (publish! st leader)
      (table-insert! 'cc-shard-pid (qk) (self))
      (table-insert! 'cc-shard-role (qk) (raft-role st))
      (table-insert! 'cc-shard-leader (qk) leader)
      (table-insert! 'cc-shard-commit (qk) (raft-commit st)))

    (let* ((st0 (make-raft node-name voters apply-fn 0))
           (stI (if solo (car (raft-campaign st0)) st0))   ; solo self-elects now
           (ldr0 (if solo node-name #f)))
      (publish! stI ldr0)
      (let loop ((st stI) (leader ldr0) (elapsed 0))
        (let ((m (raw-receive)))
          (cond
            ((not (pair? m)) (loop st leader elapsed))

            ;; ---- Raft RPC from a peer ----
            ((eq? (car m) 'engine)
             (let* ((from (cadr m)) (rpc (caddr m))
                    (old (raft-applied st))
                    (r (raft-step st from rpc)) (st2 (car r)))
               (emit! (cdr r))
               (drain! old)
               (let* ((ae? (eq? (car rpc) 'ae))
                      (ldr (cond ((raft-leader? st2) node-name) (ae? from) (else leader)))
                      (el  (if ae? 0 elapsed)))
                 (publish! st2 ldr)
                 (loop (compact st2) ldr el))))

            ;; ---- heartbeat / election tick ----
            ((eq? (car m) 'tick)
             (cond
               ((raft-leader? st)
                (let ((r (raft-tick st))) (emit! (cdr r)) (loop (car r) node-name 0)))
               (solo (loop st leader elapsed))
               ((>= elapsed timeout)
                (let* ((r (raft-campaign st)) (st2 (car r))
                       (ldr (if (raft-leader? st2) node-name #f)))
                  (emit! (cdr r))
                  (publish! st2 ldr)
                  (loop st2 ldr 0)))
               (else (loop st leader (+ elapsed 1)))))

            ;; ---- per-node admin, served directly regardless of role ----
            ;; (DBSIZE/FLUSHALL/KEYS are per-node in cluster mode)
            ((eq? (car m) 'direct)
             (send (cadr m) (shard-dispatch ctx (caddr m)))
             (loop st leader elapsed))

            ;; ---- SAVE: snapshot this shard's RocksDB via a checkpoint ----
            ((eq? (car m) 'checkpoint)
             (set! snap-n (+ snap-n 1))
             (send (cadr m)
                   (guard (e (#t (r-err "ERR checkpoint failed")))
                     (store-checkpoint handle (string-append db-path "-snap" (number->string snap-n)))
                     (r-ok)))
             (loop st leader elapsed))

            ;; ---- local client command: (conn-pid . cmd) ----
            (else
             (let* ((conn (car m)) (cmd (cdr m))
                    (name (string-upcase (utf8->string (car cmd)))))
               (cond
                 ((not (raft-leader? st))
                  (send conn (r-err "TRYAGAIN shard not leader")) (loop st leader elapsed))
                 ((not (write-cmd? name))
                  ; read on the leader: serve straight from applied state
                  (send conn (shard-dispatch ctx cmd)) (loop st leader elapsed))
                 (else
                  (let ((old (raft-applied st)) (idx (+ 1 (log-len st))))
                    (hashtable-set! pending idx conn)
                    (let* ((r (raft-propose st cmd)) (st1 (car r)))
                      (emit! (cdr r))                 ; AE to followers (cluster) / none (solo)
                      (let ((st2 (maybe-commit st1)))  ; solo commits now; cluster waits for AERs
                        (drain! old)
                        (loop (compact st2) node-name elapsed))))))))))))))
