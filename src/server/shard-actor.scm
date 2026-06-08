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
(include "src/resp.scm")                  ; resp-parse: decode the EXEC-TXN blob
(include "src/commands/transaction.scm")
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
   '("SET" "SETNX" "GETSET" "APPEND" "INCR" "DECR" "INCRBY" "DECRBY" "MSET" "CAS"
     "DEL" "UNLINK" "EXPIRE" "PEXPIRE" "PERSIST" "RENAME" "TICK"
     "HSET" "HSETNX" "HMSET" "HDEL" "HINCRBY"
     "LPUSH" "RPUSH" "LPUSHX" "RPUSHX" "LPOP" "RPOP" "LSET" "LREM" "LTRIM"
     "SADD" "SREM" "SPOP" "ZADD" "ZREM" "ZINCRBY" "FLUSHALL" "FLUSHDB"
     "EXEC-TXN")))

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

    ; ---- group-commit ack gate (durable mode) ----
    ;
    ; In durable mode a write's RocksDB ops land immediately (sync=#f) but the
    ; fsync is amortised: replies are buffered and `flush-base` remembers the
    ; `applied` value BEFORE the first deferred write, so one drain!(flush-base)
    ; later acks the whole batch in index order. CRITICAL: a waiter is NEVER
    ; acked until ctx-flush! (the single fsync) has returned for its write.
    ; Cap the batch so a non-stop write stream still bounds ack latency/memory.
    (define FLUSH-CAP 256)
    ; fsync the batch, then ack every buffered waiter from `base`.
    (define (flush-and-drain! base)
      (ctx-flush! ctx)
      (if base (drain! base) (set! acc '())))
    ; Decide what to do after applying entries (`old` = applied-before):
    ;   leader + durable + writes buffered -> defer (return earliest flush-base),
    ;                                or flush+ack now if the batch hit FLUSH-CAP;
    ;   else (follower, relaxed, or nothing written) -> ack now (drain inline,
    ;        exactly as before — a follower has no client waiters in `pending`,
    ;        so this is a no-op that just resets `acc`; its writes are still in
    ;        RocksDB (sync=#f) and get fsync'd on the next tick).
    ; Only the leader holds client waiters, so only it group-commits; this keeps
    ; the follower catch-up path identical to the pre-change behavior.
    ; Returns the new flush-base (#f = nothing deferred).
    (define (settle! leader? old flush-base)
      (cond
        ((and leader? (ctx-dirty? ctx))
         (let ((base (if flush-base flush-base old)))
           (if (>= (ctx-dirty-count ctx) FLUSH-CAP)
               (begin (flush-and-drain! base) #f)
               base)))
        ; Not deferring: a follower / relaxed / nothing-written, OR a leader that
        ; just stepped down with a batch still deferred. If a batch WAS deferred
        ; (flush-base set), fsync + ack it at flush-base — NOT `old` (this step's
        ; applied-before), which would mis-index the buffered replies and strand
        ; the waiters (HOLE 2). Otherwise drain inline at `old`. Nothing is
        ; deferred afterward, so return #f.
        (else
         (if flush-base (flush-and-drain! flush-base) (drain! old))
         #f)))
    ; solo log compaction (RocksDB is the snapshot); no-op for multi-voter.
    ; NEVER compact while acks are deferred: `pending` is keyed by absolute log
    ; index and compaction resets the log to 0, which would collide the next
    ; proposal's index with an undrained one. flush-base = #f means every
    ; applied entry's ack has been drained, so compaction is safe then.
    (define (compact st)
      (if (and solo (= (raft-applied st) (log-len st)))
          (aset* st (list 'log '() 'commit 0 'applied 0))
          st))
    (define (maybe-compact st flush-base)
      (if flush-base st (compact st)))
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
      ; `flush-base` (#f = no deferred acks) carries the group-commit ack gate:
      ; when set, durable writes are buffered awaiting their batch fsync. While
      ; deferred, poll the mailbox non-blocking — an empty mailbox flushes the
      ; batch (one fsync) and acks all waiters at once (opportunistic, lowest
      ; latency under light load); a steady stream keeps batching until a tick
      ; or FLUSH-CAP. Relaxed mode never sets flush-base, so it always blocks
      ; and acks inline exactly as before.
      (let loop ((st stI) (leader ldr0) (elapsed 0) (flush-base #f))
        (let ((m (if flush-base (raw-receive 0) (raw-receive))))
          (cond
            ;; mailbox empty while acks are pending -> flush the batch + ack now
            ((eq? m '*timeout*)
             (flush-and-drain! flush-base)
             (loop (maybe-compact st #f) leader elapsed #f))
            ((not (pair? m)) (loop st leader elapsed flush-base))

            ;; ---- Raft RPC from a peer ----
            ((eq? (car m) 'engine)
             (let* ((from (cadr m)) (rpc (caddr m))
                    (old (raft-applied st))
                    (r (raft-step st from rpc)) (st2 (car r)))
               ; HOLE 1 fix: a FOLLOWER fsyncs its applied writes (one flush)
               ; BEFORE emitting the AppendEntries reply. The AER success means
               ; "durably stored", so the leader may commit+ack a client only
               ; once a quorum has truly fsync'd. The leader itself keeps
               ; deferring (group-commit) via settle! below. No-op on solo
               ; (always leader) so the single-node fast path is unchanged.
               (if (and (not (raft-leader? st2)) (ctx-dirty? ctx)) (ctx-flush! ctx))
               (emit! (cdr r))
               ; defer (leader, has waiters) or ack inline (follower) the applied entries
               (let ((nb (settle! (raft-leader? st2) old flush-base)))
                 (let* ((ae? (eq? (car rpc) 'ae))
                        (ldr (cond ((raft-leader? st2) node-name) (ae? from) (else leader)))
                        (el  (if ae? 0 elapsed)))
                   (publish! st2 ldr)
                   (loop (maybe-compact st2 nb) ldr el nb)))))

            ;; ---- heartbeat / election tick ----
            ;; Bound durable-write ack latency to one tick: fsync any buffered
            ;; writes (leader batch AND a follower's applied-but-unsynced
            ;; entries) and ack any pending batch before doing Raft tick work.
            ;; flush-and-drain! is a no-op when nothing is dirty / deferred.
            ((eq? (car m) 'tick)
             (flush-and-drain! flush-base)
             (cond
               ((raft-leader? st)
                (let ((r (raft-tick st)))
                  (emit! (cdr r))
                  ; safe to compact: we just flushed+drained (pending is empty)
                  (loop (maybe-compact (car r) #f) node-name 0 #f)))
               (solo (loop (maybe-compact st #f) leader elapsed #f))
               ((>= elapsed timeout)
                (let* ((r (raft-campaign st)) (st2 (car r))
                       (ldr (if (raft-leader? st2) node-name #f)))
                  (emit! (cdr r))
                  (publish! st2 ldr)
                  (loop st2 ldr 0 #f)))
               (else (loop st leader (+ elapsed 1) #f))))

            ;; ---- per-node admin, served directly regardless of role ----
            ;; (DBSIZE/FLUSHALL/KEYS are per-node in cluster mode). FLUSHALL/
            ;; FLUSHDB mutate, so dispatch first (it dirties on top of any
            ;; pending batch), then ONE fsync makes both the batch and the
            ;; admin write durable, drain the batch, and only then ack the
            ;; admin caller. (Reply is a separate channel from `pending`.)
            ((eq? (car m) 'direct)
             (let ((reply (shard-dispatch ctx (caddr m))))
               (flush-and-drain! flush-base)
               (send (cadr m) reply)
               (loop (maybe-compact st #f) leader elapsed #f)))

            ;; ---- SAVE: snapshot this shard's RocksDB via a checkpoint ----
            ;; Flush + ack any pending batch first so the snapshot is taken on a
            ;; durable, fsync'd state.
            ((eq? (car m) 'checkpoint)
             (flush-and-drain! flush-base)
             (set! snap-n (+ snap-n 1))
             (send (cadr m)
                   (guard (e (#t (r-err "ERR checkpoint failed")))
                     (store-checkpoint handle (string-append db-path "-snap" (number->string snap-n)))
                     (r-ok)))
             (loop (maybe-compact st #f) leader elapsed #f))

            ;; ---- local client command: (conn-pid . cmd) ----
            (else
             (let* ((conn (car m)) (cmd (cdr m))
                    (name (string-upcase (utf8->string (car cmd)))))
               (cond
                 ((not (raft-leader? st))
                  (send conn (r-err "TRYAGAIN shard not leader")) (loop st leader elapsed flush-base))
                 ((not (write-cmd? name))
                  ; read on the leader: serve straight from applied state
                  (send conn (shard-dispatch ctx cmd)) (loop st leader elapsed flush-base))
                 (else
                  (let ((old (raft-applied st)) (idx (+ 1 (log-len st))))
                    (hashtable-set! pending idx conn)
                    (let* ((r (raft-propose st cmd)) (st1 (car r)))
                      (emit! (cdr r))                 ; AE to followers (cluster) / none (solo)
                      (let* ((st2 (maybe-commit st1)) ; solo commits now; cluster waits for AERs
                             ; this branch only runs on the leader -> #t
                             (nb (settle! #t old flush-base)))  ; defer ack (durable) or ack now
                        (loop (maybe-compact st2 nb) node-name elapsed nb))))))))))))))
