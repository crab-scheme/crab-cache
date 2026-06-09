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
         (read-q  '())                             ; ReadIndex: reads awaiting a round, (conn . cmd)
         (batch   '())                             ; ReadIndex: reads the open round is confirming
         (read-acks '())                           ; ReadIndex: peers acked (fresh) since this round opened
         (round-open? #f)                          ; ReadIndex: a confirmation heartbeat is in flight
         (round-rseq 0)                            ; ReadIndex: the rseq this round's acks must echo (>=)
         (solo    (null? (cdr voters)))            ; 1-voter group?
         ; Staggered election timeout, ROTATED by shard so leadership spreads:
         ; for shard S the voter at index S has the shortest timeout and tends
         ; to win it. Deterministic => no split votes, predictable failover.
         (timeout (+ 4 (* (modulo (- (index-of node-name voters)
                                     (let ((n (string->number shard-key))) (if n n 0)))
                                  (length voters))
                          3))))
    (define (apply-fn sm cmd)
      ; '() is the become-leader no-op barrier (§5.4.2): no state change, but it
      ; still contributes an acc slot so drain!'s positional index alignment holds.
      (set! acc (cons (if (null? cmd) #f (shard-dispatch ctx cmd)) acc))
      (+ sm 1))
    ; Persist the applied index (+ its term) into the SAME group-commit batch as
    ; the entry's mutations, so a restart restores base/applied/commit and never
    ; re-applies already-applied committed entries (idempotent recovery/rejoin —
    ; fixes the non-idempotent INCR double-apply).
    (define (persist-applied! st)
      (ctx-save-applied! ctx (raft-applied st) (entry-term st (raft-applied st))))
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

    ; On losing leadership, every conn still in `pending` is a write this node
    ; proposed as leader but never drained — its uncommitted tail is about to be
    ; (or was just) truncated by the new leader's AppendEntries. Reply TRYAGAIN so
    ; the client re-proposes against the real leader, and clear `pending` so the
    ; following drain! can't cross-wire a stale conn to a newly-applied entry's
    ; reply (the cc-idc / H1 false-ack: drain! walks `acc` positionally and would
    ; otherwise ack pending[A+1]=old_conn with the NEW leader's +OK, falsely
    ; acking a truncated write as :ok so the next read returns nil). `pending` is
    ; populated only on the leader's propose branch, so a non-empty `pending`
    ; means we led — and on solo (no peers) no `engine` RPC ever arrives, so this
    ; never fires there and the single-node path is unchanged.
    (define (fail-pending!)
      (vector-for-each
       (lambda (conn) (send conn (r-err "TRYAGAIN leadership changed")))
       (hashtable-values pending))
      (hashtable-clear! pending)
      ; Also abandon any in-flight ReadIndex reads (a round mid-confirmation +
      ; those still queued): we can no longer confirm leadership for them, so the
      ; client must retry against the real leader.
      (for-each (lambda (e) (send (car e) (r-err "TRYAGAIN leadership changed"))) batch)
      (for-each (lambda (e) (send (car e) (r-err "TRYAGAIN leadership changed"))) read-q)
      (set! batch '()) (set! read-q '()) (set! read-acks '()) (set! round-open? #f))

    ; ---- ReadIndex: linearizable GET (Raft §6.4) ----
    ; A GET is served only after a quorum heartbeat round confirms we are STILL the
    ; leader as of a point AFTER the read was issued — otherwise a just-deposed
    ; leader could serve a stale cc-str value (cc-idc). `batch` snapshots the reads
    ; a round confirms when it opens, so the quorum we then collect (read-acks,
    ; reset here) strictly follows them; reads arriving mid-round wait in read-q for
    ; the next round. Solo skips all this (served inline — see the read branch). On
    ; lost leadership fail-pending! TRYAGAINs both. The reply is read from current
    ; applied state, which (leadership confirmed, Leader Completeness) holds every
    ; committed write as of the read.
    ; open a round: snapshot the queued reads, BUMP rseq (so the round's heartbeat
    ; carries a fresh id), reset the ack set, emit the heartbeat. Returns the
    ; rseq-bumped st (the caller threads it so later AEs carry the new rseq).
    (define (start-read-round! st)
      (let ((st2 (aset st 'rseq (+ 1 (aget st 'rseq)))))
        (set! batch read-q) (set! read-q '()) (set! read-acks '()) (set! round-open? #t)
        (set! round-rseq (aget st2 'rseq))
        (emit! (cdr (broadcast-append st2)))         ; heartbeat tagged with the new rseq
        st2))
    (define (serve-batch! st)
      (for-each (lambda (e) (send (car e) (shard-dispatch ctx (cdr e)))) batch)
      (set! batch '()) (set! round-open? #f)
      (if (pair? read-q) (start-read-round! st) st)) ; reads that arrived mid-round -> next round
    ; an AER is a FRESH confirmation only if it is a success, at our current term,
    ; echoing rseq >= this round's rseq (i.e. a reply to the round's own heartbeat,
    ; not a stale in-flight/backlogged ack). Returns st (rseq-bumped if a new round
    ; opened for mid-round reads).
    (define (note-read-ack! from st ack-term ack-rseq success?)
      (if (and round-open? success? (= ack-term (raft-term st)) (>= ack-rseq round-rseq))
          (begin (set! read-acks (add-mem from read-acks))
                 (if (>= (+ 1 (length read-acks)) (majority st)) (serve-batch! st) st))
          st))

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
          ; snapshot the applied prefix into base (RocksDB IS the snapshot);
          ; keep commit/applied so the index grows monotonically and survives
          ; restart via the persisted applied-index.
          (aset* st (list 'base (raft-applied st)
                          'base-term (last-log-term st)
                          'log '()))
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

    (let* ((loaded (ctx-load-applied ctx))                 ; (idx . term) from RocksDB
           (p (car loaded)) (pt (cdr loaded))
           (st0 (make-raft node-name voters apply-fn 0))
           ; restart: RocksDB already reflects entries up to p, so start with the
           ; log compacted to base=p (applied=commit=p). The log replays only
           ; entries above p, so committed entries are never re-applied.
           (st0 (if (> p 0) (aset* st0 (list 'base p 'base-term pt 'applied p 'commit p)) st0))
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
             (let ((from (cadr m)) (rpc (caddr m)))
               (cond
                 ;; ---- PreVote request -> reply: grant iff OUR own election timer
                 ;; has expired (no live leader from our view), we don't lead, and
                 ;; the pre-candidate's log is at least as up-to-date as ours. Does
                 ;; NOT touch our term/role — the whole point of pre-vote.
                 ((eq? (car rpc) 'prv)
                  (let* ((cidx (list-ref rpc 3)) (clt (list-ref rpc 4))
                         (up (or (> clt (last-log-term st))
                                 (and (= clt (last-log-term st)) (>= cidx (log-len st)))))
                         (grant (and (not (raft-leader? st)) (>= elapsed timeout) up)))
                    (emit! (list (cons from (list 'prvr (raft-term st) grant))))
                    (loop st leader elapsed flush-base)))
                 ;; ---- PreVote reply -> tally; on a majority start the REAL
                 ;; election (raft-campaign bumps the term + sends RequestVote).
                 ((eq? (car rpc) 'prvr)
                  (if (and (eq? (raft-role st) 'pre-candidate) (list-ref rpc 2))
                      (let* ((pv (add-mem from (aget st 'pre-votes)))
                             (st2 (aset st 'pre-votes pv)))
                        (if (>= (length pv) (majority st))
                            (let* ((r (raft-campaign st2)) (st3 (car r))
                                   (ldr (if (raft-leader? st3) node-name #f)))
                              (emit! (cdr r)) (publish! st3 ldr) (loop st3 ldr 0 #f))
                            (loop st2 leader elapsed flush-base)))
                      (loop st leader elapsed flush-base)))
                 ;; ---- all other RPCs (rv / rvr / ae / aer): the normal Raft step
                 (else
                  (let* ((was-leader? (raft-leader? st))    ; role BEFORE this RPC steps us
                         (old (raft-applied st))
                         (r (raft-step st from rpc)) (st2 (car r)))
                    ; leader -> follower: abandon our in-flight proposals (TRYAGAIN +
                    ; clear pending) BEFORE any drain! can cross-wire them (cc-idc / H1).
                    (if (and was-leader? (not (raft-leader? st2))) (fail-pending!))
                    ; record the new applied-index in the batch BEFORE the fsync below
                    (if (> (raft-applied st2) old) (persist-applied! st2))
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
                      ; ReadIndex: an AER may be a fresh confirmation ack; note-read-ack!
                      ; counts it (success + current term + rseq >= round) and releases
                      ; the batch on quorum. Returns st (rseq-bumped if a round opened).
                      (let ((st3 (if (and (raft-leader? st2) (eq? (car rpc) 'aer))
                                     (note-read-ack! from st2 (list-ref rpc 1) (list-ref rpc 4) (list-ref rpc 2))
                                     st2)))
                        (let* ((ae? (eq? (car rpc) 'ae))
                               (ldr (cond ((raft-leader? st3) node-name) (ae? from) (else leader)))
                               (el  (if ae? 0 elapsed)))
                          (publish! st3 ldr)
                          (loop (maybe-compact st3 nb) ldr el nb)))))))))

            ;; ---- heartbeat / election tick ----
            ;; Bound durable-write ack latency to one tick: fsync any buffered
            ;; writes (leader batch AND a follower's applied-but-unsynced
            ;; entries) and ack any pending batch before doing Raft tick work.
            ;; flush-and-drain! is a no-op when nothing is dirty / deferred.
            ((eq? (car m) 'tick)
             (flush-and-drain! flush-base)
             (cond
               ((raft-leader? st)
                ; CheckQuorum: if we lost quorum contact this window, step down —
                ; abandon in-flight writes (fail-pending!) and republish with no
                ; leader so get-fast stops serving stale cc-str. Otherwise heartbeat.
                (let ((cq (raft-checkquorum st timeout)))
                  (if (raft-leader? cq)
                      (let ((r (raft-tick cq)))
                        (emit! (cdr r))
                        ; safe to compact: we just flushed+drained (pending is empty)
                        (loop (maybe-compact (car r) #f) node-name 0 #f))
                      (begin (fail-pending!) (publish! cq #f)
                             (loop cq #f 0 #f)))))
               (solo (loop (maybe-compact st #f) leader elapsed #f))
               ((>= elapsed timeout)
                ; PreVote round (NO term bump): become pre-candidate + solicit
                ; pre-votes. The real election (raft-campaign) starts only on a
                ; pre-vote majority — handled in the engine branch on `prvr`. Leave
                ; cc-shard-leader as-is so reads keep routing to the known leader.
                (let* ((r (raft-prevote st)) (st2 (car r)))
                  (emit! (cdr r))
                  (loop st2 leader 0 #f)))
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
                  ; READ. Solo: serve inline (always the sole leader — no quorum to
                  ; confirm, and a round would never complete). Multi-voter: ReadIndex
                  ; — enqueue + (if idle) open a confirmation round; the reply is sent
                  ; from serve-batch! once a quorum AER confirms we still lead.
                  (if solo
                      (begin (send conn (shard-dispatch ctx cmd))
                             (loop st leader elapsed flush-base))
                      (begin (set! read-q (cons (cons conn cmd) read-q))
                             (let ((st2 (if round-open? st (start-read-round! st))))
                               (loop st2 leader elapsed flush-base)))))
                 (else
                  (let ((old (raft-applied st)) (idx (+ 1 (log-len st))))
                    (hashtable-set! pending idx conn)
                    (let* ((r (raft-propose st cmd)) (st1 (car r)))
                      (emit! (cdr r))                 ; AE to followers (cluster) / none (solo)
                      (let ((st2 (maybe-commit st1)))  ; solo commits now; cluster waits for AERs
                        (if (> (raft-applied st2) old) (persist-applied! st2))
                        ; this branch only runs on the leader -> #t
                        (let ((nb (settle! #t old flush-base)))  ; defer ack (durable) or ack now
                          (loop (maybe-compact st2 nb) node-name elapsed nb)))))))))))))))
