; server/shard-actor.scm — the shard-replica actor body (Phase 5 single-host;
; extended to cross-node in Phase 6).
;
; Loaded by spawn-source into its own runtime/thread:
;   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
;                 SHARD-ID VOTERS NODE-NAME DB-PATH)
;
; It owns ONE shard's RocksDB (its own DB, default CF) and ONE Raft group
; (vendored raft.scm). Client commands arrive on the actor MAILBOX as
; (conn-pid . cmd-list); the driver proposes each through Raft and, on commit,
; applies it to RocksDB and replies to the originating conn-actor.
;
; THE COMMIT->ACK BRIDGE (design §5), done in Scheme without touching raft.scm:
;   - the Raft LOG holds only the sendable command (no PIDs — they can't cross
;     nodes), so followers replay it identically;
;   - the leader records pending[log-index] = conn-pid at propose time;
;   - apply-fn pushes each command's reply onto `acc` in apply order;
;   - after every transition, replies for indices (oldApplied+1 .. newApplied)
;     are matched to pending[index] in order and sent to the waiting conn.
; Followers have an empty pending table, so they apply (converge RocksDB) and
; simply drop the replies. 1-voter groups (Phase 5) commit immediately via an
; explicit maybe-commit; multi-voter groups (Phase 6) commit when AERs arrive.

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

; Solo-group log compaction. raft.scm appends to an in-memory list (O(n) per
; append, unbounded) — fatal under load. For a 1-voter group there are no
; followers needing the log, and every applied entry is already durable in
; RocksDB (the store IS the snapshot), so once the log is fully applied we
; can reset it to empty (and indices to 0). Bounds the log to the in-flight
; window — O(1) appends, flat memory. Multi-voter groups (Phase 6) keep the
; log for replication and rely on bounded-volume tests until real snapshots.
(define (compact-solo st)
  (if (and (null? (aget st 'peers))
           (= (aget st 'applied) (log-len st)))
      (aset* st (list 'log '() 'commit 0 'applied 0))
      st))

; shard-key : string ("0".."N-1"), the routing/table key
; voters     : list of node-name symbols in this shard's Raft group
; node-name  : this replica's Raft id = this node's name (symbol)
; db-path    : this shard's own RocksDB directory
(define (shard-main shard-key voters node-name db-path)
  (let* ((handle (store-open db-path))
         (ctx (make-ctx handle))
         (pending (make-eqv-hashtable))     ; log-index -> conn-pid
         (acc '()))                          ; replies, newest-first (apply order)
    (define (apply-fn sm cmd)
      (set! acc (cons (shard-dispatch ctx cmd) acc))
      (+ sm 1))
    ; map the in-order applied replies to the indices that produced them.
    (define (drain! old-applied)
      (let loop ((k 0) (rs (reverse acc)))     ; oldest-applied first
        (if (pair? rs)
            (let* ((idx (+ old-applied 1 k))
                   (conn (hashtable-ref pending idx #f)))
              (if conn (begin (send conn (car rs)) (hashtable-delete! pending idx)))
              (loop (+ k 1) (cdr rs)))))
      (set! acc '()))
    ; advertise this replica's pid + role so conn-actors can route to a leader.
    (define (publish! st)
      (table-insert! 'cc-shard-pid shard-key (self))
      (table-insert! 'cc-shard-role shard-key (raft-role st)))
    (let ((st0 (car (raft-campaign (make-raft node-name voters apply-fn 0)))))
      (publish! st0)
      (let loop ((st st0))
        (let ((m (raw-receive)))
          (if (not (pair? m))
              (loop st)
              (let ((conn (car m)) (cmd (cdr m)))
                (cond
                  ((not (raft-leader? st))
                   ; this node lost leadership — tell the conn to retry/redirect
                   (send conn (r-err "TRYAGAIN shard not leader"))
                   (loop st))
                  (else
                   (let ((old-applied (raft-applied st))
                         (idx (+ 1 (log-len st))))
                     (hashtable-set! pending idx conn)
                     (let* ((st1 (car (raft-propose st cmd)))
                            (st2 (maybe-commit st1)))      ; 1-voter: commits now
                       (drain! old-applied)
                       (loop (compact-solo st2)))))))))))))
