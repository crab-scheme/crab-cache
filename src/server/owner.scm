; server/owner.scm — the shard-owner actor body.
;
; Loaded into its own per-actor runtime (own OS thread) via
;   (spawn-source "(include \"src/server/owner.scm\")" 'owner store-handle)
; It owns the shard's RocksDB handle (a fixnum that indexes the process-
; global store registry, valid across runtimes) and serializes every
; command on the shard — the single-writer discipline Redis gets from being
; single-threaded per shard. In Phase 3/4 it applies directly to RocksDB;
; Phase 5 routes each mutation through this shard's Raft group instead.
;
; It pulls in the whole cache: every command module registers its handlers
; into the dispatch table at load, then `shard-dispatch` routes by name.

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

(define (owner handle)
  (let ((ctx (make-ctx handle "default")))
    (let loop ()
      (let ((m (raw-receive)))
        (if (pair? m)
            (send (car m) (shard-dispatch ctx (cdr m))))
        (loop)))))
