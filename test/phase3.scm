; test/phase3.scm — Phase 3 master test: load the cache core + every
; command module + every per-type test suite, run them against a fresh
; RocksDB-backed shard, and assert. This is the Phase 3 DoD gate.
;
; Run from the repo root:
;   rm -rf /tmp/cc-phase3-db
;   crabscheme run test/phase3.scm
; (requires a crabscheme built --features stdlib-store)

; --- cache core ---
(include "src/reply.scm")
(include "src/encoding.scm")
(include "src/store-ctx.scm")
(include "src/shard.scm")

; --- command modules (each registers its handlers at load) ---
(include "src/commands/string.scm")
(include "src/commands/keys.scm")

; --- test infra + suites ---
(include "test/harness.scm")
(include "test/string.scm")
(include "test/keys.scm")

(define H (store-open "/tmp/cc-phase3-db"))
(define ctx (make-ctx H))

(test-strings ctx)
(test-keys ctx)

(store-close H)
(done!)
