# crab-cache — exit report

> A distributed, sharded, RocksDB-durable, Raft-replicated, Redis-wire-compatible
> cache **written in CrabScheme**, built to answer one question: *can this
> language carry a real systems workload?* This is the honest answer.

## What got built (Phases 0–9)

| Phase | Deliverable | Proof |
|-------|-------------|-------|
| 1–2 | `cs-store` RocksDB FFI + durable-log substrate | 9 + 24 tests |
| 3 | Single-node cache core: 5 Redis types, ~60 commands | **306-check** semantics gate vs Redis on RocksDB |
| 4 | RESP2 + TCP server | `redis-cli` AS-1 + `redis-benchmark` clean |
| 5 | Sharding + multi-Raft (CRC16/16384, per-shard groups) | keyslot 0-mismatch vs Redis; CROSSSLOT; CLUSTER |
| 6 | Cross-node cluster, failover | **AS-3**: kill leader, no acked-write loss, ≥4/4 |
| 7 | Pub/sub (local + cross-node) | channel/pattern delivery across nodes |
| 8 | Durability + crash-recovery | **AS-4** (kill -9, all present) + restart-rejoin |
| 9 | Head-to-head vs Redis + linearizability | matched-durability **even on writes**; linearizable under failover |

**AS-6 (project DoD) satisfied:** a sharded, RocksDB-durable, Raft-replicated,
RESP-compatible cache, mostly in Scheme, benchmarked fairly against Redis, with
a passing linearizability result.

## The headline number: how much Scheme, how little net-new Rust

| | lines |
|---|---|
| **Net-new cache logic — CrabScheme** (`src/`, minus vendored raft) | **3,086** |
| Consensus engine — CrabScheme (`raft.scm`, reused from the stdlib) | 275 |
| Tests — CrabScheme | 976 |
| **Net-new Rust used by the cache** — `cs-store` RocksDB FFI binding | 867 |
| Net-new Rust — transport tweaks (net unlock + `node-detect-disconnects`) | ~28 |

Every byte of cache *behavior* — the command semantics for strings/hashes/lists/
sets/zsets, the RESP2 codec, CRC16 sharding + routing + MOVED, the commit→ack
bridge, logical-clock TTL, pub/sub fan-out, cluster bootstrap, failover, durable
apply — is **Scheme**. Even **consensus is Scheme** (the 275-line `raft.scm`; the
Rust `cs-consensus` engine turned out not to be Scheme-callable, so we drove the
pure-Scheme Raft — a forced deviation from the design's DD-1 that landed *more*
aligned with the "code is Scheme" mandate). The **only** net-new Rust the cache
depends on is an ~870-line RocksDB FFI binding plus 28 lines of transport fixes.

**~3,100 lines of Scheme over ~900 lines of net-new Rust — and the Rust contains
no cache semantics whatsoever.**

## What each language primitive bought

- **`spawn-source` actors (source text + sendable args).** The whole server is
  actors: a listener, one conn-actor per socket, one shard-replica per shard, a
  peer-poller, a pub/sub broker, per-subscription pushers. Because a Scheme value
  is `Rc`-based (`!Send`), you can't ship a *closure* to an actor — you ship
  *source*, run in a fresh per-actor runtime, and pass sendable data. The
  unlock: the cs-store / cs-net registries are **process-global**, so a RocksDB
  or socket **fixnum handle crosses thread boundaries as an ordinary message
  argument**. That single fact is what made a multi-actor server tractable.
- **Mailboxes + `(send)`/`raw-receive`.** The commit→ack bridge is pure actor
  messaging: the shard-replica records `pending[log-index] = conn-pid`, and when
  an index commits+applies it sends the reply back — no Rust futures, no shared
  locks. The conn↔shard RPC and the broker↔pusher push path are both just sends.
- **`node-send`/`node-poll` (cs-net transport).** Cross-node Raft and cross-node
  PUBLISH ride the same node-name-addressed transport; a per-node peer-poller
  fans inbound frames to the right local actor. The pure-Scheme Raft engine's
  outputs `(peer . rpc)` map *directly* onto `node-send` — the in-process sim
  (`node-link!`) and real TCP run byte-identical code.
- **Process-global tables.** PIDs can't cross node boundaries, so actors publish
  their pids/roles/leaders into named tables that any local actor reads. This is
  how routing finds the leader for a slot and how the broker finds subscribers.
- **The Rust↔C++ FFI boundary (`cs-store`).** RocksDB lives behind opaque fixnum
  handles; the state machine *is* RocksDB, written with `sync=true` in durable
  mode. Durability and the byte-apply contract sit naturally at the FFI edge
  while all the interesting logic stays in Scheme.

## What was awkward (the honest part)

- **`!Send` everywhere.** No closures across threads/nodes shaped the entire
  design. It's a real constraint, but "ship source + sendable handles" is a
  workable discipline once internalised.
- **No `sleep` primitive.** Raft has no timers, so heartbeats/elections are
  driven off a busy-poll loop counting iterations + `(yield)`. It works and is
  deterministic (staggered, per-shard-rotated timeouts → no split votes), but a
  blocking timer would be cleaner and cheaper than spinning pollers.
- **Language sharp edges.** `define-record-type` needs explicit mutator names;
  bitwise-OR is `bitwise-or` (not `bitwise-ior`); hex literals ≥ 2^63 overflow
  the reader (order-preserving codecs became byte-level sign-bit flips); the
  `call`/`receive` prelude isn't auto-loaded (we hand-rolled the 2-line RPC).
- **Two substrate gaps we had to close.** `tcp-recv`/`tcp-send` held a global
  lock across the blocking syscall (fatal for a concurrent server) — fixed with
  clone-then-unlock. And `node-poll` doesn't prune dead peers, so peer-count
  never dropped from Scheme — we exposed `node-detect-disconnects` so the mesh
  can heal after a restart. Both are transport-only; neither is cache logic.
- **In-memory Raft log growth.** Solo (single-node) groups compact the log
  after apply (RocksDB *is* the snapshot) — flat memory. Multi-voter groups keep
  the log for replication and it's still unbounded under sustained load; a
  base-index log-trim with real `InstallSnapshot` is the remaining hardening.
- **Non-idempotent replay.** Crash recovery and cluster rejoin re-apply
  committed entries; SETs are idempotent so AS-4 / rejoin are correct, but INCR
  could double-apply on a re-replicate. The fix is an atomic
  `{mutation + applied-index}` write-batch.

## The honest perf verdict

crab-cache does **not** beat Redis on raw throughput and was never meant to (NG2).
The full numbers are in `docs/measurements/2026-06-05-crab-cache-vs-redis.md`:

- **Relaxed durability:** Redis is **16–28× faster** — Scheme interpreter + a
  Raft round-trip per write vs. hand-optimised C from memory.
- **Matched full durability (fsync per write):** **write throughput is on par
  with Redis** — SET 1.0×, INCR 1.1×. When you actually pay for durability, the
  fsync dominates and the language overhead disappears.
- **Correctness under failure holds:** linearizable counter under induced
  failover (zero lost/double-counted acked updates), no acked-write loss on
  leader kill, crash recovery, node rejoin-and-converge.

## Bottom line

A real distributed systems workload — RESP wire-compat, 5 data types, CRC16
sharding, multi-Raft replication, leader failover, pub/sub, durable crash
recovery, linearizability under fault — is **~3,100 lines of CrabScheme over an
~870-line RocksDB binding**, competitive with Redis at matched durability. The
language carried it. The awkward parts were real but bounded, and every one is a
named, fixable substrate gap rather than a wall.
