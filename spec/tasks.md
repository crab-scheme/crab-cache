# crab-cache — Tasks & build plan

> **STATUS: ✅ COMPLETE — all phases (0–10) shipped.** Every DoD gate passed:
> Phase 3 306-check semantics, AS-1 RESP conformance, AS-3 failover (no acked-write
> loss), AS-4 crash-recovery, restart-rejoin, cross-node pub/sub, and a passing
> linearizability check under failover. Head-to-head numbers + effectiveness
> argument: `docs/measurements/2026-06-05-crab-cache-vs-redis.md` and
> `docs/milestones/crab-cache-exit.md`. Deferred hardening (multi-voter Raft-log
> snapshots, atomic applied-index for non-idempotent replay) is noted in the exit
> report.

> Phased, verifiable work breakdown. Each task has an explicit **verify** gate
> (per the "goal-driven execution" rule). The right column assigns a **model
> tier** for the multi-agent build team. Read `requirements.md` + `design.md` first.

## Team composition (different agent models)

| Tier | Model | Used for |
|------|-------|----------|
| **Architect/Correctness** | **Opus** | Consensus durability (Phase 2), commit bridge, linearizability harness, consistency/sharding review, cross-node failure logic, final perf analysis. |
| **Implementer** | **Sonnet** | `cs-store` crate, Scheme command semantics, RESP codec, slotmap, cluster bootstrap, benchmark harness — the bulk. |
| **Scaffolder** | **Haiku** | Feature-flag wiring (the ~8 mechanical sites), config files, boilerplate command handlers, doc/table formatting, conformance-test fixtures. |

**Language mandate (binds every tier):** per requirements **§0**, the cache is
written in **CrabScheme, not Rust**. The Rust tiers touch *only* substrate — the
`cs-store` RocksDB binding, the `cs-consensus` core + durable-log extension, and
runtime primitives. Command semantics, RESP protocol, sharding, expiry, pub/sub,
and cluster orchestration are **Scheme** (Sonnet/Haiku Scheme tasks). Any agent
that finds itself writing cache behavior in Rust must stop — that is a deviation
requiring explicit approval, not a shortcut.

**Orchestration:** once **PR #114 lands cs-consensus on `main`** (in flight,
auto-merge armed), the build runs phase-by-phase. Within a phase, independent
tasks fan out in parallel; phase boundaries are barriers gated by the phase's
**Definition of Done**. Adversarial verification (a second Opus agent trying to
break correctness claims) gates Phases 2, 6, 8, 9.

Legend: `[ ]` todo · `[~]` in progress · `[x]` done · `→ verify:` gate · `(Tier)`

---

## Phase 0 — Prerequisites & environment
- [~] **0.1** Land `cs-consensus` on `main`. → verify: PR #114 merged, `main` builds. *(in flight — auto-merge armed)* (Architect)
- [ ] **0.2** Install bench tooling: `brew install redis memcached memtier_benchmark`. → verify: `redis-server --version`, `redis-benchmark --version`, `memtier_benchmark --version` all resolve. (Scaffolder)
- [ ] **0.3** Add `rocksdb` to `[workspace.dependencies]` (pin a version; native-only). → verify: `cargo metadata` resolves; a throwaway `cargo build -p cs-store` (Phase 1) compiles the `-sys` lib once. (Scaffolder)
- [x] **0.4** Create the public app repo **`crab-scheme/crab-cache`** (this repo: Scheme app + bench + docs + spec). → verify: repo exists, README + spec pushed. (Architect)
- [ ] **0.5** In the **crabscheme monorepo**, branch `feat/cs-store` off the post-#114 `main` for the substrate (Phases 1–2, 4.1). → verify: branch exists, `cargo build --workspace` green. (Architect)

**DoD:** consensus on main; bench tools present; rocksdb resolvable; branch ready.

---

## Phase 1 — `cs-store` (RocksDB FFI host-procedure crate)
- [ ] **1.1** Scaffold `crates/cs-store` (`procs()` factory, fixnum-slab `Registry` in `OnceLock<Mutex<…>>`), mirroring `cs-stdlib-tls`. (Scaffolder)
- [ ] **1.2** Implement core procs: `store-open/close/cf-create/get/put/delete/multi-get`. (Sonnet)
- [ ] **1.3** Implement `store-write-batch` (atomic, `sync?` flag), `store-iter/iter-next` (prefix scan), `store-checkpoint`, `store-flush`. (Sonnet)
- [ ] **1.4** Bytevector marshaling via `cs_ffi::marshal`; panic-safety relies on `UntypedProc::call`'s `catch_unwind`. (Sonnet)
- [ ] **1.5** Wire the ~8 sites: root `Cargo.toml` member + `[workspace.dependencies]`; `cs-runtime/Cargo.toml` `dep:cs-store` + `stdlib-store` feature; `register_stdlib()` block ~`lib.rs:2040`; `stdlib` umbrella; `cs-cli` feature; **exclude from `wasm-stdlib`**. (Scaffolder)
- [ ] **1.6** Rust unit tests + a Scheme smoke test (`(store-open …)`/put/get/iterate/batch/checkpoint). → verify: `cargo test -p cs-store` green; `crabscheme -e '(store-put …)'` round-trips; **wasm build still green** (`cargo build --target wasm32-wasip1 -p cs-cli --no-default-features`). (Sonnet)

**DoD:** Scheme can durably put/get/scan/batch/checkpoint via RocksDB; wasm unaffected.

---

## Phase 2 — `cs-consensus` durability + commit bridge (net-new Rust, correctness-critical)
- [ ] **2.1** Define `RaftLogStore` trait; refactor `RaftNode` to use it; keep `MemLogStore` (`Vec<Entry>`) for Sim/tests (all 20 existing tests still pass). → verify: `cargo test -p cs-consensus` 20/20. (Opus)
- [ ] **2.2** `RocksLogStore` (impl over `cs-store` CFs `rlog`/`rhard`/`rsnap`), synced writes; **persist-before-ack** ordering in the driver. (Opus)
- [ ] **2.3** `node.restore_from(store)` recovery: reload hardstate+snapshot+log tail, set commit/applied, replay into RocksDB SM idempotently (resume from `meta.applied_index`). (Opus)
- [ ] **2.4** `RaftDriver` commit-notification: `pending: Index→(ActorPid,ReqId)`; emit `('applied req-id result)` on apply; `('redirect|'retry)` on overwrite/leadership-loss. (Opus)
- [ ] **2.5** Tests: crash-recovery (drop store, reopen, assert no acked loss), persist-before-ack ordering, notify-on-commit. → verify: new tests green; **adversarial Opus review** finds no lost-write window. (Opus)

**DoD:** Raft log+state durable across process restart with a correct notify path; adversarial review passes.

---

## Phase 3 — Single-node cache core in Scheme (no cluster, no RESP yet)
- [ ] **3.1** `encoding.scm`: type-tagged RocksDB key/value schema (string/hash/list/set/zset) incl. orderable-float zset scores + order-preserving list seqs (design §7). (Sonnet)
- [ ] **3.2** `commands/string.scm` + `keys.scm`: GET/SET(+EX/PX/NX/XX/KEEPTTL)/INCR/DECR/APPEND/MGET/MSET/DEL/EXISTS/TYPE/EXPIRE/TTL/PERSIST. (Sonnet)
- [ ] **3.3** `commands/hash.scm`, `list.scm`. (Sonnet)
- [ ] **3.4** `commands/set.scm`, `zset.scm`. (Sonnet)
- [ ] **3.5** Logical-clock TTL + `active-expiry` actor; lazy-expire on read. (Opus — determinism-sensitive)
- [ ] **3.6** `shard.scm` shard-owner actor: command → deterministic op-bytes → (Phase 5 wires Raft; here apply directly to a local single-shard RocksSM). → verify: a Scheme test suite exercises every command's semantics vs expected values; matches Redis semantics on a fixture set. (Sonnet)

**DoD:** every in-scope command behaves correctly against RocksDB on one shard, driven by Scheme tests (no network yet).

---

## Phase 4 — RESP frontend (single-node, redis-benchmark-able)
- [ ] **4.1** **Verify/extend** client TCP primitives (`tcp-listen`/`tcp-accept`/`tcp-read`/`tcp-write`) in `cs-stdlib-net`; add a shim if missing (OQ-1). → verify: a Scheme echo server accepts a `nc`/`redis-cli PING`. (Opus if a native shim is needed, else Sonnet)
- [ ] **4.2** `resp.scm`: RESP2 parse/serialize over bytevectors (Scheme-first per DD-2); pipelining. (Sonnet)
- [ ] **4.3** `resp-listener` + per-socket `conn-actor` (spawn-activation), dispatch to shard-owner, serialize replies. (Sonnet)
- [ ] **4.4** Server/admin commands: PING/ECHO/SELECT 0/INFO/DBSIZE/FLUSHALL/COMMAND(docs subset). (Haiku)
- [ ] **4.5** Conformance: `redis-cli` interactive AS-1 passes. → verify: AS-1 scenario green. (Sonnet)
- [ ] **4.6** **Measure** `redis-benchmark -t set,get,incr,lpush,hset` on one node. The RESP codec **stays Scheme** (§0 mandate); a Rust *tokenizer* accelerator is built **only** if measurement proves the tokenizer dominates **and** it is explicitly approved (documented deviation; semantics remain Scheme). → verify: benchmark runs clean, no protocol errors; codec decision + any approved deviation recorded. (Opus measures)

**DoD:** a single node speaks RESP well enough that `redis-cli` and `redis-benchmark` run unmodified; codec placement decided by data.

---

## Phase 5 — Sharding & multi-Raft (single host, multiple shards)
- [ ] **5.1** `slotmap.scm`: CRC16 keyslot + `{hashtag}`; slot→shard→owner map from config. → verify: `CLUSTER KEYSLOT k` matches Redis for a fixture key set. (Sonnet)
- [ ] **5.2** Per-shard `RaftNode` + `raft-driver` actor; shard-owner proposes through Raft; conn-actor awaits `('applied …)` (design §5). (Opus — wires consistency)
- [ ] **5.3** `-MOVED`/`-ASK`/`-CROSSSLOT` replies; `CLUSTER SLOTS`/`NODES`/`INFO`. (Sonnet)
- [ ] **5.4** Linearizable reads via ReadIndex; default-on. → verify: a write-then-read across shards always observes the write; multi-shard command suite green. (Opus)

**DoD:** one process hosting N shards routes by slot, every write linearizable through its shard's Raft group, redirects correct.

---

## Phase 6 — Cross-node distribution (real cluster)
- [ ] **6.1** `node.scm` bootstrap from config; `node-listen`/`node-connect` peers; `peer-poller` drains `node-poll` → routes frames to raft-drivers by shard. (Opus)
- [ ] **6.2** Raft inter-node messages over `node-send` (codec already in cs-consensus); R-replica shards across nodes. (Opus)
- [ ] **6.3** Leader failover → client redirect/retry; refresh slot-map leader hints from Raft `leader()`. → verify: AS-3 (kill leader under load, recover, linearizable, zero loss) passes; **adversarial Opus review** of the failure paths. (Opus)
- [ ] **6.4** (Stretch ST1) online membership via `propose_conf` + slot migration. (Opus)

**DoD:** a real 3-node, 3-shard×3-replica cluster serves RESP, survives a leader kill with no acked-write loss, redirects clients correctly.

---

## Phase 7 — Pub/Sub
- [ ] **7.1** `pubsub-broker` actor: channel/pattern → subscriber conn-pids; SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE; purge on conn-actor DOWN. (Sonnet)
- [ ] **7.2** Cluster-wide `PUBLISH` fan-out via `node-send` to peer brokers (best-effort, non-Raft). → verify: cross-node publish reaches a subscriber on another node; `PUBSUB CHANNELS/NUMSUB` correct. (Sonnet)

**DoD:** pub/sub works within and across nodes, matching Redis's global (non-slot) semantics.

---

## Phase 8 — Durability & crash-recovery hardening
- [ ] **8.1** Snapshot/compaction wired end-to-end (checkpoint-based for large shards). (Opus)
- [ ] **8.2** Crash-recovery harness: 50k acked SETs, `kill -9` at random points (leader & follower), restart, verify all acked present, no phantom. → verify: AS-4 passes repeatedly. (Opus)
- [ ] **8.3** Restart-rejoin: a downed node rejoins, catches up via log/snapshot. → verify: data converges. (Opus)

**DoD:** AS-4 holds across repeated random kills; nodes recover and converge.

---

## Phase 9 — Benchmark, correctness proof, measurements
- [ ] **9.1** `bench/crab-cache/run.sh`: build, env-check, start crab-cache + matched Redis (both durability regimes), run redis-benchmark + memtier, collect JSON, render markdown (mirror `bench/aot-vs-runtimes.sh` conventions). (Sonnet)
- [ ] **9.2** Scenarios 1–4 (single head-to-head, sharding scale 1→3→6, failover-under-load, crash-recovery) with fairness controls NFR-13/14/15. (Sonnet + Opus for failover)
- [ ] **9.3** Linearizability checker over a recorded concurrent+failover history (Porcupine/Knossos-style). → verify: history linearizable per key under induced failover. (Opus)
- [ ] **9.4** `docs/measurements/<date>-crab-cache-vs-redis.md` — matched-durability ranked tables, p50/p99/p999, scaling curve, failover dip/recovery. (Sonnet; Haiku formats tables)

**DoD:** AS-2/AS-5 pass; published, fair head-to-head numbers + a passing linearizability result.

---

## Phase 10 — Effectiveness argument & exit
- [ ] **10.1** LOC accounting: Scheme vs Rust (and which Rust is reused vs net-new). (Haiku)
- [ ] **10.2** `docs/milestones/crab-cache-exit.md`: what each primitive (actors/supervisors/Raft/FFI) bought; what was awkward; the **honest** perf verdict (within-Nx of Redis, correctness/durability achieved); LOC ratio as the headline language-effectiveness datum. (Opus)
- [ ] **10.3** Spec status → done; update `MEMORY.md` pointer. (Architect)

**DoD (project):** AS-6 satisfied — a sharded, RocksDB-durable, Raft-replicated,
RESP-compatible cache **written mostly in Scheme**, benchmarked fairly vs Redis,
with an exit report quantifying how much Scheme / how little net-new Rust it took.

---

## Critical path & parallelism
```
0 ──▶ 1 ──▶ 2 ──▶ 3 ──▶ 4 ──▶ 5 ──▶ 6 ──▶ 8 ──▶ 9 ──▶ 10
            │           │                  ▲      ▲
            └─ (2 ∥ 3, 2∥4 partially)      7 ─────┘ (7 after 6)
```
- Phases **2** and **3** can overlap (3 uses a direct single-shard SM until 5
  wires Raft). **7** (pub/sub) only needs cross-node transport (after **6**).
- Hard barriers (adversarial-verified): **2, 6, 8, 9**.

## Risks carried from requirements/design
- **C4/OQ-1** client TCP accept primitive — resolved first thing in Phase 4.
- **C5** RocksDB first-build cost — paid once in Phase 0/1; keep `stdlib-store`
  optional so default/wasm builds don't regress.
- **C6/DD-2** RESP codec placement — decided by Phase-4 measurement, not up front.
- **NG2** perf expectation — we trail Redis; the proof is correctness + durability
  + distribution + expressiveness at competitive speed, reported honestly.
