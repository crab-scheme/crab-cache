# crab-cache — Requirements

> A distributed, durable, Redis-compatible cache built **in CrabScheme** — cache
> logic as actors + primitives, RocksDB per node via FFI, replication and
> consistency via our homegrown Raft. Purpose: **prove the effectiveness of the
> language** by building a real, non-trivial distributed system with it and
> benchmarking it honestly against the reference implementation (Redis).

- **Working name:** `crab-cache` (storage crate: `cs-store`; node program: Scheme).
- **Status:** spec / not started.
- **Branch base:** must include `cs-consensus` (see Constraint C1).
- **Target platform:** native only (darwin-aarch64 dev, linux-x86_64 CI). Never WASM.

---

## 0. Implementation language mandate (NON-NEGOTIABLE)

**The cache is written in CrabScheme, not Rust.** This is the entire point: the
artifact is evidence about *the language*, so the cache's behavior must live in
Scheme. Rust appears only as the thinnest possible native **substrate** reached
through FFI, and every line of it is either pre-existing or a minimal binding to
a library we are deliberately not re-implementing.

| Written in **CrabScheme** — *this is the cache* | Allowed in **Rust** — *substrate only* |
|---|---|
| RESP2 protocol parse + serialize | `cs-store`: a thin RocksDB FFI binding (open/get/put/delete/iter/batch/checkpoint) |
| **All** command semantics (strings/hashes/lists/sets/zsets, keys, INCR/EXPIRE, …) | `cs-consensus`: the Raft **core** (already exists — PR #114) + its durable-log extension |
| Sharding policy, CRC16 keyslot, slot map, `MOVED`/`ASK`/`CROSSSLOT` | Low-level runtime primitives that are language substrate (TCP accept, bytevectors) — already shipped in the stdlib |
| Expiry policy (logical-clock TTL) + the active-expiry actor | — |
| Pub/sub broker + cluster-wide fan-out | — |
| Cluster bootstrap, membership orchestration, failover routing | — |
| The connection / shard-owner / raft-driver **actor topology** | — |

**Rule of thumb:** if a line encodes a *decision about how the cache behaves*, it
is **Scheme**. Rust is permitted only to (a) bind an external library we are not
re-implementing (RocksDB), (b) carry the consensus core we already built, and
(c) expose runtime substrate primitives. **No command handler, no protocol logic,
no routing / sharding / expiry / pub-sub logic is written in Rust.**

Net-new Rust is minimized and **counted in the exit report** (NFR-10): a large
net-new Rust line-count is a *spec failure*, not an optimization. If performance
ever appears to demand moving cache logic into Rust, that is a **documented
deviation requiring explicit approval** — never a default.

---

## 0.5 Repository topology (two repos)

This project deliberately spans two repositories, which *reinforces* the §0
mandate — the referenceable repo is almost entirely CrabScheme:

- **`crab-scheme/crab-cache`** *(this repo)* — the **CrabScheme application**: the
  cache itself (`src/*.scm`: RESP, command semantics, sharding, expiry, pub/sub,
  cluster orchestration, the actor topology), the benchmark harness (`bench/`),
  conformance tests (`test/`), measurements + the effectiveness report (`docs/`),
  and this spec (`spec/`). Runs on a `crabscheme` built with `--features stdlib-store`.
- **`crab-scheme/crabscheme`** *(the language monorepo)* — the **native substrate
  only**: `cs-store` (RocksDB FFI host-procedure crate) and the `cs-consensus`
  durable-log extension land there as language-level crates/stdlib (where the
  other `cs-stdlib-*` modules and the Raft core already live). Shipped in the
  `crabscheme` runtime; this repo depends on it as the execution engine.

Net-new Rust therefore lives in the monorepo and is small and bounded; **this
repo carries no cache logic in Rust** (ideally no Rust at all beyond build glue).

---

## 1. Purpose & framing (what "prove the language" means here)

This is a **showcase**, not a Redis replacement. The thesis we are testing:

> CrabScheme's concurrency model (actors + supervisors), its consensus stack
> (Raft over cs-net), and its native-FFI story (host-procedure crates) are
> expressive and fast enough to build a **correct, durable, horizontally-sharded,
> wire-compatible** distributed cache with a **small amount of Scheme** glue, and
> to do so **competitively** with a mature C system.

We therefore optimize the deliverable for **three kinds of evidence**, in priority
order:

1. **Correctness under failure** — linearizable per shard; no acknowledged write
   lost across crash/restart and leader failover. (The hardest thing to fake;
   the strongest proof.)
2. **Expressiveness** — the cache *semantics* (command logic, sharding policy,
   cluster orchestration, pub/sub fan-out) live in **Scheme**, measured in lines
   of Scheme vs lines of supporting Rust. The story is "we wrote a distributed
   database in a Lisp."
3. **Competitive performance** — head-to-head throughput/latency vs Redis on
   identical hardware **with comparable durability settings**. We expect to be
   *within a small multiple* of Redis, not to beat it; we will report this
   candidly (see §6, Non-goal NG2).

A benchmark that "wins" by being less durable or less consistent than Redis is
not evidence. Fairness controls (§5.7) are part of the requirements.

---

## 2. Scope

### In scope
- **RESP2** wire protocol over TCP (the protocol `redis-cli`, `redis-benchmark`,
  and `memtier_benchmark` speak). RESP3 is a stretch (§7, ST3).
- **Redis Cluster** slot model: 16384 hash slots, `CRC16(key) mod 16384`,
  `{hashtag}` support, `MOVED`/`ASK` redirection, `CLUSTER SLOTS`/`CLUSTER NODES`.
- **Data types & commands** (near-full surface — see §4):
  strings, hashes, lists, sets, sorted-sets, plus key admin (`DEL`/`EXPIRE`/`TTL`/
  `EXISTS`/`TYPE`/`SCAN`), `INCR`/`DECR` family, and **pub/sub**.
- **Per-key TTL / expiration** (lazy + active).
- **Sharded multi-Raft**: slots are grouped into shards; each shard is a Raft
  group replicated across R nodes; writes are linearizable through Raft; reads
  are linearizable via ReadIndex (with an opt-in fast-local-read mode).
- **Full durability**: each node persists its shard data **and its Raft log /
  hard-state** to RocksDB; an acknowledged write survives `kill -9` + restart.
- **Cluster membership**: static bootstrap (config file) for v1; online
  add/remove node and slot rebalancing via Raft joint-consensus as a stretch.
- **A benchmark suite** (§5) producing a published measurements doc + an exit
  report making the language-effectiveness argument.

### Out of scope (v1)
- Redis persistence formats (RDB/AOF file compatibility), `WAIT`, Lua scripting
  (`EVAL`), Streams, Geo, HyperLogLog, bitmaps, `CLIENT`/`ACL`, replication of
  the Redis-replica (`REPLICAOF`) kind (we replicate via Raft, not Redis repl).
- Multi-key transactions (`MULTI`/`EXEC`) across slots; cross-slot atomicity.
- TLS to clients (inter-node mTLS/QUIC is available; client-facing TLS is a
  stretch, §7 ST4).
- Eviction policies (`maxmemory`/LRU) — we are a durable store, not memory-bounded.

---

## 3. Definitions

| Term | Meaning |
|------|---------|
| **Node** | One OS process running the `crab-cache` Scheme program; owns a RocksDB instance and participates in ≥1 Raft group. |
| **Slot** | One of 16384 hash buckets, Redis-compatible (`CRC16 mod 16384`). |
| **Shard** | A contiguous-or-not set of slots replicated as one Raft group. |
| **Replica** | A node holding a copy of a shard (one Raft member). |
| **Raft group** | The consensus instance for one shard (election/replication/commit). |
| **State machine (SM)** | The deterministic shard data + apply logic behind one Raft group; RocksDB-backed. |
| **Command actor** | A Scheme `spawn-activation` actor that decodes a client command and drives the shard. |
| **Connection actor** | A Scheme actor owning one client TCP socket (RESP read/write loop). |

---

## 4. Functional requirements

### FR-1 RESP protocol
- FR-1.1 Accept TCP connections speaking **RESP2**; parse inline + multibulk
  command frames; emit RESP2 replies (simple string, error, integer, bulk
  string, array, null).
- FR-1.2 Support **pipelining** (multiple requests buffered before replies).
- FR-1.3 `redis-cli` connects, runs interactive commands, and `redis-cli -c`
  (cluster mode) follows `MOVED`/`ASK` redirects correctly.
- FR-1.4 `redis-benchmark` and `memtier_benchmark` run their default suites
  unmodified against a single node and against the cluster.

### FR-2 Command surface (semantics in Scheme)
Grouped by type; **bold** = must pass head-to-head bench. All others = conformance.

- **Strings:** **`GET` `SET`** (with `EX`/`PX`/`NX`/`XX`/`KEEPTTL`), `SETEX`,
  `GETSET`, `APPEND`, `STRLEN`, **`MGET` `MSET`**, **`INCR` `DECR`** `INCRBY`
  `DECRBY` `INCRBYFLOAT`.
- **Keys:** **`DEL`** `UNLINK` **`EXISTS`** **`EXPIRE`** `PEXPIRE` `EXPIREAT`
  **`TTL`** `PTTL` `PERSIST` `TYPE` `KEYS` (pattern) `SCAN` (cursor) `RENAME`.
- **Hashes:** **`HSET`** `HSETNX` **`HGET`** `HMGET` `HGETALL` `HDEL` `HEXISTS`
  `HLEN` `HKEYS` `HVALS` `HINCRBY` `HSCAN`.
- **Lists:** **`LPUSH` `RPUSH`** **`LPOP` `RPOP`** `LRANGE` `LLEN` `LINDEX`
  `LSET` `LREM` `LTRIM`.
- **Sets:** `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` `SPOP` `SUNION`
  `SINTER` `SDIFF` `SSCAN`.
- **Sorted sets:** `ZADD` `ZREM` `ZSCORE` `ZINCRBY` `ZRANGE` (+`WITHSCORES`,
  `REV`, `BYSCORE`) `ZRANK` `ZCARD` `ZCOUNT` `ZRANGEBYSCORE`.
- **Pub/Sub:** `SUBSCRIBE` `UNSUBSCRIBE` `PSUBSCRIBE` `PUNSUBSCRIBE` `PUBLISH`
  (cluster-wide fan-out), `PUBSUB CHANNELS/NUMSUB`.
- **Server/cluster:** `PING` `ECHO` `SELECT`(0 only) `COMMAND`(docs subset)
  `INFO` `DBSIZE` `FLUSHALL` `CLUSTER SLOTS` `CLUSTER NODES` `CLUSTER INFO`
  `CLUSTER KEYSLOT` `CLUSTER MYID`.

### FR-3 Sharding & routing
- FR-3.1 Key→slot via Redis-compatible `CRC16 mod 16384`, honoring `{hashtag}`.
- FR-3.2 A static slot→shard map (config) for v1; each shard = one Raft group.
- FR-3.3 A command whose key-slot is not owned by the receiving node returns
  `-MOVED <slot> <host:port>` (or `-ASK` during migration).
- FR-3.4 Multi-key commands spanning >1 slot return `-CROSSSLOT` (Redis behavior).

### FR-4 Consistency
- FR-4.1 **Writes** are linearizable: a command mutating a key is `propose`d to
  the owning shard's Raft group and **acked only after it is committed and
  applied** on a quorum.
- FR-4.2 **Reads** default to linearizable via Raft **ReadIndex**.
- FR-4.3 An opt-in per-connection `READONLY`/fast-read mode allows local
  (possibly-stale) reads from a replica for throughput; off by default.
- FR-4.4 The Raft commit→client-ack bridge is **actor-native**: the command
  actor receives a reply message on commit (no exposed Rust futures).

### FR-5 Durability
- FR-5.1 Each node persists shard **data** in RocksDB column families.
- FR-5.2 Each node persists the **Raft log + HardState (term/vote/commit) +
  latest snapshot** in RocksDB, with synced writes, **before** acknowledging the
  corresponding `AppendEntries` / before `propose` returns success.
- FR-5.3 On restart a node reloads log + hard-state + snapshot, rejoins its
  Raft groups, and replays to last-applied — **no acknowledged write is lost.**
- FR-5.4 Snapshots compact the log; restore reconstructs SM state from a snapshot
  + tail entries.

### FR-6 Expiration
- FR-6.1 Per-key TTL stored alongside the value; expired keys are invisible to
  reads (lazy expiration) and reclaimed by a background **active-expiry actor**.
- FR-6.2 Expiration is **deterministic across replicas** — expiry is driven by a
  logical clock advanced through the Raft log, *not* each replica's wall clock,
  so all replicas expire a key at the same log position (no divergence).

### FR-7 Cluster lifecycle
- FR-7.1 Bootstrap a cluster from a config file (nodes, addresses, slot map,
  replication factor) and a `crabscheme run crab-cache-node.scm -- --config …`
  invocation per node.
- FR-7.2 Leader failover within a shard is automatic; clients are redirected or
  retried transparently by standard Redis cluster clients.
- FR-7.3 (Stretch) online `CLUSTER ADDSLOTS`-style rebalancing via joint consensus.

### FR-8 Observability
- FR-8.1 `INFO` exposes role, shard/slot ownership, Raft term/commit/applied,
  RocksDB stats, connected clients, ops counters.
- FR-8.2 Structured logs per node; a `--metrics` line-protocol dump for the bench.

---

## 5. Non-functional requirements & the benchmark (the "proof")

### 5.1 Correctness
- NFR-1 **Linearizability:** a concurrent-history checker (Knossos/Porcupine-style,
  run offline over a recorded op log) finds the history linearizable per key,
  **including under induced leader kills** during the run.
- NFR-2 **Crash-recovery:** a test harness issues N acked writes, `kill -9`s the
  leader (and separately a follower) at random points, restarts, and asserts
  every acked write is readable and no phantom write appears.

### 5.2 Performance targets (honest, not aspirational-beats-Redis)
- NFR-3 Single-node `GET`/`SET` throughput **within 5× of Redis** at matched
  durability (Redis `appendfsync always` vs our synced Raft+RocksDB), measured by
  `redis-benchmark`. (5× is the *bar to clear*; we publish whatever we get.)
- NFR-4 Cluster throughput **scales** with shard count — show the curve from
  1→3→6 shards; near-linear for independent-key workloads is the success shape.
- NFR-5 Tail latency reported as **p50/p99/p999**, not just mean.

### 5.3 Workloads
- NFR-6 `redis-benchmark` default suite (PING, SET, GET, INCR, LPUSH, RPUSH,
  LPOP, RPOP, SADD, HSET, SPOP, ZADD, LRANGE_*, MSET) at multiple pipeline depths
  and client counts.
- NFR-7 `memtier_benchmark` mixed read/write (e.g. 1:10 and 1:1) with realistic
  key/value size distributions.
- NFR-8 A **failover-under-load** run: sustained `memtier` while a leader is
  killed; report the throughput dip + recovery time, and confirm zero data loss.

### 5.4 Comparison targets
- crab-cache single node vs **Redis** single node.
- crab-cache N-shard cluster vs **Redis Cluster** (same node count) and vs a
  single Redis (to show the sharding win).
- (Optional) vs `memcached` for the pure-GET/SET non-durable point of reference.

### 5.5 Reporting
- NFR-9 A measurements doc `docs/measurements/<date>-crab-cache-vs-redis.md`
  mirroring the format of `docs/measurements/2026-06-03-aot-vs-runtimes.md`
  (setup/versions/methodology/per-command ranked tables, both durability regimes).
- NFR-10 An exit report `docs/milestones/crab-cache-exit.md` with the
  **language-effectiveness argument**: LOC Scheme vs Rust, what each primitive
  (actors, supervisors, Raft, FFI) bought us, what was awkward, and the honest
  perf verdict.

### 5.6 Environment
- NFR-11 `redis-server`, `redis-cli`, `redis-benchmark`, `memcached` installed
  (`brew install redis memcached`); `memtier_benchmark` (`brew install memtier_benchmark`).
- NFR-12 `hyperfine` (present) for process-level timings; the networked load is
  driven by the redis tools, not hyperfine.

### 5.7 Fairness controls (mandatory — part of the proof)
- NFR-13 Same machine, same core allocation, warm caches, ≥3 repeats, report
  variance.
- NFR-14 **Matched durability:** compare against Redis configured to the closest
  durability (`appendonly yes`, `appendfsync always` for the synced regime;
  `everysec` for a relaxed regime) — and publish *both* regimes so the durability
  cost is explicit.
- NFR-15 Same value sizes, same key cardinality, same pipeline depth per row.

---

## 6. Goals & Non-goals

| # | Goal |
|---|------|
| G1 | **The cache is written in CrabScheme, not Rust** (§0): all command semantics, RESP protocol, sharding, pub/sub, expiry, and cluster orchestration are Scheme actors/primitives. Rust is substrate only. |
| G2 | RocksDB integrated **via FFI** as each node's storage engine (data + Raft log). |
| G3 | Replication & linearizable consistency via **our Raft** (`cs-consensus`), driven by actors over cs-net. |
| G4 | **Wire-compatible** enough that stock Redis tooling benchmarks it head-to-head. |
| G5 | A **published, fair** benchmark + an exit report arguing language effectiveness. |

| # | Non-goal |
|---|----------|
| NG1 | Full Redis feature parity (scripting, streams, RDB/AOF files, ACLs). |
| NG2 | **Beating Redis on raw throughput.** Redis is hand-tuned C; we expect to trail. The proof is *correctness + durability + distribution + expressiveness at competitive (within-Nx) speed*, stated honestly. |
| NG3 | A production-supported product. This is a milestone artifact. |

---

## 7. Stretch goals
- ST1 Online slot rebalancing / live node add-remove via joint consensus.
- ST2 Fast-local replica reads (`READONLY`) measured as a separate throughput line.
- ST3 RESP3 (`HELLO 3`, push frames for pub/sub).
- ST4 Client-facing TLS.
- ST5 A native **Scheme** load-driver (in addition to redis-benchmark) to also
  showcase the client side in-language.
- ST6 Inter-node QUIC transport benchmark vs TCP/mTLS (we already have QUIC).

---

## 8. Constraints, risks, prerequisites

- **C1 (prerequisite): `cs-consensus` is not on `main`.** It lives on
  `feat/sdk-consensus` (Rust) + `feat/sdk-consensus-scheme` (Scheme libs).
  Work must base on a branch that brings these in (rebase/merge the
  "M06 done, not PR'd" crates). *First task in tasks.md.*
- **C2: Raft today is in-memory** (`Vec<Entry>`) with **no commit callback**.
  FR-4.4 + FR-5.2 require extending `cs-consensus` with (a) a pluggable
  RocksDB-backed log/hard-state store and (b) an index→reply bridge. This is the
  largest net-new Rust effort. (See design.md §6–7.)
- **C3: `!Send` heap.** Scheme `Value` is `Rc`-based → not `Send`. Actors must be
  `spawn-activation`/`spawn-source` (source-text + sendable args), never closures.
  Cross-node messaging uses the proven `node-send`/`node-poll` distrib builtins,
  **not** transparent `(send remote-pid …)` (that bridge is unwired). Affects the
  cluster transport design.
- **C4: Client-facing TCP accept.** The RESP listener needs a raw
  `tcp-listen`/`tcp-accept` primitive for *client* sockets (distinct from cs-net's
  inter-node framed channels). Must verify `cs-stdlib-net` provides it; if not,
  add a small native accept shim (`cs-store`/new builtin). *Risk — verify in
  Phase 4; task included.*
- **C5: RocksDB build cost.** `rust-rocksdb` vendors & compiles the C++ lib on
  first build (multi-minute, needs `cc`/`clang` — present). Keep `cs-store`
  feature-gated (`stdlib-store`) so default builds don't pay it unless enabled.
  Exclude from `wasm-stdlib` (RocksDB cannot target wasm).
- **C6: RESP-codec placement** is a measured decision (design.md §5): Scheme-first
  (max language proof) with an optional Rust accelerator if Phase-4 numbers show
  the parser dominates. Not re-litigated up front; revisited with data.

---

## 9. Acceptance scenarios

- **AS-1 (compat):** `redis-cli` against a single node: `SET k v` → `OK`,
  `GET k` → `v`, `INCR n` ×3 → `3`, `LPUSH l a b c` + `LRANGE l 0 -1` → `c b a`,
  `EXPIRE k 100` + `TTL k` → `~100`. `TYPE`, `DEL`, `EXISTS` correct.
- **AS-2 (bench runs):** `redis-benchmark -t set,get,incr,lpush,hset -n 100000`
  completes against both a single node and `-c` cluster mode with no protocol
  errors; numbers recorded.
- **AS-3 (linearizable failover):** under concurrent `memtier` load, `kill` the
  leader of a shard; clients recover; the recorded history is linearizable;
  zero acked writes lost.
- **AS-4 (crash-recovery):** issue 50k acked `SET`s, `kill -9` a node mid-stream,
  restart; every acked key is present with correct value; no phantom keys.
- **AS-5 (sharding scales):** 1→3→6 shard cluster shows rising aggregate
  throughput on an independent-key `memtier` workload; documented curve.
- **AS-6 (the doc):** `docs/measurements/<date>-crab-cache-vs-redis.md` and
  `docs/milestones/crab-cache-exit.md` exist, with matched-durability head-to-head
  tables and the LOC-Scheme-vs-Rust effectiveness argument.

---

## 10. Success criteria (one-line)
> A stock `redis-benchmark`/`memtier_benchmark` drives a sharded, RocksDB-durable,
> Raft-replicated cache **written in CrabScheme** (Rust only as RocksDB +
> consensus substrate, per §0); it stays **linearizable and
> loses no acknowledged write** across `kill -9` and leader failover; and we
> publish a **fair, matched-durability** comparison vs Redis plus an exit report
> quantifying how little Rust and how much Scheme it took.
