# crab-cache

**A distributed, Redis-compatible cache written in [CrabScheme](https://github.com/crab-scheme/crabscheme).**

Sharded multi-Raft consensus, RocksDB-durable on every node, speaking the Redis
RESP wire protocol so stock `redis-benchmark` / `memtier_benchmark` can drive it
head-to-head against Redis itself.

> **Status: ✅ complete (Phases 0–10).** Single-node and multi-node clusters run;
> `redis-cli` / `redis-benchmark` drive it unmodified. Failover (AS-3),
> crash-recovery (AS-4), restart-rejoin, pub/sub, and a linearizability check
> under failover all pass. Honest head-to-head numbers (vs Redis **and etcd**, on
> a real-fsync host) are in
> [`docs/measurements/`](docs/measurements/2026-06-07-linux-fsync-vs-etcd.md);
> the effectiveness argument is in
> [`docs/milestones/crab-cache-exit.md`](docs/milestones/crab-cache-exit.md).
>
> **Headline:** the cache is **~3,100 lines of CrabScheme** over an **~870-line
> RocksDB FFI binding** (the only net-new Rust with no cache semantics). On a
> real-fsync host, after group-commit + a native fused GET path:
> **durable SET ~5.9k rps — ~6× etcd, ~3× behind Redis** (was ~34× behind), and
> **GET 121k rps @ -P1** (from ~5× behind toward parity) that **beats Redis when
> pipelined** (~1.5M @ -P16) — a Raft+fsync cache in Scheme vs hand-tuned C.

### Run it

```sh
# build the runtime once (RocksDB-backed), in the crabscheme repo:
cargo build -p cs-cli --features stdlib-store --release
CC=…/crabscheme/target/release/crabscheme

# single node, 3 shards
$CC run src/node.scm -- --port 6400 --db /tmp/cc --shards 3
redis-cli -p 6400 set foo bar && redis-cli -p 6400 get foo

# clusters + the gates
bash bench/cluster.sh failover      # AS-3: kill leader, no acked-write loss
bash bench/cluster.sh rejoin        # downed node restarts + converges
bash bench/cluster.sh pubsub        # cross-node pub/sub
bash bench/crash-recovery.sh        # AS-4: kill -9, all acked SETs survive
bash bench/linearizability.sh       # linearizable counter under failover
bash bench/vs-redis.sh              # head-to-head throughput vs Redis
```

---

## Why this exists

crab-cache is a **showcase for the CrabScheme language**. The thesis:

> CrabScheme's actors, supervisors, consensus stack, and native-FFI story are
> expressive and fast enough to build a **correct, durable, horizontally-sharded,
> wire-compatible** distributed cache with a **small amount of Scheme**, and to do
> so **competitively** with a mature C system.

We weren't out to beat Redis on raw throughput — Redis is hand-tuned C — yet
after group-commit + a native fused GET, crab-cache **leads etcd ~6× on durable
writes** and **beats Redis on pipelined GET** (~1.5M rps @ -P16), while staying
within a small factor on single-op latency. The proof is *correctness + durability
+ distribution + expressiveness at competitive (and in places leading) speed*,
reported honestly with matched-durability benchmarks.

## The one rule: the cache is CrabScheme, not Rust

This is the whole point. Everything that decides *how the cache behaves* is
written in Scheme:

| Written in **CrabScheme** — *this repo* | Native substrate in **Rust** — *the [crabscheme](https://github.com/crab-scheme/crabscheme) repo* |
|---|---|
| RESP2 protocol parse + serialize | `cs-store` — a thin RocksDB FFI binding |
| All command semantics (strings/hashes/lists/sets/zsets, keys, TTL, INCR…) | `cs-consensus` — the Raft core + its durable-log extension |
| Sharding (CRC16 slots, `MOVED`/`ASK`), expiry, pub/sub | runtime primitives (TCP accept, bytevectors) — already in the stdlib |
| Cluster orchestration, failover routing, the actor topology | |

If a line encodes a decision about cache behavior, it is **Scheme**. Net-new Rust
is minimized and counted in the [effectiveness report](docs/) — a large Rust
line-count is a project failure, not an optimization.

## Architecture (one paragraph)

A client speaks RESP to a node; a per-connection Scheme actor decodes the
command; the router maps its key to a slot → shard; the shard-owner actor
proposes the mutation to that shard's **Raft group**; Raft commits it across
replicas (each persisting log + data to **RocksDB**); on commit the driver
replies to the shard-owner via plain actor messaging, which answers the client.
**All orchestration is Scheme; storage and the consensus core are Rust behind
FFI.** See [`spec/design.md`](spec/design.md) for the full picture.

## Running it (planned)

```sh
# Needs a crabscheme built with the RocksDB substrate:
#   cargo install --path crates/cs-cli --features stdlib-store   (in the crabscheme repo)

crabscheme --features stdlib-store run src/node.scm -- --config conf/node-1.toml
# ... start node-2, node-3; they discover each other and form the cluster

redis-cli -p 7000 SET hello world
redis-cli -p 7000 GET hello
```

## Benchmarking it (planned)

```sh
bench/run.sh        # boots a crab-cache cluster + a matched Redis, runs
                    # redis-benchmark / memtier, emits docs/measurements/*.md
```

## Repository layout

```
crab-cache/
├── spec/        # requirements.md · design.md · tasks.md  (the full spec)
├── src/         # the CrabScheme cache (RESP, commands/, sharding, pubsub, cluster, node)
├── conf/        # cluster node configs
├── bench/       # benchmark harness + scenarios (redis-benchmark / memtier)
├── test/        # Scheme conformance tests
└── docs/        # measurements vs Redis + the language-effectiveness report
```

## Relationship to CrabScheme

crab-cache runs *on* the [`crabscheme`](https://github.com/crab-scheme/crabscheme)
runtime and depends on two native modules that live there (the RocksDB binding
and the Raft engine). Everything else — the cache — is in this repo, in Scheme.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), matching
the CrabScheme project.
