# crab-cache

**A distributed, Redis-compatible cache written in [CrabScheme](https://github.com/crab-scheme/crabscheme).**

Sharded multi-Raft consensus, RocksDB-durable on every node, speaking the Redis
RESP wire protocol so stock `redis-benchmark` / `memtier_benchmark` can drive it
head-to-head against Redis itself.

> **Status: 🚧 in active development.** The spec is complete (`spec/`); the
> implementation is gated on the `cs-consensus` crate landing in the language
> runtime ([crabscheme#114](https://github.com/crab-scheme/crab-scheme/pull/114)).

---

## Why this exists

crab-cache is a **showcase for the CrabScheme language**. The thesis:

> CrabScheme's actors, supervisors, consensus stack, and native-FFI story are
> expressive and fast enough to build a **correct, durable, horizontally-sharded,
> wire-compatible** distributed cache with a **small amount of Scheme**, and to do
> so **competitively** with a mature C system.

We are **not** trying to beat Redis on raw throughput — Redis is hand-tuned C.
The proof is *correctness + durability + distribution + expressiveness at
competitive (within-Nx) speed*, reported honestly with matched-durability
benchmarks.

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
