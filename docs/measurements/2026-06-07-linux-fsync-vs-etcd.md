# crab-cache vs etcd vs Redis — real-fsync (Linux) measurements (2026-06-07)

> **UPDATE (later the same day) — the two levers identified below were SHIPPED.**
> Same host, same A/B method (one crabscheme binary, crab-cache main vs the perf
> branch):
>
> | workload | before | after | vs peers |
> |---|---|---|---|
> | **Durable SET** (group-commit, PR crab-cache#6 / crabscheme#126) | ~688 rps (p50 74ms) | **~5,852 rps** (p50 7.6ms) | **~6× etcd** (956); ~3× behind Redis (~18k) — was ~34× behind |
> | **GET @ -P1** (native fused GET, PR crab-cache#7 / crabscheme#127) | 41,468 rps | **121,139 rps** | ~1.8× behind Redis (214k) — was ~5.3× |
> | **GET @ -P16** | 39,828 (flat) | **1,519,757 rps** | **beats Redis pipelined** |
> | **GET @ -P32** | 27,905 | **2,840,909 rps** | — |
>
> Durable: a deferred-fsync group commit (one `store-flush-wal` per batch/tick).
> GET: a native `conn-serve-gets` builtin that parses+slot-hashes+looks-up+frames
> the leading run of locally-led GET hits in one Rust call. Crash-recovery (AS-4)
> + cluster failover (AS-3, no acked-write loss) + conformance (AS-1, GET
> value-correct) all PASS. **The tables below are the PRE-improvement baseline**
> (the problem + the levers).

This run **corrects the durable-write claim** in
[2026-06-05-crab-cache-vs-redis.md](2026-06-05-crab-cache-vs-redis.md). That doc
(measured on macOS) reported durable SET as **"1.0× — even"** with Redis. That was
an **fsync artifact**: macOS `fsync()` is not a true durability barrier (only
`F_FULLFSYNC` is), so on macOS every "durable" write is effectively free and the
comparison flatters every fsync-per-write design. Re-run on a host where `fsync()`
actually persists, the picture is very different.

It also adds **etcd** — crab-cache's true architectural peer (Raft log + per-write
fsync), where Redis (group-commit AOF) is the speed ceiling rather than the peer.

## Setup & fairness

- **Host:** Linux 6.17.8 (OrbStack VM), aarch64, 10 cores, single host. `fsync()`
  is a real barrier here (~6.7 ms, measured via etcd's sequential `check perf`).
- **Driver:** the same `redis-benchmark` speaks RESP to crab-cache and Redis.
  etcd is measured with its own `etcdctl check perf --load=m` (gRPC) — no RESP
  driver can hit it, and its concurrency differs from `-c 50`, so **treat etcd as
  an order-of-magnitude reference, not a same-tool number.**
- **Params:** `-c 50`, 256 B values. Durable runs `N=8000` (they're slow);
  relaxed/read runs `N=50000`/`100000`.
- **Durability matched per regime:** crab-cache `--durable yes` (RocksDB WAL
  fsync per write) ↔ Redis `appendfsync always` ↔ etcd (Raft fsync). Relaxed:
  crab-cache `--durable no` ↔ Redis `appendonly no`.
- Reproduce: [`bench/vs-etcd.sh`](../../bench/vs-etcd.sh) (built `crabscheme` +
  `etcd`/`etcdctl` + `redis-*` on a real-fsync host). Numbers below are one run;
  the durable Redis figure is concurrency/co-tenancy sensitive and ranged
  ~9.7k–17k across runs.

## Durable writes — per-write fsync (the comparison that needs a real barrier)

| system | SET rps (p50) | mechanism |
|---|---|---|
| crab-cache `--durable yes` | **688** (67.6 ms) | RocksDB WAL fsync/write, **no group-commit** |
| etcd (Raft + fsync) | **952** writes/s | batches Raft log entries per fsync |
| Redis `appendfsync always` | **9,732** (4.4 ms) | **group-commit AOF** |

**Headline:** on a real-fsync host, crab-cache durable writes are in **etcd's order
of magnitude** (both ~hundreds–1k/s; both Raft-style per-write fsync) but **~15–30×
behind Redis**. Redis wins because it group-commits the AOF — one `fsync` per
event-loop tick amortised across all concurrent writers — while crab-cache and
etcd pay per-write durability. (The macOS "1.0× even" was the fsync artifact; the
real gap is here.) Absolute rates are low for everyone because the OrbStack VM's
virtualised `fsync` is slow (~6.7 ms); real server NVMe lifts all three, but the
*relative* ordering holds.

## crab-cache durable SET vs shard count

| shards | durable SET rps | p50 |
|---|---|---|
| 3 | 644 | 71.8 ms |
| 6 | 312 | 110 ms |
| 12 | 226 | 160 ms |

Sharding does **not** help durable throughput — it makes it **worse**. That rules
out per-shard fsync serialization and points to a **single global fsync chokepoint**
on the durability path: more shards add coordination/contention on that one
barrier rather than parallelising it. **The actionable lever is group-commit /
batched fsync in crab-cache's WAL** — the single change that would close most of
the durable gap to Redis and pull ahead of etcd.

## Relaxed writes — no per-write fsync

| system | SET rps (p50) |
|---|---|
| crab-cache `--durable no` | 10,277 (4.5 ms) |
| Redis `appendonly no` | 204,918 (0.14 ms) |

## Reads (GET)

| system | GET rps (p50) |
|---|---|
| crab-cache | 29,325 (0.87 ms) |
| Redis | 205,338 (0.14 ms) |

Relaxed writes (~20×) and reads (~5–7×) match the macOS numbers — those gaps are
**platform-independent** (the interpreted actor pipeline: RESP parse → route →
serve, plus green per-op + VM dispatch), not fsync. Only the *durable* regime is
fsync-sensitive, and only the durable regime changed from the macOS run.

## Takeaways

1. **Durability comparisons must be made on a real-fsync host.** The macOS
   "even with Redis" result was a measurement artifact; corrected here.
2. **crab-cache durable ≈ etcd's order of magnitude, ~15–30× behind Redis** — a
   reasonable place for a Raft-style per-write-fsync design, with one clear gap:
   **no group-commit.**
3. **Group-commit WAL is the highest-value durable-write optimization.**
4. Reads/relaxed are unchanged and platform-independent.

(This eval was unblocked by the crabscheme arm64-Linux NaN-box fix
— crabscheme/crab-scheme#124 — without which `crabscheme run` SIGSEGVs on
arm64 Linux and crab-cache cannot start there.)
