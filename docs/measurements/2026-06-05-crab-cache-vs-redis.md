# crab-cache vs Redis — measurements (2026-06-05)

> **⚠️ SUPERSEDED — read [2026-06-07-linux-fsync-vs-etcd.md](2026-06-07-linux-fsync-vs-etcd.md) instead.**
> The "durable SET 1.0× — even with Redis" result below is a **macOS fsync
> artifact** (`fsync()` isn't a true barrier on macOS; only `F_FULLFSYNC` is).
> On a real-fsync host the durable picture differs, and crab-cache has since
> gained **group-commit** (durable SET ~6× etcd, ~3× behind Redis) and a
> **native fused GET** (121k rps @ -P1, beats Redis pipelined). This doc is kept
> for the correctness/linearizability results only.

crab-cache is a sharded, RocksDB-durable, Raft-replicated, RESP-compatible
cache **written in CrabScheme**. These are fair head-to-head numbers against
real Redis, plus the linearizability result under failover.

## Setup & fairness

- **Host:** Darwin 25.2.0, arm64, 10 cores. Both servers on the same machine.
- **Driver:** the same `redis-benchmark` binary (8.8.0) speaks RESP to both.
- **Params:** `-c 50 -P 1`, default value size, 3 repeats, mean reported.
- **Durability matched per regime, both regimes published** (NFR-13/14/15):
  - *Relaxed:* crab-cache async RocksDB WAL ↔ Redis `appendonly no`.
  - *Full:* crab-cache `--durable` (fsync every write) ↔ Redis `appendfsync always`.
- crab-cache routes **every write through Raft** (propose → commit → apply);
  reads are served by the shard leader.

## Head-to-head throughput

### Relaxed durability

| cmd  | crab-cache rps (p50) | Redis rps (p50) | Redis is |
|------|----------------------|-----------------|----------|
| SET  | 6,995  (6.98 ms)     | 193,568 (0.14 ms) | 27.7× faster |
| GET  | 11,881 (4.34 ms)     | 187,529 (0.14 ms) | 15.8× faster |
| INCR | 6,104  (7.78 ms)     | 169,595 (0.14 ms) | 27.8× faster |

This is the honest raw-speed gap: a Scheme interpreter + a Raft round-trip per
write vs. hand-optimised C serving from memory.

### Matched full durability (the comparison that matters)

| cmd  | crab-cache rps (p50) | Redis rps (p50) | Redis is |
|------|----------------------|-----------------|----------|
| SET  | 6,131  (7.81 ms)     | 6,123  (7.94 ms) | **1.0× — even** |
| GET  | 11,089 (4.57 ms)     | 174,309 (0.14 ms) | 15.7× faster |
| INCR | 5,903  (7.90 ms)     | 6,229  (7.95 ms) | **1.1× — even** |

**The headline: when durability is actually required, crab-cache matches Redis
on writes.** Forcing Redis to fsync every write (`appendfsync always`) makes the
fsync dominate, and crab-cache's Raft + interpreter overhead disappears under
it — SET is dead even (1.0×) and INCR within 1.1×. GET stays Redis-favoured
because reads are never fsync-bound for Redis, while crab-cache serves reads
through the Raft leader.

## Sharding scale (relaxed, single host)

| shards | SET rps | GET rps |
|--------|---------|---------|
| 1      | 6,266   | 11,147  |
| 3      | 6,363   | 12,031  |
| 6      | 7,135   | 12,312  |

Modest positive scaling on a single host (the shards share the same box and a
single benchmark client spreads keys across them). The design intent — linear
scaling across *nodes* — is what Phase 6's multi-node cluster provides;
single-host multi-shard mostly buys parallel apply, not more cores.

## Correctness under failure

- **AS-3 failover** (`bench/cluster.sh failover`, green ≥4/4): a 3-node cluster,
  kill the leader of a key's shard, the surviving quorum re-elects, writes
  resume, and the pre-kill acked write survives — **no acked-write loss**.
- **AS-4 crash-recovery** (`bench/crash-recovery.sh`): 10,000 fsynced SETs,
  `kill -9`, restart → all present, exact count (no phantom). 50k = same × time.
- **Restart-rejoin** (`bench/cluster.sh rejoin`): a downed node restarts,
  reopens RocksDB, and catches up the writes it missed via Raft re-replication.

### Linearizability under induced failover (`bench/linearizability.sh`)

A counter is a register with a read-modify-write; if INCR is linearized, every
acked INCR returns a unique value, the acked values are a gap-free prefix of
the counter, and the final value never undercounts the acks. We drove 6
concurrent INCR clients on one key and killed that shard's leader mid-run:

| run | leader killed | acked | distinct | duplicates | final |
|-----|---------------|-------|----------|------------|-------|
| 1   | node c        | 675   | 675      | **0**      | 675   |
| 2   | node b        | 596   | 596      | **0**      | 596   |

**Every acked update got a unique value with no gaps and no double-counts
across the failover — a passing linearizability witness.** (Attempts that
errored during the brief leader-down window simply weren't acked — at-least-once,
as expected; none were silently lost or duplicated.)

## Honest verdict

crab-cache does **not** beat Redis on raw throughput, and it isn't meant to
(NG2). What the numbers show:

1. **At matched durability, write throughput is on par with Redis** — the case
   where you'd actually run a durable cache.
2. **Correctness holds under failure** — linearizable counter under failover,
   no acked-write loss, crash recovery, node rejoin.
3. All of the above is **cache logic written in Scheme** over a thin Rust
   substrate (see `docs/milestones/crab-cache-exit.md`).

Reproduce: `bash bench/{vs-redis,linearizability,cluster,crash-recovery}.sh`.
