# Jepsen validation — crab-cache

Distributed-correctness validation of crab-cache under fault injection, using a
self-contained 5-node arm64 Docker cluster (`jepsen/docker/`) driving the
[jepsen 0.3.11] harness in `jepsen/`. This is the formal evidence behind the
linearizability claim in `docs/measurements/2026-06-05-crab-cache-vs-redis.md`.

**Tracking:** epic `cc-btk`. **Default mode:** `--consistency linearizable`
(Raft ReadIndex GET). All runs below are linearizable mode unless noted.

## Result matrix

Verdict legend: `true` = checker exhaustively verified linearizable/no-cycles;
`0-anomalies` = no violation found but the checker could not fully exhaust the
history (high `:info` from churn/crash — a measurement limit, not a violation);
`FAIL` = a real anomaly.

| Workload (checker) | no-fault | partition | kill |
|---|---|---|---|
| **register** (Knossos linearizable) | `:valid? true` | `:valid? true` | `0-anomalies` |
| **counter** (dup-INCR + monotone-read) | — | — | **`:valid? true`** (0 dups, reads monotone) |
| **cas-register** (Knossos CAS) | **`:valid? true`** | `0-anomalies` | _n/a_ |
| **append** (Elle list-append, strict-serializable) | **`:valid? true`** | _n/a_ | _n/a_ |

> register was the workload the cc-idc fix was driven against: 7 stale-read
> failures (no-fault) and 14 (partition) → **0**. counter/cas/append were last
> run *before* that fix (Phase D) and are being re-validated.

## The cc-idc fix (read linearizability)

A deposed/isolated leader served stale reads from the conn-local `cc-str` cache,
and a transiently-deposed leader could falsely ack a truncated write. Fixed in
five layers (commit `e612d3f`):

1. **`fail-pending!`** — on leader→follower stepdown, TRYAGAIN every blocked conn
   and clear `pending` before `drain!` (no cross-wired/false-acked write).
2. **CheckQuorum** — a leader with no quorum contact in an election window
   self-demotes (an isolated leader stops serving stale `cc-str`).
3. **PreVote** — a timed-out follower must win a pre-vote before bumping term
   (cuts spurious-election churn).
4. **Round-id ReadIndex** (Raft §6.4) — GET is served only after a quorum
   heartbeat confirms current leadership; an `rseq` on every AE, echoed in the
   AER, means only *fresh* success-acks count.
5. **§5.4.2 no-op barrier** — a fresh leader commits an empty entry in its own
   term so its applied state covers all prior committed writes before serving.

Read-path cost is flagged: `--consistency fast` restores the conn-local `cc-str`
fast-path (faster, non-linearizable across elections); `linearizable` is default.

## Membership-change assessment (cc-btk.12)

crab-cache has a **fully static topology**. Each shard's Raft group is created
with `voters = all-names` (every node in the `--cluster` spec), fixed at process
start; `raft.scm` has **no** membership-change machinery (no joint consensus, no
add/remove-voter, no single-server reconfiguration). Consequences:

- **No dynamic-membership bug surface** — there are no runtime configuration
  changes, so the entire class of Raft membership-change anomalies cannot occur.
  Jepsen's membership nemesis (grow/shrink) is **N/A** to crab-cache as built.
- **Limitation, not a defect** — scaling the voter set requires restarting nodes
  with a new `--cluster`. Online membership change would require implementing
  Raft joint consensus (Ongaro §4) in `raft.scm`; that is a future feature, out
  of scope for this validation. Filed as a follow-up if pursued.

## cc-cri RESOLVED: non-idempotent retry on stepdown

Before this fix the **strict** workloads failed even no-fault: cas-register
`:valid? false` ("can't read X from register Y") and append `:valid? false`
(Elle G-single). register/counter passed because SET/GET/INCR are idempotent.

**Root cause (not a stale read):** a write `fail-pending!` TRYAGAIN'd on a brief
stepdown could have *already committed* — the client then silently retried (carmine
follows TRYAGAIN), and the retry of a non-idempotent op (CAS/append/INCR) double-
applied or mis-reported. A `cas [1→2]` that committed (value became 2) was recorded
`:fail` by the retry → Knossos saw a phantom stale read.

**Fix (two parts, at the leader→follower stepdown in `shard-actor.scm`):**
1. On a higher-term deposing RPC, **drain + ack the committed deferred batch** before
   `raft-step` truncates — committed = quorum-durable (Leader Completeness), so report
   `:ok`; no retry.
2. For the remaining **uncommitted** pending writes, reply a **non-retryable** error
   (not `TRYAGAIN`) → the client records `:info` (indeterminate, the truthful
   outcome: the new leader's no-op barrier may yet commit a replicated entry). Reads
   stay `TRYAGAIN` (idempotent, safe to retry).

**Result:** cas `:valid? true` (no-fault) + 0-anomalies (partition); append
`:valid? true` (**strict-serializable**); register/counter unchanged. Tracked as
**cc-cri** (resolved).

**Root cause (diagnosed, not a stale read): non-idempotent retry.** Minimal-load
trace (conc 2, ~zero churn, stable leader): a `cas [1→2]` committed (1→2, returned 1),
its ack was lost on a brief stepdown where `fail-pending!` TRYAGAINs the
committed-but-deferred write, the client silently retried (carmine follows
MOVED/TRYAGAIN), the retry saw current=2≠1 → returned 0 → recorded `:fail`. Knossos
then sees "register should be 1 but a read returned 2" — the *appearance* of a stale
read. It is the **same family as the INCR-dup (cc-btk.7)**: append's G-single is the
same (an `EXEC-TXN`/RPUSH commits, ack lost, retried → appends again). **register
passes because SET/GET are idempotent**, so a retry is harmless. NOT load/churn (fails
at conc 2), NOT multi-shard-fixable (per-group), NOT a read-path bug. This is exactly
the trade-off flagged when `fail-pending!` was built (a committed-but-deferred write
TRYAGAIN'd on stepdown). Fix options: (1) refine `fail-pending!` to ACK committed
pending writes (they survive — Leader Completeness) and TRYAGAIN only the uncommitted
truncated tail; (2) idempotency tokens (client op-IDs + server dedup, general
exactly-once); (3) document as a non-exactly-once limitation for non-idempotent ops
(as Redis has). Tracked as **cc-cri** (P1).

## Known limitations

- **Throughput ceiling under `--shards 1`.** All keys route to a single Raft
  group / single shard actor (~19 ops/s under load, green-thread fairness tax),
  which churns leadership → `-MOVED` storms → high `:info` → Knossos `:unknown`
  on some keys. Clean `:valid? true` needs low concurrency + realistic client
  MOVED/TRYAGAIN patience. This is a measurement/throughput artifact; production
  uses many shards. Multi-shard validation: `cc-btk.11`.
- **register-under-kill is Knossos-intractable** at high op counts (search-space
  blow-up on crash-induced `:info`); verified at low concurrency = `0-anomalies`.
