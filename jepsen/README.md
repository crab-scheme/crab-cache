# Jepsen tests for crab-cache

Formal correctness testing for crab-cache under fault injection — real network
partitions, process kills, pauses, and clock skew — with consistency verdicts
from [Knossos](https://github.com/jepsen-io/knossos). This is the rigorous
successor to the ad-hoc `bench/*.sh` oracles: instead of checking a few
invariants after a single `kill -9`, Jepsen records a full operation history and
proves (or refutes) that it was achievable under the claimed consistency model,
handing you a minimal counterexample when it wasn't.

Adapted from the official [`jepsen-io/redis`](https://github.com/jepsen-io/redis)
Redis-Raft test.

## What it checks

| Workload (`--workload`) | Ops | Checker | Catches |
|---|---|---|---|
| `register` | `GET` / `SET` over many independent keys | Knossos linearizable register | stale reads, lost acked writes, split-brain |
| `counter` | `INCR` / `GET` on one key | `checker/counter` + a duplicate-return checker | lost increments **and** double-applied INCRs (the known non-idempotent-replay gap) |

### Why `--shards 1` for the first cut

The default is `--shards 1`: a single Raft group across all nodes. This mirrors
the redis-raft test (one consensus group) and keeps the client simple. The client
still follows `-MOVED` redirects (a follower leads nothing and redirects every
keyed command to the current leader), which also exercises the failover-recovery
path — when the leader is killed and a survivor is elected, in-flight clients are
redirected to the new leader.

### Current limitation (and the upgrade path)

crab-cache has no `WATCH`/`MULTI`/`EXEC`, so there is **no `cas` op** and we can't
run the gold-standard [Elle](https://github.com/jepsen-io/elle) list-append
workload (strict-serializability) that the redis-raft test uses. The read/write
register is a weaker check than a cas register, though it still detects
split-brain and lost writes. **Adding `WATCH`+`MULTI`/`EXEC` to crab-cache would
unlock both a cas register and the full Elle append workload** — at which point
this suite becomes directly comparable to the official Redis-Raft analysis.

## Prerequisites

- **Control node** (where you run `lein`): JDK 11+, [Leiningen](https://leiningen.org),
  `gnuplot` (perf graphs) and `graphviz`/`dot` (Knossos diagrams).
- **DB nodes**: 5 Linux hosts (Debian/Ubuntu), SSH-reachable from the control
  node, resolvable by name (`n1`..`n5`), with `iptables` (partitions), `pkill`
  (kill/pause), and the ability to skew the clock (clock nemesis). The Jepsen
  Docker images provide all of this.
- **A crabscheme binary built with `--features stdlib-store`**, for the DB nodes'
  architecture. On arm64 Linux nodes this **must** include the NaN-box arm64-Linux
  fix (crabscheme PR #124) — older builds SIGSEGV on bit-47-set heap pointers.

## Provisioning the DB nodes

crab-cache is interpreted, so each node just needs the binary + the `src/` tree
under `/opt/crabcache`. Sync them once (and again whenever either changes):

```bash
CRABSCHEME=/path/to/crabscheme NODES="n1 n2 n3 n4 n5" SSH_USER=root \
  ./bin/sync-nodes.sh
```

The Jepsen `db` layer then starts `crabscheme run src/node-cluster.scm` from
`/opt/crabcache`, wiping RocksDB state on `setup!` and on each fresh test (but
**not** on a nemesis kill/restart — that's the crash-recovery path under test).

## Running

From this `jepsen/` directory, on the control node:

```bash
# Linearizable register under network partitions:
lein run test --workload register --nemesis partition \
              --nodes n1,n2,n3,n4,n5 --concurrency 10n --time-limit 120

# Counter under the full fault menu:
lein run test --workload counter --nemesis all \
              --nodes n1,n2,n3,n4,n5 --concurrency 10n --time-limit 300 --test-count 5

# Browse results (timeline, latency, Knossos analysis) at http://localhost:8080
lein run serve
```

### Options

On top of Jepsen's built-ins (`--nodes`/`--nodes-file`, `--concurrency`,
`--time-limit`, `--test-count`, `--username`, `--ssh-private-key`, ...):

| Option | Default | Meaning |
|---|---|---|
| `--workload` | `register` | `register` or `counter` |
| `--nemesis` | `partition` | subset of `partition,kill,pause,clock`, or `none` / `all` |
| `--shards` | `1` | shards per node (1 = single Raft group) |
| `--[no-]durable` | `durable` | fsync every write (RocksDB durable mode) |
| `--rate` | `50` | approx requests/sec/thread |

Results land in `store/<test-name>/<timestamp>/`; `store/latest` symlinks the most
recent. `results.edn` holds the verdict (`:valid? true|false|:unknown`).

## Running the nodes with Docker

The upstream [`jepsen-io/jepsen` `docker/`](https://github.com/jepsen-io/jepsen/tree/main/docker)
Compose setup brings up a `jepsen-control` container plus `n1`..`n5`:

```bash
# in a checkout of jepsen-io/jepsen
cd docker && ./bin/up        # then ./bin/console to enter jepsen-control
```

Inside `jepsen-control`, clone crab-cache, `cd crab-cache/jepsen`, run
`bin/sync-nodes.sh`, then `lein run test ...` as above.

> **Apple Silicon / arm64:** the upstream images are x86-centric and use
> systemd-in-container; running them on arm64 Docker Desktop / OrbStack is fiddly.
> The smoother path on this machine is to provision **5 OrbStack Linux machines**
> with `sshd`, point the test at them via `--nodes-file`, and `--username` your
> SSH user. (And remember the arm64 NaN-box fix prerequisite above.)

## Local single-node smoke test (no Jepsen orchestration)

To sanity-check the wire protocol the client relies on, without 5 nodes:

```bash
cd ..   # crab-cache repo root
/path/to/crabscheme run src/node.scm -- --port 7799 --db /tmp/cc-smoke --shards 1 &
redis-cli -p 7799 set foo bar && redis-cli -p 7799 get foo && redis-cli -p 7799 incr ctr
```

## Layout

```
project.clj                     deps: jepsen 0.3.11, carmine 3.5.0
bin/sync-nodes.sh               provision binary + src onto DB nodes
src/jepsen/crabcache/
  core.clj                      test map + CLI
  db.clj                        start/stop/kill/pause/logs for a node-cluster process
  client.clj                    Carmine client + MOVED-redirect following
  register.clj                  linearizable read/write register workload
  counter.clj                   INCR counter workload + duplicate-INCR checker
```
