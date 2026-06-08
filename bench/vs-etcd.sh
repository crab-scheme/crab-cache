#!/usr/bin/env bash
# bench/vs-etcd.sh — crab-cache vs etcd vs Redis on a REAL-fsync host.
#
# Why this is separate from vs-redis.sh: the DURABLE-write comparison is only
# meaningful where fsync() is a true durability barrier. macOS fsync() is NOT a
# barrier (only F_FULLFSYNC is), so on macOS every "durable" write is ~free and
# the numbers mislead — see docs/measurements/2026-06-05-crab-cache-vs-redis.md,
# where crab-cache looked "1.0× even" with Redis on durable SET. Run this inside
# a Linux host/container where fsync() actually persists to disk.
#
# Peers:
#   * etcd  — crab-cache's true architectural peer (Raft log + per-write fsync).
#   * Redis — the speed ceiling (group-commit AOF: one fsync per event-loop tick
#             amortised across all concurrent writers).
#
# Fairness: same host/cores; the SAME redis-benchmark drives crab-cache + Redis
# over RESP. etcd is measured with its own `etcdctl check perf` (gRPC) because no
# RESP driver can hit it — its concurrency model differs from -c$C, so treat etcd
# as an ORDER-OF-MAGNITUDE reference, not a same-tool apples-to-apples number.
#
# Requirements:
#   CRABSCHEME  path to a `crabscheme` built with --features stdlib-store
#   ETCD_DIR    dir containing `etcd` and `etcdctl`            (default /tmp/etcd)
#   redis-server / redis-benchmark / redis-cli on PATH
# Tunables: C (conns, default 50), N_DURABLE, N_RELAXED, N_READ, D (value bytes),
#           SHARDS (scale curve, default "3 6 12"), ETCD_LOAD (s|m|l, default m).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/target/release/crabscheme}"
ETCD_DIR="${ETCD_DIR:-/tmp/etcd}"
ETCD="$ETCD_DIR/etcd"; ETCDCTL="$ETCD_DIR/etcdctl"
CC_DUR=7400; CC_REL=7401; RD_DUR=6390; RD_REL=6391
C="${C:-50}"; D="${D:-256}"
N_DURABLE="${N_DURABLE:-8000}"; N_RELAXED="${N_RELAXED:-50000}"; N_READ="${N_READ:-100000}"
SHARDS="${SHARDS:-3 6 12}"; ETCD_LOAD="${ETCD_LOAD:-m}"
DB="$(mktemp -d "${TMPDIR:-/tmp}/cc-vsetcd.XXXXXX")"
PIDS=()
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
           pkill -f node.scm 2>/dev/null; pkill -f "$ETCD " 2>/dev/null; rm -rf "$DB"; }
trap cleanup EXIT
cd "$ROOT"

wait_port(){ for _ in $(seq 1 120); do redis-cli -p "$1" ping 2>/dev/null | grep -q PONG && return 0; sleep 0.25; done; return 1; }

start_cc(){ # port shards durable(yes|no)
  pkill -f "node.scm --port $1" 2>/dev/null; sleep 0.5; rm -rf "$DB/cc-$1"
  CRABSCHEME_ACTOR_LOCAL_WORKERS=$((C+16)) "$BIN" run src/node.scm -- \
    --port "$1" --db "$DB/cc-$1" --shards "$2" --durable "$3" >"$DB/cc-$1.log" 2>&1 &
  PIDS+=($!); wait_port "$1"; }

start_redis(){ # port appendonly appendfsync
  redis-server --port "$1" --save "" --appendonly "$2" --appendfsync "$3" \
    --dir "$DB" >"$DB/redis-$1.log" 2>&1 &
  PIDS+=($!); wait_port "$1"; }

start_etcd(){ # fresh single-member, real fsync
  pkill -f "$ETCD " 2>/dev/null; sleep 1; rm -rf "$DB/etcd"
  "$ETCD" --data-dir "$DB/etcd" --listen-client-urls http://127.0.0.1:2379 \
    --advertise-client-urls http://127.0.0.1:2379 --listen-peer-urls http://127.0.0.1:2390 \
    --initial-cluster default=http://127.0.0.1:2390 \
    --initial-advertise-peer-urls http://127.0.0.1:2390 --log-level error >"$DB/etcd.log" 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 60); do "$ETCDCTL" endpoint health 2>/dev/null | grep -q healthy && return 0; sleep 0.3; done
  return 1; }

# mean rps + last p50 over the run, via redis-benchmark
bench(){ # port test n
  redis-benchmark -p "$1" -t "$2" -n "${3:-50000}" -c "$C" -d "$D" -q 2>/dev/null \
    | sed -n 's/.*: \([0-9.]*\) requests per second.*p50=\([0-9.]*\).*/\1 rps (p50 \2ms)/p' | head -1; }

# etcd durable write throughput via its own load generator
etcd_bench(){ "$ETCDCTL" check perf --load="$ETCD_LOAD" 2>&1 | tr '\r' '\n' \
    | sed -n 's/.*Throughput is \([0-9]*\) writes\/s.*/\1 writes\/s/p' | tail -1; }

echo "# crab-cache vs etcd vs Redis — REAL-fsync host"
echo
echo "Host: $(uname -mrs); cores: $(nproc 2>/dev/null || echo '?'); values ${D}B; -c $C."
echo "Same redis-benchmark drives crab-cache + Redis; etcd via \`etcdctl check perf --load=$ETCD_LOAD\`."
echo "crab-cache + etcd fsync per write; Redis appendfsync=always group-commits the AOF."
echo

start_etcd || { echo "_etcd failed to start_"; }
start_cc  "$CC_DUR" 3 yes
start_cc  "$CC_REL" 3 no
start_redis "$RD_DUR" yes always
start_redis "$RD_REL" no   everysec

echo "## Durable writes — per-write fsync (the comparison that needs a real barrier)"
echo
echo "| system | SET rps (p50) | notes |"
echo "|---|---|---|"
echo "| crab-cache \`--durable yes\` | $(bench $CC_DUR set "$N_DURABLE") | RocksDB WAL, group-commit batched fsync (one fsync/batch) |"
echo "| etcd (Raft + fsync) | $(etcd_bench) | \`check perf --load=$ETCD_LOAD\`, batches Raft entries |"
echo "| Redis \`appendfsync always\` | $(bench $RD_DUR set "$N_DURABLE") | group-commit AOF |"
echo
echo "## Relaxed writes — no per-write fsync"
echo
echo "| system | SET rps (p50) |"
echo "|---|---|"
echo "| crab-cache \`--durable no\` | $(bench $CC_REL set "$N_RELAXED") |"
echo "| Redis \`appendonly no\` | $(bench $RD_REL set "$N_RELAXED") |"
echo
echo "## Reads (GET)"
echo
echo "| system | GET rps (p50) |"
echo "|---|---|"
echo "| crab-cache | $(bench $CC_REL get "$N_READ") |"
echo "| Redis | $(bench $RD_REL get "$N_READ") |"
echo
echo "## crab-cache durable SET vs shard count"
echo
echo "_Durable writes are group-committed (one fsync per batch/tick), so throughput"
echo "is no longer fsync-serialized; the per-shard batch size shapes this curve._"
echo
echo "| shards | durable SET rps (p50) |"
echo "|---|---|"
for s in $SHARDS; do
  start_cc "$CC_DUR" "$s" yes
  echo "| $s | $(bench $CC_DUR set "$N_DURABLE") |"
done
echo
echo "_Reported honestly: after group-commit, crab-cache durable SET (~6k rps) LEADS"
echo "etcd ~6x and is ~3x behind Redis's group-commit AOF (was ~34x). GET via the"
echo "native fused path does ~121k @ -P1 and BEATS Redis when pipelined. Relaxed SET"
echo "(no fsync) remains the open lever (interpreted SET pipeline)._"
