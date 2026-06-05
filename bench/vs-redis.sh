#!/usr/bin/env bash
# bench/vs-redis.sh — fair head-to-head: the SAME redis-benchmark driven
# against crab-cache and a matched Redis, in both durability regimes, plus a
# crab-cache sharding-scale curve. Prints a markdown report on stdout.
#
# Fairness (NFR-13/14/15): same host/cores, identical benchmark params (value
# size, key cardinality, pipeline depth), durability matched per regime and
# BOTH regimes reported, 3 repeats with the mean taken.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
CC_PORT=7400; RD_PORT=7401
DB="$(mktemp -d "${TMPDIR:-/tmp}/cc-vsredis.XXXXXX")"
C="${C:-50}"; P="${P:-1}"; REPEATS="${REPEATS:-3}"
CCPID=""; RDPID=""
cleanup(){ [ -n "$CCPID" ] && kill -9 "$CCPID" 2>/dev/null; [ -n "$RDPID" ] && kill "$RDPID" 2>/dev/null;
           pkill -f "node.scm --port $CC_PORT" 2>/dev/null; rm -rf "$DB"; }
trap cleanup EXIT
cd "$ROOT"

wait_port(){ for _ in $(seq 1 100); do redis-cli -p "$1" ping 2>/dev/null | grep -q PONG && return 0; sleep 0.2; done; return 1; }
start_cc(){ # shards durable
  rm -rf "$DB"/cc-*; pkill -f "node.scm --port $CC_PORT" 2>/dev/null; sleep 0.5
  CRABSCHEME_ACTOR_LOCAL_WORKERS=$((C+16)) "$BIN" run src/node.scm -- \
     --port "$CC_PORT" --db "$DB/cc" --shards "$1" --durable "$2" >"$DB/cc.log" 2>&1 &
  CCPID=$!; wait_port "$CC_PORT"; }
start_redis(){ # appendonly appendfsync
  [ -n "$RDPID" ] && kill "$RDPID" 2>/dev/null; sleep 0.3
  redis-server --port "$RD_PORT" --save "" --appendonly "$1" --appendfsync "$2" \
     --dir "$DB" >"$DB/redis.log" 2>&1 &
  RDPID=$!; wait_port "$RD_PORT"; }

# mean rps over REPEATS for one command (prints "rps p50")
bench(){ # port test
  local sum=0 p50="" r
  for _ in $(seq 1 "$REPEATS"); do
    r=$(redis-benchmark -p "$1" -t "$2" -n "${N:-50000}" -c "$C" -P "$P" -q 2>/dev/null \
        | sed -n 's/.*: \([0-9.]*\) requests per second.*p50=\([0-9.]*\).*/\1 \2/p' | head -1)
    sum=$(awk -v s="$sum" -v x="${r%% *}" 'BEGIN{print s+x}'); p50="${r##* }"
  done
  awk -v s="$sum" -v n="$REPEATS" -v p="$p50" 'BEGIN{printf "%.0f %s", s/n, p}'
}

row(){ # label cc-port-result redis-result
  printf "| %-6s | %s | %s | %sx |\n" "$1" "$2" "$3" \
    "$(awk -v a="${2%% *}" -v b="${3%% *}" 'BEGIN{ if(a>0) printf "%.1f", b/a; else print "-"}')"
}

echo "# crab-cache vs Redis — head-to-head"
echo
echo "Host: $(uname -mrs); cores: $(sysctl -n hw.ncpu 2>/dev/null || nproc); -c $C -P $P, ${REPEATS}× mean."
echo "Same redis-benchmark binary drives both. crab-cache routes every write through Raft."
echo

for regime in relaxed durable; do
  if [ "$regime" = relaxed ]; then
    N="${N_RELAXED:-50000}"; start_cc 3 no; start_redis no everysec
    echo "## Relaxed durability (crab-cache async-WAL · Redis appendonly no)"
  else
    N="${N_DURABLE:-8000}"; start_cc 3 yes; start_redis yes always
    echo "## Matched full durability (crab-cache fsync-per-write · Redis appendfsync always)"
  fi
  export N
  echo; echo "| cmd | crab-cache rps (p50ms) | Redis rps (p50ms) | Redis/cc |"
  echo "|-----|-----|-----|-----|"
  for t in set get incr; do
    cc=$(bench "$CC_PORT" "$t"); rd=$(bench "$RD_PORT" "$t")
    row "$t" "$cc" "$rd"
  done
  echo
done

echo "## crab-cache sharding scale (relaxed, SET, ${REPEATS}× mean)"
echo; echo "| shards | SET rps | GET rps |"; echo "|-----|-----|-----|"
N="${N_RELAXED:-50000}"; export N
for s in 1 3 6; do
  start_cc "$s" no
  st=$(bench "$CC_PORT" set); gt=$(bench "$CC_PORT" get)
  printf "| %-6s | %s | %s |\n" "$s" "${st%% *}" "${gt%% *}"
done
echo
echo "_crab-cache trails Redis (every write goes through Raft propose→commit→apply); the"
echo "proof is correctness + durability + distribution at competitive speed, reported honestly._"
