#!/usr/bin/env bash
# bench/single-node.sh — start a single crab-cache node, run a redis-cli
# conformance pass (AS-1) and redis-benchmark, then tear down.
#
#   bench/single-node.sh                # default port 6400, modest load
#   PORT=6401 N=200000 C=50 P=4 bench/single-node.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
PORT="${PORT:-6400}"
N="${N:-100000}"          # requests per benchmark test
C="${C:-50}"              # parallel connections
P="${P:-1}"               # pipeline depth
DB="$(mktemp -d "${TMPDIR:-/tmp}/crab-cache-bench.XXXXXX")"
# spawn-source conn-actors use the blocking-thread pool; keep headroom > C.
export CRABSCHEME_ACTOR_LOCAL_WORKERS="${CRABSCHEME_ACTOR_LOCAL_WORKERS:-$((C + 16))}"

NODE_PID=""
cleanup() { [ -n "$NODE_PID" ] && kill "$NODE_PID" 2>/dev/null; rm -rf "$DB"; }
trap cleanup EXIT

cd "$ROOT"
echo "== starting crab-cache :$PORT (db=$DB) =="
"$BIN" run src/node.scm -- --port "$PORT" --db "$DB" &
NODE_PID=$!

# wait for the listener
ready=0
for _ in $(seq 1 50); do
  if redis-cli -p "$PORT" ping 2>/dev/null | grep -q PONG; then ready=1; break; fi
  sleep 0.2
done
[ "$ready" = 1 ] || { echo "node did not become ready"; exit 1; }

q() { redis-cli -p "$PORT" "$@"; }
fails=0
ck() { # ck <name> <expected> <actual>
  if [ "$2" = "$3" ]; then echo "  ok   $1";
  else echo "  FAIL $1: expected [$2] got [$3]"; fails=$((fails+1)); fi
}

echo "== conformance (AS-1) =="
ck "set"        "OK"     "$(q set foo bar)"
ck "get"        "bar"    "$(q get foo)"
ck "append"     "6"      "$(q append foo baz)"
ck "strlen"     "6"      "$(q strlen foo)"
ck "incr"       "1"      "$(q incr ctr)"
ck "incrby"     "11"     "$(q incrby ctr 10)"
ck "exists"     "1"      "$(q exists foo)"
ck "type"       "string" "$(q type foo)"
ck "del"        "1"      "$(q del foo)"
ck "get-after-del" ""    "$(q get foo)"
ck "hset"       "2"      "$(q hset h f1 a f2 b)"
ck "hget"       "a"      "$(q hget h f1)"
ck "hlen"       "2"      "$(q hlen h)"
ck "rpush"      "3"      "$(q rpush l x y z)"
ck "lrange"     "x y z"  "$(q lrange l 0 -1 | tr '\n' ' ' | sed 's/ $//')"
ck "lpop"       "x"      "$(q lpop l)"
ck "sadd"       "3"      "$(q sadd s a b c)"
ck "scard"      "3"      "$(q scard s)"
ck "sismember"  "1"      "$(q sismember s a)"
ck "zadd"       "2"      "$(q zadd z 1 a 2 b)"
ck "zscore"     "2"      "$(q zscore z b)"
ck "zrange"     "a b"    "$(q zrange z 0 -1 | tr '\n' ' ' | sed 's/ $//')"
ck "expire"     "1"      "$(q expire ctr 100)"
ck "ttl>0"      "1"      "$([ "$(q ttl ctr)" -gt 0 ] && echo 1 || echo 0)"
ck "ping"       "PONG"   "$(q ping)"
ck "dbsize>0"   "1"      "$([ "$(q dbsize)" -gt 0 ] && echo 1 || echo 0)"

echo
if [ "$fails" -gt 0 ]; then echo "CONFORMANCE FAILED: $fails"; else echo "CONFORMANCE: all passed"; fi

echo
echo "== redis-benchmark (n=$N c=$C P=$P) =="
redis-benchmark -p "$PORT" -t set,get,incr,lpush,rpush,hset,sadd -n "$N" -c "$C" -P "$P" -q

echo
echo "== flush + done =="
q flushall >/dev/null
[ "$fails" -gt 0 ] && exit 1 || exit 0
