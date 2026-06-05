#!/usr/bin/env bash
# bench/crash-recovery.sh — AS-4: write N acked SETs in DURABLE mode (each
# fsynced before the ack), kill -9 the node, restart, and verify every acked
# key is present with the right value and there are no phantom keys.
#
#   N=10000 bench/crash-recovery.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
PORT="${PORT:-6530}"
N="${N:-10000}"
DB="$(mktemp -d "${TMPDIR:-/tmp}/cc-crash.XXXXXX")"
NODE=""
cleanup(){ [ -n "$NODE" ] && kill -9 "$NODE" 2>/dev/null; rm -rf "$DB"; }
trap cleanup EXIT
cd "$ROOT"

start_node(){
  CRABSCHEME_ACTOR_LOCAL_WORKERS=30 "$BIN" run src/node.scm -- \
      --port "$PORT" --db "$DB/db" --shards 1 --durable yes >"$DB/node.log" 2>&1 &
  NODE=$!
  for _ in $(seq 1 80); do redis-cli -p "$PORT" ping 2>/dev/null | grep -q PONG && return 0; sleep 0.25; done
  echo "node failed to start"; cat "$DB/node.log"; exit 1
}

echo "== run 1: write $N durable (fsynced) SETs =="
start_node
# pipe N sequential SETs; --pipe reports replies/errors
seq 0 $((N-1)) | awk -v p="$PORT" '{print "SET k:"$1" v:"$1}' | redis-cli -p "$PORT" --pipe
acked=$(redis-cli -p "$PORT" dbsize)
echo "  dbsize after writes = $acked (expect $N)"

echo "== kill -9 (simulate crash) =="
kill -9 "$NODE" 2>/dev/null; wait "$NODE" 2>/dev/null; NODE=""
sleep 1

echo "== run 2: restart, recover from RocksDB, verify =="
start_node
recovered=$(redis-cli -p "$PORT" dbsize)
echo "  dbsize after restart = $recovered (expect $N)"
fails=0
ck(){ if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  FAIL $1: want [$2] got [$3]"; fails=$((fails+1)); fi; }
ck "all acked present (count)" "$N" "$recovered"
ck "no phantom (exact count)"  "$N" "$recovered"
ck "first key"  "v:0"          "$(redis-cli -p "$PORT" get k:0)"
ck "mid key"    "v:$((N/2))"   "$(redis-cli -p "$PORT" get k:$((N/2)))"
ck "last key"   "v:$((N-1))"   "$(redis-cli -p "$PORT" get k:$((N-1)))"
ck "absent key" ""             "$(redis-cli -p "$PORT" get k:nonexistent)"

echo
[ "$fails" = 0 ] && echo "AS-4 PASS (all $N acked SETs survived kill -9)" || echo "AS-4 FAIL ($fails)"
exit "$fails"
