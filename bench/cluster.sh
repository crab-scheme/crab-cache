#!/usr/bin/env bash
# bench/cluster.sh — launch a 3-node crab-cache cluster over real TCP and
# exercise cross-node routing (MOVED) + replication. With `failover` as $1,
# also kills a shard leader mid-run and checks recovery + no acked-write loss.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
SPEC="a:127.0.0.1:7001:6001,b:127.0.0.1:7002:6002,c:127.0.0.1:7003:6003"
DB="$(mktemp -d "${TMPDIR:-/tmp}/cc-cluster.XXXXXX")"
declare -A CPORT=([a]=6001 [b]=6002 [c]=6003)
declare -A PID

cleanup(){ for n in a b c; do [ -n "${PID[$n]:-}" ] && kill "${PID[$n]}" 2>/dev/null; done; rm -rf "$DB"; }
trap cleanup EXIT
cd "$ROOT"

echo "== launching 3-node cluster =="
DURABLE="${DURABLE:-no}"
start_one(){ # start_one <node>
  CRABSCHEME_ACTOR_LOCAL_WORKERS=60 "$BIN" run src/node-cluster.scm -- \
      --node "$1" --shards 3 --db "$DB/$1" --durable "$DURABLE" --cluster "$SPEC" >"$DB/$1.log" 2>&1 &
  PID[$1]=$!
}
for n in a b c; do start_one "$n"; done

# wait for every client port to answer PING
for n in a b c; do
  ok=0
  for _ in $(seq 1 100); do
    redis-cli -p "${CPORT[$n]}" ping 2>/dev/null | grep -q PONG && { ok=1; break; }
    sleep 0.25
  done
  [ "$ok" = 1 ] || { echo "node $n never came up"; echo "--- $n log ---"; cat "$DB/$n.log"; exit 1; }
  echo "  node $n up on ${CPORT[$n]}"
done

echo "== MOVED routing (plain client hits a node; non-led shards redirect) =="
# Show a MOVED for a key whichever node we ask that doesn't lead its shard.
for k in foo bar user1000 counter; do
  resp=$(redis-cli -p 6001 set "$k" "v-$k" 2>&1)
  echo "  set $k @a -> $resp"
done

echo "== functional via redis-cli -c (follows MOVED across the cluster) =="
fails=0
ck(){ if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  FAIL $1: want [$2] got [$3]"; fails=$((fails+1)); fi; }
ck "set foo"   "OK"   "$(redis-cli -c -p 6001 set foo bar 2>&1)"
ck "get foo@a" "bar"  "$(redis-cli -c -p 6001 get foo 2>&1)"
ck "get foo@b" "bar"  "$(redis-cli -c -p 6002 get foo 2>&1)"   # different entry node, same value
ck "get foo@c" "bar"  "$(redis-cli -c -p 6003 get foo 2>&1)"
redis-cli -c -p 6001 set counter 0 >/dev/null 2>&1
redis-cli -c -p 6001 incr counter >/dev/null 2>&1
ck "incr"      "2"    "$(redis-cli -c -p 6002 incr counter 2>&1)"
redis-cli -c -p 6001 rpush mylist a b c >/dev/null 2>&1
ck "lrange"    "a b c" "$(redis-cli -c -p 6003 lrange mylist 0 -1 2>&1 | tr '\n' ' ' | sed 's/ $//')"

echo
[ "$fails" = 0 ] && echo "CLUSTER ROUTING: all passed" || echo "CLUSTER ROUTING FAILED: $fails"

if [ "${1:-}" = "pubsub" ]; then
  echo
  echo "== cross-node pub/sub: subscribe on b, publish on a and c =="
  timeout 6 redis-cli -p 6002 subscribe global-ch >"$DB/csub.out" 2>&1 & CSUB=$!
  sleep 1.5
  echo "  publish @a -> $(redis-cli -p 6001 publish global-ch hello-from-a)"
  echo "  publish @c -> $(redis-cli -p 6003 publish global-ch hello-from-c)"
  wait "$CSUB" 2>/dev/null
  echo "  --- node-b subscriber stream ---"; sed 's/^/    /' "$DB/csub.out"
  if grep -q hello-from-a "$DB/csub.out" && grep -q hello-from-c "$DB/csub.out"; then
    echo "  CROSS-NODE PUBSUB: ok"
  else
    echo "  CROSS-NODE PUBSUB: FAIL"; fails=$((fails+1))
  fi
fi

if [ "${1:-}" = "rejoin" ]; then
  echo
  echo "== restart-rejoin: fill, kill a node, write more while it's down, restart, converge =="
  # every node replicates every shard, so node b's per-node DBSIZE == cluster keys.
  base=$(redis-cli -p 6002 dbsize)
  for i in $(seq 1 30); do redis-cli -c -p 6001 set "rk:$i" "v$i" >/dev/null 2>&1; done
  echo "  node-b dbsize before kill = $(redis-cli -p 6002 dbsize) (base $base + 30)"
  echo "  killing node b"
  kill "${PID[b]}" 2>/dev/null; PID[b]=""; sleep 1
  # write 20 MORE keys while b is down (commit on the a+c quorum)
  for i in $(seq 31 50); do redis-cli -c -p 6001 set "rk:$i" "v$i" >/dev/null 2>&1; done
  echo "  wrote 20 more keys while b was down"
  echo "  restarting node b"
  start_one b
  for _ in $(seq 1 120); do redis-cli -p 6002 ping 2>/dev/null | grep -q PONG && break; sleep 0.25; done
  # b must catch up the 20 it missed via re-replication -> per-node dbsize reaches base+50
  expect=$((base + 50)); conv=0
  for _ in $(seq 1 100); do
    [ "$(redis-cli -p 6002 dbsize 2>/dev/null)" = "$expect" ] && { conv=1; break; }
    sleep 0.5
  done
  echo "  node-b dbsize after rejoin = $(redis-cli -p 6002 dbsize) (expect $expect)"
  ck "rejoined node caught up the writes it missed" "$expect" "$(redis-cli -p 6002 dbsize)"
  ck "rejoined node converged" "1" "$conv"
fi

if [ "${1:-}" = "failover" ]; then
  echo
  echo "== failover: acked write on foo's shard, kill THAT shard's leader, verify =="
  # foo lands on a specific shard; commit a value on it (acked => quorum-durable).
  redis-cli -c -p 6001 set foo before-kill >/dev/null 2>&1
  # find foo's current leader node (the SET above followed MOVED there).
  slotinfo=$(redis-cli -p 6001 set foo before-kill 2>&1)
  leadhost=6001
  case "$slotinfo" in MOVED*) leadhost=$(echo "$slotinfo" | awk '{print $3}' | cut -d: -f2);; esac
  for n in a b c; do [ "${CPORT[$n]}" = "$leadhost" ] && killn=$n; done
  echo "  foo-shard leader = node $killn ($leadhost); killing it"
  kill "${PID[$killn]}" 2>/dev/null; PID[$killn]=""
  others=$(for n in a b c; do [ "$n" != "$killn" ] && echo "${CPORT[$n]}"; done)
  # 1) the pre-kill acked write must still be readable on the surviving quorum
  surv=""
  for _ in $(seq 1 80); do
    for p in $others; do
      v=$(redis-cli -c -p "$p" get foo 2>&1)
      if [ "$v" = "before-kill" ]; then surv="before-kill"; break; fi
    done
    [ -n "$surv" ] && break
    sleep 0.5
  done
  ck "no acked-write loss (same shard)" "before-kill" "$surv"
  # 2) writes must resume under the new leader
  recovered=0
  for _ in $(seq 1 80); do
    for p in $others; do
      [ "$(redis-cli -c -p "$p" set foo after-failover 2>&1)" = "OK" ] && { recovered=1; break; }
    done
    [ "$recovered" = 1 ] && break
    sleep 0.5
  done
  ck "writes resume on new leader" "1" "$recovered"
fi

echo
[ "$fails" = 0 ] && echo "DONE ok" || echo "DONE with $fails failures"
exit "$fails"
