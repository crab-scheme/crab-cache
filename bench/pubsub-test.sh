#!/usr/bin/env bash
# bench/pubsub-test.sh — single-node pub/sub smoke: a subscriber, a publisher,
# pattern subscribe, and PUBSUB introspection.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
PORT="${PORT:-6520}"
DB="$(mktemp -d "${TMPDIR:-/tmp}/cc-pubsub.XXXXXX")"
NODE=""
cleanup(){ [ -n "$NODE" ] && kill "$NODE" 2>/dev/null; rm -rf "$DB"; }
trap cleanup EXIT
cd "$ROOT"

CRABSCHEME_ACTOR_LOCAL_WORKERS=40 "$BIN" run src/node.scm -- --port "$PORT" --db "$DB/db" --shards 1 >"$DB/node.log" 2>&1 &
NODE=$!
for _ in $(seq 1 50); do redis-cli -p "$PORT" ping 2>/dev/null | grep -q PONG && break; sleep 0.2; done

# a channel subscriber and a pattern subscriber, both detached to files
timeout 5 redis-cli -p "$PORT" subscribe ch1 >"$DB/sub.out" 2>&1 & SUB1=$!
timeout 5 redis-cli -p "$PORT" psubscribe 'news.*' >"$DB/psub.out" 2>&1 & SUB2=$!
sleep 1.5

echo "publish ch1 hello1   -> $(redis-cli -p "$PORT" publish ch1 hello1)"
echo "publish ch1 hello2   -> $(redis-cli -p "$PORT" publish ch1 hello2)"
echo "publish news.sport x -> $(redis-cli -p "$PORT" publish news.sport goal)"
echo "pubsub channels      -> $(redis-cli -p "$PORT" pubsub channels | tr '\n' ' ')"
echo "pubsub numsub ch1    -> $(redis-cli -p "$PORT" pubsub numsub ch1 | tr '\n' ' ')"
wait "$SUB1" "$SUB2" 2>/dev/null

echo "=== channel subscriber stream ==="; cat "$DB/sub.out"
echo "=== pattern subscriber stream ==="; cat "$DB/psub.out"
fails=0
grep -q hello1 "$DB/sub.out"  || { echo "MISS hello1"; fails=$((fails+1)); }
grep -q hello2 "$DB/sub.out"  || { echo "MISS hello2"; fails=$((fails+1)); }
grep -q goal   "$DB/psub.out" || { echo "MISS pattern goal"; fails=$((fails+1)); }
echo; [ "$fails" = 0 ] && echo "PUBSUB OK" || echo "PUBSUB FAIL ($fails)"
exit "$fails"
