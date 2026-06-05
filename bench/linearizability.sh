#!/usr/bin/env bash
# bench/linearizability.sh — linearizability witness under induced failover.
#
# A counter is a register with a read-modify-write (INCR). If INCR is
# linearized, every successful INCR returns a UNIQUE value (two INCRs can't
# both observe the same pre-value), the acked values are a gap-free prefix of
# the final counter, and the final value never undercounts the acks. We drive
# many concurrent INCRs on one key, kill that shard's leader mid-run, and check
# those invariants on the recorded responses — a passing check means no acked
# update was lost or double-counted across the failover.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
SPEC="a:127.0.0.1:7501:6501,b:127.0.0.1:7502:6502,c:127.0.0.1:7503:6503"
DB="$(mktemp -d "${TMPDIR:-/tmp}/cc-lin.XXXXXX")"
declare -A CPORT=([a]=6501 [b]=6502 [c]=6503) PID
CLIENTS="${CLIENTS:-6}"; PER="${PER:-120}"
cleanup(){ for n in a b c; do [ -n "${PID[$n]:-}" ] && kill "${PID[$n]}" 2>/dev/null; done; rm -rf "$DB"; }
trap cleanup EXIT
cd "$ROOT"

start_one(){ CRABSCHEME_ACTOR_LOCAL_WORKERS=40 "$BIN" run src/node-cluster.scm -- \
    --node "$1" --shards 3 --db "$DB/$1" --durable yes --cluster "$SPEC" >"$DB/$1.log" 2>&1 & PID[$1]=$!; }
for n in a b c; do start_one "$n"; done
for n in a b c; do
  for _ in $(seq 1 100); do redis-cli -p "${CPORT[$n]}" ping 2>/dev/null | grep -q PONG && break; sleep 0.25; done
done
echo "cluster up; driving $((CLIENTS*PER)) concurrent INCRs on key 'ctr' with a mid-run leader kill"

redis-cli -c -p 6501 set ctr 0 >/dev/null 2>&1   # ensure exists (INCR from 0)
redis-cli -c -p 6501 del ctr >/dev/null 2>&1

# concurrent INCR clients: each records every value INCR returns
for c in $(seq 1 "$CLIENTS"); do
  ( for _ in $(seq 1 "$PER"); do
      v=$(redis-cli -c -p "${CPORT[a]}" incr ctr 2>/dev/null)
      case "$v" in (''|*[!0-9]*) :;; (*) echo "$v" >>"$DB/vals";; esac
    done ) &
done

# mid-run: kill the leader of ctr's shard
sleep 2
slotinfo=$(redis-cli -p 6501 incr ctr 2>&1)   # OK if local leader, MOVED otherwise
lead=6501; case "$slotinfo" in MOVED*) lead=$(echo "$slotinfo" | awk '{print $3}' | cut -d: -f2);; esac
for n in a b c; do [ "${CPORT[$n]}" = "$lead" ] && killn=$n; done
echo "killing ctr-shard leader: node $killn ($lead)"
kill "${PID[$killn]}" 2>/dev/null; PID[$killn]=""

wait $(jobs -p 2>/dev/null | grep -v "${PID[a]:-x}\|${PID[b]:-x}\|${PID[c]:-x}") 2>/dev/null
sleep 1

# the kill probe above also did one INCR; fold it in if it returned a number
case "$slotinfo" in (''|*[!0-9]*) :;; (*) echo "$slotinfo" >>"$DB/vals";; esac

total=$(wc -l <"$DB/vals" | tr -d ' ')
distinct=$(sort -n "$DB/vals" | uniq | wc -l | tr -d ' ')
dups=$(sort -n "$DB/vals" | uniq -d | wc -l | tr -d ' ')
maxv=$(sort -n "$DB/vals" | tail -1)
# read the final counter from a surviving node
others=$(for n in a b c; do [ "$n" != "$killn" ] && echo "${CPORT[$n]}"; done)
final=""; for p in $others; do f=$(redis-cli -c -p "$p" get ctr 2>/dev/null); [ -n "$f" ] && final=$f; done

echo
echo "  acked INCRs (responses recorded) = $total"
echo "  distinct values                  = $distinct"
echo "  duplicate values                 = $dups   (must be 0)"
echo "  max acked value                  = $maxv"
echo "  final counter value              = $final"
fails=0
[ "$dups" = 0 ]                     || { echo "  FAIL: duplicate acked values (double-count)"; fails=$((fails+1)); }
[ "$total" = "$distinct" ]          || { echo "  FAIL: non-unique acks"; fails=$((fails+1)); }
[ -n "$final" ] && [ "$final" -ge "$maxv" ] || { echo "  FAIL: final counter < max acked (lost update)"; fails=$((fails+1)); }
[ -n "$final" ] && [ "$final" -ge "$distinct" ] || { echo "  FAIL: final counter < acked count"; fails=$((fails+1)); }
echo
[ "$fails" = 0 ] && echo "LINEARIZABLE under failover: no lost or double-counted acked updates" \
                 || echo "LINEARIZABILITY VIOLATION ($fails)"
exit "$fails"
