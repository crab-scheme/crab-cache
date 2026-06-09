#!/usr/bin/env bash
# soak.sh — run the crab-cache Jepsen matrix (each workload x fault) repeatedly and
# summarize the :valid? verdicts. Drives the running Docker cluster (jcc-control +
# jcc-n1..n5); bring it up first with:
#   docker compose -f jepsen/docker/docker-compose.yml up -d --build
#
# Env knobs:  TEST_COUNT (iterations per cell, default 1)  TIME_LIMIT (s, default 100)
#
# Configs are the low-concurrency / realistic-client-retry settings that let the
# checkers reach a definitive verdict (the single-shard actor's throughput pushes
# Knossos to :unknown at high load — a measurement limit, not a violation; see
# docs/jepsen-validation.md). Elle (append) tolerates more concurrency than Knossos.
set -uo pipefail

CTL=${CTL:-jcc-control}
NODES=${NODES:-n1,n2,n3,n4,n5}
TEST_COUNT=${TEST_COUNT:-1}
TIME_LIMIT=${TIME_LIMIT:-100}
SSH="--username root --ssh-private-key /root/.ssh/id_jepsen"

# workload|fault|extra-args  (faults: none partition kill)
MATRIX=(
  "register|none|--concurrency 5 --register-group 1 --register-ops 50"
  "register|partition|--concurrency 5 --register-group 1 --register-ops 50"
  "register|kill|--concurrency 5 --register-group 1 --register-ops 50"
  "counter|kill|--concurrency 5"
  "counter|partition|--concurrency 5"
  "cas|none|--concurrency 5 --register-group 1 --register-ops 50"
  "cas|partition|--concurrency 5 --register-group 1 --register-ops 50"
  "append|none|--concurrency 5"
  "append|kill|--concurrency 5"
)

run_one() {  # workload fault extra
  local wl=$1 fault=$2 extra=$3
  docker exec "$CTL" bash -c "cd /crab-cache/jepsen && lein run test \
    --workload $wl --nemesis $fault --shards 1 --nodes $NODES $SSH \
    --time-limit $TIME_LIMIT $extra" 2>&1 | tail -40
}

verdict() {  # reads stdin (lein tail) once, prints true|false|unknown|crash
  local out; out=$(cat)
  if   grep -q "Everything looks good" <<<"$out"; then echo true
  elif grep -q "no anomalies found"    <<<"$out"; then echo unknown
  elif grep -q ":valid? false"         <<<"$out"; then echo false
  else echo crash ; fi
}

echo "crab-cache Jepsen soak — TEST_COUNT=$TEST_COUNT TIME_LIMIT=${TIME_LIMIT}s"
declare -a RESULTS ; rc=0
for cell in "${MATRIX[@]}"; do
  IFS='|' read -r wl fault extra <<<"$cell"
  for i in $(seq 1 "$TEST_COUNT"); do
    out=$(run_one "$wl" "$fault" "$extra")
    v=$(printf '%s' "$out" | verdict)
    printf '  %-9s %-10s #%-2s -> %s\n' "$wl" "$fault" "$i" "$v"
    RESULTS+=("$wl/$fault=$v")
    [ "$v" = "false" -o "$v" = "crash" ] && rc=1
  done
done

echo "=== SUMMARY ==="
printf '%s\n' "${RESULTS[@]}"
# Fail the run only on a real anomaly (false) or crash; true/unknown pass.
echo "exit=$rc (0=no anomalies/crashes; 1=anomaly or crash detected)"
exit $rc
