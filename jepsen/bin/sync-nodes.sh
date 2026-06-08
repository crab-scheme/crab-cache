#!/usr/bin/env bash
# Provision crab-cache onto Jepsen DB nodes.
#
# crab-cache is interpreted, so each node just needs the crabscheme binary (built
# with --features stdlib-store) plus the crab-cache `src/` tree, both under
# /opt/crabcache. The Jepsen db.clj starts `crabscheme run src/node-cluster.scm`
# from there. Run this ONCE before your first `lein run test` (and again whenever
# the binary or src changes).
#
# Usage:
#   CRABSCHEME=/path/to/crabscheme NODES="n1 n2 n3 n4 n5" SSH_USER=root ./bin/sync-nodes.sh
#
# Env:
#   CRABSCHEME  (required) path to the crabscheme binary on THIS (control) host
#   NODES       (default "n1 n2 n3 n4 n5") space-separated DB node hostnames
#   SSH_USER    (default root)
#   DEST        (default /opt/crabcache)
set -euo pipefail

CRABSCHEME="${CRABSCHEME:?set CRABSCHEME to a crabscheme binary built with --features stdlib-store}"
CC_SRC="$(cd "$(dirname "$0")/../.." && pwd)"     # crab-cache repo root (jepsen/.. )
NODES="${NODES:-n1 n2 n3 n4 n5}"
SSH_USER="${SSH_USER:-root}"
DEST="${DEST:-/opt/crabcache}"

[ -x "$CRABSCHEME" ] || { echo "error: $CRABSCHEME is not an executable" >&2; exit 1; }
[ -d "$CC_SRC/src" ] || { echo "error: $CC_SRC/src not found" >&2; exit 1; }

for n in $NODES; do
  echo ">> $n"
  ssh "$SSH_USER@$n" "mkdir -p $DEST"
  scp -q "$CRABSCHEME" "$SSH_USER@$n:$DEST/crabscheme"
  ssh "$SSH_USER@$n" "chmod +x $DEST/crabscheme"
  rsync -a --delete "$CC_SRC/src/" "$SSH_USER@$n:$DEST/src/"
done

echo "done: crabscheme + src under $DEST on: $NODES"
