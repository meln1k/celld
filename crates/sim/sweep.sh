#!/bin/sh
# Judge a range of swarm seeds against the known failures, on every core.
#
#   crates/sim/sweep.sh BINARY FIRST LAST
#
# Runs seeds FIRST..LAST of `BINARY --swarm --known`, split across the
# machine's cores. Prints a NEW or STALE line for each seed that did not
# fail as crates/sim/known-failures.txt lists, and fails if there is one.
set -eu
bin=$1 first=$2 last=$3
list=$(dirname "$0")/known-failures.txt
jobs=$(nproc)
chunks=$((jobs * 4))
step=$(((last - first + chunks - 1) / chunks))
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
seq 0 $((chunks - 1)) | xargs -P "$jobs" -I{} sh -c '
  a=$(($2 + {} * $4)); b=$((a + $4)); [ "$b" -gt "$3" ] && b=$3
  [ "$a" -lt "$b" ] || exit 0
  "$1" --swarm --known "$5" --seeds "$a..$b" > "$6/{}.txt" 2>/dev/null
' sh "$bin" "$first" "$last" "$step" "$list" "$out" && status=0 || status=1
cat "$out"/*.txt | grep -E '^(NEW|STALE) ' || true
total=$(cat "$out"/*.txt | awk '/seeds did not fail as listed/ {s += $1} END {print s + 0}')
echo "$total of $((last - first)) seeds did not fail as listed"
exit "$status"
