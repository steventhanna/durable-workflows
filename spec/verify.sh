#!/usr/bin/env bash
# Bounded model checking with Apalache (via `quint verify`). Slow: minutes per step count.
# Usage: ./verify.sh [steps]   (default 3; depth 4 takes more than 2 hours)
set -u
cd "$(dirname "$0")"
STEPS=${1:-3}
mkdir -p results
SUM=results/apalache_summary.txt
: > "$SUM"
check() { # module kind name steps
  local mod=$1 kind=$2 name=$3 steps=$4 out="results/apalache_${1}_${3}_${4}.txt" start=$(date +%s)
  npx quint verify durable_mc.qnt --main "$mod" "--$kind" "$name" --max-steps "$steps" > "$out" 2>&1
  printf "apalache %-12s %-9s %-12s max-steps=%-2s %-20s %ss\n" "$mod" "$kind" "$name" "$steps" \
    "$(grep -oE 'No violation found|Found an issue|^error'  "$out" | head -1)" "$(( $(date +%s) - start ))" | tee -a "$SUM"
}
check durable_mc invariant safetyRc "$STEPS"
