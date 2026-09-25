#!/usr/bin/env bash
# Runs every check reported in README.md. Usage: ./check.sh [samples] [steps]
set -u
cd "$(dirname "$0")"
SAMPLES=${1:-20000}
STEPS=${2:-40}
Q="npx quint"
mkdir -p results
SUM=results/summary.txt
: > "$SUM"

run() { # module invariant expectation (uses $STEPS)
  local mod=$1 inv=$2 expect=$3 out="results/${1}__${2}.txt"
  local start=$(date +%s)
  $Q run durable_mc.qnt --main "$mod" --invariant "$inv" --max-samples "$SAMPLES" --max-steps "$STEPS" > "$out" 2>&1
  local verdict=$(grep -oE "No violation found|Found an issue|error" "$out" | head -1)
  local stat=$(grep -oE "\([0-9]+ms at [0-9]+ traces/second\)" "$out" | head -1)
  local len=$(grep -c "^\[State" "$out")
  printf "%-18s %-32s expect=%-9s got=%-20s trace_len=%-3s %s %ss\n" \
    "$mod" "$inv" "$expect" "$verdict" "$len" "$stat" "$(( $(date +%s) - start ))" | tee -a "$SUM"
}

$Q typecheck durable_mc.qnt >/dev/null && echo "typecheck ok" | tee -a "$SUM"
for m in durable_tests durable_tests_rr durable_tests_drift durable_tests_env; do
  $Q test durable_tests.qnt --main "$m" 2>&1 | grep -E "ok |failed|passing|failing" | tee -a "$SUM"
done

# expected to hold (the code: READ COMMITTED)
run durable_mc safety hold
run durable_mc safetyRc hold                      # safety + S17 at claim and between commits (G7 closed)
run durable_mc inv_S24_exceptTX2 hold
run durable_mc_act safetyRc hold                  # activity-only instance: more T-W1 interleavings per sample
run durable_mc inv_G10_noClaimAbort hold          # G10 needs an invalid row: none without external writes
run durable_mc_env safety hold                    # external writes (invalid bounds) break no other invariant
# historical REPEATABLE READ T-W1 (pre-P4)
run durable_mc_rr safety hold
run durable_mc_rr inv_S17_capAlways violate       # G7
run durable_mc_act_rr inv_S17_capAlways violate   # G7
run durable_mc_act_rr inv_S17_capAtClaim violate  # G7
# suspected gaps (expected to be violated)
run durable_mc inv_S24_parentWakes violate        # G2
run durable_mc inv_G11_cancelReachesChildren violate
run durable_mc_env inv_G10_noClaimAbort violate   # G10: one invalid row aborts every topic's claims
run durable_mc inv_N1_tx2OwnLineage violate       # N1
run durable_mc inv_S13_topicConcurrency violate   # N2
run durable_mc_act inv_S13_topicConcurrency violate
run durable_mc inv_S19_sourceTerminal violate     # N3
run durable_mc inv_G1_noSelfCancelFromOperator violate  # G1: operator pause/cancel during a claim
run durable_mc_drift inv_S13_oneHandler violate   # S13 clock assumption
# witnesses (non-vacuity; expected to be violated)
for w in wit_S3_concurrentStep wit_blocked wit_childSucceeded wit_activitySucceeded wit_revivedLease \
         wit_coordFenceMiss wit_reconciled wit_tw1Interleaved wit_selfCancelled wit_pausedActivity; do
  run durable_mc "$w" violate
done
# deeper runs for properties whose traces need more steps now that T-W1 takes ~5 steps
STEPS2=${3:-80}
for spec in "durable_mc_drift inv_S13_oneHandler" "durable_mc inv_S13_topicConcurrency" "durable_mc wit_revivedLease" "durable_mc inv_S24_parentWakes"; do
  set -- $spec
  STEPS=$STEPS2 run "$1" "$2" violate
done
