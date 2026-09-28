#!/usr/bin/env bash
# Runs every check reported in README.md: typecheck, directed tests, random simulation.
# Usage: ./check.sh [--quick|--full]    (default --full)
#   --quick  a smoke budget, about 5 minutes on a 12-core machine
#            (scripts/verify-invariants.sh runs it for every change)
#   --full   the thorough budget README.md reports (before a push or a release)
#
# Every simulation row states an expectation:
#   hold     an invariant. A violation, or a quint error, fails the run.
#   violate  a witness (non-vacuity) or a gap: sampling must reach a state that
#            violates it. The budget is a cap; a run stops at the first hit.
#            The --full cap is at least 7x the measured mean samples to a hit,
#            so a miss fails the run. The --quick cap is smaller: a miss is a
#            WARN, listed at the end, and does not fail the run.
#   seek     random simulation has not reached it at any budget tried; the named
#            directed test (run above) asserts the violation and is the evidence.
#            A miss is listed at the end and never fails. --full only.
# A row whose quick budget is 0 runs only in --full. Writes results/summary.txt;
# the exit status is 1 when something failed.
set -u
cd "$(dirname "$0")"
MODE=full
case "${1:-}" in
  --quick) MODE=quick ;;
  --full | "") MODE=full ;;
  *) echo "usage: $0 [--quick|--full]" >&2; exit 2 ;;
esac
Q="npx quint"
mkdir -p results
SUM=results/summary.txt
: > "$SUM"
failed=0
notes=""

sim() { # module property expect quick_samples full_samples steps [evidence]
  local mod=$1 inv=$2 expect=$3 steps=$6 evidence=${7:-} samples out start verdict stat len status
  if [ "$MODE" = quick ]; then samples=$4; else samples=$5; fi
  [ "$samples" -gt 0 ] || return 0
  out="results/${mod}__${inv}$([ "$steps" = 40 ] || echo "__$steps").txt"
  start=$(date +%s)
  $Q run durable_mc.qnt --main "$mod" --invariant "$inv" --max-samples "$samples" --max-steps "$steps" > "$out" 2>&1
  verdict=$(grep -oE "No violation found|Found an issue|error" "$out" | head -1)
  stat=$(grep -oE "\([0-9]+ms at [0-9]+ traces/second\)" "$out" | head -1)
  len=$(grep -c "^\[State" "$out")
  case "$expect:$verdict" in
    "hold:No violation found" | "violate:Found an issue" | "seek:Found an issue") status=ok ;;
    "violate:No violation found")
      if [ "$MODE" = full ]; then status=FAIL; failed=1; else status=WARN; fi
      notes="${notes}${status}  $mod $inv: not reached in $samples samples x $steps steps\n" ;;
    "seek:No violation found")
      status=MISS
      notes="${notes}MISS  $mod $inv: not reached in $samples samples x $steps steps (evidence: $evidence)\n" ;;
    *)
      status=FAIL; failed=1
      notes="${notes}FAIL  $mod $inv: expect=$expect got=${verdict:-nothing} ($out)\n" ;;
  esac
  printf "%-4s %-18s %-24s expect=%-7s got=%-18s samples=%-6s steps=%-2s trace_len=%-3s %s %ss\n" \
    "$status" "$mod" "$inv" "$expect" "${verdict// /_}" "$samples" "$steps" "$len" "$stat" \
    "$(($(date +%s) - start))" | tee -a "$SUM"
}

echo "mode $MODE" | tee -a "$SUM"
if $Q typecheck durable_mc.qnt > /dev/null; then
  echo "typecheck ok" | tee -a "$SUM"
else
  echo "typecheck FAILED" | tee -a "$SUM"; failed=1; notes="${notes}FAIL  typecheck\n"
fi
for m in durable_tests durable_tests_rr durable_tests_drift durable_tests_env; do
  if ! $Q test durable_tests.qnt --main "$m" > "results/tests__$m.txt" 2>&1; then
    failed=1; notes="${notes}FAIL  directed tests $m (results/tests__$m.txt)\n"
  fi
  grep -E "ok |failed|passing|failing" "results/tests__$m.txt" | tee -a "$SUM"
done

#   module             property                expect  quick  full    steps
# expected to hold (the code: READ COMMITTED)
sim durable_mc         safety                  hold    2000   20000   40
sim durable_mc         safetyRc                hold    2000   20000   40 # + S17 at claim and between commits (G7 closed)
sim durable_mc_act     safetyRc                hold    2000   20000   40 # activity-only: more T-W1 interleavings per sample
sim durable_mc_env     safety                  hold    2000   20000   40 # external writes (invalid bounds) break nothing else
sim durable_mc_rr      safety                  hold    2000   20000   40 # historical REPEATABLE READ T-W1 (pre-P4)
# witnesses (non-vacuity)
sim durable_mc_env     wit_quarantined         violate 2000   20000   40 # G10 fixed: an invalid row is quarantined
sim durable_mc         wit_S3_concurrentStep   violate 2000   20000   40
sim durable_mc         wit_childSucceeded      violate 2000   20000   40
sim durable_mc         wit_coordFenceMiss      violate 2000   20000   40
sim durable_mc         wit_tw1Interleaved      violate 2000   20000   40
sim durable_mc         wit_pausedActivity      violate 2000   20000   40
# The activity path. durable_mc reaches these in fewer than 1 in 10^4 samples;
# durable_mc_act (no children, no crashes) 10 to 100 times as often. Measured
# mean samples to a hit: reconciled ~1500, blocked ~9000, activitySucceeded ~30000.
sim durable_mc_act     wit_reconciled          violate 20000  20000   40
sim durable_mc_act     wit_blocked             violate 40000  100000  40
sim durable_mc_act     wit_activitySucceeded   violate 0      250000  40
# Not reached by random simulation: 2000-20000 samples x 40 and 80 steps, and
# 6000-18000 fixed-seed samples on durable_mc and on the activity-only instances.
# 40 steps: at 80 steps these two rows ran at ~20 samples/s against ~150 at 40
# (32 minutes together; likely the exhaustion the old durable_mc_act showed, see
# durable_mc.qnt) and never reached either property.
sim durable_mc_rr      inv_S17_capAlways       seek    0      20000   40 g7CapExceededTest
sim durable_mc_act_rr  inv_S17_capAlways       seek    0      20000   40 g7CapExceededTest
sim durable_mc_act_rr  inv_S17_capAtClaim      seek    0      20000   40 g7CapExceededTest
sim durable_mc_drift   inv_S13_oneHandler      seek    0      20000   40 driftTwoHandlersTest
sim durable_mc         wit_revivedLease        seek    0      20000   40 g7ClosedUnderRcTest

if [ -n "$notes" ]; then printf '\nNot reached or failed:\n%b' "$notes" | tee -a "$SUM"; fi
exit "$failed"
