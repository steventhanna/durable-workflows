#!/usr/bin/env bash
# Runs every mechanical check that ties the code to docs/INVARIANTS.md, in an
# order that keeps them from disturbing each other (see the verify-invariants
# skill in .claude/skills/ for the loop around it).
#
# Usage: scripts/verify-invariants.sh [--quick|--full] [--backend mysql|postgres|both]
#   --quick  (default) compiler checks, both test suites, the Quint checks at
#            the quick budget (spec/check.sh --quick, about 5 minutes; an
#            unreached witness is a WARN), and trace checking on each backend
#   --full   the same, but the Quint simulation at the full budget
#            (spec/check.sh --full; an unreached witness fails) and Apalache
#            bounded model checking (spec/verify.sh, depth 3); run before a
#            push or a release. Keeps the rewritten spec/results/.
#   --backend  limit the test and trace stages to one backend (default both)
# Env: MYSQL_URL, POSTGRES_URL (default: the local docker servers of
#      CONTRIBUTING.md).
#
# Stages run one at a time: trace-pipeline.sh drops every dwt_* database on its
# server, and CPU contention breaks the timing-sensitive tests. Every stage runs
# even after a failure; the summary at the end lists each result, and the exit
# status is 1 if any stage failed. Logs go to target/verify-invariants/.
set -uo pipefail
cd "$(dirname "$0")/.."

mode=quick
backends="mysql postgres"
while [ $# -gt 0 ]; do
  case "$1" in
    --quick) mode=quick ;;
    --full) mode=full ;;
    --backend)
      shift
      case "${1:-}" in
        mysql | postgres) backends=$1 ;;
        both) backends="mysql postgres" ;;
        *) echo "--backend takes mysql, postgres or both" >&2; exit 2 ;;
      esac
      ;;
    *) echo "usage: $0 [--quick|--full] [--backend mysql|postgres|both]" >&2; exit 2 ;;
  esac
  shift
done

MYSQL_URL=${MYSQL_URL:-mysql://root:durable@127.0.0.1:33306/mysql}
POSTGRES_URL=${POSTGRES_URL:-postgres://postgres:durable@127.0.0.1:55432/postgres}
logs=target/verify-invariants
mkdir -p "$logs"
summary=""
failed=0

url_for() { if [ "$1" = mysql ]; then echo "$MYSQL_URL"; else echo "$POSTGRES_URL"; fi; }

stage() { # name command...
  local name=$1 log="$logs/$1.log" start
  shift
  start=$(date +%s)
  printf '== %s ... ' "$name"
  if "$@" >"$log" 2>&1; then
    printf 'ok (%ss)\n' "$(($(date +%s) - start))"
    summary="${summary}ok    ${name}\n"
  else
    printf 'FAILED (%ss) — %s\n' "$(($(date +%s) - start))" "$log"
    summary="${summary}FAIL  ${name}  (${log})\n"
    failed=1
  fi
}

features() { echo "durable-workflows/$1,durable-workflows/fake-clock${2:+,durable-workflows/$2}"; }

# 1. The compiler: formatting, lints in every feature combination, docs.
stage fmt cargo fmt --all --check
for b in mysql postgres; do
  stage "clippy-$b" cargo clippy --workspace --all-targets --no-default-features \
    --features "$(features "$b")" -- -D warnings
  stage "clippy-$b-trace" cargo clippy --workspace --all-targets --no-default-features \
    --features "$(features "$b" trace-model)" -- -D warnings
done
stage doc env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps \
  --no-default-features --features durable-workflows/mysql

# 2. The test suites (including the compile-fail UI cases), one backend at a time.
for b in $backends; do
  stage "test-$b" env DURABLE_WORKFLOWS_TEST_DATABASE_URL="$(url_for "$b")" \
    cargo test --workspace --all-targets --no-default-features --features "$(features "$b")"
done

# 3. The Quint model on its own: typecheck, directed tests, simulation.
# check.sh fails on a failing directed test, a violated `hold` row, a quint error
# and, in --full, a `violate` witness its budget did not reach. In --quick an
# unreached witness is a WARN: the stage stays ok and the summary lists it.
if [ "$mode" = full ]; then
  stage quint-simulation sh -c 'cd spec && ./check.sh --full'
  stage apalache sh -c 'cd spec && ./verify.sh 3 && ! grep -vE "No violation found" results/apalache_summary.txt'
else
  stage quint-simulation sh -c 'cd spec && ./check.sh --quick'
  git checkout -q -- spec/results 2>/dev/null || true
fi
quint_notes=$(sed -n '/^Not reached or failed:/,$p' "$logs/quint-simulation.log" | grep -E '^(WARN|MISS|FAIL) ' || true)
if [ -n "$quint_notes" ]; then
  summary="${summary}$(printf '%s\n' "$quint_notes" | sed 's/^/      quint /')\n"
fi

# 4. Trace checking: the code's recorded runs replayed through the model.
for b in $backends; do
  stage "trace-$b" env DURABLE_WORKFLOWS_TEST_DATABASE_URL="$(url_for "$b")" \
    scripts/trace-pipeline.sh "$b"
done

printf '\n%b' "$summary"
if [ -f spec/results/summary.txt ]; then
  echo "Quint summary: spec/results/summary.txt (quick mode restores the committed copy)"
fi
exit "$failed"
