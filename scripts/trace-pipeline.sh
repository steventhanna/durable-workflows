#!/usr/bin/env bash
# Trace-checking pipeline for one backend (docs/TRACE_CHECKING.md):
#   1. drops the dwt_* test databases on the server,
#   2. runs the in-scope suites with the trace-model recorder (fully parallel),
#   3. runs the ignored gap tests named in spec/traces/gaps.yaml (their failures
#      are expected: each one reproduces a known gap),
#   4. dumps every recorded trace, generates the Quint runs, and
#   5. replays them with spec/trace-check.sh --baseline <backend>.
#
# Usage: scripts/trace-pipeline.sh <mysql|postgres> [--suites a,b,...] [--update-baseline]
#   --suites           test binaries to record (default: every in-scope suite, incl. gaps);
#                      a subset is replayed without the baseline check
#   --update-baseline  rewrite spec/traces/expected.json[<backend>] from this run
# Env: DURABLE_WORKFLOWS_TEST_DATABASE_URL (default: the local docker servers of
#      CONTRIBUTING.md), JOBS (trace-check parallelism).
# Needs: cargo, jq, node + `npm ci` in spec/, and the mysql or psql client.
# A failing non-gap test binary is retried once (known timing flakes); a second
# failure fails the pipeline.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$PWD

backend=${1:-}
case "$backend" in
  mysql) default_url=mysql://root:durable@127.0.0.1:33306/mysql ;;
  postgres) default_url=postgres://postgres:durable@127.0.0.1:55432/postgres ;;
  *) echo "usage: $0 <mysql|postgres> [--suites a,b,...] [--update-baseline]" >&2; exit 2 ;;
esac
shift
suites=(activity_execution workflow_activation child_workflow application_cancellation
  continuation_priority workflow_start runtime_shutdown trace_model gaps)
subset=0
update=0
while [ $# -gt 0 ]; do
  case "$1" in
    --suites) IFS=, read -r -a suites <<<"${2:?--suites needs a list}"; subset=1; shift 2 ;;
    --update-baseline) update=1; shift ;;
    *) echo "trace-pipeline: unknown argument $1" >&2; exit 2 ;;
  esac
done
# The baseline counts the full in-scope run, so a subset is checked without it.
check_args=()
if [ "$subset" = 0 ]; then
  check_args=(--baseline "$backend")
  [ "$update" = 0 ] || check_args+=(--update-baseline)
elif [ "$update" = 1 ]; then
  echo "trace-pipeline: --update-baseline needs the full suite list" >&2
  exit 2
fi
url=${DURABLE_WORKFLOWS_TEST_DATABASE_URL:-$default_url}
export DURABLE_WORKFLOWS_TEST_DATABASE_URL=$url
features=durable-workflows/$backend,durable-workflows/fake-clock,durable-workflows/trace-model
logs=$root/target/trace-pipeline-$backend
traces=$root/target/traces-$backend
mkdir -p "$logs"

phase_start=$(date +%s)
phase() {
  local now
  now=$(date +%s)
  echo "== [$((now - phase_start))s] $*"
}

# --- 1. clean ---------------------------------------------------------------
phase "dropping dwt_* databases ($backend)"
case "$backend" in
  mysql)
    re='^mysql://([^:@/]+)(:([^@]*))?@([^:/]+)(:([0-9]+))?/'
    [[ $url =~ $re ]] || { echo "trace-pipeline: cannot parse $url" >&2; exit 2; }
    my_user=${BASH_REMATCH[1]} my_pass=${BASH_REMATCH[3]} my_host=${BASH_REMATCH[4]} my_port=${BASH_REMATCH[6]:-3306}
    my() {
      MYSQL_PWD=$my_pass command mysql --protocol=TCP -h "$my_host" -P "$my_port" -u "$my_user" -N -B -e "$1"
    }
    for db in $(my "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'dwt\\_%'"); do
      my "DROP DATABASE IF EXISTS \`$db\`"
    done
    ;;
  postgres)
    for db in $(psql "$url" -Atc "SELECT datname FROM pg_database WHERE datname LIKE 'dwt\\_%'"); do
      psql "$url" -qc "DROP DATABASE IF EXISTS \"$db\" WITH (FORCE)"
    done
    ;;
esac

# --- 2. in-scope suites -----------------------------------------------------
cargo_test() { cargo test -p durable-workflows --no-default-features --features "$features" "$@"; }

phase "building durable-trace and the test binaries"
cargo build -p durable-trace --no-default-features --features "$features" 2>&1 | tail -n 3
test_args=()
for suite in "${suites[@]}"; do test_args+=(--test "$suite"); done
cargo_test "${test_args[@]}" --no-run 2>&1 | tail -n 3

phase "recording: ${suites[*]}"
if ! cargo_test "${test_args[@]}" --no-fail-fast >"$logs/suites.log" 2>&1; then
  failed=$(grep -oE 'to rerun pass `[^`]*--test [A-Za-z0-9_]+`' "$logs/suites.log" | awk '{print $NF}' | tr -d '`' | sort -u)
  if [ -z "$failed" ]; then
    tail -n 60 "$logs/suites.log"
    echo "trace-pipeline: the suites failed before running (see $logs/suites.log)" >&2
    exit 1
  fi
  grep -E '^test .* FAILED$|^---- ' "$logs/suites.log" || true
  echo "trace-pipeline: retrying once: $failed"
  retry_args=()
  for suite in $failed; do retry_args+=(--test "$suite"); done
  if ! cargo_test "${retry_args[@]}" --no-fail-fast >"$logs/suites-retry.log" 2>&1; then
    grep -E '^test .* FAILED$|^test result:|panicked' "$logs/suites-retry.log" | head -n 40
    echo "trace-pipeline: test failures persist after one retry (see $logs/suites-retry.log)" >&2
    exit 1
  fi
fi
grep -E '^test result:' "$logs/suites.log" | awk '{p+=$4; f+=$6; i+=$8} END {print "   passed", p, "failed", f, "ignored", i}'

# --- 3. gap tests -----------------------------------------------------------
if printf '%s\n' "${suites[@]}" | grep -qx gaps; then
  # Each gaps.yaml key is a glob over the test name; its text before the first
  # `*` is the cargo filter.
  patterns=()
  while IFS= read -r key; do patterns+=("${key%%\**}"); done < <(
    sed -nE 's/^([A-Za-z0-9_*]+):.*/\1/p' spec/traces/gaps.yaml)
  phase "recording ignored gap tests: ${patterns[*]}"
  cargo_test --test gaps -- --ignored "${patterns[@]}" >"$logs/gaps.log" 2>&1 || true
  if ! grep -qE '^test result:' "$logs/gaps.log"; then
    tail -n 60 "$logs/gaps.log"
    echo "trace-pipeline: the gap tests did not run (see $logs/gaps.log)" >&2
    exit 1
  fi
  grep -E '^test .* (ok|FAILED)$' "$logs/gaps.log" | sed 's/^/   /'
fi

# --- 4. dump and generate ---------------------------------------------------
phase "dumping traces to $traces"
rm -rf "$traces"
target/debug/durable-trace dump --server "$url" --out "$traces" 2>"$logs/dump.err"
echo "   $(find "$traces" -name '*.json' | wc -l | tr -d ' ') traces"

phase "generating Quint runs in spec/traces"
find spec/traces -maxdepth 1 -name 'trace_*.qnt' -delete
rm -f spec/traces/index.json
target/debug/durable-trace gen --in "$traces" --out spec/traces \
  --expect-violation spec/traces/gaps.yaml >"$logs/gen.log"

# --- 5. replay --------------------------------------------------------------
phase "replaying (spec/trace-check.sh ${check_args[*]})"
status=0
DURABLE_TRACE=$root/target/debug/durable-trace spec/trace-check.sh ${check_args[@]+"${check_args[@]}"} || status=$?
phase "done (exit $status)"
exit "$status"
