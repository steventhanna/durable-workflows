#!/usr/bin/env bash
# Replays every trace listed in <dir>/index.json (written by `durable-trace gen`)
# through the Quint model, JOBS traces at a time. `expect_pass` traces must
# pass `run trace`; on a failure the per-step runs name the first failing step
# and `durable-trace report` prints a field-level model-vs-recorded diff. When
# that step is an external write whose own state breaks an invariant, the
# trace is reported as excluded (`external_invariant:<inv>`). `expect_violation`
# traces must pass `run trace` (every step, without the gap invariant) and some
# `viol_k` run (the gap invariant is violated after step k).
#
# Usage: ./trace-check.sh [--dir traces] [--baseline <mysql|postgres> [--update-baseline]]
#   --baseline B        also compare the counts with traces/expected.json[B]: fail if
#                       pass or confirmed counts drop or any exclusion reason's count rises
#   --update-baseline   rewrite traces/expected.json[B] from this run (needs no FAIL)
# Env: JOBS (default: CPU count), QUINT (default: node_modules/.bin/quint),
#      DURABLE_TRACE (the durable-trace binary for field diffs; default
#      ../target/debug/durable-trace when built).
# Needs jq and `npm ci` in spec/. See docs/TRACE_CHECKING.md.
# Exit: 0 all expected verdicts (and baseline held), 1 any FAIL or baseline
# regression, 2 setup error.
set -euo pipefail
self="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$self")"

if [ -z "${QUINT:-}" ]; then
  if [ -x node_modules/.bin/quint ]; then QUINT="$PWD/node_modules/.bin/quint"; else QUINT="npx quint"; fi
fi
if [ -z "${DURABLE_TRACE:-}" ] && [ -x ../target/debug/durable-trace ]; then
  DURABLE_TRACE="$PWD/../target/debug/durable-trace"
fi
# The per-step runs of a long trace (hundreds of records) need more than
# Node's default heap.
NODE_OPTIONS=${NODE_OPTIONS:---max-old-space-size=12288}
export QUINT DURABLE_TRACE NODE_OPTIONS

# ---------------------------------------------------------------------------
# Worker: checks trace number $2 of $TRACE_DIR/index.json and writes
# $TRACE_OUT/<module>.row (`name|verdict|detail|reason key`) plus
# $TRACE_OUT/<module>.err (diagnostics for a FAIL).
if [ "${1:-}" = --worker ]; then
  n=$2
  dir=$TRACE_DIR
  out=$TRACE_OUT
  index=$dir/index.json
  IFS=$'\t' read -r module file verdict source reason expect < <(
    jq -r ".traces[$n] | [.module, .file, .verdict, .source, (.reason // \"-\"), (.expect_violation // \"-\")] | @tsv" "$index")
  logs=$out/$module
  err=$out/$module.err
  : >"$err"
  row() { printf '%s|%s|%s|%s\n' "$module" "$1" "$2" "${3:--}" >"$out/$module.row"; }
  qtest() { $QUINT test "$dir/$file" --main "$module" --match "$1" >"$2" 2>&1; }

  # Names the first failing step and writes a FAIL row, or an excluded row
  # when an external step breaks an invariant by itself.
  analyse_failure() {
    local prefix=$1 step seq class action broken
    qtest '^step_' "$logs.steps.log" || true
    step=$(grep -E 'step_[0-9]+ failed' "$logs.steps.log" \
      | sed -E 's/.*step_([0-9]+) failed.*/\1/' | sort -n | head -n 1 || true)
    # Long traces can crash Quint in the per-step runs (one run per prefix);
    # the `trace` failure's source line then names the step.
    if [ -z "$step" ]; then
      local line
      line=$(grep -oE "$file:[0-9]+:" "$logs.log" | head -n 1 | cut -d: -f2 || true)
      if [ -n "$line" ]; then
        step=$(head -n "$line" "$dir/$file" | grep -oE 'run step_[0-9]+ ' | tail -n 1 | grep -oE '[0-9]+' || true)
      fi
    fi
    if [ -z "$step" ]; then
      row FAIL "${prefix}no step failed on its own; see quint output"
      cat "$logs.log" >>"$err"
      return
    fi
    seq=$(jq -r ".traces[$n].step_seqs[$((step - 1))]" "$index")
    class=$(jq -r ".traces[$n].step_class[$((step - 1))] // \"?\"" "$index")
    action=$(jq -r ".records[] | select(.seq == $seq) | \"\(.actor) \(.action)\"" "$source" 2>/dev/null || echo "?")
    if [ "$class" = external ]; then
      qtest "^extinv_${step}_" "$logs.ext.log" || true
      broken=$(grep -oE "extinv_${step}_[A-Za-z0-9_]+ failed" "$logs.ext.log" \
        | sed -E "s/^extinv_${step}_//; s/ failed$//" | sort -u | paste -sd, - || true)
      if [ -n "$broken" ]; then
        row excluded "external_invariant:$broken (step_$step, seq $seq)" "external_invariant:$broken"
        return
      fi
    fi
    row FAIL "${prefix}first failing step: step_$step (seq $seq, $action, $class)"
    {
      echo "---- quint says:"
      sed -n '/^  1) step_/,/^  2) /p' "$logs.steps.log" | grep -vE '^\s+\^+$|^  2\) ' | head -n 30 || true
      if [ -n "${DURABLE_TRACE:-}" ]; then
        echo "---- field diff (model vs recorded):"
        "$DURABLE_TRACE" report --trace "$source" --qnt "$dir/$file" --step "$step" --quint "${QUINT%% *}" 2>&1 \
          || echo "(durable-trace report failed)"
      else
        echo "---- no field diff: build durable-trace or set DURABLE_TRACE"
      fi
      echo "---- recorded record (seq $seq):"
      jq ".records[] | select(.seq == $seq)" "$source" 2>/dev/null || echo "(trace $source not readable)"
    } >>"$err"
  }

  case "$verdict" in
    excluded)
      # A reason's trailing seq (`malformed:hb:17`) is not part of its key.
      row excluded "$reason" "$(sed -E 's/:[0-9]+$//' <<<"$reason")"
      ;;
    expect_pass)
      if qtest '^trace$' "$logs.log"; then row pass ""; else analyse_failure ""; fi
      ;;
    expect_violation)
      if qtest '^trace$' "$logs.log"; then
        qtest '^viol_' "$logs.viol.log" || true
        at=$(grep -oE 'ok viol_[0-9]+' "$logs.viol.log" | sed 's/ok viol_//' | sort -n | head -n 1 || true)
        if [ -n "$at" ]; then
          row violation "confirmed: $expect (first at step_$at)"
        else
          row FAIL "violation of $expect not observed at any step"
        fi
      else
        analyse_failure "expecting $expect: "
      fi
      ;;
    *)
      row FAIL "unknown verdict '$verdict'"
      ;;
  esac
  exit 0
fi

# ---------------------------------------------------------------------------
dir=traces
backend=
update=0
while [ $# -gt 0 ]; do
  case "$1" in
    --dir) dir=${2:?--dir needs a directory}; shift 2 ;;
    --baseline) backend=${2:?--baseline needs mysql or postgres}; shift 2 ;;
    --update-baseline) update=1; shift ;;
    -h | --help) sed -n '2,20p' "$self"; exit 0 ;;
    *) echo "trace-check: unknown argument $1" >&2; exit 2 ;;
  esac
done
dir=${dir%/}
baseline_file=traces/expected.json
index=$dir/index.json
if [ ! -f "$index" ]; then
  echo "trace-check: $index not found; run \`durable-trace gen --out spec/$dir\` first" >&2
  exit 2
fi
command -v jq >/dev/null || { echo "trace-check: jq is required" >&2; exit 2; }
if [ "$update" = 1 ] && [ -z "$backend" ]; then
  echo "trace-check: --update-baseline needs --baseline <backend>" >&2
  exit 2
fi
if [ -n "$backend" ]; then
  seen=$(jq -r '[.traces[].backend] | unique | join(",")' "$index")
  if [ "$seen" != "$backend" ]; then
    echo "trace-check: $index holds $seen traces, not $backend" >&2
    exit 2
  fi
fi

jobs=${JOBS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)}
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
count=$(jq '.traces | length' "$index")
start=$(date +%s)
if [ "$count" -gt 0 ]; then
  seq 0 $((count - 1)) | TRACE_DIR=$dir TRACE_OUT=$out xargs -P "$jobs" -I{} "$self" --worker {}
fi

# A worker that died without a row is a FAIL.
jq -r '.traces[].module' "$index" | while read -r module; do
  [ -f "$out/$module.row" ] || printf '%s|FAIL|worker wrote no verdict|-\n' "$module" >"$out/$module.row"
done
if [ "$count" -gt 0 ]; then LC_ALL=C sort "$out"/*.row >"$out/rows.tsv"; else : >"$out/rows.tsv"; fi

echo
printf '%-70s %-9s %s\n' TRACE VERDICT DETAIL
while IFS='|' read -r name verdict detail _; do
  printf '%-70s %-9s %s\n' "$name" "$verdict" "$detail"
done <"$out/rows.tsv"

while IFS='|' read -r name verdict _; do
  if [ "$verdict" = FAIL ] && [ -s "$out/$name.err" ]; then
    echo "==== $name" >&2
    cat "$out/$name.err" >&2
  fi
done <"$out/rows.tsv"

counts=$(jq -Rn '[inputs | split("|")] as $rows | {
  pass: ($rows | map(select(.[1] == "pass")) | length),
  violation_confirmed: ($rows | map(select(.[1] == "violation")) | length),
  fail: ($rows | map(select(.[1] == "FAIL")) | length),
  excluded: ($rows | map(select(.[1] == "excluded")) | group_by(.[3])
             | map({key: .[0][3], value: length}) | from_entries)
}' "$out/rows.tsv")
passed=$(jq .pass <<<"$counts")
confirmed=$(jq .violation_confirmed <<<"$counts")
failed=$(jq .fail <<<"$counts")
excluded=$(jq '[.excluded[]] | add // 0' <<<"$counts")
echo
echo "pass: $passed  violation confirmed: $confirmed  FAIL: $failed  excluded: $excluded  ($count traces, $jobs jobs, $(($(date +%s) - start))s)"

status=0
[ "$failed" -eq 0 ] || status=1
[ -n "$backend" ] || exit "$status"

current=$(jq '{pass, violation_confirmed, excluded}' <<<"$counts")
if [ "$update" = 1 ]; then
  if [ "$failed" -ne 0 ]; then
    echo "trace-check: not updating spec/$baseline_file: $failed FAIL" >&2
    exit 1
  fi
  [ -f "$baseline_file" ] || echo '{}' >"$baseline_file"
  jq --arg b "$backend" --argjson c "$current" '.[$b] = $c' "$baseline_file" >"$out/expected.json"
  mv "$out/expected.json" "$baseline_file"
  echo "trace-check: wrote $backend counts to spec/$baseline_file"
  exit 0
fi

base=$(jq --arg b "$backend" '.[$b] // empty' "$baseline_file" 2>/dev/null || true)
if [ -z "$base" ]; then
  echo "trace-check: no $backend entry in spec/$baseline_file; run with --update-baseline" >&2
  exit 2
fi
problems=$(jq -rn --argjson cur "$current" --argjson base "$base" '
  (if $cur.pass < $base.pass then "pass dropped: \($base.pass) -> \($cur.pass)" else empty end),
  (if $cur.violation_confirmed < $base.violation_confirmed
   then "violation confirmed dropped: \($base.violation_confirmed) -> \($cur.violation_confirmed)" else empty end),
  ($cur.excluded | to_entries[] | select(.value > ($base.excluded[.key] // 0))
   | "excluded \(.key) rose: \($base.excluded[.key] // 0) -> \(.value)")')
if [ -n "$problems" ]; then
  echo "trace-check: counts regressed against spec/$baseline_file ($backend):" >&2
  sed 's/^/  /' <<<"$problems" >&2
  echo "  (if the change is intended, rerun with --update-baseline and commit the file)" >&2
  exit 1
fi
if [ "$(jq -S . <<<"$current")" != "$(jq -S '{pass, violation_confirmed, excluded}' <<<"$base")" ]; then
  echo "trace-check: counts improved on spec/$baseline_file ($backend); tighten it with --update-baseline"
fi
echo "trace-check: baseline held ($backend)"
exit "$status"
