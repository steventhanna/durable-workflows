# Trace checking

Trace checking replays what the engine really did against the Quint model in
[`spec/`](../spec). The design and its reasoning are in
[`design/trace-checking.md`](design/trace-checking.md); this page is the
how-to.

## What it checks

With the test-only feature `trace-model`, every engine transaction that
touches workflow, activity, attempt or event rows writes one row to a
`durable_trace` table in the same transaction: the model action it performs
(`TX1_Start`, `TC1_Claim`, `TW3_Finish`, ...), the values it read, and the
post-image of every row it wrote. Database triggers also record writes that
tests make directly (external writes). Commit order of these rows is the lock
order, so a test's trace is a sequential history of the database.

`durable-trace gen` turns each trace into a Quint module with one run per
record:

```
run step_k = step_{k-1}.then(all { keepPrev, TC1_Claim(1, 0, 1, 1, ...) })
  .expect(and { lastAction == "TC1_Claim", safety, viewWf(db, 1) == { status: "running", ... } })
```

A step passes when the model can take that action from the previous state
(enabledness), the model's view of every touched row equals the recorded
post-image, the recorded events are deliverable, and the safety invariants
hold. A trace passes when its last step passes.

The in-scope suites are `activity_execution`, `workflow_activation`,
`child_workflow`, `application_cancellation`, `continuation_priority`,
`workflow_start`, `runtime_shutdown`, `trace_model` (directed runs) and
`gaps` (the checker's self-test: each gap test must show its violation).

## Running it locally

One command per backend. It drops the `dwt_*` test databases on the server,
records the in-scope suites and the ignored gap tests, dumps, generates, and
replays with the baseline check:

```sh
cd spec && npm ci && cd ..           # once: Quint
scripts/trace-pipeline.sh mysql      # DURABLE_WORKFLOWS_TEST_DATABASE_URL, default
scripts/trace-pipeline.sh postgres   #   the docker servers of CONTRIBUTING.md
scripts/trace-pipeline.sh mysql --suites trace_model,gaps   # a subset (no baseline check)
```

It needs `cargo`, `jq`, Node.js and the `mysql` or `psql` client. Do not run
it while another test run uses the same server: step 1 drops every `dwt_*`
database. The ignored gap tests fail by design and do not fail the script. A
failing non-gap test binary is run once more (the suites have a few known
timing flakes); a second failure fails the script. Logs are in
`target/trace-pipeline-<backend>/`, trace JSON in `target/traces-<backend>/`,
generated modules in `spec/traces/`.

The steps by hand:

```sh
F=durable-workflows/mysql,durable-workflows/fake-clock,durable-workflows/trace-model
cargo test -p durable-workflows --no-default-features --features $F --test trace_model
cargo test -p durable-workflows --no-default-features --features $F --test gaps -- --ignored g2_ n1_
cargo run -p durable-trace --no-default-features --features $F -- \
    dump --server "$DURABLE_WORKFLOWS_TEST_DATABASE_URL" --out target/traces-mysql
cargo run -p durable-trace --no-default-features --features $F -- \
    gen --in target/traces-mysql --out spec/traces --expect-violation spec/traces/gaps.yaml
spec/trace-check.sh                        # every trace in spec/traces/index.json
spec/trace-check.sh --baseline mysql       # and compare the counts with the baseline
```

`trace-check.sh` runs `JOBS` traces at a time (default: the CPU count) and
gives Node a 12 GB heap limit (`NODE_OPTIONS`), which the per-step runs of
traces with hundreds of records need. If those runs still crash, the first
failing step is taken from the source line of the `trace` failure.
Exit codes: 0 all verdicts as expected, 1 a FAIL or a baseline regression, 2
a setup error.

CI runs the pipeline in the `trace-check` job (MySQL 8.4 and Postgres 17)
after the test jobs pass, and uploads the traces, logs and generated modules
when it fails.

## Reading the report

`trace-check.sh` prints one line per trace, sorted by name:

| Verdict | Meaning |
|---|---|
| `pass` | Every step replays in the model. |
| `violation` | A gap trace replays and its gap invariant is violated at the step shown (`confirmed: inv_S24_parentWakes (first at step_7)`). This is the expected result for the tests listed in `spec/traces/gaps.yaml`. |
| `excluded` | The trace is out of the model's scope; the detail is the reason (below). |
| `FAIL` | The code and the model disagree, or a gap trace did not show its violation. |

For a FAIL the line names the first failing step, its record `seq`, actor,
action and class, and the output after the table shows the Quint error, a
field diff, and the recorded record. The class says how much to trust the
divergence:

- `strict`: no other transaction committed while this one ran
  (`begin_seq == seq - 1`). A divergence is a real mismatch between the code
  and the model.
- `concurrent`: another transaction committed in between. Under READ
  COMMITTED the transaction may have read older data than the sequential model
  assumes; look at the reads before blaming the model.
- `external`: a write the test made directly (a trigger-recorded `Env*`
  step), not an engine transaction.

The field diff replays the step in the Quint REPL and lists only what
differs. The model column is the model's state after the action; if the
action is not enabled, the report says so and shows the state before it
(which usually shows why the guard failed):

```
failing step: step_6 (seq 6, rt1:dispatcher TW3_Finish, strict)
action: all { keepPrev, TW3_Finish(1, 1, 2, "succeeded", 1790360347158, 1790360347158) }

expectation / field              model                          recorded
viewWf(db, 1)
  .status                        "ready"                        "waiting_activity"   <-- differs
(29 other expectations or fields agree)
```

To run it alone: `durable-trace report --trace target/traces-mysql/<test>.json
--qnt spec/traces/<module>.qnt --step 6 --quint spec/node_modules/.bin/quint`.

### Exclusion reasons

| Reason | Meaning |
|---|---|
| `unmodeled:<action>` | The code declared an action the model does not have (timers, approvals, admin operations). |
| `unmodeled:wait_kind:<k>`, `unmodeled:event:<typ>` | A row or event kind the model abstracts away. |
| `unmodeled:missing_definition`, `unmodeled:start_available_at` | An engine path the model does not represent (a claim of an unregistered kind, a delayed start). |
| `unsupported_action:<a>`, `unsupported:<...>` | The model has the action, but not this variant; a generator or model extension. |
| `unknown_action` | A transaction ran without declaring its action (a recorder gap). |
| `external_write:<table>:<op>` | The test wrote a table in a way no `Env*` action models. |
| `external_invariant:<inv>` | The test's own external write breaks `<inv>` (a test that corrupts a row on purpose); detected by `trace-check.sh`. |
| `concurrent:<what>` | A known read anomaly the atomic model action cannot replay (`tx2_dedup_race`). |
| `deferred_commit` | A transaction whose commit the trace cannot place. |
| `versions:<kind>` | The trace uses more than one flow version. |
| `model_mismatch:<name>` | Declared in `gaps.yaml` with `exclude:`: a known difference between a gap test and the model's invariant. |
| `malformed:<what>`, `empty`, `missing_topic_cap:<t>` | The trace is broken or empty: a recorder or generator bug to fix, not scope. |

### Expected violations

`spec/traces/gaps.yaml` maps gap-test name globs to the invariant each gap
violates. `gen --expect-violation` marks those traces `expect_violation`; they
must replay without the invariant and then violate it at some step. When a
gap is fixed, its test stops being ignored and its entry leaves `gaps.yaml`;
the trace then has to pass.

## The baseline

`spec/traces/expected.json` holds, per backend, the number of passing traces,
of confirmed violations, and of exclusions per reason (a reason's trailing
`:<seq>` is dropped). `trace-check.sh --baseline <backend>` fails when the
pass or confirmed count drops or when any reason's count rises, so scope
cannot shrink silently. When the counts improve it says so and still passes.

After a change that moves traces on purpose (a new model action, a new
exclusion rule, a new test in a suite), refresh the baseline from a green
run on each backend and commit the file:

```sh
scripts/trace-pipeline.sh mysql --update-baseline
scripts/trace-pipeline.sh postgres --update-baseline
# or, on already generated traces: spec/trace-check.sh --baseline mysql --update-baseline
```

`--update-baseline` refuses to write while any trace FAILs.

## Adding a directed trace

1. Add a `#[tokio::test]` to `durable-workflows/tests/trace_model.rs` (the
   file is compiled only with `trace-model`). Use `support::fresh_pool()`,
   name runtimes `rt<N>:coordinator` / `rt<N>:dispatcher`, drive the scenario,
   and leave the database in place (the dump reads it).
2. Keep it deterministic: one runtime unless the point is contention, and
   `fake-clock` for time. Steps classed `concurrent` need a human look.
3. Run `scripts/trace-pipeline.sh <backend> --suites trace_model` and read
   its verdict. An `excluded` verdict names what the model or generator lacks.
4. Refresh the baseline on both backends and commit it with the test.

A new existing-suite test is picked up automatically; it only needs the
baseline refreshed.
