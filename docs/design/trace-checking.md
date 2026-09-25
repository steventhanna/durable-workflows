# Design: trace checking the engine against the Quint model

Status: accepted (2026-09-25). Goal: record what the real engine does in
tests, then replay each recording through the Quint model in `spec/` and
check that (a) every step is an action the model allows, (b) the model's
post-state equals the abstraction of the recorded post-state, and (c) the
model's invariants hold after each step.

`CRATE` = `durable-workflows/`. Line numbers are as of commit 4866070.

## 0. Decisions

| Problem | Decision |
|---|---|
| Commit order | A `durable_trace` table. Every library transaction inserts one trace row as its **last statement before COMMIT**; the row's auto-increment `seq` is the trace order. Local (non-DB) steps insert their own autocommit row. A trace row commits iff its transaction commits. |
| Post-state | Post-images of the rows the transaction **touched**, read inside the transaction after all writes (own writes are final under row locks), stored in the trace row. |
| Action + parameters | Declared at each transaction's code site (`trace::declare(..)`) from the transaction's own inputs and outputs. Touched rows come from the shared write helpers plus the sites. No before/after diffing. |
| Unmodeled transactions | Recorded as `Unmodeled{name}`. A trace with an unmodeled write to a modeled table is **excluded** (reported with a reason), not failed. Unmodeled writes to unmodeled tables only are stutters. |
| Replay | A Rust tool generates one Quint file per trace: `run step_k = step_{k-1}.then(all{keepPrev, Action(params)}).expect(match_k and safety)`, checked with `quint test`. Quint 0.32 has no trace-input mode, so generated runs are the exact replay. |
| Recording | Test-only cargo feature `trace-model`. Hook in `dialect::transaction` (both backends) plus the four outermost-capable `connection.transaction` sites in `store.rs`. Task-local scope. Off = inline no-ops. |
| Workloads | (1) directed `tests/trace_model.rs`; (2) existing in-scope tests with zero edits (test name from the test thread name); (3) `tests/gaps.rs` G2, G11, N1, N2, G10 as checker self-test (must show the violation); (4) seeded concurrent driver `tests/trace_workload.rs`. |
| CI | Job `trace-check` on MySQL 8.4 and Postgres 17 after the test jobs, 10-minute budget; failure report names the step, action, and a field diff. Nightly workload run. |

## 1. Facts the design rests on

- Every library-owned transaction goes through `crate::dialect::transaction`
  (29 sites). Outermost-capable exceptions: `DurableStore::cancel_with_conn`
  (`store.rs:81-103`), `start_with_conn` (`:140-152`),
  `start_or_restart_recoverable_with_conn` (`:211-336`),
  `start_prepared_with_conn` (`:357-369`); through `start` /
  `start_or_restart_recoverable` they are savepoints (`:116`, `:187`).
- Fences return `DurableError::FencedWrite` and roll back
  (`coordinator.rs:911-917`, `activity_worker.rs:1275-1281`); the coordinator
  propagates them (`coordinator.rs:217`, G1).
- Each transaction samples `now` once (`persistence::database_now_millis`).
- Deliverable events are appended only via `persistence::append_event`
  (`events.rs:40-49`) and the `started` insert in `insert_started`
  (`workflows.rs:35-48`). Parent wakes go through
  `wake_loaded_parent_on_child_terminal` (`workflows.rs:154-245`); activity
  cancels through `cancel_activities` (`store.rs:611-647`).
- Test DBs are fresh per test; the tokio test body runs on the test thread,
  which libtest names after the test path.
- Coordinator id `"{runtime_id}:coordinator"` (`supervisor.rs:928`),
  dispatcher `"{runtime_id}:dispatcher"` (`:1040`).
- Ids have gaps (dedup hits and rollbacks consume ids); the model assigns
  dense ids, so the checker maps real ids to model ids. Tokens are UUIDs; the
  checker interns them by first appearance.
- Quint 0.32 CLI: `test`, `run`, `verify`; runs can reference other runs;
  no command takes a trace as input.
- diesel-async exposes transaction depth via
  `TransactionManager::transaction_manager_status_mut(conn)`.
- bb8 0.9.1 `Builder::connection_customizer` / `on_acquire` (for a shared
  fake clock in the workload driver).

## 2. Commit order under concurrency

Chosen: **DB-assigned sequence from a trace row inserted as the transaction's
last statement.**

- For two transactions that conflict on any row lock, the one that inserts
  its trace row later acquired the contended lock later, so the other had
  already committed or aborted: recorded order = lock order for every
  write-write and locking-read conflict. Under READ COMMITTED nothing else
  orders transactions.
- Non-conflicting transactions may commit in the other order; they touch
  disjoint rows and their reads did not see each other, so the model cannot
  tell.
- A rolled-back transaction loses its trace row. A lost COMMIT
  acknowledgement resolves itself: the row exists iff the COMMIT landed. The
  recorder also writes a local `TW2_Drop` when a heartbeat future is dropped;
  the generator discards it when a `TW2_Commit` with the same heartbeat id
  exists.
- Remaining ambiguity is consistent reads made before a later-committed
  transaction (the G7 class). The recorder records the **observed values**
  of effect-relevant reads (`in_flight`, candidates, reconciled lists) and
  passes them as parameters; invariants judge the consequence. Each row also
  stores `begin_seq` (`SELECT MAX(seq)` as the transaction's first
  statement): `begin_seq == seq - 1` → divergence is `strict` (real
  model/code mismatch); otherwise `concurrent` (read anomaly, needs a human
  look).

Rejected: in-process counter at commit (fetch races the commit); Postgres
xids / commit timestamps (not commit order, no MySQL analogue); a global
serializing mutex (removes the concurrency the model exists to check; kept as
debug flag `DURABLE_TRACE_SERIALIZE=1`); effects-only with order search
(exponential).

Still exercised: N runtimes with real lock contention, SKIP LOCKED races,
stale-lease coordinator races, heartbeat vs reconcile, crash by dropping
tasks, fence misses.

## 3. Recording in the Rust code

Feature `trace-model = []` next to `fake-clock`. New module
`CRATE/src/trace/` (feature on) and `CRATE/src/trace/noop.rs` with the same
signatures as `#[inline(always)]` no-ops (feature off).

```rust
// Opens a trace scope if none is active on this task; nested calls pass through.
pub(crate) async fn scoped<R, E, F>(conn: &mut DurableConnection, f: F) -> Result<R, E>;
pub(crate) fn declare(action: impl FnOnce() -> Action);       // once per scope; closure not evaluated when off
pub(crate) fn touch_wf(id: i64); pub(crate) fn touch_act(id: i64);
pub(crate) fn touch_att(act: i64, n: i32);
pub(crate) fn touch_event(wf: i64, dseq: i32, typ: &str);     // deliverable events only
pub(crate) fn note(key: &str, value: serde_json::Value);     // observed reads
pub async fn record_local(pool: &DurablePool, actor: &str, action: Action);
pub async fn begin_trace(conn: &mut DurableConnection, name: &str);
pub const TRACE_UP_SQL: &str;                                // per backend
```

- `scoped` uses `tokio::task_local!`; nested scope → pass through.
- On open: record transaction depth; `begin_seq = SELECT MAX(seq) FROM
  durable_trace`.
- On `Ok`: nothing declared and nothing touched → no row. Otherwise read
  post-images of touched rows on the same connection, take `end_now`, insert
  the trace row. Touches without a declaration → action `"Unknown"` (the
  generator fails the trace). On `Err`: nothing; fence misses are recorded as
  local records after the transaction returns.
- `record_local` only when the task holds no pooled connection (debug
  assertion: no active scope).
- `dialect::transaction` becomes `connection.transaction(|c|
  trace::scoped(c, callback))` (same for the Postgres builder).

Trace tables (created by the test fixture when the feature is on; never in
the baseline migration):

```sql
CREATE TABLE durable_trace (
  seq BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,   -- postgres: BIGSERIAL
  txn_id CHAR(36) NOT NULL, actor VARCHAR(191) NOT NULL,
  action VARCHAR(64) NOT NULL, depth INT NOT NULL, begin_seq BIGINT NULL,
  now_sampled BIGINT NULL, end_now BIGINT NULL,
  params_json LONGTEXT NOT NULL, post_json LONGTEXT NOT NULL   -- postgres: TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE durable_trace_meta (k VARCHAR(64) PRIMARY KEY, v LONGTEXT NOT NULL);
```

**External writes** (phase 3a). Tests set up state with raw SQL or direct
Diesel writes that no traced transaction declares. With the feature on, the
fixture (`trace::trace_up_sql()`) also installs `AFTER INSERT/UPDATE/DELETE`
triggers on `durable_workflow`, `durable_activity`,
`durable_activity_attempt` and deliverable `durable_workflow_event` rows.
Each captured change appends an `External` row (`params = {table, op}`,
`post = {row}`) to `durable_trace` itself, so it shares the `seq` order with
recorded steps. A traced transaction suppresses the triggers for its own
writes: MySQL inserts a `durable_trace_marker` row for `CONNECTION_ID()` at
scope open and deletes it at scope close (a rollback or a dropped
transaction removes it, so a pooled connection never carries it; a user
variable would survive a cancelled future); Postgres sets the
transaction-local `durable.trace_scope`. `dump` turns the row into the
recorder's post-image shape; `gen` replays it as `EnvSetWf` / `EnvSetAct` /
`EnvSetAtt` / `EnvAppendEvent` (step class `external`).

## 4. Trace record format

One JSON file per trace, produced by `durable-trace dump`:

```json
{
  "schema": 1, "test": "activity_execution::success_finishes_attempt_and_wakes_workflow_with_typed_event",
  "backend": "mysql", "topics": {"emails": 4},
  "records": [
    {"seq": 1, "txn": "6f2c…", "actor": "app", "action": "TX1_Start", "depth": 1, "begin_seq": 0,
     "now": 1758800000123, "end_now": 1758800000124,
     "params": {"kind": "trace_flow", "version": 1, "dedup_key": null, "workflow_id": 1, "inserted": true},
     "post": {"wf": {"1": {"status": "ready", "kind": "trace_flow", "wait_kind": null, "wait_reference_id": null,
                           "available_at": 1758800000123, "lease_token": null, "lease_expires_at": null,
                           "command_sequence": 0, "delivered_event_sequence": 0, "activation_attempts": 0,
                           "deduplication_key": null, "parent_workflow_id": null, "root_workflow_id": null,
                           "restarted_from_workflow_id": null}},
              "act": {}, "att": {}, "events": [{"wf": 1, "dseq": 1, "type": "started"}]}},
    {"seq": 2, "actor": "rt1:coordinator", "action": "TC1_Claim", "depth": 1, "begin_seq": 1,
     "now": 1758800000201,
     "params": {"recovered": null, "claimed": 1, "token": "0d1e…", "lease_expires_at": 1758800030201},
     "post": {"wf": {"1": {"status": "running", "lease_token": "0d1e…", "lease_expires_at": 1758800030201}}}},
    {"seq": 5, "actor": "rt1:dispatcher", "action": "HandlerReturn", "depth": 0,
     "params": {"activity_id": 1, "attempt": 1, "token": "9a…", "outcome": "succeeded"}, "post": {}}
  ]
}
```

`depth: 0` = local record; `depth > 1` = write inside a caller's transaction
(commit deferred; the generator excludes the trace, reason
`deferred_commit`). `post` holds only touched rows. Payloads, JSON columns,
error text, `lease_owner`, `updated_at` and history events are not recorded.

## 5. Abstraction function (rows → model state)

Generator maps: `WfMap` (real wf id → 1..n in order of first insertion
record), `ActMap`, `TokMap` (uuid → 1..k in issue order), `RtMap` (actor with
`:coordinator`/`:dispatcher` stripped), `KeyMap` (domain dedup key → 1..j);
`child:{P}:{C}` → `autoKey(WfMap[P], C)`.

| Model field | Column | Abstraction |
|---|---|---|
| `wf[w].status` | `durable_workflow.status` | identical strings |
| `wf[w].kind` | `kind` | verbatim |
| version | `version` | dropped; traces where one `(kind, dedup)` has two versions are excluded |
| `wf[w].dedup` | `deduplication_key` | `KeyMap` / `autoKey`; NULL → 0 |
| `waitKind`, `waitRef` | `wait_kind`, `wait_reference_id` | NULL → `""`/0; ref via `ActMap` or `WfMap`; timer/approval → excluded |
| `availableAt` | `available_at` | raw millis |
| `token`, `leaseExp` | `lease_token`, `lease_expires_at` | `TokMap`; NULL → 0 |
| `cmdSeq`, `delivered`, `actAttempts` | `command_sequence`, `delivered_event_sequence`, `activation_attempts` | raw |
| `parent`, `root`, `restartedFrom` | `*_workflow_id` | `WfMap`; NULL → 0 |
| `act[a].{status, wf, topic, attemptCount, maxAttempts, availableAt, token, leaseExp}` | same columns | as above; replacement number, timeouts, retry policy, errors dropped |
| `att[a][n] = {used, token, open}` | `durable_activity_attempt` (a, n) | exists / `TokMap` / `finished_at IS NULL` |
| `events[w] = {dseq, typ, ref, cmd}` | `durable_workflow_event` with `delivery_sequence` set | `typ` verbatim; `ref` from params; `cmd` from the post-image; history rows dropped |
| `now` | `now_sampled` | per-action `tnow`, `now' = max(now, tnow)` |
| `TOPIC_CAP` | `durable_topic_lock.max_concurrency` | header |
| `proc.*` | — | reconstructed from local records |

Backoff/jitter are not recomputed: the model receives `availableAt` and
checks `availableAt >= tnow`.

## 6. Action mapping

| Code site | Record | Quint action | Parameters (source) |
|---|---|---|---|
| `store.rs` `insert_prepared` | `TX1_Start` | `TX1_Start(wNew, kind, key, inserted, tnow)` | kind, key, id, inserted |
| `store.rs:195-337` | `TX2_RecoverableStart` | `TX2_RecoverableStart(kind, key, orig, latest, superseded, sNew, tnow)` | original, latest, successor; latest not failed/blocked → `Noop` |
| `store.rs:69-104` | `TX3_Cancel` | `TX3_Cancel(w, tnow)` | terminal → not recorded |
| `coordinator.rs:228-393` | `TC1_Claim` | `TC1_Claim(r, rec, cl, tok, leaseExp, tnow)` | recovered id, claimed id, token, lease |
| `coordinator.rs:139-149` | `LC1_NoEvent` (local) | `LC1_NoEvent(r, w)` | |
| `coordinator.rs:496-700` | `TC2_Commit` | `TC2_Continue` / `TC2_Complete` / `TC2_RunActivity(…, aNew, topic, maxAttempts, availableAt, tnow)` / `TC2_RunChild(…, key, existing, cNew, tnow)` | variant, new ids, child key; SleepUntil / WaitForApproval → `Unmodeled` |
| `coordinator.rs:409-493` | `TC3_ActivationFailure` | `TC3_ActivationFailure(r, w, tok, attempt, maxActivation, availableAt, tnow)` | |
| `coordinator.rs:205-224` on `FencedWrite` | `CoordFenceMiss` (local) | `CoordFenceMiss(r, w, tok)` | |
| `activity_worker.rs` `claim_one` / `claim_batch` | `TW1_Claim` | `TW1_Claim(r, tnow, localAvail, reconciled, inFlightSeen, claimed)` | per-topic `in_flight`, reconciled list, claims; error (G10) → `TW1_Error` (local) |
| `activity_worker.rs` outcome dispatch / timeout | `HandlerReturn` (local) | `HandlerReturn(r, a, tok, outcome)` | |
| heartbeat failure past `lease_deadline` | `LocalDeadline` (local) | `LocalDeadline(r, a, tok)` | |
| `heartbeat_once` | `TW2_Send` (local) + `TW2_Commit` | `TW2_Send(r, a, tok, hb)`, `TW2_Commit(hb, sample, leaseExp)` | fence miss → `TW2_FenceMiss`; dropped future → `TW2_Drop` |
| `finish_on_connection` | `TW3_Finish` | `TW3_Finish(r, a, tok, outcome, availableAt, tnow)` | fence miss → `TW3_FenceMiss` (local) |
| driver aborts a runtime | `Crash` (local) | `Crash(r)` | |
| `temporal.rs`, `schedule.rs`, `schedule_materializer.rs`, `admin/control.rs`, `progress.rs` | `Unmodeled{name}` | — | touched rows still recorded |

## 7. Requirements on the Quint model interface

Pinned by `pure val TRACE_IFACE_VERSION = 1`; the generator refuses other
versions. Documented in `spec/README.md` "Trace-checking interface".

1. Every choice is a parameter; `nondet` only in `step`.
2. Observed absolute values are parameters (`tnow`, `leaseExp`,
   `availableAt`, new ids with `== db.next*` guards, tokens,
   `maxAttempts`, `maxActivation`).
3. Each DB action takes `tnow` and sets `now' = max(now, tnow)`; no
   `now < MAX_TIME` guards in DB actions.
4. Executions addressed by `(r, a, tok)`, heartbeats by `hb`.
5. `TW1_Claim` is a one-shot action with observed-read parameters; guards
   check only current-read fences; `inv_S17_capAtClaim` judges the result.
6. `TX1_Start` has a dedup-hit variant; `TC2_RunChild` takes `existing`.
7. `kind: str` free; versions unmodeled.
8. Explicit local actions: `CoordFenceMiss`, `TW3_FenceMiss`,
   `TW2_FenceMiss`, `TW2_Drop`, `LocalDeadline`, `HandlerReturn`,
   `LC1_NoEvent`, `Crash`, `TW1_Error`.
9. Views: `viewWf`, `viewAct`, `viewAtt`, `deliverable`.
10. Instance constants sized per trace.
11. Stable invariant names: `safety`, `inv_S17_capAtClaim`,
    `inv_S24_parentWakes`, `inv_G11_cancelReachesChildren`,
    `inv_N1_tx2OwnLineage`, `inv_S13_topicConcurrency`,
    `inv_G10_noAttemptCapError`.

## 8. Replay

Generated `spec/traces/<trace>.qnt`:

```
module trace_activity_happy {
  import durable(RUNTIMES = Set(1), MAX_WF = 2, MAX_ACT = 2, TOPICS = Set("emails"),
    TOPIC_CAP = Map("emails" -> 4), ...).* from "../durable"
  run step_1 = init.then(all { keepPrev, TX1_Start(1, "trace_flow", 0, true, 1758800000123) })
    .expect(safety and viewWf(db, 1) == { status: "ready", ... } and deliverable(db, 1).contains({ dseq: 1, typ: "started", ref: 0, cmd: 0 }))
  run step_2 = step_1.then(all { keepPrev, TC1_Claim(1, 0, 1, 1, 1758800030201, 1758800000201) }).expect(...)
  run trace = step_6
}
```

A disabled action makes `then` fail, which checks enabledness.

```bash
# record (feature on), existing tests + trace tests
cargo test -p durable-workflows --no-default-features \
  --features durable-workflows/mysql,durable-workflows/fake-clock,durable-workflows/trace-model \
  --test trace_model --test activity_execution --test workflow_activation --test child_workflow \
  --test application_cancellation --test continuation_priority --test workflow_start --test runtime_shutdown
# gap self-test
... --test gaps -- --include-ignored g2_ g11_ n1_ n2_ g10_
# dump every dwt_* database's durable_trace
cargo run -p durable-trace -- dump --server "$DURABLE_WORKFLOWS_TEST_DATABASE_URL" --out target/traces
# generate Quint runs + scope report
cargo run -p durable-trace -- gen --in target/traces --out spec/traces --expect-violation spec/traces/gaps.yaml
# check / localize / report
npx quint test spec/traces/<trace>.qnt --main <module> --match '^trace$'
npx quint test spec/traces/<trace>.qnt --main <module> --match '^step_' --verbosity 1
cargo run -p durable-trace -- report --trace target/traces/<test>.json --quint-output <log>
```

`spec/trace-check.sh` runs check + localization for every entry of
`spec/traces/index.json` and fails on any unexpected verdict. Gap traces are
generated with `.expect(not(<inv>))` on the final step and must pass.

## 9. Workloads

(a) Existing tests, zero edits, predicted in scope: `activity_execution.rs`
(minus the per-connection fake-clock test), `workflow_activation.rs` (minus
timer/approval tests and the isolation probe), `child_workflow.rs` (minus
paused-parent and version tests), `application_cancellation.rs` +
`timeout_cleanup.rs`, `continuation_priority.rs`, `workflow_start.rs` (minus
outer-transaction tests), `runtime_shutdown.rs`. The generator decides by
record content.

(b) `tests/trace_workload.rs`: 2–3 runtimes on one pool, short leases, cap-1
and cap-2 topics, seeded random flows (Continue / RunActivity / RunChild /
Complete) and handlers (success / retryable / permanent / hang), random
starts, cancels, and runtime crashes; 20 s per seed; 4 seeds in CI, 20
nightly. Optional shared fake clock via a bb8 connection customizer.

(c) `tests/gaps.rs` G2, G11, N1, N2, G10 as the checker self-test.

## 10. CI and reporting

Job `trace-check` (MySQL 8.4, Postgres 17), `needs: [test-mysql,
test-postgres]`, 15-minute timeout: run the recording tests, `npm ci` in
`spec/`, dump, gen, `trace-check.sh`; upload traces on failure. Report:
trace, backend, failing step, record, `strict`/`concurrent`, field-level
model-vs-recorded diff, `lastAction`; excluded traces with reasons, with the
excluded count asserted so scope cannot shrink silently.

## 11. Phases

| Phase | Scope | Acceptance |
|---|---|---|
| 0 | Model interface (§7), `TRACE_IFACE_VERSION`, README section | `quint test durable_tests.qnt` passes against new signatures |
| 1a | Recorder core: feature, `trace/` + noop, hooks in both dialects and `store.rs`, event/insert touches, declarations for `TX1_Start`, `TC1_Claim`, `TC2_Commit{run_activity, complete}`, `TW1_Claim` (claims only), `HandlerReturn`, `TW3_Finish{succeeded}` | clippy clean with and without the feature; `activity_execution` green with and without; with the feature, the happy-path test's `durable_trace` holds `TX1_Start, TC1_Claim, TC2_Commit, TW1_Claim, HandlerReturn, TW3_Finish` with non-empty `post_json` |
| 1b | End-to-end slice: fixture creates trace tables, `tests/trace_model.rs`, `tools/durable-trace` (`dump`, `gen` for the six actions), `spec/trace-check.sh` | the happy-path trace passes `quint test`; a hand-corrupted trace fails at the right step with a field diff; an `Unknown` action is excluded with a reason |
| 2 | Full modeled coverage (all actions of §6, `Unmodeled` declarations, generator exclusion rules, `begin_seq` classification) | a source-scan unit test asserts every transaction site declares; child, reconcile and stale-lease traces pass |
| 3a | External-write capture (triggers, `Env*` actions, interface v3), `gen --expect-violation` with `spec/traces/gaps.yaml`, suite replay on both backends | every in-scope trace passes, confirms its expected violation, or is excluded with a specific reason; every FAIL analysed |
| 3b | CI job, field-level `report`, `docs/TRACE_CHECKING.md`, excluded count asserted | ≤ 10 min in CI |
| 4 | Concurrent workload driver + nightly | 4 seeds × 20 s pass or are classified `concurrent`; at least one seed covers Crash, recovery, reconcile, `TW3_FenceMiss`; a deliberately removed fence is caught |
| 5 (optional) | serialize debug flag, ITF diffs, timers/approvals once modeled | — |

## 12. Risks

- Model interface drift → `TRACE_IFACE_VERSION`; phase 1b waits for phase 0.
- READ COMMITTED read anomalies produce `concurrent` divergences that need a
  human look; observed-read parameters keep replay deterministic.
- The trace insert adds a round trip per transaction; it cannot change lock
  order on engine tables but widens races. The recorded behavior is still a
  real behavior.
- `record_local` needs a pool connection; guarded by the no-active-scope rule.
- Tests that create their pool inside a spawned task lose the thread name;
  those traces are named by database only.
- Long workload traces: localization is quadratic; bounded by binary search
  and a 400-step cap.
