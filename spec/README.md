# Quint specification of the durable workflow protocol

This directory holds a Quint model of the core protocol described in
`docs/INVARIANTS.md`. The model follows the code in `durable-workflows/src/`
at commit `4866070` (after P4: every library transaction runs at READ
COMMITTED), including its fences, and not an idealized design. The first
iteration (commit `0085691`) modeled the code before P4; its REPEATABLE READ
T-W1 is kept as the historical instances `durable_mc_rr` and
`durable_mc_act_rr`.

| File | Contents |
|---|---|
| `durable.qnt` | The parameterized model: state, one action per DB transaction (T-W1 split by statement), invariants, witnesses, temporal properties. The header maps actions to code and invariants to S/G/N ids. |
| `durable_mc.qnt` | Instances: `durable_mc` (the code), `durable_mc_drift` (process clock lags DB time), `durable_mc_act` (activity-only, small), `durable_mc_rr` and `durable_mc_act_rr` (historical: T-W1 under REPEATABLE READ). |
| `durable_tests.qnt` | Directed scenarios (`quint test`): `durable_tests` (READ COMMITTED) and `durable_tests_rr` (historical RR). |
| `check.sh` | Typecheck, both test modules, and random simulation of every invariant and witness. |
| `verify.sh` | Bounded model checking with Apalache (`quint verify`). |
| `results/` | `summary.txt` (simulation), `apalache_summary.txt`, `check.log`, `verify.log`, and one output file per run. |

## How to run

```bash
cd spec
npm install                      # installs @informalsystems/quint locally
npx quint typecheck durable_mc.qnt
npx quint test durable_tests.qnt --main durable_tests
npx quint test durable_tests.qnt --main durable_tests_rr
./check.sh 20000 40              # samples, steps; writes results/summary.txt
./verify.sh 3                    # needs Java 17+; downloads Apalache on first use (depth 4: > 2 h)
```

One invariant by hand:

```bash
npx quint run durable_mc.qnt --main durable_mc --invariant safetyRc \
  --max-samples 20000 --max-steps 40
```

## What changed since 0085691

The engine commits `d0f64f7..4866070` (P3b to P8) change one modeled thing:
isolation. `dialect::transaction` (`src/dialect/mysql.rs`,
`src/dialect/postgres.rs`) pins READ COMMITTED for every transaction the
library opens: T-X1/T-X2 through `start` / `start_or_restart_recoverable`,
T-X3 through `cancel`, T-C1/T-C2/T-C3 in `runtime/coordinator.rs`, and
T-W1/T-W2/T-W3 in `runtime/activity_worker.rs`. The rest of the diff is the
dialect seam, Postgres, snake_case and reindentation, with no change to a
modeled guard or effect. The P3a start semantics (dedup hit returns the row,
restart-key collision returns `Conflict`, both keys rejected) predate
`0085691` but were not modeled then; they are now.

| Action | Change |
|---|---|
| T-W1 | Rewritten. The RR snapshot (`TW1_Begin`/`TW1_Commit`, `TW1_Atomic`) is replaced by one action per statement group with the READ COMMITTED interleaving points (see below). Adds `claim_one` (`TW1_BeginOne`). The RR snapshot survives as the constant `RR_SNAPSHOT` (historical instances only). |
| T-X1 | `TX1_Start(key, from)`: `from` = `StartOptions.restarted_from_workflow_id`. Dedup hit → `TX1_StartExisting` (no write). Restart-key collision → `TX1_StartConflict` (no row; caller transaction usable). Both set → disabled (`InvalidDefinition` before any SQL). |
| T-X2 | All branches explicit: no original → `TX2_StartNew`; newest generation not `failed`/`blocked` → `TX2_ReturnLatest`; successor insert collides → `TX2_Conflict` (the savepoint rolls back the cancel); else `TX2_RecoverableStart` as before. |
| All | Trace-checking interface v3 (below): every choice is a parameter, observed values (`tnow`, lease expiries, `availableAt`, new ids, tokens) are parameters, T-C2 is split by transition, T-W1 has a one-step replay form `TW1_Claim` and an explicit `TW1_Error`, heartbeats have ids and an explicit `TW2_FenceMiss`. Every transaction that locks a row blocks (is disabled) while an in-flight T-W1 holds that row. |
| G4 | Nothing to remove: the model has no history `sequence`. Under READ COMMITTED `next_event_sequence` after the workflow row lock sees every committed append, so G4 is closed for library transactions (INVARIANTS.md). |

## What is modeled

Constants in `durable_mc`: 2 runtimes, at most 4 workflow rows and 3 activity
rows, 1 topic with `maxConcurrency = 1`, 1 local executor slot per runtime,
`maxAttempts = 2`, 2 activation attempts, workflow and activity leases of 3
ticks, clock bound 10. `durable_mc_act`: no children, no crashes, 2 workflows,
2 activities, clock bound 6.

State (`INVARIANTS.md` §7.3, reduced):

- `db`: committed workflow rows (status, wait fields, lease token and expiry,
  `commandSequence`, `deliveredEventSequence`, `activationAttempts`, kind,
  dedup key, parent, root, `restartedFrom`), activity rows, attempt rows,
  deliverable events per workflow, and the auto-increment counters `nextWf`,
  `nextAct`.
- `proc` (lost on `Crash`): each coordinator's claim, activity executions
  with their local lease deadline, in-flight heartbeat transactions, the
  topic-lock holder, and each runtime's in-flight T-W1 (`tw1`: phase, sampled
  `now`, remaining topics, candidate set, `wanted`, locked rows, buffered own
  writes, claims made so far).
- `ghost`: history variables, and `usedTokens` (every UUID generated).
- `now`: DB time, advanced by `Tick`.

Actions, one per committed transaction (T-W1: one per statement group):

| Action | T-id | Fences and checks modeled |
|---|---|---|
| `TX1_Start` | T-X1 | dedup hit returns the existing row; restart-key collision → `Conflict`, no row; both keys rejected |
| `TX2_RecoverableStart` | T-X2 | lineage = same kind and (`id = root` or `rootWorkflowId = root`); only a `failed`/`blocked` newest row; blocked → cancelled with no parent wake; restart-key collision → `Conflict` (all rolled back) |
| `TX3_Cancel` | T-X3 | cancels own activities (closes open attempts), wakes waiting parents |
| `TC1_Claim` | T-C1 | at most one expired-lease recovery, then at most one ready claim, fresh token; SKIP LOCKED |
| `LC1_NoEvent` | L-C1 | no deliverable event after `delivered` → task error |
| `TC2_Continue`, `TC2_Complete`, `TC2_RunActivity`, `TC2_RunChild` | T-C2 | fence `status=running ∧ leaseToken`; Continue, Complete (+ parent wake), RunActivity, RunChild (new, attach, attach-to-terminal + wake) |
| `TC3_ActivationFailure` | T-C3 | fence; retry with backoff or fail when exhausted (+ parent wake) |
| `CoordFenceMiss` | T-C2/T-C3 rollback | counts a task error (G1) |
| `TW1_*` | T-W1 | simulation form: see the next table; replay form `TW1_Claim`; `TW1_Error` (G10) |
| `TW2_Send` / `TW2_Commit` / `TW2_FenceMiss` / `TW2_Drop` | T-W2 | `now` sampled at send; commit fenced on `status, attemptCount, leaseToken` and open attempt, no expiry check; blocks on T-W1's row lock |
| `HandlerReturn`, `LocalDeadline` | L-W | handler returns only before the local deadline; passing the deadline ends the execution without T-W3 |
| `TW3_Finish` / `TW3_FenceMiss` | T-W3 | fence on `status, attemptCount, leaseToken`; success needs the workflow wait; dead-letter blocks the workflow; blocks on T-W1's row locks |
| `Crash` | §5.3 | claims, executions and the in-flight T-W1 (its writes and locks) lost; heartbeat COMMITs already sent can still land |

### T-W1 under READ COMMITTED

`claim_batch` and `claim_one` (`src/runtime/activity_worker.rs`) run in one
READ COMMITTED transaction that holds the topic row lock throughout, so no
other T-W1 interleaves. Other transactions can commit between any two
statements. Each plain read sees what was committed when that statement
started, plus T-W1's own writes; each locking read (`FOR UPDATE`, with or
without `SKIP LOCKED`) and each `UPDATE` reads the current row. T-W1's writes
stay in `proc.tw1` until `TW1_Commit`; a row it has locked blocks every other
transaction that would lock it (those actions are disabled until the commit).

| Action | Statements (code order) | Read kind | Interleaving point before it |
|---|---|---|---|
| `TW1_BeginBatch(r)` | topic rows `FOR UPDATE SKIP LOCKED` (all or nothing), `UTC_TIMESTAMP` | locking | yes |
| `TW1_BeginOne(r, t, tnow)` | `UTC_TIMESTAMP` (= `tnow`), then topic row `FOR UPDATE` (blocking) | locking | yes; `tnow ≤ now` covers the lock wait |
| `TW1_ReconcileScan(r, t)` | reconcile candidate `SELECT` (running, lease ≤ now) | plain | yes (the old G7 window starts here) |
| `TW1_ReconcileRow(r, a)` | workflow `FOR UPDATE`; activity `FOR UPDATE` with the filters; the updates; history; block | locking | yes |
| `TW1_Count(r)` | `in_flight` count; if `wanted > 0`, the candidate join | plain, plain | yes |
| `TW1_ClaimRow(r, a, tok)` / `TW1_SkipRow(r, a)` | workflow and activity `FOR UPDATE SKIP LOCKED` with the filters; update; attempt insert | locking | yes |
| `TW1_Commit(r)` | `COMMIT` | | yes |

Two merges keep the model small. (1) Statements inside `TW1_ReconcileRow` and
`TW1_ClaimRow` are atomic: every read in them is a locking read of a row that
is then held, so a commit between them is equivalent to one before the first.
(2) `TW1_Count` merges the count and the candidate join: a commit between
them either only adds candidates (a new pending row, which the join then
sees) or lowers the real count (cancel, finish), which makes the stored count
conservative; every candidate is rechecked by a locking read. Heartbeat
revival cannot happen between them (see G7 below).

Other transactions are atomic. `*_with_conn` callers may run T-X1, T-X2 and
T-X3 at REPEATABLE READ on MySQL; the model keeps them atomic, which is exact
for their locking reads and a superset for the dedup plain read (a missed
concurrent insert falls back to `DeduplicationConflict` and a locking reload).

## What is abstracted

- Payloads, state JSON, history (non-deliverable) events and their
  `sequence`, error text, backoff and the reconcile retry delay (a
  nondeterministic 0 or 1 tick in simulation; a trace supplies the real
  `availableAt`; a RetryPolicy with 100% jitter can give a 0 s delay), jitter,
  continuation streaks.
- Timeout and lease durations: only whether an activity's bounds are valid
  (`invalidBounds`, the `claim_locked_candidate` check). `ActivityCommand`
  rejects invalid bounds, so in the code they come from a serde-built command
  or an external write; the model has both only when `ENABLE_ENV_EDITS`
  (`TC2_RunActivity(.., invalidBounds = true, ..)`, `EnvCorruptActivityBounds`).
- Registry filters: every runtime has every definition (F3). `(kind, version)`
  is reduced to kind (`P` top-level, `C` child); versions (G6) are not modeled.
- Candidate order: claims pick any candidate (parameter), a superset of the
  code's `(availableAt, id)` order. SKIP LOCKED against a transaction that is
  itself in flight is `TW1_SkipRow`, which may skip any candidate.
- `StartOptions.root_workflow_id` and `available_at` on T-X1; admin restart
  and retry (T-A5, T-A6), progress events (T-W4), timers, approvals,
  schedules. The supervisor restart budget is modeled per runtime and task
  (`coordErrors`, `dispErrors` against `MAX_TASK_RESTARTS`), not its backoff.
- Pause sizing: a pause bump needs `maxAttempts + 1 <= MAX_ATTEMPTS` (the
  attempt map's domain); `step` inserts activities with `MAX_ATTEMPTS - 1` or
  `MAX_ATTEMPTS` attempts so a pause can happen.
- Deadlocks (G9, and T-W1's workflow-then-activity lock order against T-W3's
  activity-then-workflow order): an InnoDB deadlock aborts one side, which the
  model shows as that transaction not happening.
- `step` is a nondeterministic choice of transition at commit time (S3).

## Trace-checking interface

`pure val TRACE_IFACE_VERSION = 4` (in `durable.qnt`) versions the action
names, parameters and views below. Every action takes all its choices as
parameters; there is no `nondet` inside an action (only in `step`). A trace
checker calls `all { keepPrev, Action(args) }` once per record, in commit
order. After each call `lastAction` names the branch taken (for example
`TX1_StartConflict`, `TX2_ReturnLatest`, `TC2_RunChild_AttachTerminal`,
`TW3_DeadLetter`); a checker compares it with the outcome in the record.

Changes in v4 (from v3):

- New operator actions (`admin/control.rs`; the recorder declares them in
  place of `Unmodeled{admin_pause|admin_resume|admin_cancel}`):
  `AdminPause(w, tnow)` (T-A2; `lastAction` is `AdminPauseActivity` when the
  workflow waited on a running activity, which goes back to `pending` at
  `tnow` with `maxAttempts + 1`, its attempt closed, S36),
  `AdminResume(w, tnow)` (T-A3; status derived from the wait, `availableAt =
  tnow` only for `ready`), `AdminCancel(w, tnow)` (T-A4, same effect as
  `TX3_Cancel`). A pause or cancel of a `running` row records the revoked
  token in `ghost.opRevoked`. `restart` and `retry` stay `Unmodeled`.
- `status = "paused"` is a workflow status. A child's terminal transaction
  wakes a parent that is `waiting_child` or `paused` on it; a paused parent
  stays paused with its wait cleared (`wakeParents`). S15 allows a `pending`
  activity whose workflow is paused on it.
- `TC2_RunActivity(r, w, tok, aNew, topic, maxAttempts, invalidBounds, prio,
  availableAt, tnow)`: new `prio` (continuation priority,
  `continuation_priority` in the record). `prio` requires `availableAt ==
  CONTINUATION_READY_AT` (0, `transition.rs` `CONTINUATION_READY_AT_MILLIS`);
  otherwise `availableAt >= tnow`.
- `TX2_ReturnLatest` sets `ghost.tx2OutsideLineage` when the returned row is
  not on the keyed row's restart chain (N1, second variant).
- `crashLoses(r)`: `Crash(r)`'s guard (the runtime holds a claim, an
  execution or an in-flight T-W1). A trace checker replays the recorded crash
  of an idle runtime as a stutter, `commit(db, proc, ghost, "Crash")`: it
  loses nothing, and a crashed runtime id never acts again.
- New constant `MAX_TASK_RESTARTS` (`RuntimeConfig.max_task_restarts`;
  `durable-trace gen` writes the default 8, the recorder does not see the
  runtime config). New ghost fields `coordErrors`, `dispErrors` (restart
  count per task, reset by `Crash`), `opRevoked`, `opTaskErrors`. A runtime
  whose coordinator or dispatcher count exceeds `MAX_TASK_RESTARTS` is
  stopped (`stoppedRt`): `TC1_Claim`, `TW1_Begin*` and `TW1_Claim` are
  disabled for it. `CoordFenceMiss` on a token an operator revoked counts in
  `opTaskErrors` (G1).
- New invariant `inv_G1_noSelfCancelFromOperator` (not in `safety`); new
  witnesses `wit_selfCancelled`, `wit_pausedActivity`. `safety` is unchanged.

Changes in v3 (from v2):

- New environment actions for recorded external writes (trace replay only,
  not part of `step`; each needs `ENABLE_ENV_EDITS` and blocks while an
  in-flight T-W1 holds the row): `EnvSetWf(w, row: WfRow)`,
  `EnvSetAct(a, row: ActRow)`, `EnvSetAtt(a, n, row: AttRow)`,
  `EnvAppendEvent(w, ev: Event)`. `lastAction` is the action name.
  `safety` is unchanged.

Changes in v2 (from v1):

- `TW1_Claim`: each `reconciled` entry is `{a, exhausted, availableAt}`.
  `availableAt` is the row's new `available_at`: `== tnow` when exhausted
  (dead-lettered), `>= tnow` otherwise (now + the retry policy's delay).
  `TW1_ReconcileRow(r, a, availableAt)` (simulation) takes the same value.
- `TC2_RunActivity(r, w, tok, aNew, topic, maxAttempts, invalidBounds,
  availableAt, tnow)`: new `invalidBounds` (needs `ENABLE_ENV_EDITS`).
- `TW1_Error(r, a, reason)`: names the row `a`; `reason` is `"attempt_cap"`
  or `"invalid_bounds"` (`"missing_definition"` is not modeled, F3).
- New environment action `EnvCorruptActivityBounds(a)` (needs
  `ENABLE_ENV_EDITS`), new constant `ENABLE_ENV_EDITS`.
- `ActRow` / `viewAct` gain `invalidBounds`.
- `inv_G10_noAttemptCapError` is now `inv_G10_noClaimAbort` (any
  `TW1_Error`), and it is no longer part of `safety`.

Rules:

- **Observed values are parameters.** `tnow` (DB time the transaction saw),
  lease expiries, `availableAt`, `maxAttempts`, `maxActivation`, heartbeat
  samples and new ids come from the trace. The model checks them only where
  the code constrains them: new ids `== db.nextWf` / `db.nextAct`, tokens
  `== db.nextToken`, `leaseExp > tnow` (or `> sample`), `availableAt >= tnow`
  where the code adds a backoff, `1 <= maxAttempts <= MAX_ATTEMPTS`.
- **Time.** `var now` stays. Every DB action takes `tnow`, compares with
  `tnow`, and sets `now' = max(now, tnow)` (`TW2_Commit` uses `sample`).
  No DB action guards on `MAX_TIME`; only `Tick` (simulation) does. Local
  steps (`HandlerReturn`, `LocalDeadline`, `TW2_Send`) compare the local
  deadline with `now`.
- **Identifiers.** Workflow ids, activity ids and lease tokens are counters
  (`db.nextWf`, `db.nextAct`, `db.nextToken`): the n-th insert or generated
  token is n. A trace renumbers them densely in generation order (MySQL
  auto-increment gaps and UUIDs included). A token generated inside a T-W1
  that rolls back is consumed, like a UUID. Heartbeat ids: `ghost.nextHb`.
- **Addresses.** An execution is `(r, a, tok)`; a heartbeat is its id `hb`.
  The model keeps the local deadline itself (`leaseExp - 1 + DRIFT`).
- **Values.** `kind` is any string. Keys: `NO_KEY = 0`; a domain key is any
  other int (the directed tests use `DOMAIN_KEY = 1`); a child's auto key
  `child:{parent}:{cmd}` is `autoKey(parent, cmd) = 1000 + parent*10 + cmd`.
  Versions are not modeled.
- **Sizing.** An instance for a trace sets `RUNTIMES`, `MAX_WF`, `MAX_ACT`,
  `MAX_ATTEMPTS`, `TOPICS`, `TOPIC_CAP`, `LOCAL_SLOTS`, `MAX_ACTIVATION`,
  `LEASE_W`, `LEASE_A` (simulation only), `MAX_TIME` (only `Tick`), `DRIFT`,
  `ENABLE_CHILDREN`, `ENABLE_CRASH`, `ENABLE_ENV_EDITS`, `MAX_TASK_RESTARTS`, and
  `RR_SNAPSHOT = false`.
- **External writes.** A write made outside a traced library transaction (raw
  SQL or a direct Diesel write in a test) is an environment step (step class
  `external`). With `trace-model`, the test fixture installs triggers on
  `durable_workflow`, `durable_activity`, `durable_activity_attempt` and
  `durable_workflow_event` (deliverable rows only) that append an `External`
  row with the changed row to `durable_trace`, in the same `seq` order as the
  recorded steps. A traced transaction marks itself so its own writes are not
  captured (MySQL: a `durable_trace_marker` row for `CONNECTION_ID()`,
  inserted at scope open and deleted at scope close, rolled back with the
  transaction; Postgres: the transaction-local setting
  `durable.trace_scope`). `durable-trace gen` replays a row write as
  `EnvSetWf` / `EnvSetAct` / `EnvSetAtt` with the abstraction of the new row
  (a delete sets the empty row) and a deliverable event insert as
  `EnvAppendEvent`. Ids and tokens an external write mentions count as
  issued (`db.nextWf`, `db.nextAct`, `db.nextToken` move past them), and a
  non-zero lease token becomes the row's latest issued lease
  (`ghost.lastIssuedWf` / `lastIssuedAct`). A write the model cannot hold
  (an event update or delete, an event whose reference is unknown) excludes
  the trace with `external_write:<table>:<detail>`. `safety` is checked
  after every step; when an external step itself breaks an invariant,
  `trace-check.sh` reports the trace as excluded
  (`external_invariant:<inv>`), not as a failure. A `TW1_Error{invalid_bounds}`
  naming an activity whose last image had valid bounds still gets an
  `EnvCorruptActivityBounds(a)` before it (a write that no trigger saw).
- **Expected violations.** `durable-trace gen --expect-violation
  traces/gaps.yaml` maps test-name globs to the gap invariant each test
  reproduces. For such a trace every step checks `safety` (without that
  invariant, if it is a conjunct), and a run `viol_k` asserts `not(<inv>)`
  after step k (verdict `expect_violation`); `trace-check.sh` counts the
  trace as the violation confirmed when `trace` and some `viol_k` pass, and
  fails when the violation is observed at no step (a gap can be transient).
  A value `exclude:<reason>` excludes the matching trace with that reason.
  Consecutive external steps check the invariants only after the last one
  (no recorded step observes the states between them).
- **T-W1.** A trace replays a whole committed T-W1 with `TW1_Claim`; the
  statement-level `TW1_BeginBatch` ... `TW1_Commit` actions are for
  simulation. `TW1_Claim` checks only the current-read fences; whether the
  count it saw was right is judged by `inv_S17_capAtClaim`
  (`tw1ReplayDetectsCapTest`).
- **Stable invariant names:** `safety`, `safetyRc`, `inv_S17_capAtClaim`,
  `inv_S17_capAlways`, `inv_S24_parentWakes`, `inv_G11_cancelReachesChildren`,
  `inv_N1_tx2OwnLineage`, `inv_S13_topicConcurrency`,
  `inv_G10_noClaimAbort`, `inv_S19_sourceTerminal`, `inv_G1_noSelfCancelFromOperator`.

Views for state comparison (`durable.qnt`):

| View | Fields |
|---|---|
| `viewWf(d, w): WfRow` | `status, kind, dedup, waitKind, waitRef, availableAt, token, leaseExp, cmdSeq, delivered, actAttempts, parent, root, restartedFrom` |
| `viewAct(d, a): ActRow` | `status, wf, topic, attemptCount, maxAttempts, availableAt, token, leaseExp, invalidBounds` (`timeout_millis <= 0 or lease_duration_millis <= timeout_millis`) |
| `viewAtt(d, a, n): AttRow` | `used, token, open` |
| `deliverable(d, w): Set[Event]` | `dseq, typ, ref, cmd` (deliverable events only) |

Actions (reads and writes name state fields; "locks" = rows held by an
in-flight simulation T-W1, which block the action):

| Action (parameters) | Code (file:function) | Reads | Writes |
|---|---|---|---|
| `TX1_Start(wNew, kind, key, from, inserted, tnow)` — `inserted=false`: dedup hit (`wNew` = existing row) or restart-key `Conflict` (`wNew = 0`); key and `from` both set is not a step | `store.rs:start_with_conn` → `insert_prepared` → `persistence/workflows.rs:insert_started` → `dialect/mysql.rs:insert_workflow` | `db.wf`, `db.nextWf`, locks | `db.wf[wNew]`, `db.events[wNew]`, `db.nextWf`, `now` |
| `TX2_RecoverableStart(kind, key, orig, latest, superseded, sNew, tnow)` — `orig`/`latest` = rows locked (0 = none), `sNew` = inserted row (0 = none) | `store.rs:start_or_restart_recoverable_with_conn` | `db.wf`, `db.act`, locks | `db.act` (dead-lettered → cancelled), `db.wf[latest]`, `db.wf[sNew]`, `db.events[sNew]`, `db.nextWf`, `ghost.tx2OutsideLineage`, `now` |
| `TX3_Cancel(w, tnow)` — terminal `w` is not a step | `store.rs:cancel_with_conn`, `cancel_locked_workflow`, `cancel_activities`; `persistence/workflows.rs:wake_waiting_parents_on_child_terminal` | `db.wf`, `db.act`, `db.att`, `db.events`, locks | `db.wf[w]`, parents, `db.act`, `db.att`, parents' `db.events`, `now` |
| `TC1_Claim(r, rec, cl, tok, leaseExp, tnow)` — `rec`/`cl` = 0 when absent | `runtime/coordinator.rs:claim_one` | `db.wf[rec, cl]`, `db.nextToken`, `proc.claims[r]`, locks | `db.wf[rec, cl]`, `db.nextToken`, `proc.claims[r]`, `ghost.lastIssuedWf`, `now` |
| `LC1_NoEvent(r, w)` | `coordinator.rs:activate_claim_inner` | `proc.claims[r]`, `db.events[w]` | `proc.claims[r]`, `ghost.noEventError`, `ghost.taskErrors` |
| `CoordFenceMiss(r, w, tok)` | `coordinator.rs:commit_transition` / `record_activation_failure` (fence miss) | `proc.claims[r]`, `db.wf[w]` | `proc.claims[r]`, `ghost.taskErrors` |
| `TC2_Continue(r, w, tok, availableAt, tnow)` | `coordinator.rs:commit_on_connection` | `proc.claims[r]`, `db.wf[w]`, `db.events[w]` | `db.wf[w]`, `db.events[w]`, `proc.claims[r]`, `now` |
| `TC2_Complete(r, w, tok, tnow)` | `commit_on_connection` + `wake_waiting_parents_on_child_terminal` | same, parents | `db.wf[w]`, parents, parents' `db.events`, `proc.claims[r]`, `now` |
| `TC2_RunActivity(r, w, tok, aNew, topic, maxAttempts, invalidBounds, prio, availableAt, tnow)` — `prio`: `availableAt == CONTINUATION_READY_AT` | `commit_wait_transition` → `commit_activity` | same, `db.nextAct` | `db.act[aNew]`, `db.nextAct`, `db.wf[w]`, `proc.claims[r]`, `now` |
| `TC2_RunChild(r, w, tok, kind, key, existing, cNew, tnow)` — `existing` = row with `(kind, key)` (0 = insert `cNew`) | `commit_wait_transition` → `commit_child` → `store.rs:insert_child` | same, `db.wf[existing]`, `db.nextWf`, locks | `db.wf[w, cNew]`, `db.events[cNew]`, `db.nextWf`; attach to terminal: `db.wf[w]` woken, `db.events[w]`; `proc.claims[r]`, `now` |
| `TC3_ActivationFailure(r, w, tok, attempt, maxActivation, availableAt, tnow)` | `coordinator.rs:record_activation_failure` | `proc.claims[r]`, `db.wf[w]` | `db.wf[w]`, parents, `db.events`, `proc.claims[r]`, `now` |
| `TW1_Claim(r, tnow, localAvail: str->int, reconciled: List[{a, exhausted, availableAt}], inFlightSeen: str->int, claimed: List[{a, tok, leaseExp}])` | `runtime/activity_worker.rs:claim_batch` / `claim_one` (`reconcile_expired`, `claim_locked_candidate`), one commit | `db.act`, `db.att`, `db.wf`, `db.nextToken`, `proc.topicHolder` | `db.act`, `db.att`, `db.wf` (blocked), `db.nextToken`, `proc.execs`, `ghost.capExceededAtClaim`, `ghost.lastIssuedAct`, `now` |
| `TW1_Error(r, a, reason)` — `reason` = `"attempt_cap"` (checked first) or `"invalid_bounds"`, on claimable row `a` (G10) | `claim_locked_candidate` error → rollback of the whole T-W1 | `db.act`, `db.wf`, `proc.tw1[r]` | `proc.tw1[r]`, `proc.topicHolder`, `ghost.claimAborted`, `ghost.taskErrors` |
| `EnvCorruptActivityBounds(a)` — needs `ENABLE_ENV_EDITS`; blocks while a T-W1 holds `a` | external write (raw SQL; the G10 gap test) | `db.act[a]`, locks | `db.act[a].invalidBounds` |
| `EnvSetWf(w, row)`, `EnvSetAct(a, row)`, `EnvSetAtt(a, n, row)`, `EnvAppendEvent(w, ev)` — trace replay only; need `ENABLE_ENV_EDITS`; block while a T-W1 holds the row | recorded external write (trigger-captured `External` record) | locks | the row / `db.events[w]`; `db.nextWf`, `db.nextAct`, `db.nextToken`; `ghost.lastIssuedWf` / `lastIssuedAct` |
| `TW1_BeginBatch(r)`, `TW1_BeginOne(r, t, tnow)`, `TW1_ReconcileScan(r, t)`, `TW1_ReconcileRow(r, a, availableAt)`, `TW1_Count(r)`, `TW1_ClaimRow(r, a, tok)`, `TW1_SkipRow(r, a)`, `TW1_Commit(r)` (simulation) | `claim_batch` / `claim_one` statement groups (table above) | `db`, `proc.tw1[r]`, `proc.topicHolder`, `proc.execs` | `proc.tw1[r]`, `proc.topicHolder`; `TW1_ClaimRow`: `db.nextToken`; `TW1_Commit`: `db.act`, `db.att`, `db.wf`, `proc.execs`, ghost |
| `HandlerReturn(r, a, tok, outcome)` | `activity_worker.rs:execute_claim` (dispatch result) | `proc.execs`, `now` | `proc.execs` |
| `LocalDeadline(r, a, tok)` | `execute_claim`, `wait_for_lease_deadline` | `proc.execs`, `now` | `proc.execs` |
| `TW2_Send(r, a, tok, hb)` | `activity_worker.rs:heartbeat_loop` → `heartbeat_once` starts | `proc.execs`, `proc.hbs`, `ghost.nextHb` | `proc.hbs`, `ghost.nextHb` |
| `TW2_Commit(hb, sample, leaseExp)` | `heartbeat_once` fenced updates, COMMIT | `proc.hbs`, `db.act`, `db.att`, `proc.execs`, locks | `db.act[a].leaseExp`, `proc.execs` (deadline), `proc.hbs`, ghost, `now` |
| `TW2_FenceMiss(hb)` | `heartbeat_once` fence miss → `HeartbeatFailure` | `proc.hbs`, `db.act`, `db.att`, locks | `proc.hbs`, `proc.execs` |
| `TW2_Drop(hb)` | `heartbeat_loop` future dropped | `proc.hbs` | `proc.hbs`, `proc.execs` |
| `TW3_Finish(r, a, tok, outcome, availableAt, tnow)` | `activity_worker.rs:finish_on_connection` (`finish_attempt`, `wake_workflow`, `dead_letter`, `block_workflow`) | `proc.execs`, `db.act`, `db.att`, `db.wf`, `db.events`, locks | `db.act[a]`, `db.att[a]`, `db.wf[w]`, `db.events[w]`, `proc.execs`, ghost, `now` |
| `TW3_FenceMiss(r, a, tok)` | `finish_on_connection` rollback | same | `proc.execs` |
| `AdminPause(w, tnow)` — `lastAction` `AdminPause` / `AdminPauseActivity`; paused/terminal `w` is not a step; blocks on a T-W1 lock of `w` or its wait activity | `admin/control.rs:pause_workflow`, `pause_activity`; `store.rs:close_attempt` | `db.wf[w]`, `db.act[waitRef]`, `db.att`, locks | `db.wf[w]` (paused, lease cleared), `db.act[waitRef]` (pending, `maxAttempts+1`), `db.att`, `ghost.opRevoked`, `now` |
| `AdminResume(w, tnow)` — `w` paused; a resume the code rejects (`Conflict`) is not a step | `admin/control.rs:resume_workflow`, `resume_status` | `db.wf[w]`, `db.act[waitRef]`, `db.wf[waitRef]`, locks | `db.wf[w]`, `now` |
| `AdminCancel(w, tnow)` — terminal `w` is not a step | `admin/control.rs:cancel_workflow` → `store.rs:cancel_locked_workflow` | as `TX3_Cancel` | as `TX3_Cancel`, `ghost.opRevoked` |
| `Crash(r)` | process exit (INVARIANTS.md §5.3) | `proc` | `proc.claims[r]`, `proc.execs`, `proc.tw1[r]`, `proc.topicHolder` |
| `Tick` | simulation clock | `now` | `now` |

## Results (commit 4866070, `durable_mc` = READ COMMITTED unless stated)

### Directed scenarios (`quint test`)

All 39 pass (`durable_tests` 32, `durable_tests_rr` 2, `durable_tests_drift` 1, `durable_tests_env` 4).

| Test | Module | Shows |
|---|---|---|
| `g7ClosedUnderRcTest` | RC | The old G7 schedule: the late heartbeat lands between the reconcile scan and the relock. The relock skips a1, `in_flight` sees it live, no claim. S17 holds. Also witnesses the revived expired lease. |
| `g7RcHeartbeatBlockedTest` | RC | The other order: the relock reconciles a1 (pending again after a 1-tick retry delay) and holds it; the heartbeat blocks, then misses the fence. S17 holds. |
| `tw1ReplayDetectsCapTest` | RC | Replay form: a recorded T-W1 that under-counted `in_flight` violates `inv_S17_capAtClaim`. |
| `n2CancelFreesSlotTest` | RC | N2: 2 handlers execute on a cap-1 topic after a cancel. |
| `g2StrandedParentTest` | RC | G2: T-X2 strands the parent (S24). |
| `n1WrongLineageTest` | RC | N1: T-X2 on a child key supersedes a sibling. |
| `g11CancelTest` | RC | G11: a cancelled parent leaves its child `ready`. |
| `staleCoordinatorTest` | RC | S2/S3: the stale coordinator loses the fence. |
| `activitySuccessTest` | RC | Happy path with the replay form of T-W1. |
| `startSemanticsTest` | RC | T-X1: dedup hit returns the row; restart-key collision → `Conflict`, no row. |
| `startBothKeysRejectedTest` | RC | T-X1 with a key and a restart source is not a step. |
| `n3LiveSourceTest` | RC | N3: a public start with `restarted_from_workflow_id` = a live row. |
| `tx2ConflictTest` | RC | T-X2 restart-key collision → `Conflict`, nothing written. |
| `g7CapExceededTest` | RR (historical) | G7: 2 live leases on a cap-1 topic under the RR snapshot. |
| `g7NeedsSnapshotTest` | RR (historical) | Same schedule with the heartbeat before the snapshot: no violation. |
| `driftTwoHandlersTest` | DRIFT=2 | S13 fails when the process clock lags DB time. |
| `reconcileReplayDelayTest`, `reconcileReplayEarlyRejectedTest` | RC | A replayed reconcile returns the row to pending at `availableAt` (the retry delay); an `availableAt` before `tnow` is rejected. |
| `g10InvalidBoundsTest` | RC, `ENABLE_ENV_EDITS` | An external write gives a1 (topic t) invalid bounds; one `claim_batch` claims a2 on topic u, then `TW1_Error(2, 1, "invalid_bounds")` rolls it back: a2 stays pending, no execution, task error. `inv_G10_noClaimAbort` fails; `safety` holds. |
| `pauseResumeTest`, `pauseTwiceRejectedTest`, `resumeNotPausedRejectedTest` | RC | T-A2/T-A3: pausing a claimed row clears its lease; the coordinator's commit misses the fence (operator-caused, `inv_G1_noSelfCancelFromOperator` fails); resume → `ready` at `tnow`. Pausing a paused row and resuming a non-paused row are not steps. |
| `g1SelfCancelTest`, `g1StoppedRuntimeClaimsNothingTest`, `g1AppCancelNotOperatorTest` | RC, `MAX_TASK_RESTARTS = 1` | G1: two operator pauses during claims stop runtime 1 (`wit_selfCancelled`), which then claims nothing; an application cancel's fence miss is a task error but not operator-caused. |
| `n2PauseFreesSlotTest`, `pausedActivityNotClaimedTest` | RC | S36 + N2: pausing during a running activity sets it `pending` with `maxAttempts + 1` and closes the attempt; the paused handler still executes while r2 claims the cap-1 slot (`inv_S13_topicConcurrency` fails); its finish misses the fence; resume → `waiting_activity`. A paused workflow's activity is not claimable. |
| `pausedParentWokenTest`, `pausedParentResumesWaitingTest` | RC | A paused parent gets its child's outcome and stays paused (wait cleared), then resumes to `ready`; resumed before the child ends → `waiting_child`. |
| `g11AdminCancelTest`, `adminCancelPausedTest` | RC | G11 through the operator cancel; admin cancel of a paused workflow cancels its pending activity. |
| `continuationPriorityTest`, `continuationPriorityNowRejectedTest`, `noPriorityEarlyRejectedTest` | RC | A continuation-priority activity is inserted at `CONTINUATION_READY_AT` (0 < `tnow`) and claimed; `prio` with `availableAt = now`, or no `prio` with `availableAt < tnow`, is not a step. |
| `n1ReturnLatestTest`, `n1ReturnOwnRowTest` | RC | N1, second variant: T-X2 on the child key returns a newer live sibling (`TX2_ReturnLatest` flags it); returning the keyed row itself does not. |
| `g10InvalidRowNotClaimedTest`, `g10WrongReasonTest`, `g10ReplayTest` | RC, `ENABLE_ENV_EDITS` | The invalid row cannot be claimed; the `attempt_cap` reason does not match it; the replay form (no T-W1 in flight) takes the same error. |

### Random simulation (`quint run`, 20,000 samples, 40 steps; 80 steps where noted)

From `results/summary.txt`. Interface v4 (operator actions in `step`): `safety`,
`safetyRc` and `inv_S24_exceptTX2` on `durable_mc` were rerun at 20,000 x 40
and hold; the other rows are from v3 and were not rerun (the host was
overloaded; `step` is about 3 times slower per sample with the new branches).

| Instance | Property | Expected | Result |
|---|---|---|---|
| `durable_mc` | `safety` (S1, S2, S5, S6-S12, S13 per activity, S14-S16, S18, S19, S23, S24 except T-X2, S25) | hold | no violation |
| `durable_mc` | `safetyRc` (= `safety` + S17 at claim and between commits) | hold | no violation |
| `durable_mc` | `inv_S24_exceptTX2` | hold | no violation |
| `durable_mc_act` | `safetyRc` | hold | no violation |
| `durable_mc_rr` | `safety` | hold | no violation |
| `durable_mc_rr`, `durable_mc_act_rr` | `inv_S17_capAlways`, `inv_S17_capAtClaim` (G7, historical) | violate | not found at this budget; found by `g7CapExceededTest` |
| `durable_mc` | `inv_G10_noClaimAbort` | hold | no violation (no invalid row without external writes) |
| `durable_mc_env` | `safety` (external writes and invalid-bounds commands on) | hold | no violation |
| `durable_mc_env` | `inv_G10_noClaimAbort` (G10) | violate | violated (17 states) |
| `durable_mc` | `inv_G11_cancelReachesChildren` | violate | violated (11 states) |
| `durable_mc` | `inv_N1_tx2OwnLineage` (N1) | violate | violated (24 states; first time found by simulation) |
| `durable_mc` | `inv_S19_sourceTerminal` (N3) | violate | violated (3 states) |
| `durable_mc` | `inv_G1_noSelfCancelFromOperator` (G1, interface v4) | violate | violated at 2,000 samples (7 states: claim, `AdminPause`, `CoordFenceMiss`) |
| `durable_mc` | `inv_S24_parentWakes` (G2) | violate | not found at 40 or 80 steps; found by `g2StrandedParentTest` |
| `durable_mc`, `durable_mc_act` | `inv_S13_topicConcurrency` (N2) | violate | not found at 40 (or 80) steps; found by `n2CancelFreesSlotTest` |
| `durable_mc_drift` | `inv_S13_oneHandler` | violate | not found at 40 or 80 steps; found by `driftTwoHandlersTest` |
| `durable_mc` | witnesses S3, blocked, child succeeded, activity succeeded, coordinator fence miss, reconcile row, commit inside an open T-W1 | violate | all violated (reachable) |
| `durable_mc` | `wit_revivedLease` | violate | not found at 40 or 80 steps; reached in `g7ClosedUnderRcTest` |

T-W1 now takes 5 or more steps, and `step` has more always-enabled branches
(dedup hits, `TX2_ReturnLatest`, `TW1_SkipRow`), so uniform simulation reaches
long schedules less often than before. The directed tests carry every gap.

### Apalache (`quint verify`)

| Property | Bound | Result | Time |
|---|---|---|---|
| `safetyRc` (`safety` + both S17 forms) on `durable_mc` | depth 3, all interleavings | no violation | ~4 min |
| `safetyRc` on `durable_mc` | depth 4 | stopped, no violation so far (10 of ~41 transitions at step 4 checked) | stopped at 36 min; estimated 2 h more |

The new state (per-runtime T-W1 buffers and locks) makes each Apalache step
slower than in the first iteration (depth 4 took 16 min then). The depth-4
run was stopped to stay inside the time budget. Depth 3 is complete. Note
that a T-W1 alone takes 5 steps, so Apalache at this depth cannot reach any
T-W1 interleaving; the G7 argument rests on the directed tests and the
reasoning below.

The temporal properties typecheck but were not model-checked.

### Temporal properties (stated, not checked)

`L1_readyClaimed`, `L2_expiredRecovered`, `L5_expiredReconciled` in
`durable.qnt`. Fairness assumptions are hypotheses of each formula: weak
fairness of `TC1_Claim` (recovery and claim) for every runtime and workflow
(F2, F3), and of every simulation T-W1 step (F2, L5's local capacity). F1
(time advances) is `Tick` with a bounded clock. Crashes must eventually stop.

## Gap status

| Gap | Status in this model | Evidence |
|---|---|---|
| G1 (operator pause/cancel during a claim costs the restart budget) | Reproduces: the fence miss is an operator-caused task error; with `MAX_TASK_RESTARTS = 1` two of them stop the runtime | `pauseResumeTest`, `g1SelfCancelTest`, simulation; both recorded G1 gap tests (`inv_G1_noSelfCancelFromOperator`) |
| G2 (T-X2 strands the parent) | Reproduces, unchanged | `g2StrandedParentTest` |
| G4 (stale `sequence`) | Not modeled; closed for library transactions by READ COMMITTED per INVARIANTS.md | — |
| G7 (topic cap exceeded by one) | **Closed** under READ COMMITTED; reproduces only in the historical RR instance | `g7ClosedUnderRcTest`, `g7RcHeartbeatBlockedTest`, `safetyRc` holds in simulation and in Apalache at depth 4; `g7CapExceededTest` (RR) |
| G10 (one invalid row aborts every topic's claims) | Reproduces with external writes (`ENABLE_ENV_EDITS`): a claimable row with invalid bounds makes `TW1_Error(r, a, "invalid_bounds")` roll back the whole T-W1. Holds without them (the attempt-cap error has no reachable row) | `g10InvalidBoundsTest`, `g10ReplayTest`; `durable_mc_env` / `durable_mc` simulation; the recorded G10 gap test trace |
| G11 (cancel does not reach children) | Reproduces through T-X3 and the operator cancel (T-A4) | `g11CancelTest`, `g11AdminCancelTest`, simulation, the recorded G11 gap test |
| N1 (T-X2 lineage on a child key) | Reproduces in both variants: supersession of a blocked sibling, and `TX2_ReturnLatest` of a live one | `n1WrongLineageTest`, `n1ReturnLatestTest`, simulation, both recorded N1 gap tests |
| N2 (revoke frees the slot, cap exceeded in execution) | Reproduces through application cancel and operator pause | `n2CancelFreesSlotTest`, `n2PauseFreesSlotTest`, both recorded N2 gap tests |
| N3 (new: public restart source not checked) | Reproduces | `n3LiveSourceTest`, simulation (3 states) |
| S13 with clock drift | Reproduces, unchanged | `driftTwoHandlersTest` |

### Why G7 is closed under READ COMMITTED

For a claim to exceed the cap, `in_flight` (a plain read) must miss an
activity A that is live after the commit. Under READ COMMITTED `in_flight`
sees every commit before its statement. So A must be not live at that point
and become live before T-W1 commits. Only a claim (needs the topic lock,
which T-W1 holds) or a heartbeat (only moves `leaseExpiresAt` forward: one
heartbeat at a time per claim, `heartbeat_loop` is sequential) can make it
live. If A is running with an expired lease at the count, it was also expired
at the earlier reconcile scan, so reconcile relocked it; the relock is a
current read, and it either reconciled A (not running any more) or skipped A
because a heartbeat had revived it (then the count sees it live). A heartbeat
after the relock blocks on the row lock and then misses the fence. INVARIANTS.md
and `docs/design/multi-backend.md` say G7 is "narrowed"; the model says closed,
under two conditions: (1) `lease_expires_at` is written only by the claim and
the heartbeat (checked: `activity_worker.rs` claim and `heartbeat_once` are
the only writers), and (2) at most one heartbeat per claim is in flight. The
lease can still be revived after it expired (`wit_revivedLease`), which costs
the activity an attempt later but does not exceed the cap.

### N3 (new): the public restart field accepts a live source

`StartOptions.restarted_from_workflow_id` is public. `insert_prepared` stores
it after only the restart-key uniqueness check, so an application can start a
"successor" of a running workflow (`n3LiveSourceTest`). S19 as written in
INVARIANTS.md (one successor per source) still holds; the model's stronger
form (the source is terminal) does not. The same start can also take the
restart key of a failed row outside its lineage, which then makes T-X2's
recovery of that row return `Conflict` (`tx2ConflictTest`).

## Counterexamples (unchanged from the first iteration)

G2, N1, N2, G11 and the drift case follow the same steps as before (see the
directed tests); only the T-W1 steps are now statement groups. G7 now needs
`RR_SNAPSHOT = true`.

## What to model next

1. Operator restart and retry (T-A5 supersession with `child_superseded`
   wakes, T-A6 replacement activity). T-A5 runs `insert_prepared` inside its
   transaction, so its declaration must replace the nested `TX1_Start`.
2. The recorder does not see `RuntimeConfig.max_task_restarts`; record it so
   a trace can reproduce the self-cancel itself, not only its cause.
3. Timers and approvals (T-T1, T-A1, T-A7; S20 to S22).
4. `(kind, version)` for children (G6).
5. Schedules (T-S1, T-S2, T-A8, T-A9); G5 and G12.
6. A guided simulation (or TLC through the TLA+ transpiler) that reaches the
   long schedules (G2, N2, drift) without directed tests.
7. Trace checking with the replay interface above.
