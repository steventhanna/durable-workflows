# Durable Workflows: Protocol Invariants

This document is the specification baseline for a Quint/TLA+ model of the
durable workflow runtime and for later trace-checking the implementation
against model-generated traces. It describes what the code does today, not
what it should do.

## Conventions

- Paths: `src/...`, `tests/...`, and `migrations/...` are relative to the
  `durable-workflows/` crate. The schema is the single baseline migration
  `migrations/mysql/2026-09-24-000000_durable_baseline/up.sql` (abbreviated
  `M`). Database columns are snake_case; this document names them in
  camelCase (`leaseExpiresAt` is the column `lease_expires_at`).
- Keys compare by exact bytes: every table uses `utf8mb4_bin`, so `provider`
  and `PROVIDER` (or `cafe` and `café`) are distinct topics, kinds, and keys.
- `now` always means database time in epoch milliseconds: `UTC_TIMESTAMP(3)`
  on MySQL (`src/dialect/mysql.rs`), `clock_timestamp() AT TIME ZONE 'UTC'`
  on Postgres (`src/dialect/postgres.rs`), read through
  `persistence::database_now_millis`; sampled once per transaction unless
  noted.
- A **fence** is a `WHERE` predicate on an `UPDATE` whose affected-row count
  must be exactly 1; otherwise the code returns `DurableError::FencedWrite`
  and the enclosing transaction rolls back (e.g. `src/runtime/coordinator.rs` `ensure_fenced`,
  `src/runtime/activity_worker.rs` `ensure_fenced`).
- `FOR UPDATE` is a blocking locking read; `FOR UPDATE SKIP LOCKED` skips rows
  locked by other transactions. A plain `SELECT` is a consistent (snapshot)
  read; its snapshot depends on the isolation level (see §5.1).
- Transaction identifiers (`T-C1`, `T-W3`, ...) are defined in §2 and are
  intended to become model actions.
- Confidence tags: **ENFORCED** (mechanism traced), **ASSUMED** (relied upon,
  nothing enforces it), **UNCLEAR** (could not decide from the code).

---

## 1. Entities and state machines

### 1.1 Workflow (`durable_workflow`, M:1-45)

Fields that matter for the protocol: `status`, `waitKind`, `waitReferenceId`,
`availableAt`, `leaseOwner/leaseToken/leaseExpiresAt`, `commandSequence`,
`deliveredEventSequence`, `stateJson/stateVersion`, `activationAttempts`,
`maxActivationAttempts` (always 8 at insert, `src/store.rs` `DEFAULT_MAX_ACTIVATION_ATTEMPTS`),
`consecutiveContinuations`, `deduplicationKey` (unique with `kind`, M:33),
`restartedFromWorkflowId` (unique, M:34), `rootWorkflowId`,
`parentWorkflowId/parentCommandSequence` (informational only; never read by
the runtime), `scheduleRunId`.

Statuses (`src/persistence/mod.rs` `WorkflowStatus`): `ready`, `running`,
`waiting_activity`, `waiting_child`, `sleeping`, `waiting_approval`, `paused`,
`blocked`, `succeeded`, `failed`, `cancelled`. Terminal: `succeeded`,
`failed`, `cancelled`.

Wait coupling (by construction; no DB constraint):

| status | waitKind | waitReferenceId |
|---|---|---|
| ready, running | NULL | NULL |
| waiting_activity, blocked | `activity` | activity id |
| waiting_child | `child` | child workflow id |
| sleeping | `timer` | the workflow's `commandSequence` |
| waiting_approval | `approval` | approval id |
| paused | kept from the prior status, or cleared by a wake while paused | |
| terminal | NULL, except rows cancelled by restart/recovery (see T-X2, T-A4) | |

Transitions:

| from | to | trigger | actor | code |
|---|---|---|---|---|
| (none) | ready | insert + `started` event (seq 1, delivery 1) | app, coordinator (child), materializer, admin | `src/persistence/workflows.rs` `insert_started` |
| ready | running | claim, new UUID lease | coordinator | `src/runtime/coordinator.rs` `WorkflowCoordinator::claim_row` |
| running | ready | expired-lease recovery | any coordinator | `src/runtime/coordinator.rs` `WorkflowCoordinator::claim_row` |
| running | ready | `Continue` commit | coordinator | `src/runtime/coordinator.rs` `commit_on_connection` |
| running | ready | activation failure, not exhausted | coordinator | `src/runtime/coordinator.rs` `WorkflowCoordinator::record_activation_failure` |
| running | failed | activation failure, exhausted | coordinator | `src/runtime/coordinator.rs` `WorkflowCoordinator::record_activation_failure` |
| running | succeeded | `Complete` commit | coordinator | `src/runtime/coordinator.rs` `commit_on_connection` |
| running | sleeping | `SleepUntil` commit | coordinator | `src/runtime/coordinator.rs` `commit_wait_transition` |
| running | waiting_approval | `WaitForApproval` commit | coordinator | `src/runtime/coordinator.rs` `commit_wait_transition` |
| running | waiting_activity | `RunActivity` commit | coordinator | `src/runtime/coordinator.rs` `commit_activity` |
| running | waiting_child | `RunChild` commit | coordinator | `src/runtime/coordinator.rs` `commit_child` |
| waiting_activity | ready | activity success wake | activity worker | `src/runtime/activity_worker.rs` `wake_workflow` |
| waiting_activity | blocked | activity dead-letter | activity worker (finish, reconcile, or claim quarantine) | `src/runtime/activity_worker.rs` `block_workflow` |
| waiting_child | ready | child terminal wake | whoever makes the child terminal | `src/persistence/workflows.rs` `wake_loaded_parent_on_child_terminal` |
| sleeping | ready | timer fired | timer materializer | `src/runtime/temporal.rs` `TimerMaterializer::materialize_one` |
| waiting_approval | ready | approval resolved | admin | `src/admin/control.rs` `AdminControlService::resolve_approval` |
| waiting_approval | ready | approval expired | approval materializer | `src/runtime/temporal.rs` `ApprovalExpiryMaterializer::materialize_one` |
| any non-terminal except paused (includes running) | paused | pause | admin | `src/admin/control.rs` `AdminControlService::pause_workflow` |
| paused | paused (wait cleared, event appended) | child wake / approval resolve / approval expiry | as above | `src/persistence/workflows.rs` `wake_loaded_parent_on_child_terminal`, `src/admin/control.rs` `AdminControlService::resolve_approval`, `src/runtime/temporal.rs` `clear_wait` |
| paused | ready / sleeping / waiting_activity / waiting_child / waiting_approval / blocked | resume (derived from wait) | admin | `src/admin/control.rs` `AdminControlService::resume_workflow`, `resume_status` |
| blocked | waiting_activity | retry dead-lettered activity | admin | `src/admin/control.rs` `AdminControlService::retry` |
| any non-terminal (includes running) | cancelled | cancel | app (`cancel_with_conn`) or admin | `src/store.rs` `cancel_locked_workflow` |
| paused, blocked | cancelled | superseded by restart | admin | `src/admin/control.rs` `AdminControlService::restart` |
| blocked | cancelled | superseded by recoverable start | app | `src/store.rs` `DurableStore::start_or_restart_recoverable_with_conn` |

No transition leaves a terminal status (see S6).

### 1.2 Activity (`durable_activity`, M:65-104)

Statuses (`src/persistence/mod.rs`): `pending`, `running`, `cancelling`,
`succeeded`, `dead_lettered`, `cancelled`. `cancelling` is a revoked
`running` attempt whose handler may still execute: it keeps its lease, its
open attempt and its topic slot until the handler stops (T-W3) or the lease
expires (reconcile), which settles it (N2). Unique
`(workflowId, commandSequence, replacementNumber)` (M:94); `maxAttempts > 0`
(M:103).

| from | to | trigger | actor | code |
|---|---|---|---|---|
| (none) | pending | `RunActivity` commit (`availableAt = 0` if continuation priority) | coordinator | `src/runtime/coordinator.rs` `commit_activity` |
| (none) | pending | replacement of a dead-lettered activity (`replacementNumber+1`) | admin | `src/admin/control.rs` `AdminControlService::retry` |
| pending | running | claim, `attemptCount+1`, new UUID lease | activity worker | `src/runtime/activity_worker.rs` `ActivityWorker::claim_locked_candidate` |
| running | succeeded | finish success | activity worker | `src/runtime/activity_worker.rs` `finish_on_connection` |
| running | pending | retryable failure, attempts left (backoff) | activity worker | `src/runtime/activity_worker.rs` `finish_on_connection` |
| running | pending | expired lease, attempts left (backoff) | activity worker (claim txn) | `src/runtime/activity_worker.rs` `reconcile_expired` |
| running | cancelling | pause of the waiting workflow; `maxAttempts+1`, `lastErrorCategory=operator_paused`; lease and open attempt kept | admin | `src/admin/control.rs` `pause_activity` |
| running, cancelling | cancelling | workflow cancel / restart; `lastErrorCategory` = `application_cancelled` or `operator_cancelled`; lease and open attempt kept | app, admin | `src/store.rs` `cancel_activities` |
| cancelling | pending | revoke settled (handler stopped, or lease expired) while the workflow is not terminal; `availableAt = now` | activity worker | `src/runtime/activity_worker.rs` `settle_revoked` |
| cancelling | cancelled | revoke settled while the workflow is terminal | activity worker | `src/runtime/activity_worker.rs` `settle_revoked` |
| running | dead_lettered | permanent failure, or retryable on last attempt | activity worker | `src/runtime/activity_worker.rs` `dead_letter` |
| running | dead_lettered | expired lease on last attempt | activity worker (claim txn) | `src/runtime/activity_worker.rs` `reconcile_expired` |
| pending | dead_lettered | quarantine at claim: attempt cap reached, invalid timeout/lease bounds or a stored retry policy out of bounds (`invalid_row`, G10) | activity worker (claim txn) | `src/runtime/activity_worker.rs` `quarantine_candidate` |
| pending | cancelled | workflow cancel / restart | app, admin | `src/store.rs` `cancel_activities` |
| dead_lettered | cancelled | recoverable start of a blocked/failed lineage | app | `src/store.rs` `DurableStore::start_or_restart_recoverable_with_conn` |

### 1.3 Activity attempt (`durable_activity_attempt`, M:106-122)

Primary key `(activityId, attemptNumber)`, `attemptNumber > 0`. States: open
(`finishedAt IS NULL`, `outcome IS NULL`) and closed. Created only by claim
(`src/runtime/activity_worker.rs` `ActivityWorker::claim_locked_candidate`). Closed exactly once, fenced on
`leaseToken = claim token AND finishedAt IS NULL`:

| outcome | code |
|---|---|
| `succeeded`, `retryable_failure`, `dead_lettered` | `src/runtime/activity_worker.rs` `finish_attempt` |
| `lease_expired` | `src/runtime/activity_worker.rs` `reconcile_expired` |
| `operator_paused`, `operator_cancelled`, `application_cancelled` (the row's `lastErrorCategory`) | when the revoked handler stops: `src/runtime/activity_worker.rs` `settle_revoked` via `finish_on_connection` |
| `lease_expired` for a `cancelling` row | `src/runtime/activity_worker.rs` `reconcile_expired` → `settle_revoked` |

A `cancelling` row's attempt stays open: the revoke only records its outcome
in `lastErrorCategory`/`lastErrorMessage` (N2).

Heartbeats update `heartbeatAt` on the open attempt only
(`src/runtime/activity_worker.rs` `heartbeat_once`). Progress events (≤100 per attempt,
M:138) hang off the attempt (`src/progress.rs` `ProgressReporter::report`).

### 1.4 Approval (`durable_approval`, M:142-164)

Statuses: `pending`, `resolved`, `expired`, `cancelled` (plain strings). Unique
`(workflowId, commandSequence)` (M:159).

| from | to | actor | code |
|---|---|---|---|
| (none) | pending | coordinator (`WaitForApproval`) | `src/runtime/coordinator.rs` `commit_wait_transition` |
| pending | resolved | admin (`resolve_approval`), only while `expiresAt > now` | `src/admin/control.rs` `AdminControlService::resolve_approval` |
| pending | expired | approval materializer, only when `expiresAt <= now` | `src/runtime/temporal.rs` `ApprovalExpiryMaterializer::materialize_one` |
| pending | cancelled | workflow cancel / admin restart | `src/store.rs` `cancel_approvals` |

### 1.5 Timer

Timers have no table. An armed timer is the workflow tuple
`(status=sleeping, waitKind=timer, waitReferenceId=commandSequence, availableAt=wakeAt)`
set at `src/runtime/coordinator.rs` `commit_wait_transition`. States: armed → fired
(`src/runtime/temporal.rs` `TimerMaterializer::materialize_one`: `TimerFired` delivery event, workflow →
ready), suspended (workflow paused keeps the wait; the materializer only
selects `status=sleeping`, `src/runtime/temporal.rs` `TimerMaterializer::materialize_one`), resumed (resume
restores `sleeping` with the original `availableAt`,
`src/admin/control.rs` `resume_status`, `AdminControlService::resume_workflow`), cancelled (workflow cancelled).

### 1.6 Child workflow

A child is an ordinary workflow row inserted in the parent's `RunChild` commit
(`src/store.rs` `DurableStore::insert_child`) with deduplication key `child:{parentId}:{command}`
or a caller-supplied domain key (`src/runtime/coordinator.rs` `commit_child`). A dedup
hit reuses the newest recovery generation of the existing row (any parent):
`insert_child` locks the keyed row and follows its `restartedFromWorkflowId`
chain (`lock_newest_generation`, D4), then checks the version. A key the
pre-read misses but the insert collides with (a concurrent commit inserted it)
takes the same path: the conflict returns the keyed row locked, and the same
walk and version check follow (G6, fixed); a mismatch is `DefinitionMismatch`.
A key that resolves to the caller or one of its ancestors
(`parentWorkflowId` chain) is `InvalidDefinition` (G8, fixed). Both roll back
the commit and are T-C3 activation failures. Parent-side
states:

| parent state | meaning |
|---|---|
| `waiting_child`, `waitReferenceId=C` | awaiting C |
| `paused`, `waitKind=child`, `waitReferenceId=C` | paused while awaiting C; still woken |
| `ready` + undelivered `child_succeeded`/`child_failed` event | outcome delivered, not yet consumed |

Delivery happens in the child's terminal transaction through
`wake_waiting_parents_on_child_terminal` (`src/persistence/workflows.rs`),
called from: `Complete` (`src/runtime/coordinator.rs` `commit_on_connection`), activation
exhaustion (`WorkflowCoordinator::record_activation_failure`), cancel (`src/store.rs` `cancel_one_workflow`), admin restart
supersession (`src/admin/control.rs` `AdminControlService::restart`), attach-to-already-terminal
child (`src/runtime/coordinator.rs` `commit_child`), and recoverable-start
supersession when the successor runs another version (`child_superseded`).
When the successor runs the same version, recoverable-start supersession
re-attaches the waiting parents to it instead (`src/store.rs`
`hand_waiting_parents_to_successor`, history `child_wait_reattached`); the
successor's terminal transaction then delivers the outcome (G2 fixed).

### 1.7 Schedule state and schedule run

`durable_schedule_state` (M:166-181): one row per schedule key with cursor
`(nextLocalOccurrence, nextOccurrenceAt)`, `pausedAt`, and the pinned
`(definitionVersion, definitionFingerprint)`.

| change | actor | code |
|---|---|---|
| insert (cursor = first occurrence after `now`) | runtime spawn / materializer tick | `src/schedule.rs` `ScheduleRegistry::reconcile_state` |
| upgrade (new version; cursor reset to first occurrence after `now`) | runtime spawn / materializer tick | `src/schedule.rs` `ScheduleRegistry::reconcile_state` |
| cursor advance | materializer | `src/runtime/schedule_materializer.rs` `ScheduleMaterializer::materialize_schedule` |
| pause / resume | admin | `src/admin/control.rs` `AdminControlService::set_schedule_paused` |

`durable_schedule_run` (M:183-199), unique `(scheduleKey, localOccurrence)`
(M:195). Statuses: `materializing` (inserted and changed to `started` in the
same transaction; never committed), `started`, `queued`, `skipped`,
`coalesced`.

| from | to | actor | code |
|---|---|---|---|
| (none) | materializing → started | materializer | `src/runtime/schedule_materializer.rs` `materialize_occurrence` |
| (none) | queued | materializer (QueueOne, active run) | `src/runtime/schedule_materializer.rs` `materialize_occurrence` |
| (none) | skipped / coalesced | materializer | `src/runtime/schedule_materializer.rs` `materialize_occurrence` |
| queued | started | materializer promotion | `src/runtime/schedule_materializer.rs` `promote_queued` |
| (none) | materializing → started, `localOccurrence = manual:{t}` | admin run-now | `src/admin/control.rs` `AdminControlService::run_schedule_now` |
| started (workflowId X) | started (workflowId = successor) | admin restart | `src/admin/control.rs` `AdminControlService::restart` |

### 1.8 Workflow event log (`durable_workflow_event`, M:47-63)

Unique `(workflowId, sequence)` and `(workflowId, deliverySequence)`
(M:59-60). Two kinds:

- **Deliverable** (`deliverySequence` non-NULL): `started`, `continued`,
  `activity_succeeded`, `child_succeeded`, `child_failed`, `timer_fired`,
  `approval_resolved`, `approval_expired`. Only these reach workflow code
  (`src/runtime/coordinator.rs` `WorkflowCoordinator::activate_claim_inner`, `decode_event`).
- **History** (`deliverySequence` NULL): everything else (lease recovery,
  scheduling, dead-letter, operator events).

`sequence` is `MAX(sequence)+1` from a consistent read
(`src/persistence/events.rs` `next_event_sequence`); `deliverySequence` is
`deliveredEventSequence + 1` read from the locked workflow row (or
`delivered + 1` of the event just consumed for `continued`).

### 1.9 Topic lock (`durable_topic_lock`, M:201-207)

One row per topic holding `maxConcurrency`. Rows are inserted once per
registry (`INSERT IGNORE`) and must match the registered cap
(`src/registry.rs` `TopicRegistry::seed_locks`, `seed_locks_once`). The rows are used only as a mutex for claims.

---

## 2. Actors and their atomic steps

### 2.1 Supervisor (`src/runtime/supervisor.rs`)

One runtime spawns these tasks (`supervise`): activity-execution collector,
coordinator (one sequential loop), health (read-only), timer, approval-expiry,
one task per schedule key, and the activity dispatcher. Any task that returns
`Err` or panics is restarted after `restart_backoff` (`spawn_child`). The restart
counter per task kind counts restarts within `restart_window` (default 10
minutes; the count starts again once the window since its first counted
restart has passed); when it exceeds `max_task_restarts` (default 8, `RuntimeConfig::default`) in
one window, the whole runtime cancels itself (`RestartBudget`). A collector failure cancels immediately (`supervise`).
Startup checks readiness and topic caps before any claim (`DurableRuntime::spawn`).

### 2.2 Coordinator (`run_task` Coordinator, `src/runtime/supervisor.rs`)

Per iteration: `activate_one` (`src/runtime/coordinator.rs`). If no
claim, sleep `idle_delay` (2 s). A lost fence (`FencedWrite`) or a transient
database error (deadlock, serialization failure, lock wait timeout;
`dialect::is_transient_error`) from T-C2 or T-C3 is logged and the loop goes
on: an operator action or lease recovery already moved the row on, or the
rolled-back row stays `running` until lease recovery (L2). Each skipped
activation is counted by kind (`BenignActivationKind::FenceMiss` or
`Transient`) in the runtime's process-local `ActivationCounters`
(`RuntimeHandle::activation_counters`). When more than
`RuntimeConfig::max_transient_activation_errors` (10) transient errors fall
in one `transient_activation_error_window` (5 min), the next health report
carries `HealthAlert::TransientActivationErrors` (logged and passed to the
`HealthAlertSink`), so a database that keeps aborting activations is not
silent. `WorkflowClaim::activate` itself still returns the error and counts
nothing. Any other error ends the task (G1, fixed).

A coordinator holds at most one outstanding claim: `claim_one` takes
`&mut self` and the returned `WorkflowClaim` borrows the coordinator until
`WorkflowClaim::activate` consumes it, so the model's one-claim-per-runtime
rule (`TC1_Claim` requires `not(proc.claims[r].active)`) is enforced by the
type system per coordinator. One worker id should map to one coordinator:
the fence is the claim's own lease token, so extra coordinators are safe, but
the trace checker maps a worker id to one model runtime.

**T-C1 claim** (`src/runtime/coordinator.rs` `WorkflowCoordinator::claim_row`), one transaction:
1. `now` ← DB.
2. Expired-lease recovery: page through `status=running AND leaseExpiresAt <= now`
   (consistent read, 32 per page, cursor on `(leaseExpiresAt, id)`); for each
   id, `FOR UPDATE SKIP LOCKED` and recheck; for the first match, fenced
   `UPDATE ... WHERE status=running AND leaseToken=<old>` → `ready`,
   `availableAt=now`, lease cleared; append history `lease_recovered`
   (`WorkflowCoordinator::claim_row`). At most one recovery per transaction. `activationAttempts`
   is not changed.
3. Ready claim: consistent read of ≤32 ids with `status=ready`,
   `availableAt <= now`, `(kind, version)` in the local registry, ordered
   `(availableAt, id)`; for each, `FOR UPDATE SKIP LOCKED` with the same
   filter; first hit is updated with fence `status=ready` → `running`,
   `leaseToken=uuid4`, `leaseExpiresAt=now+lease_duration` (30 s default)
   (`WorkflowCoordinator::claim_row`).

There is no workflow lease renewal anywhere in the code.

**L-C1 activation (no transaction, no lock)** (`WorkflowCoordinator::activate_claim_inner`): read the lowest
deliverable event with `deliverySequence > deliveredEventSequence`
(`src/persistence/events.rs` `next_delivery_event`); error if none. Run
`WorkflowRegistry::step_stored(input, state, event)` (user code) under an
unwind boundary and `CoordinatorConfig::step_timeout` (default 30 s): a panic
or a timeout is an activation failure (T-C3; G3, fixed). A build with
`panic = "abort"` still aborts the process on a panic.
Validate that a `RunChild`/`RunActivity` target is registered locally and the
activity topic matches.

**T-C2 commit** (`src/runtime/coordinator.rs` `WorkflowCoordinator::commit_transition`, `commit_on_connection`, `commit_wait_transition`, `commit_activity`, `commit_child`), one
transaction. `now` ← DB. Every branch includes the fenced update
`WHERE id AND status=running AND leaseToken=claim` (`fenced_workflow!`); `RunActivity`
and `WaitForApproval` first lock the row under the same fence
(`SELECT ... FOR UPDATE`, `lock_fence`) before they insert their command row,
so a stale claim gets `FencedWrite` rather than a unique-key error (N4). The
update sets
`deliveredEventSequence = consumed event`, the new `stateJson`,
`stateVersion+1`, clears the lease, resets `activationAttempts`:
- `Continue`: → `ready`; append deliverable `continued` with
  `delivery = consumed+1`. After `max_consecutive_continuations` (16) in a row,
  `availableAt = now + continuation_delay` and the streak resets (`commit_on_connection`).
- `Complete`: → `succeeded`, `resultJson`; history; wake parents (child-terminal
  scan `FOR UPDATE`) (`commit_on_connection`).
- `SleepUntil`: `commandSequence+1`; → `sleeping`, wait `timer`, `availableAt=wakeAt` (`commit_wait_transition`).
- `WaitForApproval`: fence lock; INSERT approval (pending); `commandSequence+1`;
  → `waiting_approval`.
- `RunActivity`: fence lock; INSERT activity (pending) before the fenced
  update; `commandSequence+1`; → `waiting_activity`.
- `RunChild`: dedup lookup (consistent read) and upsert child row; a dedup
  hit on either path resolves to the newest generation, whose version must
  match (G6, fixed), and must not be the caller or an ancestor (G8, fixed);
  if the child already existed, lock it `FOR UPDATE` (current read) **before** the
  fenced parent update (G9, fixed); fenced parent update → `waiting_child`;
  history; if the locked child is terminal, wake waiting parents in the same
  transaction. It takes no fence lock first: that would lock the parent
  before the child, and `insert_child` resolves a duplicate key to the
  existing row, so a stale claim still ends in `FencedWrite` at the update.
A fence miss rolls back the whole transaction, including the inserts.

**T-C3 activation failure** (`WorkflowCoordinator::record_activation_failure`), one transaction: lock the row with
the claim fence (`FOR UPDATE`); `attempt = activationAttempts+1`; exhausted iff
`attempt >= min(maxActivationAttempts, config.max_activation_attempts)`;
fenced update → `failed` (+`completedAt`, wake parents) or `ready` with
`availableAt = now + backoff(attempt)`. The deliverable event is not consumed.
Used for handler errors, a `step` panic or `step_timeout`, unregistered
child/activity, topic mismatch, and `DefinitionMismatch`/`InvalidDefinition`
from T-C2.

### 2.3 Activity dispatcher and executions (`src/runtime/supervisor.rs` `run_task`)

Per iteration: surface a pending execution panic (task error); compute
`available = Σ local topic caps − active executions`; if 0, wait for a change;
otherwise T-W1 and spawn one execution task per claim. Empty sweeps back off
1/2/5/10 s with per-runtime jitter.

**T-W1 claim_batch** (`src/runtime/activity_worker.rs`), one
transaction:
1. Lock **all** registered topic rows `FOR UPDATE SKIP LOCKED`, ordered by
   topic. If any is missing, return no claims (`ActivityWorker::claim_batch`).
2. Sample the local monotonic instant, then `now` ← DB (`ActivityWorker::claim_batch`).
3. For each topic:
   a. `reconcile_expired`: consistent read of lease holders
      (`status IN LEASE_HOLDERS` = `running`, `cancelling`) with
      `leaseExpiresAt <= now OR NULL`; for each: lock workflow `FOR UPDATE`
      (blocking), relock the activity `FOR UPDATE` and recheck. A `running`
      row: fenced `UPDATE WHERE status=running AND attemptCount=k AND leaseToken=t`
      → `pending` (backoff) or `dead_lettered` (if `attemptCount >= maxAttempts`);
      close the attempt `lease_expired`; history `activity_lease_expired`; if
      dead-lettered and the workflow waits on it, block the workflow. A
      `cancelling` row: `settle_revoked` with outcome `lease_expired` (close
      the attempt; fenced `WHERE status=cancelling AND attemptCount=k AND
      leaseToken=t` → `cancelled` if the workflow is terminal, else `pending`
      with `availableAt=now`; lease cleared; history `activity_revoke_settled`).
      No attempt is added to the budget and nothing is dead-lettered.
   b. `in_flight` = count of slot holders (`status IN SLOT_HOLDERS` =
      `running`, `cancelling`) with `leaseExpiresAt > now` (consistent read).
   c. `wanted = min(cap − in_flight, local capacity, remaining batch)`.
   d. Candidates: consistent read joining workflow on
      `workflow.status=waiting_activity AND workflow.waitReferenceId=activity.id`,
      `activity.status=pending`, `availableAt <= now`, local `(kind, version)`,
      ≤32, ordered `(availableAt, continuation tie-break, id)`.
   e. `claim_locked_candidate`: workflow `FOR UPDATE SKIP LOCKED`
      with the wait recheck; activity `FOR UPDATE SKIP LOCKED` with recheck;
      skip (`Ok(None)`) if the definition is missing; quarantine
      (`quarantine_candidate`, then `Ok(None)`; G10, fixed) if the attempt
      cap is reached (`attemptCount >= maxAttempts`) or `timeout <= 0` or
      `leaseDuration <= timeout`: fenced `UPDATE WHERE status=pending AND
      attemptCount=k` → `dead_lettered`, `lastErrorCategory=invalid_row`,
      `completedAt=now`; history `activity_quarantined`; block the workflow
      (history `activity_dead_lettered`). Otherwise fenced
      `UPDATE WHERE status=pending AND attemptCount=k` → `running`,
      `attemptCount=k+1`, `leaseToken=uuid4`, `leaseExpiresAt=now+leaseDuration`;
      INSERT attempt `k+1`.
4. Local lease deadline = sampled instant + `leaseDuration − 1 ms`
   (`lease_deadline_from_sample`).

`claim_one` is the single-topic variant: it locks one topic row
with a blocking `FOR UPDATE` and otherwise follows steps 2–4.

**L-W execution** (`ActivityWorker::execute_claim`), local state machine. Inputs: handler future,
heartbeat loop, `timeout` (local timer), runtime cancellation, forced
cancellation, `shutdown_grace` (30 s). Refuses to start if the local lease
deadline already passed.
- Handler returns → outcome from the result.
- Timeout → cancel the handler token; outcome `Retryable(timeout)`; the handler
  may run cleanup up to `shutdown_grace` while heartbeats continue.
- Runtime cancellation → cancel the handler token, start the grace timer;
  if grace elapses first, outcome `Retryable(cancelled)`.
- Heartbeat failure (fence miss or local deadline) → cancel the handler token,
  wait at most `min(grace, last confirmed lease deadline)`, then return the
  error **without** T-W3.
- Heartbeat reports the row revoked (`Renewed::Revoked`, a pause or cancel
  moved it to `cancelling`) → cancel the handler token, wait at most
  `min(grace, lease deadline)` for the handler to stop, outcome
  `ExecutionOutcome::Revoked`; the heartbeat loop stops (N2).
- Forced cancellation → stop heartbeats and return without T-W3.
Then T-W3 with the outcome.

**T-W2 heartbeat** (`heartbeat_once`), every `min(heartbeat_interval, lease/3)`:
separate connection, one transaction: sample local instant, `now` ← DB;
lock the activity `FOR UPDATE` under the lease fence
`WHERE id AND status IN LEASE_HOLDERS AND attemptCount=k AND leaseToken=t`
(`leased_activity!`: `running` or `cancelling`); fenced update of that row
sets `leaseExpiresAt=now+leaseDuration`; fenced attempt update
(`finishedAt IS NULL`) sets `heartbeatAt`. The result is `Renewed::Held` for
a `running` row and `Renewed::Revoked` for a `cancelling` row: the lease is
still renewed, so the executor keeps its slot while it stops the handler.
Raced against the local deadline; losing the race drops the in-flight
transaction future.

**T-W3 finish** (`finish_on_connection`) runs after the executor has
dropped the handler future (N5), in one transaction: lock workflow
`FOR UPDATE`; `now` ← DB; lock the activity `FOR UPDATE` under the lease
fence (`status IN LEASE_HOLDERS`, attempt, token; a miss is `FencedWrite`);
then:
- The row is `cancelling` (revoked, N2), whatever the handler outcome:
  `settle_revoked` with the revoke outcome stored in `lastErrorCategory`
  (`operator_paused`, `operator_cancelled` or `application_cancelled`):
  close the attempt; → `cancelled` if the workflow is terminal, else
  `pending` with `availableAt=now`; lease cleared; history
  `activity_revoke_settled`. The handler's result is not applied.
- The row is `running` and the outcome is `Revoked`: `InvalidState` (a
  revoke is only reported for a `cancelling` row).
- Success: fenced activity → `succeeded`; close attempt; `wake_workflow`
  requires `status=waiting_activity AND waitReferenceId=activity` (else
  FencedWrite → rollback); append `activity_succeeded` with
  `delivery = deliveredEventSequence+1`; workflow → `ready`.
- Retryable with `attempt < maxAttempts`: fenced → `pending`,
  `availableAt = now + backoff(attempt)`; close attempt; history.
- Permanent, or retryable on the last attempt: fenced → `dead_lettered`; close
  attempt; block workflow (requires the same wait, else rollback).

**T-W4 progress report** (`src/progress.rs` `ProgressReporter::report`): lock the activity with
the claim fence `status=running AND attemptCount=k AND leaseToken=t` (a
`cancelling` row gets `FencedWrite`); if fewer than 100 events for the
attempt, insert the next.

### 2.4 Timer and approval-expiry materializers (`src/runtime/temporal.rs`)

Both loops sample `now` on one connection, then run the transaction on
another, so `now` can be slightly stale (conservative). Poll interval ≤ 60 s
when idle (`src/runtime/supervisor.rs` `valid_temporal_poll_interval`).

**T-T1 fire timer** (`TimerMaterializer::materialize_one`): select one `status=sleeping AND waitKind=timer AND availableAt <= now`
ordered `(availableAt, id)` `FOR UPDATE SKIP LOCKED`; require
`waitReferenceId = commandSequence` (else error); append `timer_fired`
(`delivery+1`); fenced `clear_wait` on `(status, waitKind, waitReferenceId)`
→ `ready`.

**T-A1 expire approval** (`ApprovalExpiryMaterializer::materialize_one`): consistent read of the oldest pending
approval with `expiresAt <= now` (outside the transaction); in a transaction,
lock workflow then approval `FOR UPDATE`; if no longer pending/expired, return;
if the workflow does not wait on it (status `waiting_approval`/`paused`, kind,
version, command) → **error** (task restart); append `approval_expired`;
fenced approval → `expired`; `clear_wait` (paused stays paused).

### 2.5 Schedule materializer (`src/runtime/schedule_materializer.rs`, one task per key)

Per tick: `now` ← DB; T-S1; T-S2. Sleep the poll interval if nothing happened.
Retryable errors (database, pool, serialization) end the task
(`src/runtime/supervisor.rs` `run_task`, `schedule_error_is_retryable`); others are alerted and
retried next tick.

**T-S1 reconcile state** (`src/schedule.rs` `ScheduleRegistry::reconcile_state`): `INSERT IGNORE` a
state row with cursor `next_after(now)`; lock it `FOR UPDATE`; persisted
version greater → `NewerPersisted` (T-S2 returns Conflict); equal version with
different fingerprint → Conflict; smaller → overwrite version, fingerprint,
and reset the cursor to `next_after(now)` (`Upgraded`). `Inserted`/`Upgraded`
end the tick.

**T-S2 materialize** (`ScheduleMaterializer::materialize_schedule`), one transaction: lock state `FOR UPDATE`;
require the pinned version and fingerprint; return if paused; `active` = count
of non-terminal workflows joined through `scheduleRunId` for this key, and
`queued` = exists queued run (consistent reads taken after the lock); if
QueueOne and `active=0` and queued, promote the oldest queued run (starts a
workflow). Parse and verify the cursor; plan ≤10,000 due occurrences
(`plan_due_chunk`) and classify them (`classify`); per occurrence
insert a run row and, for `Start`, call user `start_occurrence` on the same
connection and mark the run `started`; advance the cursor with a fence on the
old cursor, version, and fingerprint (`ScheduleMaterializer::materialize_schedule`).

### 2.6 Application API (`src/store.rs`)

- **T-X1 start** (`DurableStore::start`, `start_with_conn`, `start_prepared_with_conn`, `insert_prepared`): optional dedup consistent read; upsert
  (`ON DUPLICATE KEY UPDATE id = id + LAST_INSERT_ID(0)`,
  `src/dialect/mysql.rs` `insert_workflow`) plus reload `FOR UPDATE` on a
  dedup conflict; a restart-key collision returns `Conflict`; `started`
  event. Options with both a dedup key and a restart source are rejected
  (`InvalidDefinition`). Can run inside a caller transaction
  (`start_with_conn`).
- **T-X2 start_or_restart_recoverable**: lock the row with
  `(kind, dedupKey)` `FOR UPDATE`; follow its `restartedFromWorkflowId` chain,
  locking each successor `FOR UPDATE`, to the newest generation
  (`lock_newest_generation`; N1 fixed); if the newest is not
  `failed`/`blocked`, return it; else insert a successor with no dedup key,
  the original's root and `restartedFromWorkflowId` = the newest (a
  restart-key collision returns `Conflict` before any other write), cancel the
  newest's `dead_lettered` activities, and move a `blocked` newest to
  `cancelled` (history `workflow_superseded_by_recovery`; its own wait fields
  kept). The parents that wait on a blocked newest (`waiting_child`, or
  `paused` with `waitKind=child`) are locked; when the successor runs the same
  version (`W::VERSION`) their `waitReferenceId` moves to the successor
  (status and `availableAt` unchanged; history `child_wait_reattached` with
  `{from, to}`), otherwise they are woken with `child_failed`
  (`child_superseded`) as after an operator restart (G2 fixed).
- **T-X3 cancel_with_conn**: inside the caller's transaction; lock
  workflow `FOR UPDATE`; terminal → no-op; else `cancel_locked_workflow`:
  `cancel_activities` moves `pending` activities to `cancelled`
  and `running` (or already `cancelling`) ones to `cancelling`, fenced on
  status, attempt and token, with `lastErrorCategory` =
  `application_cancelled` (T-X3) or `operator_cancelled` (T-A4); a
  `cancelling` row keeps its lease and open attempt, which only T-W3 or
  reconcile close (`settle_revoked`, N2); cancel pending approvals, fenced workflow → `cancelled` (wait and lease
  cleared), history, wake waiting parents with `child_cancelled`. Then
  `cancel_owned_descendants` (G11): a locking read of the parent's
  children whose key is `child:{parent}:{parentCommandSequence}` (the key
  `commit_child` generates when the flow gives none), terminal ones
  included, in id order; each is locked with `lock_workflow_by_id`, then each
  successor on its `restartedFromWorkflowId` chain (T-X2 and T-A5
  generations), oldest first. Every non-terminal generation is cancelled the
  same way (its activities, approvals, status, history `workflow_cancelled`
  with reason `parent workflow {p} cancelled: {reason}`, its waiting
  parents), then its own owned children, to any depth (a work list, not
  async recursion). Domain-keyed children are not touched. Descendants are
  locked after their parent (§2.8).

### 2.7 Admin control (`src/admin/control.rs`)

Each action is one transaction that first locks the workflow `FOR UPDATE`
(schedule actions lock the schedule state row).

- **T-A2 pause** (`AdminControlService::pause_workflow`): reject paused/terminal; if waiting on an
  activity that is `running`, `pause_activity` locks it `FOR UPDATE` and
  revokes it: fenced `UPDATE WHERE status=running AND attemptCount=k AND
  leaseToken=t` → `cancelling` with `maxAttempts+1`,
  `lastErrorCategory=operator_paused`; the lease and the open attempt are
  kept (S36, N2). The worker's next heartbeat learns of the revoke, and T-W3
  (or reconcile, once the lease expires) settles it with `settle_revoked`:
  the attempt closes and the row returns to `pending`. A `cancelling` row is
  left alone. Fenced workflow → `paused`, lease cleared.
- **T-A3 resume** (`AdminControlService::resume_workflow`): status derived from the wait
  (`resume_status`); `availableAt=now` only when resuming to `ready`.
- **T-A4 cancel** (`AdminControlService::cancel_workflow`): terminal → Conflict; else
  `cancel_locked_workflow`, which cascades to the owned children as in T-X3
  (actor `operator`, attempt outcome `operator_cancelled`).
- **T-A5 restart / correct-and-restart** (`AdminControlService::restart`): source must be
  terminal, paused, or blocked and have no successor; insert successor (same
  schedule run, same root); a non-terminal source is cancelled with its
  activities and approvals (wait fields kept), its waiting parents get
  `child_superseded`, and `cancel_owned_descendants` cancels the children it
  owns (G11); transfer the schedule run's `workflowId`. The successor starts
  its own children under new keys.
- **T-A6 retry / correct-and-retry activity** (`AdminControlService::retry`): workflow must be
  `blocked` on this `dead_lettered` activity; insert replacement
  (`replacementNumber+1`, same `commandSequence`, same operation key unless
  corrected); fenced workflow → `waiting_activity` on the replacement.
- **T-A7 resolve approval** (`AdminControlService::resolve_approval`): approval pending and not expired;
  workflow waits on it; validate decision; append `approval_resolved`
  (`delivery+1`); fenced approval → `resolved`; workflow → `ready`
  (paused stays paused).
- **T-A8 pause/resume schedule** (`AdminControlService::set_schedule_paused`), **T-A9 run now** (`AdminControlService::run_schedule_now`):
  lock state; run-now returns `Conflict` when the overlap policy is
  `SkipIfActive` or `QueueOne` and a run of the key is active (S29), else
  inserts a `manual:{t}` run with `t = max(last scheduledFor + 1, now)` and
  starts a workflow. It ignores `pausedAt` on purpose (an operator override)
  and does not move the cursor.

### 2.8 Lock order summary

- Claims: topic row(s) → workflow → activity → attempt insert.
- Finish, reconcile, pause, cancel, retry: workflow → activity → attempt;
  approvals after the workflow.
- `RunActivity`/`WaitForApproval` commit: workflow (fence lock) → INSERT new
  row (new rows are invisible to others, so no cycle).
- Child terminal: child → every waiting parent (locking scan on
  `waitKind, waitReferenceId, status`; there is no index on
  `waitReferenceId`, so the scan can lock many rows; UNCLEAR how many).
- `RunChild` commit attaching to an existing child: child → parent, the same
  order as the previous line (G9, fixed; it was parent → child).
- Cancel cascade (T-X3, T-A4, T-A5; G11): parent → each owned child (siblings
  in id order) → each later generation on its restart chain, oldest first
  (the order T-X2 uses) → each cancelled generation's activities and
  approvals → its owned children. This is
  the reverse of the two lines above, on purpose: the callers lock the target
  first to decide whether it is terminal, and the owned subtree is found
  from the parent. A child that commits a terminal transition (or is
  cancelled on its own) while its parent's cancel runs can deadlock with it.
  The database aborts one of the two transactions; either outcome is correct
  (the child ends terminal). An aborted coordinator commit is benign (G1): the
  row waits for lease recovery, whose commit then misses the fence. An
  aborted cancel returns `DurableError::Database`, for which
  `DurableError::is_transient()` holds; the caller retries the transaction.
  Locking the subtree leaf-first before the parent was considered and not
  done: it needs an unlocked read of the tree before the parent lock, so a
  child committed in that window is still locked parent-first, and it moves
  the first lock of T-X3, T-A4 and T-A5.
- Progress: activity only. Schedules: state → run rows → new workflow rows.

**Type enforcement.** N4 and G9 are enforced by the compiler
(`src/tx.rs`). A lock is a `Locked<'tx, Row>` witness, which only the
`tx::lock_*` functions make, inside the transaction `'tx`.
`dialect::insert_activity` and `insert_approval` need a
`Locked<ClaimFence>` (from `lock_fence`) or a `Locked<WorkflowRow>`, so a
command row cannot be inserted before its workflow is locked (N4).
`commit_child`'s fenced parent update needs the `ChildStart` from
`insert_child`, whose `Existing` arm is the locked child, so the parent
cannot be locked before an existing child (G9). The helpers that need an
earlier lock (`cancel_locked_workflow`, `cancel_owned_descendants`, `wake_waiting_parents_on_child_terminal`,
`hand_waiting_parents_to_successor`, `settle_revoked`, `quarantine_candidate`,
`block_workflow`, `MaterializedFloor::load`, `active_workflow_count`) take a
witness too. The types do not prove which row was locked (a witness for
workflow X passed with a command for workflow Y still compiles; the command
inserts `debug_assert` the ids match), that the row is fresh, or what the
database contains. The SQL fences (lease token, status filters) stay.

---

## 3. Safety invariants

### Workflow lease and delivery

**S1. At most one valid workflow lease.** `status=running ⇔ leaseToken ≠ NULL ∧ leaseExpiresAt ≠ NULL`,
and a token is issued only on `ready → running`. **ENFORCED**: claim is
`FOR UPDATE SKIP LOCKED` plus fence `status=ready`
(`src/runtime/coordinator.rs` `WorkflowCoordinator::claim_row`); every exit from `running` clears the
lease (`WorkflowCoordinator::claim_row`, `WorkflowCoordinator::record_activation_failure`, `commit_on_connection`, `commit_wait_transition`,
`commit_activity`, `commit_child`; `src/admin/control.rs` `AdminControlService::pause_workflow`; `src/store.rs` `cancel_one_workflow`).

**S2. A stale workflow lease cannot commit.** Every coordinator write is
fenced on `status=running ∧ leaseToken = claim token`
(`src/runtime/coordinator.rs` `fenced_workflow!`, `WorkflowCoordinator::record_activation_failure`), and recovery,
pause, and cancel all change or clear the token. **ENFORCED**. The fence does
not test `leaseExpiresAt > now`: a holder whose lease expired but was not yet
recovered still commits (by design).

**S3. Workflow `step` may run concurrently for the same event and must be
side-effect free.** No renewal and no local deadline exist for workflow
leases, so after expiry a second coordinator can run `step` while the first
still runs; only one commit wins (S2). **ASSUMED** (user code).

**S4. Event consumption is atomic with its effects.** The fenced workflow
update sets `deliveredEventSequence`, `stateJson`, `status`, and wait fields
in the same transaction that inserts the activity/approval/child and appends
the follow-on event (`src/runtime/coordinator.rs` `commit_on_connection`, `commit_wait_transition`, `commit_activity`, `commit_child`). A failed
activation (T-C3) or a lost fence consumes nothing. **ENFORCED**.

**S5. Delivery-sequence discipline.** Per workflow: (a) delivery sequences
are unique (M:60); (b) every deliverable append uses
`deliveredEventSequence+1` and, in the same transaction, moves the workflow
out of its wait (or appends `continued` while leaving `running`); (c) hence at
most one undelivered deliverable event exists; (d) `status=ready` implies one
exists. **ENFORCED** by construction (append sites:
`src/persistence/workflows.rs` `insert_started`, `wake_loaded_parent_on_child_terminal`;
`src/runtime/coordinator.rs` `commit_on_connection`; `src/runtime/activity_worker.rs` `wake_workflow`;
`src/runtime/temporal.rs` `append_delivery_event`, `clear_wait`; `src/admin/control.rs` `AdminControlService::resolve_approval`) plus the
unique key. The resume branches that yield `ready` without an event
(`src/admin/control.rs` `resume_status`) are unreachable on traced paths. A
violation of (d) makes the coordinator fail (`src/runtime/coordinator.rs` `WorkflowCoordinator::activate_claim_inner`).

**S6. Workflow terminal states are absorbing.** Every write into
`succeeded`/`failed`/`cancelled` is fenced on a non-terminal prior status,
and no write selects a terminal row for a status change. Restarts create new
rows. **ENFORCED** (`src/runtime/coordinator.rs` `WorkflowCoordinator::record_activation_failure`, `commit_on_connection`;
`src/store.rs` `DurableStore::cancel_with_conn`, `DurableStore::start_or_restart_recoverable_with_conn`, `cancel_one_workflow`; `src/admin/control.rs` `AdminControlService::cancel_workflow`,
`AdminControlService::restart`).

**S7. Wait coupling** (table in §1.1) holds for non-terminal rows. **ENFORCED**
by construction; no DB constraint. Terminal rows cancelled through T-X2 or
T-A5 keep stale wait fields (`src/store.rs` `start_or_restart_recoverable_with_conn`,
`src/admin/control.rs` `AdminControlService::restart`); all readers also filter on `status`. A
parent that T-X2 re-attaches keeps `waitKind=child` with the successor as
`waitReferenceId`, so S7 holds for it.

**S8. Activation attempts are bounded for handler errors.** Each T-C3
increments `activationAttempts`; at `min(maxActivationAttempts, config)` the
workflow fails; any successful commit resets it to 0. **ENFORCED**
(`record_activation_failure`). A `step` panic and a `step` that exceeds
`step_timeout` are T-C3 failures too (G3, fixed). Lease recovery does not
increment it, by design: a runtime crash is not the workflow's fault, so
repeated crashes while a workflow is claimed stay unbounded.

### Activities

**S9. At most one live lease per activity.** Only `pending → running` issues a
token, fenced on `status=pending ∧ attemptCount=k` under
`FOR UPDATE SKIP LOCKED` (`src/runtime/activity_worker.rs` `ActivityWorker::claim_locked_candidate`). A
`running` or `cancelling` row holds a lease (`ActivityStatus::holds_lease`);
`running → cancelling` keeps it, and every exit from those two clears it.
**ENFORCED**.

**S10. A stale activity lease cannot commit, heartbeat, or report progress.**
Every such write is fenced on `attemptCount=k ∧ leaseToken=t` and a status:
a heartbeat (T-W2) and T-W3's first lock on
`status ∈ LEASE_HOLDERS` (`running` or `cancelling`, `leased_activity!`),
so a revoked attempt still renews its lease and learns of the revoke
(`Renewed::Revoked`); T-W3's result writes (success, retry, dead-letter) on
`status=running` (`fenced_activity!`), so a revoked attempt can only settle
(`settle_revoked`); progress (T-W4) on `status=running`, so a revoked
attempt reports nothing (`src/runtime/activity_worker.rs`;
`src/progress.rs` `ProgressReporter::report`). **ENFORCED** (tests
`stale_lease_cannot_emit_progress_or_commit_a_result`,
`heartbeat_and_completion_are_fenced_by_attempt_and_token`). No fence tests
expiry, so a heartbeat can revive an expired but unreconciled lease
(`heartbeat_once`; see G7: the revival does not exceed the topic cap).

**S11. Running or cancelling ⇔ exactly one open attempt.**
`activity.status ∈ {running, cancelling} ⇔` attempt `(id, attemptCount)`
exists with `finishedAt IS NULL` and the same `leaseToken`; each attempt
closes exactly once. **ENFORCED**: `running → cancelling` keeps the attempt
open, and every exit from those two closes it in the same transaction with a
`finishedAt IS NULL` fence (§1.3 table).

**S12. Attempts are monotonic and bounded.** Per activity row,
`attemptCount` only increases by 1 at claim, attempt numbers are contiguous
from 1, and `attemptCount ≤ maxAttempts`. `maxAttempts` only increases (by 1
per operator pause of a running attempt). **ENFORCED**
(`src/runtime/activity_worker.rs` `ActivityWorker::claim_locked_candidate`, `reconcile_expired`, `finish_on_connection`;
`src/admin/control.rs` `pause_activity`). No traced path creates a pending row at the
cap; if one exists (a manual edit), T-W1 quarantines it: dead-lettered with
`invalid_row`, its workflow blocked (G10, fixed). Retries create new rows
with `attemptCount=0` (`src/admin/control.rs` `AdminControlService::retry`).

**S13. At most one handler executes per activity at any instant.**
**ASSUMED**. It relies on: the local deadline being at or before the DB
expiry (`src/runtime/activity_worker.rs` `ActivityWorker::claim_one`, `ActivityWorker::claim_batch`, `lease_deadline_from_sample`);
the process monotonic clock not running slower than the DB clock; the DB
clock not jumping forward; handlers yielding so the dropped future stops; and
no detached tasks inside handlers. Across attempts, activities are
at-least-once: a result committed after the lease was reconciled is rejected
and the activity runs again. A cancel or pause does not free the row for a
new attempt while the revoked handler may still execute: the row is
`cancelling` until it stops or its lease expires (N2, fixed).

**S14. Activity terminal states.** `succeeded` and `cancelled` are absorbing;
`dead_lettered` can only move to `cancelled`, via T-X2
(`src/store.rs` `DurableStore::start_or_restart_recoverable_with_conn`). **ENFORCED**.

**S15. Activity/workflow coupling.** `activity.status ∈ {pending, running}` ⇒
its workflow waits on it (`waiting_activity`, or `paused` with the activity
`pending`); `activity.status = cancelling` ⇒ its workflow is terminal, or
waits on it (`waiting_activity` after a resume, or `paused`);
`workflow.status=blocked` ⇒ it waits on a `dead_lettered`
activity. Claims require the wait (`src/runtime/activity_worker.rs` `ActivityWorker::claim_one`,
`ActivityWorker::claim_locked_candidate`); success and dead-letter roll back without it (`wake_workflow`,
`block_workflow`); pause converts `running` to `cancelling`, which settles to
`pending` (`src/admin/control.rs` `pause_activity`, `settle_revoked`).
**ENFORCED** by construction.

**S16. Activity success is delivered at most once.** Success, attempt close,
event append, and workflow wake are one transaction under the workflow lock,
and the wake clears the wait. **ENFORCED**.

**S17. Topic concurrency cap at claim time.** When T-W1/`claim_one` commits,
`|{(running ∨ cancelling) ∧ leaseExpiresAt > now}|` on the topic ≤
`maxConcurrency` (`ActivityStatus::SLOT_HOLDERS`; a revoked handler keeps
its slot until it stops, N2). Claims on
a topic serialize on its lock row, and reconcile runs first in the same
transaction. **ENFORCED**. The one known exception, an overrun by one
through heartbeat revival (G7), is closed under READ COMMITTED, which the
library pins for every T-W1; it reproduces only under REPEATABLE READ.

### Deduplication and lineage

**S18. Workflow deduplication.** At most one row per `(kind, deduplicationKey)`
(M:33); a duplicate start returns the original without mutating it
(`src/persistence/workflows.rs` `insert_started`). **ENFORCED**.

**S19. One successor per source; one live generation per recovery root.** At
most one row per `restartedFromWorkflowId` (M:34; admin pre-check
`src/admin/control.rs` `AdminControlService::restart`). A second admin restart of a source returns
`Conflict` (test
`second_restart_of_a_source_conflicts_and_leaves_the_caller_transaction_usable`);
a restart-key collision inside `insert_prepared` also returns `Conflict` and
leaves the caller's transaction usable. Only the engine sets a restart
source: `StartOptions::restarted_from_workflow_id` and `root_workflow_id` are
crate-private (N3 fixed), and T-X2 and T-A5 restart only a terminal,
`blocked` or (T-A5) `paused` source, cancelling a non-terminal one in the
same transaction, so every source with a successor is terminal. T-X2 locks the original and the newest
generation (the end of the original's `restartedFromWorkflowId` chain, each
row locked on the way) and restarts only a `failed`/`blocked` newest, so
concurrent recoveries and admin retries leave one live generation per chain,
and a recovery of a keyed child never touches another row under the same
root (N1 fixed). **ENFORCED** (tests
`recoverable_start_fences_dead_letter_retry_and_races_to_one_live_generation`,
`g2_keyed_child_lineage_after_two_recoveries`,
`n1_recoverable_start_on_child_key_leaves_blocked_sibling_alone`,
`n1_recoverable_start_on_child_key_returns_its_own_row`).

### Approvals and timers

**S20. An approval resolves at most once, and resolution excludes expiry.**
All exits from `pending` are fenced on `status='pending'` under the workflow
lock; resolve rejects `expiresAt <= now`; expiry requires `expiresAt <= now`.
**ENFORCED** (`src/admin/control.rs` `AdminControlService::resolve_approval`;
`src/runtime/temporal.rs` `ApprovalExpiryMaterializer::materialize_one`; test
`approval_resolution_and_expiry_have_one_transactional_winner`).

**S21. A pending approval is awaited by its workflow**
(`waiting_approval` or `paused`, `waitReferenceId` = approval, same kind,
version, and command). **ENFORCED** by construction (created with the wait;
every path that ends the wait resolves, expires, or cancels it). If violated,
T-A1 fails the task on every tick (`src/runtime/temporal.rs` `ApprovalExpiryMaterializer::materialize_one`) and the
restart budget runs out.

**S22. A timer fires at most once per command and never while paused.**
**ENFORCED** (`src/runtime/temporal.rs` `TimerMaterializer::materialize_one`, `clear_wait`; test
`timers_wake_once_at_the_exact_command_and_preserve_pause`).

### Children and cancellation

**S23. A child outcome reaches each waiting parent at most once, and never a
parent that stopped waiting.** The wake is fenced on
`(status, waitKind, waitReferenceId)` and clears the wait
(`src/persistence/workflows.rs` `wake_loaded_parent_on_child_terminal`). A T-X2 re-attach is
fenced the same way and moves the wait to the successor, so the superseded
row never delivers to that parent and the successor delivers once.
**ENFORCED**.

**S24. A child outcome reaches each parent that waits on it when the child
becomes terminal, for every terminal transition.** Covers the attach race: a
parent that attaches to an existing child locks the child and wakes itself if
it is already terminal (`src/runtime/coordinator.rs` `commit_child`). T-X2
supersession of a blocked child either re-attaches its waiting parents to the
successor (same version) or wakes them with `child_superseded` in the same
transaction, so no parent waits on a terminal child. **ENFORCED** (G2 fixed;
model `inv_S24_parentWakes` in `safety`; tests
`g2_recoverable_start_wakes_parent_of_superseded_blocked_child`,
`g2_reattached_parent_completes_with_the_successor_output`,
`admin_controls.rs`
`recoverable_start_at_a_new_version_fails_the_waiting_parent_as_superseded`).

**S25. Cancellation is atomic for the workflow's own work and for the
children it owns.** One transaction cancels the workflow and its pending
activities, moves its running ones to `cancelling` (their attempts close when
the revoked handler stops or the lease expires, and the row then becomes
`cancelled`), cancels its pending approvals, and wakes waiting parents
(`src/store.rs` `cancel_locked_workflow`). The same transaction does the same
for every live generation of every child the workflow owns (key
`child:{parent}:{command}`, and the T-X2/T-A5 successors on its restart
chain), and for theirs (G11 fixed: a parent's cancel reaches every
generation of its owned children); a T-A5 supersession of a non-terminal
source does it for the source's owned children. A child started with a domain key is not
owned and keeps running (intended: the key may be shared by other parents).
`cancel_with_conn` is idempotent on terminal workflows; admin cancel returns
Conflict. **ENFORCED** (model `inv_S25_cancelAtomic` and
`inv_G11_cancelReachesChildren` and `inv_G11_cancelReachesGenerations` in
`safety`; tests `g11_*` in
`tests/gaps.rs`, `admin_controls.rs`
`restart_supersession_cancels_the_owned_child`).

### Schedules

**S26. Each schedule occurrence materializes at most once.** Unique
`(scheduleKey, localOccurrence)` (M:195), the state row lock, and the cursor
fence (`src/runtime/schedule_materializer.rs` `ScheduleMaterializer::materialize_schedule`).
**ENFORCED** (test `run_latest_records_backlog_starts_one_and_is_multi_instance_exactly_once`).
Run-now uses `manual:{t}` with `t` strictly increasing per key under the same
lock.

**S27. The cursor is always strictly after the last materialized occurrence
(the largest local occurrence key with a run row; `manual:{t}` rows do not
count), and a tick advances it strictly in local time.** So the cursor never
names an occurrence that already has a run row. A failed tick rolls back its
rows and cursor together. Every cursor write goes through `ScheduleCursor`
(`src/schedule/cursor.rs`): `initial` for a new state row (no runs yet),
`advance_to` for a tick (rejects a value that is not strictly later), and
`upgrade` for T-S1 `Upgraded`: the first occurrence of the new calendar
strictly after `now` as an instant and strictly after the
`MaterializedFloor`, read by one query under the state lock. **ENFORCED**. An
upgrade may move the cursor back in local time (a new calendar with an
earlier slot runs it), also across a timezone change, but never onto a
materialized occurrence. It writes no run rows for the span it skips:
dropping that span is intended (G5).

**S28. Misfire policies.** `Skip`: start iff `dueAt + grace ≥ now`;
`RunLatest`: only the latest runnable occurrence of the whole backlog starts,
earlier ones are `coalesced`; `CatchUp{n}`: only the latest `n` runnable
occurrences start; DST-gap occurrences are `skipped(dst_gap)`; chunking
(10,000) keeps these global (`src/runtime/schedule_materializer.rs` `plan_due_chunk`, `classify`).
**ENFORCED** (tests in `tests/schedule_materialization.rs` and the unit
test at `src/runtime/schedule_materializer.rs` `over_bound_backlog_recovers_without_repeating_the_latest_n_budget`, `chunk_boundary_preserves_dst_gap_and_fold_policies`).

**S29. Overlap policies for materializer runs.** `Allow`: no overlap check;
every occurrence the misfire policy starts is started, whatever runs of the
key are active. `SkipIfActive`: no start
while any run's workflow of the key is non-terminal (blocked and paused count
as active); `QueueOne`: at most one `queued` run, promoted only when
`active=0`. Evaluated under the state lock with a snapshot taken after the
lock. **ENFORCED**. T-A9 (run-now) applies the same check under the same lock
and returns `Conflict` instead of queueing or skipping; it bypasses the pause
on purpose (G12).

**S30. Schedule definition pinning.** T-S2 runs only when the persisted
`(version, fingerprint)` equals the local definition; versions never
decrease; same-version drift is rejected (`src/schedule.rs` `ScheduleRegistry::reconcile_state`).
**ENFORCED**.

### Flows (`src/flow.rs`)

**S31. Replay is structurally deterministic.** Each await at position `i`
replays journal entry `i` only if `(stepKind, stepVersion)` match; otherwise
the flow fails closed. A delivered result must carry
`commandSequence = journal length + 1`. A completed flow must have consumed
the whole journal. **ENFORCED** (`WfCtx::replay`, `flow_step`, `apply_event`, `ensure_sequence`).

**S32. Flow code between awaits is deterministic in its arguments.** Replay
does not compare payloads or inputs. **ASSUMED**.

**S33. A journaled step never re-executes its activity.** Replayed positions
never create commands (`WfCtx::run_step`); the journal is persisted atomically with
event consumption (S4). Only the first unjournaled step can emit a command,
and its activity is at-least-once. **ENFORCED**.

### Idempotency, limits, fencing by operators

**S34. `operationKey` idempotency.** The engine stores the key and passes it
to the handler (`src/definition.rs` `ActivityContext::operation_key`); it has no uniqueness constraint
and deduplicates nothing. Replacements reuse the key; corrections derive
`durable:activity:{root}:correction:{n}` (`src/admin/control.rs` `AdminControlService::retry`);
the flow auto-key `wf:{workflowId}:step:{n}` (`src/flow.rs` `WfCtx::auto_operation_key`) changes
across restart generations. **ASSUMED** (handler and provider).

**S35. Payload limits** for values built through the typed constructors:
workflow input/state ≤ 256 KiB (`src/store.rs` `DurableStore::start_with_conn`,
`src/registry.rs` `store_transition`, `src/transition.rs` `ChildWorkflowCommand::new`); activity payload
≤ 256 KiB (`src/transition.rs` `ActivityCommand::new`); activity and workflow output ≤ 64 KiB
(`src/registry.rs` `ActivityAdapter::execute_stored`, `store_transition`, `src/transition.rs` `ActivityResult::new`, `ChildResult::new`); approval
request/decision and temporal event metadata ≤ 16 KiB (`src/registry.rs` `store_transition`,
`WorkflowAdapter::validate_approval`, `src/runtime/temporal.rs` `append_delivery_event`, `src/admin/control.rs` `AdminControlService::resolve_approval`);
error category ≤ 64 bytes and message ≤ 2 KiB by truncation (`src/error.rs` `WorkflowError::new`, `ActivityError::retryable`, `ActivityError::permanent`);
progress ≤ 100 events per attempt and description ≤ 2 KiB (`src/progress.rs` `ProgressReporter::report`,
`validate_event`, M:138-139); keys ≤ 191 characters (`src/store.rs` `validate_options`,
`src/transition.rs` `ActivityCommand::new`). **ENFORCED** in that scope. Not bounded:
`activity_succeeded`/`child_succeeded` metadata (up to 64 KiB output plus
envelope, which exceeds the 16 KiB metadata constant). `ActivityCommand` and
`ChildWorkflowCommand` implement `Deserialize`, so a serde-built command skips
constructor checks; the coordinator does not re-check them. An activity row
with invalid timeout/lease bounds is quarantined at claim (G10, fixed). The
same holds for `RetryPolicy`: its constructors bound every delay by
`MAX_RETRY_DELAY_SECS` (`i64::MAX / 2_000` s), so a delay with +100% jitter
fits the millisecond range. Deserialization checks the same bounds
(`#[serde(try_from)]`), and `from_checked`, which the derive macros emit
in a `const` block, fails compilation (or panics outside a const context)
out of bounds. No public constructor skips them (the unchecked
`from_validated` is removed; `retry_policy_unchecked_constructor`
compile-fail case). A stored `retry_policy_json` that does
not decode is quarantined at claim as `invalid_bounds` (G10); lease
recovery requeues such a running row due now so the next claim quarantines
it. On a policy outside the bounds, `delay_for_attempt` saturates at
`u64::MAX` seconds instead of wrapping (the jitter overflow, fixed).

**S36. Operator pause fences in-flight work.** Pausing a running workflow
invalidates the coordinator's token; pausing while an activity runs moves it
to `cancelling` with `maxAttempts+1` so the paused attempt does not consume
the budget. The worker's next heartbeat learns of the revoke and stops the
handler; its result is never applied. The attempt closes (`operator_paused`)
and the row returns to `pending` when the handler stops or the lease expires
(N2). **ENFORCED** (tests `pause_fences_a_workflow_transition_claimed_before_the_operator_action`,
`pausing_the_final_activity_attempt_preserves_one_execution_attempt`,
`n2_settled_paused_activity_is_pending_with_one_more_attempt`).

### Rejected candidates

- **"Cancellation eventually stops all descendants."** Accepted for owned
  descendants since G11 (S25): cancel and restart supersession cancel every
  generation of the owned children, recursively, in their transaction, and
  L12 bounds how long a revoked handler runs on. Rejected for the rest, on
  purpose: a child started with a domain key (`child_with_key`) may be
  shared by other parents and keeps running; its later outcome is dropped
  because the cancelled parent no longer waits (S23). A generation that an
  operator (T-A5) or the application (T-X2) creates after the cancel is a
  new decision and is not cancelled.
- **"Child results are delivered exactly once."** Holds as S23 + S24 only
  (per wait; a re-attached parent receives the successor's outcome).
- **"Operation keys are idempotent."** The engine does not enforce this (S34).

---

## 4. Liveness properties

Global fairness assumptions used below:
- **F1** The DB is eventually available, and DB time advances.
- **F2** At least one runtime stays up and has not self-cancelled. Operator
  actions, lease-recovery races (including stale commits, N4), transient
  database errors, step panics and hung steps no longer cost restart budget
  (G1, G3, fixed); the budget counts within `restart_window`.
- **F3** The runtime serving an entity has its exact `(kind, version)` and
  topic registered (claims filter on local definitions).
- **F4** Handlers and `step` return, or yield to cancellation, in finite time.

**L1. A ready workflow with `availableAt ≤ now` is eventually claimed.**
Needs F1–F3 and a coordinator that polls. Order is `(availableAt, id)`;
continuation streaks yield after 16 (`src/runtime/coordinator.rs` `commit_on_connection`).
**ENFORCED**.

**L2. An expired workflow lease is eventually recovered.** Needs F1 and any
coordinator that polls (recovery runs before the definition filter,
`src/runtime/coordinator.rs` `WorkflowCoordinator::claim_row`). **ENFORCED**.

**L3. A claimed workflow eventually leaves `running`.** `step` is bounded by
`step_timeout` and an unwind boundary, or L2 recovers the row after the lease
expires. **ENFORCED**, except for a `step` that blocks its thread without
yielding (the timeout cannot preempt it) or a `panic = "abort"` build.

**L4. A pending activity whose workflow waits on it is eventually claimed.**
Needs F1–F3, `availableAt ≤ now`, free topic capacity, and a T-W1 sweep that
acquires every registered topic row (all-or-nothing,
`src/runtime/activity_worker.rs` `ActivityWorker::claim_batch`). System-wide progress holds because a
sweep that skips a lock implies another sweep holds it. **ENFORCED** for valid
rows. A row past its attempt cap, with invalid timeout/lease bounds or with
a stored retry policy out of bounds is not claimed but quarantined (dead-lettered, its workflow blocked for an operator);
it no longer stops the claims of other rows and topics (G10, fixed).

**L5. An expired activity lease is eventually reconciled.** Needs a dispatcher
for that topic with local capacity > 0 (T-W1 returns early otherwise,
`src/runtime/activity_worker.rs` `ActivityWorker::claim_batch`). **ENFORCED**.

**L6. Every activity eventually becomes `succeeded`, `dead_lettered`, or
`cancelled`.** Attempts are bounded (S12); each attempt ends by T-W3 or by
lease expiry plus L5; claims follow from L4. Operator pauses add attempts.
Needs F1–F4. **ENFORCED**.

**L7. A due timer eventually fires.** Needs F1, F2 and the timer task (poll
≤ 60 s). **ENFORCED**.

**L8. An expired pending approval eventually expires.** Needs S21 and the
approval task. **ENFORCED**.

**L9. A due occurrence of an unpaused schedule is eventually recorded.**
Needs the schedule task, a matching persisted definition, and a
`start_occurrence` that succeeds (a failing start rolls back the tick and
blocks the cursor). **ENFORCED**.

**L10. A QueueOne queued run is eventually promoted** once every active run's
workflow becomes terminal (blocked or paused ones block it indefinitely).
**ENFORCED**.

**L11. A parent waiting on a child becomes ready when the child becomes
terminal** (same transaction; after a T-X2 re-attach, when the successor
becomes terminal). Not guaranteed if the child
never terminates, for example in a wait cycle through keyed rows that are not
ancestors of each other (G8). A child cancelled by the cascade (G11) wakes no
one in its owner: the owner is already cancelled in the same transaction and
no longer waits. **ENFORCED**.

**L12. Cancellation, pause, or restart stops an in-flight activity handler
within `heartbeat interval + min(shutdown_grace, remaining lease)`**: the
heartbeat renews a `cancelling` row and reports it revoked
(`Renewed::Revoked`), the executor cancels the handler, waits at most
`min(shutdown_grace, lease deadline)`, and settles the revoke
(`ExecutionOutcome::Revoked`). Until then the row keeps its topic slot (N2).
Cooperative (F4). **ENFORCED**.

**L13. Graceful shutdown finishes within `deadline + forced_shutdown_timeout`**
(`src/runtime/supervisor.rs` `RuntimeHandle::shutdown`). Leases left behind are recovered by
other runtimes (L2, L5). **ENFORCED**.

Not guaranteed: workflow termination. `blocked`, `paused`,
`waiting_approval` without `expiresAt`, and `waiting_child` on a
non-terminating child are stable states that need an operator.

---

## 5. Environment assumptions

### 5.1 Database

- Supported backends, one per build (cargo feature `mysql` or `postgres`):
  MySQL 8.0.16+ / 8.4 InnoDB (`SKIP LOCKED` and enforced `CHECK` constraints
  are required; with binary logging, `binlog_format=ROW`, the 8.x default),
  and PostgreSQL 14+.
- DB clock: MySQL `UTC_TIMESTAMP(3)` is statement time; Postgres
  `clock_timestamp() AT TIME ZONE 'UTC'` is call time (never `now()`, which is
  transaction-start time).
- One primary; all reads and writes go through one pool. No replica reads.
- **Isolation: READ COMMITTED, ENFORCED** for every transaction the library
  opens on its own pool: `dialect::transaction` (`src/dialect/mysql.rs`) runs
  `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` immediately before `BEGIN`
  (test `library_transactions_pin_read_committed_before_begin`);
  `src/dialect/postgres.rs` opens `BEGIN TRANSACTION ISOLATION LEVEL READ
  COMMITTED`. The `*_with_conn` APIs run in the caller's transaction at the
  caller's level.
- Under READ COMMITTED each consistent read sees the rows committed before
  that statement; locking reads and `UPDATE` are current reads and take no gap
  locks. A read taken before a lock can still miss a commit that lands between
  the read and the lock, so every such read is rechecked under the lock
  (T-W1's reconcile relocks each candidate; G7 is closed because T-W1's
  `in_flight` count is a new statement that sees the revived lease). Under
  REPEATABLE READ (a caller's `*_with_conn`
  transaction) the snapshot starts at the first consistent read of an InnoDB
  table, so later reads in that transaction can be staler still (G4).
- Postgres aborts a transaction on any failed statement (`25P02` for every
  later statement). `ScheduleHandler::start_occurrence` runs inside the
  materializer transaction on the library's connection: the handler must
  return every error and must not continue after a failed statement; to
  recover from one it wraps that step in `connection.transaction(..)` (a
  savepoint). Any handler error rolls back the whole tick on both backends.

### 5.2 Clocks

- Every persisted timestamp and every due/expiry comparison uses DB time
  (`persistence::database_now_millis`). The process wall clock is not used
  and the crate exposes no host wall-clock function; test fixtures read the
  database clock too (`tests/support::db_now`). Tests confirm session time is
  honoured (`tests/database_time.rs`).
- Each transaction samples `now` once, usually at the start, so stored times
  are sample times, not commit times. Timer, approval, and schedule loops
  sample `now` before opening their transaction.
- Process time (tokio monotonic `Instant`) is used only for: the local
  activity lease deadline, activity timeout, heartbeat cadence, shutdown
  grace, and poll/backoff sleeps.
- ASSUMED: the DB clock is monotonic and does not jump forward, and the
  process monotonic clock does not run slower than the DB clock. S13 depends
  on this.

### 5.3 Crash model

- A process can stop at any `.await`. Transactions are atomic; an
  uncommitted transaction rolls back when its connection drops. A commit
  whose acknowledgement is lost is committed but reported as an error.
- In-flight effects of a crash:

| in flight | effect |
|---|---|
| coordinator between T-C1 and T-C2 | lease expires → L2 recovery; `step` runs again; no activation attempt consumed |
| activity execution | lease expires → reconcile consumes the attempt (may dead-letter and block); a `cancelling` row is settled instead (`settle_revoked`: `pending` or `cancelled`) |
| claims returned by T-W1 but not yet spawned (dispatcher error at `src/runtime/supervisor.rs` `run_task`) | same as an activity execution: attempt consumed without running |
| T-W2 future dropped after its COMMIT was sent | lease extended with no executor (liveness delay only) |
| any transaction | rolled back, or committed with the error lost |

- Panics: a panic in `step`, like a `step` that exceeds `step_timeout`, is
  caught by the coordinator and recorded as a T-C3 activation failure
  (`step panicked: <message>`); it does not fail the coordinator task or
  cost restart budget (G3, fixed). A build with `panic = "abort"` still
  aborts the process. A panic in a handler is collected by the
  activity-execution manager and fails the dispatcher task, which counts
  toward the restart budget (`src/runtime/supervisor.rs` `supervise`); the
  runtime is fail-stop there by design (test
  `panicking_worker_is_reported_restarted_and_recovered_to_dead_letter`).

### 5.4 Shutdown

- Graceful (`RuntimeHandle::shutdown`): loops stop at their next check; an
  activation in progress runs to completion; executions get the cancellation
  token and `shutdown_grace`. A handler that finishes in the grace window
  records its real outcome. Otherwise the outcome is `Retryable(cancelled)`,
  which dead-letters the activity if it was the last attempt
  (`src/runtime/activity_worker.rs` `ActivityWorker::execute_claim`, `finish_on_connection`).
- Forced (deadline elapsed): executions stop heartbeating and return without
  T-W3 (`ActivityWorker::execute_claim`); leases expire and are recovered elsewhere.
- Abort (forced timeout elapsed, or `RuntimeHandle` dropped): the supervisor
  task is aborted, which drops all tasks at their current await
  (`src/runtime/supervisor.rs` `impl Drop for RuntimeHandle`, `RuntimeHandle::shutdown`).

### 5.5 User code contracts (ASSUMED)

- `step` is deterministic in `(input, state, event)` and side-effect free (S3).
- `start_occurrence` starts exactly one workflow on the given connection with
  `schedule_run_id` set, and does not commit it separately.
- `cancel_with_conn` and `start_with_conn` run inside the caller's
  transaction; the caller commits.
- Handlers honour the cancellation token and do not spawn detached work.

---

## 6. Suspected gaps

Status: G4 is closed for library transactions (P4, READ COMMITTED). G7 is
closed under READ COMMITTED, which the library pins for every T-W1 (the
model shows why; see its entry). G1, G2, G3, G6, G8, G9 and G10 are fixed, N4 (found by the concurrent trace workload)
is found and fixed, and the model findings N1 and N3 (`spec/README.md`) are fixed;
their tests in `tests/gaps.rs` run un-ignored. The model finding N2 is fixed
(see its entry at the end of this section), and so is N5, found in a
concurrent MySQL run (its test is in
`tests/application_cancellation/timeout_cleanup.rs`). G5 and G12 are fixed; their tests
are in `tests/schedule_state.rs`, `tests/props_schedule.rs` and
`tests/schedule_overlap.rs`. G11 is fixed; its tests in `tests/gaps.rs` run
un-ignored. No suspected gap is open; `spec/traces/gaps.yaml` lists none.

Each item below was traced from the code; the status line above records
which ones a test has since confirmed or closed.

**G1. Benign races and transient DB errors use up a restart budget that never
resets, which stops the runtime.** `activate_claim_inner` returns
`FencedWrite` and database errors from T-C2 and T-C3
(`src/runtime/coordinator.rs` `WorkflowCoordinator::record_activation_failure`); the coordinator task
propagates them (`src/runtime/supervisor.rs` `run_task`); the restart counter is
cumulative (`supervise`). Interleaving: coordinator claims W (T-C1); operator
pauses or cancels W (T-A2/T-A4; both accept `running`,
`src/admin/control.rs` `AdminControlService::pause_workflow`, `AdminControlService::cancel_workflow`); T-C2 misses the fence → task error. Nine
such events in one process lifetime (default budget 8) cancel the whole
runtime. Other triggers: lease-recovery races with slow steps, InnoDB
deadlocks (G9), duplicate-key aborts (G4), and claim-batch errors in the
dispatcher (`src/runtime/supervisor.rs` `run_task`). The test
`pause_fences_a_workflow_transition_claimed_before_the_operator_action`
confirms that `activate_claim` (now `WorkflowClaim::activate`) returns `Err(FencedWrite)`.
**Fixed**: `activate_one` logs a `FencedWrite` or a transient database error
(`dialect::is_transient_error`) from T-C2/T-C3 and returns the claimed id;
`WorkflowClaim::activate` still returns the error to a direct caller. The restart
budget counts within `RuntimeConfig::restart_window` (default 10 minutes).
The skipped activations stay visible: `activate_one` counts them by
`BenignActivationKind` (fence miss or transient) in `ActivationCounters`,
which the application reads through `RuntimeHandle::activation_counters`
(or `WorkflowCoordinator::activation_counters`), and more than
`max_transient_activation_errors` transient errors within
`transient_activation_error_window` latch a
`HealthAlert::TransientActivationErrors` for the next health report (§2.2).
The dispatcher logs a transient `claim_batch` error and backs off as after an
empty sweep. Tests: `g1_pause_during_step_is_not_a_coordinator_error`,
`g1_repeated_operator_pauses_do_not_stop_the_runtime`,
`g1_activation_failure_after_operator_cancel_is_not_a_coordinator_error`,
`activate_one_counts_a_lost_fence_as_a_fence_miss`, the `RestartBudget` unit
test and the `TransientErrorWindow` unit tests.

**N4. A stale RunActivity commit reported a duplicate key, not a fence miss**
(found by the concurrent workload, `tests/trace_workload.rs`). A coordinator
whose lease expired while its `step` ran, and whose workflow another runtime
recovered and advanced to the same `RunActivity`, inserted the activity row
before its fenced workflow update. `uq_durable_activity_command` (workflow,
command sequence, replacement) was already taken, so the transaction failed
with a duplicate-key database error: no `CoordFenceMiss`, a coordinator task
error (G1), and a restart. `WaitForApproval` had the same shape
(`uq_durable_approval_command`). `RunChild` did not: `insert_child` resolves
the key to the recovering commit's child. **Fixed**: both paths lock the
workflow row under the claim fence before the insert (`lock_fence`), so the
stale commit gets `FencedWrite`. Tests: `n4_stale_run_activity_commit_is_a_fence_miss`,
`n4_stale_run_child_commit_is_a_fence_miss`; the workload's slow steps may
now outlast the lease on every transition.

**G2. Recoverable start strands parents of a superseded blocked child.**
T-X2 moves a `blocked` newest generation to `cancelled` without calling
`wake_waiting_parents_on_child_terminal` (`src/store.rs` `DurableStore::start_or_restart_recoverable_with_conn`). Interleaving:
parent P runs `child_with_key(C, "k")` → child row C (kind CK, key k) →
P `waiting_child` on C; C's activity dead-letters → C `blocked`; the
application calls `start_or_restart_recoverable(CK, key "k")` → C
`cancelled`, successor C' with no dedup key and no parent link. P waits on C
forever; resuming P after a pause fails with Conflict
(`src/admin/control.rs` `resume_status`). A later `child_with_key(..., "k")` resolves
to C and fails at once.
**Fixed**: T-X2 inserts the successor first, then locks the parents waiting on
the blocked row (`waiting_child`, or `paused` with a child wait). When the
successor runs the same version it re-points their wait to the successor
(fenced; history `child_wait_reattached` `{from, to}`); otherwise it wakes
them with `child_failed` (`child_superseded`), as T-A5 does. A later
`child_with_key(..., "k")` resolves to the newest generation (D4). The model
always re-attaches (versions are not modeled). Tests:
`g2_recoverable_start_wakes_parent_of_superseded_blocked_child`,
`g2_reattached_parent_completes_with_the_successor_output`,
`g2_keyed_child_lineage_after_two_recoveries`,
`d4_child_with_key_after_recovery_attaches_to_the_newest_generation`, and
`admin_controls.rs`
`recoverable_start_at_a_new_version_fails_the_waiting_parent_as_superseded`
(not recorded for trace checking: a version change is excluded as
`versions:<kind>`).

**N1. T-X2 on a child key used the root's whole tree as the lineage**
(model finding). T-X2 took the newest row of the same kind with
`id = root OR rootWorkflowId = root`. For a keyed child, root is the parent's
root, so that set holds every child of the same kind in the tree: recovering
key k superseded a blocked auto-keyed sibling, or returned a newer live
sibling. **Fixed**: the newest generation is the end of the keyed row's
`restartedFromWorkflowId` chain (`lock_newest_generation`; no schema change,
the unique restart key makes the chain linear). Tests:
`n1_recoverable_start_on_child_key_leaves_blocked_sibling_alone`,
`n1_recoverable_start_on_child_key_returns_its_own_row`.

**N3. The public restart field accepted a live source** (model finding).
`StartOptions::restarted_from_workflow_id` was public, and `insert_prepared`
stored it after only the restart-key uniqueness check, so an application
could start a "successor" of a running workflow, or take a failed row's
restart key outside T-X2. **Fixed**: `restarted_from_workflow_id` and
`root_workflow_id` are `pub(crate)`; only T-X2 and the admin restart (T-A5)
set them. The model's `TX1_Start` needs a terminal `from`, and
`inv_S19_sourceTerminal` is part of `safety`. Test: the trybuild case
`tests/ui/fail/start_options_restart_field_private.rs`.

**G3. A poison-pill `step` stops runtimes.** Lease recovery does not count
activation attempts (`src/runtime/coordinator.rs` `WorkflowCoordinator::claim_row`). A `step` that
panics fails the coordinator task, the row is recovered after 30 s, claimed
again, and panics again; each runtime that claims it uses up its restart
budget. A `step` that never returns blocks that runtime's only coordinator
loop (`src/runtime/supervisor.rs` `run_task`; no timeout around
`src/runtime/coordinator.rs` `WorkflowCoordinator::activate_claim_inner`), and after lease expiry the next
runtime that claims it also blocks. **Fixed**: `step` runs under an unwind
boundary and `CoordinatorConfig::step_timeout` (default 30 s, the default
lease); a panic (`step panicked: <message>`) or a timeout (`step exceeded
step_timeout`) is a T-C3 activation failure, so the workflow fails after
`min(max_activation_attempts, 8)` attempts. Lease recovery still does not
count an attempt (intended; S8). A `panic = "abort"` build still aborts, and a
`step` that blocks its thread without yielding cannot be timed out. Tests:
`g3_panicking_step_fails_at_the_activation_cap`,
`g3_step_exceeding_step_timeout_is_bounded_by_activation_attempts`.

**G4. Stale snapshot for `sequence` causes spurious aborts.** **Closed** for
library transactions by READ COMMITTED (§5.1): `next_event_sequence` after the
row lock sees every committed append (test
`g4_child_completion_sees_a_parent_pause_committed_after_its_first_read`, which
fails with the duplicate key under REPEATABLE READ). It remains possible in a
caller's REPEATABLE READ `*_with_conn` transaction. Original interleaving:
child C's T-C2 `Complete` locks C and appends history, so its first consistent
read (`next_event_sequence(C)`, `src/persistence/events.rs`) fixes
snapshot S. Then an operator pauses parent P and commits (history event
`k+1` on P). Then C's wake locks P (current read: paused, still waiting on C)
and computes `next_event_sequence(P)` from S = `k+1` → duplicate
`uq_durable_workflow_event_sequence` → T-C2 rolls back → G1. C runs `step`
again after lease expiry. Safety holds through the unique key. The same
pattern applies to any transaction whose first consistent read happens before
it locks the workflow it appends to.

**G5. A schedule upgrade during a DST fall-back hour can re-target an
already-materialized occurrence.** T-S1 `Upgraded` resets the cursor to
`next_after(now)` computed on naive local time (`src/schedule.rs` `ScheduleCalendar::next_after`, `next_after_local`,
`ScheduleRegistry::reconcile_state`). If `now` is in the second pass of a repeated hour, the next
local occurrence (e.g. `01:30`) may already have a run row from the first
pass. Every T-S2 then fails on `uq_durable_schedule_run_occurrence`
(`src/runtime/schedule_materializer.rs` `materialize_occurrence`), a retryable database error →
task error each tick (`src/runtime/supervisor.rs` `run_task`) → G1. The cursor
never advances because each tick rolls back. Separately, every upgrade drops
the unmaterialized span between the old cursor and `now` without run rows.
**Fixed**: `ScheduleCalendar::next_after` returns the first occurrence
strictly after `after` as an instant; an occurrence in a repeated hour fires
once, at the earlier pass, so in the second pass it is skipped. An upgrade
sets the cursor strictly after the last materialized occurrence (S27,
`ScheduleCursor::upgrade`), which also covers a timezone change. Dropping the
unmaterialized span is intended. Tests: `next_after_is_later_as_an_instant`,
`next_after_instant_minimal_counterexample` (`tests/props_schedule.rs`),
`reconcile_in_the_second_pass_of_a_fall_back_hour_yields_a_cursor_after_now`,
`upgrade_in_the_second_pass_does_not_retarget_the_materialized_occurrence`,
`upgrade_to_a_western_timezone_does_not_retarget_a_materialized_occurrence`,
`upgrade_to_an_earlier_slot_runs_it_today` (`tests/schedule_state.rs`), and
the unit tests in `src/schedule/cursor.rs`.

**G6. A concurrent child dedup race skips the version check.** `insert_child`
checks the version only on its consistent-read pre-check
(`src/store.rs`); the upsert conflict path returns the winner's id
without comparing versions (`src/persistence/workflows.rs` `insert_started`). Two
parents that start key k at versions 1 and 2 at the same moment can both wait
on the v1 child. The v2 parent's flow replay then fails closed
(`src/flow.rs` `WfCtx::replay`) and the parent fails after its activation attempts.
**Fixed**: `insert_started` returns the keyed row (locked `FOR UPDATE`) on a
conflict, and `insert_child` runs one version check for the pre-read and the
conflict path, after resolving the row to its newest generation (D4). The v2
parent's commit fails with `DefinitionMismatch`, a T-C3 activation failure,
so it never waits on the v1 child. Test:
`g6_child_dedup_race_rejects_version_mismatch`.

**G7. The topic cap can be exceeded by one through heartbeat revival.**
Interleaving: activity A's lease expires in DB time while its worker gives up
locally and drops a heartbeat whose COMMIT was already sent. T-W1 starts its
snapshot with the reconcile candidate read (A expired). The heartbeat commits
(`leaseExpiresAt = now+L`; no expiry check, `src/runtime/activity_worker.rs` `heartbeat_once`).
Reconcile relocks A (current read: live) and skips it (`reconcile_expired`). The
`in_flight` count still uses the snapshot (`ActivityWorker::claim_batch`) and misses A. T-W1
claims up to the cap, so live leases = cap + 1. Under S13's clock assumption
A has no executor, so real concurrency stays within the cap; A is reconciled
later and loses an attempt without running. **Closed** under READ
COMMITTED: the interleaving needs the `in_flight` count to read the
snapshot that the reconcile scan started, which only REPEATABLE READ does.
Under READ COMMITTED the count is a new statement and sees every commit
before it, so it counts a revived A as live; a heartbeat after the relock
blocks on the row lock and then misses the fence. T-W1 (`claim_batch`,
`claim_one`) always runs in a library transaction, which
`dialect::transaction` pins to READ COMMITTED (§5.1), so the REPEATABLE READ
form cannot occur. The heartbeat can still revive an expired lease, which
costs A an attempt later but does not exceed the cap. Evidence
(`spec/README.md`, "Why G7 is closed under READ COMMITTED"): `safetyRc`
(`safety` plus S17 at claim and between commits) holds on `durable_mc` in
simulation and in Apalache at depth 3; the directed tests
`g7ClosedUnderRcTest` and `g7RcHeartbeatBlockedTest` run both orders. The
historical REPEATABLE READ instances (`durable_mc_rr`, `durable_mc_act_rr`,
test `g7CapExceededTest`) still violate S17, as expected.

**G8. Domain keys can form wait cycles.** `child_with_key` can resolve to the
calling workflow or an ancestor (same kind and key). `commit_child` then
waits on a non-terminal row (`src/runtime/coordinator.rs`). There is
no cycle detection. Liveness only. **Fixed** for the caller and its
ancestors: after a dedup hit, `commit_child` walks the caller's
`parentWorkflowId` chain and returns `InvalidDefinition` ("child key {k}
resolves to workflow {id}, which is the caller or an ancestor"), a T-C3
activation failure, before the parent update. For the caller's own key the
keyed row `insert_child` locks is the caller's row, the only lock the
transaction holds when it rolls back, so there is no second lock and no
wait. Intended (not fixed): cycles through keyed rows that are not ancestors
of each other (A waits on B's key while B waits on A's) stay unguarded. Tests:
`g8_child_key_resolving_to_self_does_not_wait_on_itself`,
`g8_child_key_resolving_to_a_grandparent_is_an_activation_failure`. Model:
`TC2_RunChild` never attaches to the caller or an ancestor
(`inv_G8_noAncestorWait`, in `safety`).

**G9. Lock-order inversion between a child's terminal commit and a parent
attaching to it.** A child's terminal transaction locks the child, then scans
waiting parents `FOR UPDATE` (`src/persistence/workflows.rs` `wake_waiting_parents_on_child_terminal`). A
parent's `RunChild` commit on an existing child locks the parent, then the
child (`src/runtime/coordinator.rs` `commit_child`). With a domain-keyed child that
completes while a parent attaches, InnoDB can deadlock and abort one side →
G1, and the aborted step runs again after lease expiry. Safety holds.
**Fixed**: `commit_child` locks an existing child before the fenced parent
update, so both sides lock child → parent. Any remaining deadlock (for
example between two parents' wake scans) is a transient error, which G1's fix
makes benign.

**G10. One invalid row stops all activity claims.** `claim_locked_candidate`
returns an error, not a skip, for a missing definition, an attempt cap
overrun, or `leaseDuration <= timeout` (`src/runtime/activity_worker.rs`).
The error aborts the whole T-W1 across all topics and ends the dispatcher
task (G1); the same row is selected again next sweep. No traced engine path
creates such a row, but a `Deserialize`-built `ActivityCommand` or a manual
edit can. **Fixed**: a missing definition is a skip (`Ok(None)`, debug log;
the candidate query already filters on local definitions). An attempt-cap
overrun (`attemptCount >= maxAttempts`) or invalid bounds quarantines the row
in the same T-W1 (`quarantine_candidate`): fenced on `status=pending AND
attemptCount=k`, the row becomes `dead_lettered` with
`lastErrorCategory=invalid_row`, `lastErrorMessage="<reason>: <detail>"` and
`completedAt=now`; history `activity_quarantined`; the workflow (already
locked) is blocked with `errorCategory=invalid_row`. The claim goes on with
the next candidate and topic. `retry_activity` recovers the workflow with a
replacement row built from the registered definition. The trace records the
row in `TW1_Claim.quarantined` (interface v5). Tests:
`g10_invalid_activity_row_does_not_stop_other_claims`,
`g10_quarantined_row_is_dead_lettered_and_blocks_its_workflow`, and
`admin_controls.rs` `retry_recovers_a_workflow_blocked_by_a_quarantined_activity`.

**G11. Parent cancellation does not reach descendants** (specification gap).
See the rejected candidate in §3. Child workflows and their external side
effects continue after the parent is cancelled or superseded.
**Fixed** for owned children: `cancel_locked_workflow` (T-X3, T-A4) and the
T-A5 supersession of a non-terminal source call `cancel_owned_descendants`,
which cancels every live generation of every child whose key is
`child:{parent}:{command}` (the child and the T-X2/T-A5 successors on its
restart chain, to which G2 may have re-attached the parent), and their owned
children, in the same transaction (S25; lock order in §2.8). Rule: a
parent's cancel reaches every generation of its owned children. The child's
history records `workflow_cancelled` with reason
`parent workflow {p} cancelled: {reason}`, and its running activities become
`cancelling` (N2). Documented as intended: a child started with a domain key
is not owned and keeps running. Model: `cancelWrite` cancels
`cancelTargets`; `inv_G11_cancelReachesChildren` (owned children) and the
action property `inv_G11_cancelReachesGenerations` are in `safety`. Tests:
`g11_parent_cancellation_cancels_child_workflow`,
`g11_cancel_reaches_grandchildren`,
`g11_cancel_reaches_the_restart_successor_of_an_owned_child`,
`g11_domain_keyed_child_survives_parent_cancellation`,
`g11_cascade_revokes_a_running_child_activity` (`tests/gaps.rs`) and
`restart_supersession_cancels_the_owned_child` (`tests/admin_controls.rs`),
`cancel_parent_with_running_child` (`tests/trace_model.rs`).

**G12. Run-now ignores the schedule pause and the overlap policy**
(`src/admin/control.rs` `AdminControlService::run_schedule_now`). **Fixed**: run-now applies the overlap
policy under the state lock; with `SkipIfActive` or `QueueOne` and an active
run it returns `Conflict("schedule {k} has an active run; overlap policy {p}
rejects run-now")`. Ignoring the pause is intended: it is the operator
override. Test: `run_now_respects_the_overlap_policy_and_ignores_the_pause`
(`tests/schedule_overlap.rs`).

---

**N2. A cancel or pause freed the topic slot while the revoked handler still
ran (FIXED).** Cancel and pause closed the attempt and cleared the lease at
once; the handler learned of it only at its next heartbeat and could clean
up for `shutdown_grace`, so a cap-1 topic could run two handlers. Fixed with
the `cancelling` status (§1.2): the revoked row keeps its lease, open attempt
and topic slot until its handler stops or its lease expires
(`settle_revoked`). Tests `n2_*` in `tests/gaps.rs`.

**N5. A given-up handler's progress transaction blocked its own finish
(FIXED).** When a handler that ignores cancellation outlived its timeout
plus `shutdown_grace` (or the worker's shutdown grace), the executor left
its poll loop with the handler future still alive and ran T-W3. The handler
could be inside a T-W4 progress report whose transaction holds the activity
row `FOR UPDATE` on its own pooled connection. T-W3 locks the workflow, then
waits for the same row; nothing polls the handler again, so its session sits
idle, the database's deadlock detector sees no cycle, and T-W3 waits until
`innodb_lock_wait_timeout` (MySQL, 50 s by default) or forever (Postgres,
`lock_timeout` 0). The row stays `running` until its lease expires, and on
Postgres the executor task never returns. The revoke path
(`ExecutionOutcome::Revoked`) finishes through the same code, but a T-W4
report cannot hold the lock there: its fence requires `running`, and the
row is `cancelling`. **Fixed**: the executor drops the handler future before
T-W3 (`handler::RunningHandler::stop` in `src/runtime/activity_worker.rs`;
`finish_claim` takes the `Stopped` outcome it returns, so finishing while
the future is alive does not compile). Dropping the future drops its
connection mid-transaction: diesel-async reports a connection with an open
transaction as broken, bb8 closes it instead of pooling it, and the server
rolls the transaction back. Test:
`timed_out_handler_holding_the_activity_row_lock_does_not_block_its_finish`
(`tests/application_cancellation/timeout_cleanup.rs`), on both backends.
The model does not represent handler futures or their connections, so it
has no N5 scenario; a lock held by application code outside the engine's
connections is not covered (§5.5).

## 7. Modeling notes

### 7.1 Order of work

1. **Activity lease protocol** with one workflow waiting on one activity:
   T-W1 (with reconcile), T-W2, T-W3, the local deadline, `Crash`, `Tick`.
   Check S9–S13, S15–S17, L4–L6. Give the local clock a drift parameter to
   explore S13 and G7.
2. **Workflow lease and delivery**: T-X1, T-C1, L-C1, T-C2 (all variants),
   T-C3. Check S1–S8. Model `step` as a nondeterministic choice of transition
   (S3 lets two coordinators pick different results; S2 must keep only one).
3. **Operator actions** against 1 and 2: T-A2 through T-A6, T-X3. Check S14,
   S25, S36, and the budget effect in G1.
4. **Children**: `RunChild` with auto keys and a small domain-key space,
   T-X2. Check S23, S24; reproduce G2, G6, G8, G9.
5. **Temporal**: T-T1, T-A1, T-A7. Check S20–S22.
6. **Schedules**: model the calendar as a finite list of occurrences
   `(localKey, dueAt, disposition ∈ {exact, gap, ambiguous})`, with the misfire
   classification as a pure function. T-S1, T-S2, T-A8, T-A9. Check S26–S30;
   reproduce G5 with a repeated local key.
7. **Supervisor**: a per-runtime restart counter and a `selfCancelled` flag for
   G1/G3 liveness.

### 7.2 Abstract away

JSON payloads, outputs, and state (use opaque values or hashes; keep the
flow journal as a list of `(stepKind, version)`), error text, progress events
(keep a count only if needed), observability, health scans, admin queries and
metrics, backoff and jitter (nondeterministic `availableAt ≥ now`), cron and
time zones (use the occurrence list), and payload size limits
(check those with unit tests).

Keep: `(kind, version)` for workflows, activities, and children (G6);
dedup keys; UUID tokens as a monotonic counter; DB time as an integer.

### 7.3 Suggested state variables

```
now: int                                        // DB time
wf:  WfId -> { kind, version, status, waitKind, waitRef, availableAt,
               leaseToken: Option[Token], leaseExp: Option[int],
               cmdSeq, delivered, stateVer, activationAttempts,
               dedupKey: Option[Key], restartedFrom: Option[WfId],
               root: Option[WfId], scheduleRun: Option[RunId],
               journal: List[(StepKind, Version)] }
events: WfId -> List[{ seq, deliverySeq: Option[int], type }]
act: ActId -> { wf, cmdSeq, replacementNo, topic, kind, version, status,
                attemptCount, maxAttempts, availableAt,
                leaseToken: Option[Token], leaseExp: Option[int] }
attempt: (ActId, int) -> { token, open: bool, outcome }
approval: ApprId -> { wf, cmdSeq, status, expiresAt: Option[int] }
topicCap: Topic -> int
schedState: Key -> { cursor: OccIdx, paused, version }
schedRun: (Key, LocalKey) -> { status, wf: Option[WfId] }
nextToken: int
// process-local, lost on Crash(runtime)
coordClaim: Runtime -> Option[{ wf, token, eventDelivery, chosenTransition }]
exec: (Runtime, ActId) -> { attempt, token, localDeadline, phase }
restarts: (Runtime, TaskKind) -> int
selfCancelled: Runtime -> bool
```

### 7.4 Action mapping for trace checking

One model action per committed transaction (§2 identifiers), plus
`FenceMiss(txn)` for transactions that roll back on a fence. Suggested log
record per transaction: action id, runtime id, DB `now`, the rows written
(`id`, `status`, `attemptCount`, `leaseToken` fingerprint, `waitKind`,
`waitRef`, `delivered`, `cmdSeq`), and for events `(sequence, deliverySequence, type)`.
Local steps that the model needs (`StepDecided`, `HandlerReturned`,
`LocalDeadlinePassed`, `Crash`) can be logged as instrumentation-only records.
Transactions that run on the caller's connection (T-X1, T-X3) are logged at
the caller's commit.

### 7.5 Suggested model invariants to write first

- `∀w: wf[w].status = running ⇔ wf[w].leaseToken ≠ None` (S1)
- `∀a: act[a].status ∈ {running, cancelling} ⇔ attempt[(a, act[a].attemptCount)].open` (S11)
- `∀a: act[a].attemptCount ≤ act[a].maxAttempts` (S12)
- `∀a: act[a].status ∈ {pending, running} ⇒ wf[act[a].wf].waitRef = a` (S15)
- `∀w: |{e ∈ events[w] : e.deliverySeq > wf[w].delivered}| ≤ 1` (S5)
- `∀w: wf[w].status = ready ⇒ ∃e ∈ events[w]: e.deliverySeq = wf[w].delivered + 1` (S5)
- terminal-absorbing as an action property (S6, S14, S20)
- `∀t: |{a : act[a].topic = t ∧ status ∈ {running, cancelling} ∧ leaseExp > now}| ≤ topicCap[t]`
  at T-W1 commits (S17); the model also checks it between commits, which
  holds under READ COMMITTED and fails only in the historical REPEATABLE READ
  instances (G7)
- at most one committed `schedRun` per `(key, localKey)` (S26)
