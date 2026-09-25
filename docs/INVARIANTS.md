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
- `now` always means database time: `UTC_TIMESTAMP(3)` in epoch milliseconds
  (`src/persistence/mod.rs:42-69`), sampled once per transaction unless noted.
- A **fence** is a `WHERE` predicate on an `UPDATE` whose affected-row count
  must be exactly 1; otherwise the code returns `DurableError::FencedWrite`
  and the enclosing transaction rolls back (e.g. `src/runtime/coordinator.rs:917-923`,
  `src/runtime/activity_worker.rs:1281-1287`).
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
`maxActivationAttempts` (always 8 at insert, `src/store.rs:19`),
`consecutiveContinuations`, `deduplicationKey` (unique with `kind`, M:33),
`restartedFromWorkflowId` (unique, M:34), `rootWorkflowId`,
`parentWorkflowId/parentCommandSequence` (informational only; never read by
the runtime), `scheduleRunId`.

Statuses (`src/persistence/mod.rs:130-142`): `ready`, `running`,
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
| (none) | ready | insert + `started` event (seq 1, delivery 1) | app, coordinator (child), materializer, admin | `src/persistence/workflows.rs:35-88` |
| ready | running | claim, new UUID lease | coordinator | `src/runtime/coordinator.rs:371-390` |
| running | ready | expired-lease recovery | any coordinator | `src/runtime/coordinator.rs:296-321` |
| running | ready | `Continue` commit | coordinator | `src/runtime/coordinator.rs:512-560` |
| running | ready | activation failure, not exhausted | coordinator | `src/runtime/coordinator.rs:446-469` |
| running | failed | activation failure, exhausted | coordinator | `src/runtime/coordinator.rs:446-492` |
| running | succeeded | `Complete` commit | coordinator | `src/runtime/coordinator.rs:561-591` |
| running | sleeping | `SleepUntil` commit | coordinator | `src/runtime/coordinator.rs:608-635` |
| running | waiting_approval | `WaitForApproval` commit | coordinator | `src/runtime/coordinator.rs:636-683` |
| running | waiting_activity | `RunActivity` commit | coordinator | `src/runtime/coordinator.rs:705-781` |
| running | waiting_child | `RunChild` commit | coordinator | `src/runtime/coordinator.rs:783-875` |
| waiting_activity | ready | activity success wake | activity worker | `src/runtime/activity_worker.rs:1160-1218` |
| waiting_activity | blocked | activity dead-letter | activity worker (finish or reconcile) | `src/runtime/activity_worker.rs:1220-1259` |
| waiting_child | ready | child terminal wake | whoever makes the child terminal | `src/persistence/workflows.rs:183-274` |
| sleeping | ready | timer fired | timer materializer | `src/runtime/temporal.rs:31-59` |
| waiting_approval | ready | approval resolved | admin | `src/admin/control.rs:630-650` |
| waiting_approval | ready | approval expired | approval materializer | `src/runtime/temporal.rs:140-156` |
| any non-terminal except paused (includes running) | paused | pause | admin | `src/admin/control.rs:59-101` |
| paused | paused (wait cleared, event appended) | child wake / approval resolve / approval expiry | as above | `src/persistence/workflows.rs:248-252`, `src/admin/control.rs:630-634`, `src/runtime/temporal.rs:226-230` |
| paused | ready / sleeping / waiting_activity / waiting_child / waiting_approval / blocked | resume (derived from wait) | admin | `src/admin/control.rs:103-146`, `940-1020` |
| blocked | waiting_activity | retry dead-lettered activity | admin | `src/admin/control.rs:471-488` |
| any non-terminal (includes running) | cancelled | cancel | app (`cancel_with_conn`) or admin | `src/store.rs:503-568` |
| paused, blocked | cancelled | superseded by restart | admin | `src/admin/control.rs:258-304` |
| blocked | cancelled | superseded by recoverable start | app | `src/store.rs:256-293` |

No transition leaves a terminal status (see S6).

### 1.2 Activity (`durable_activity`, M:65-104)

Statuses (`src/persistence/mod.rs:144-150`): `pending`, `running`,
`succeeded`, `dead_lettered`, `cancelled`. Unique
`(workflowId, commandSequence, replacementNumber)` (M:94); `maxAttempts > 0`
(M:103).

| from | to | trigger | actor | code |
|---|---|---|---|---|
| (none) | pending | `RunActivity` commit (`availableAt = 0` if continuation priority) | coordinator | `src/runtime/coordinator.rs:719-757` |
| (none) | pending | replacement of a dead-lettered activity (`replacementNumber+1`) | admin | `src/admin/control.rs:434-468` |
| pending | running | claim, `attemptCount+1`, new UUID lease | activity worker | `src/runtime/activity_worker.rs:477-509` |
| running | succeeded | finish success | activity worker | `src/runtime/activity_worker.rs:1015-1042` |
| running | pending | retryable failure, attempts left (backoff) | activity worker | `src/runtime/activity_worker.rs:1043-1091` |
| running | pending | expired lease, attempts left (backoff) | activity worker (claim txn) | `src/runtime/activity_worker.rs:913-937` |
| running | pending | pause of the waiting workflow; `maxAttempts+1` | admin | `src/admin/control.rs:888-938` |
| running | dead_lettered | permanent failure, or retryable on last attempt | activity worker | `src/runtime/activity_worker.rs:1099-1131` |
| running | dead_lettered | expired lease on last attempt | activity worker (claim txn) | `src/runtime/activity_worker.rs:898-937` |
| pending, running | cancelled | workflow cancel / restart | app, admin | `src/store.rs:578-614` |
| dead_lettered | cancelled | recoverable start of a blocked/failed lineage | app | `src/store.rs:241-255` |

### 1.3 Activity attempt (`durable_activity_attempt`, M:106-122)

Primary key `(activityId, attemptNumber)`, `attemptNumber > 0`. States: open
(`finishedAt IS NULL`, `outcome IS NULL`) and closed. Created only by claim
(`src/runtime/activity_worker.rs:494-509`). Closed exactly once, fenced on
`leaseToken = claim token AND finishedAt IS NULL`:

| outcome | code |
|---|---|
| `succeeded`, `retryable_failure`, `dead_lettered` | `src/runtime/activity_worker.rs:1133-1158` |
| `lease_expired` | `src/runtime/activity_worker.rs:938-952` |
| `operator_paused` | `src/admin/control.rs:907-914` via `src/store.rs:616-644` |
| `operator_cancelled`, `application_cancelled` | `src/store.rs:593-595`, `616-644` |

Heartbeats update `heartbeatAt` on the open attempt only
(`src/runtime/activity_worker.rs:805-814`). Progress events (≤100 per attempt,
M:138) hang off the attempt (`src/progress.rs:83-150`).

### 1.4 Approval (`durable_approval`, M:142-164)

Statuses: `pending`, `resolved`, `expired`, `cancelled` (plain strings). Unique
`(workflowId, commandSequence)` (M:159).

| from | to | actor | code |
|---|---|---|---|
| (none) | pending | coordinator (`WaitForApproval`) | `src/runtime/coordinator.rs:641-660` |
| pending | resolved | admin (`resolve_approval`), only while `expiresAt > now` | `src/admin/control.rs:512-658` |
| pending | expired | approval materializer, only when `expiresAt <= now` | `src/runtime/temporal.rs:82-159` |
| pending | cancelled | workflow cancel / admin restart | `src/store.rs:646-675` |

### 1.5 Timer

Timers have no table. An armed timer is the workflow tuple
`(status=sleeping, waitKind=timer, waitReferenceId=commandSequence, availableAt=wakeAt)`
set at `src/runtime/coordinator.rs:612-633`. States: armed → fired
(`src/runtime/temporal.rs:31-59`: `TimerFired` delivery event, workflow →
ready), suspended (workflow paused keeps the wait; the materializer only
selects `status=sleeping`, `src/runtime/temporal.rs:36`), resumed (resume
restores `sleeping` with the original `availableAt`,
`src/admin/control.rs:946`, `124-129`), cancelled (workflow cancelled).

### 1.6 Child workflow

A child is an ordinary workflow row inserted in the parent's `RunChild` commit
(`src/store.rs:409-479`) with deduplication key `child:{parentId}:{command}`
or a caller-supplied domain key (`src/runtime/coordinator.rs:793-796`). A dedup
hit reuses the existing row (any parent, any lineage). Parent-side states:

| parent state | meaning |
|---|---|
| `waiting_child`, `waitReferenceId=C` | awaiting C |
| `paused`, `waitKind=child`, `waitReferenceId=C` | paused while awaiting C; still woken |
| `ready` + undelivered `child_succeeded`/`child_failed` event | outcome delivered, not yet consumed |

Delivery happens in the child's terminal transaction through
`wake_waiting_parents_on_child_terminal` (`src/persistence/workflows.rs:148-181`),
called from: `Complete` (`src/runtime/coordinator.rs:581-589`), activation
exhaustion (`482-492`), cancel (`src/store.rs:559-567`), admin restart
supersession (`src/admin/control.rs:292-303`), and attach-to-already-terminal
child (`src/runtime/coordinator.rs:835-873`). It is **not** called by
recoverable-start supersession (`src/store.rs:256-293`; see G2).

### 1.7 Schedule state and schedule run

`durable_schedule_state` (M:166-181): one row per schedule key with cursor
`(nextLocalOccurrence, nextOccurrenceAt)`, `pausedAt`, and the pinned
`(definitionVersion, definitionFingerprint)`.

| change | actor | code |
|---|---|---|
| insert (cursor = first occurrence after `now`) | runtime spawn / materializer tick | `src/schedule.rs:343-388` |
| upgrade (new version; cursor reset to first occurrence after `now`) | runtime spawn / materializer tick | `src/schedule.rs:402-412` |
| cursor advance | materializer | `src/runtime/schedule_materializer.rs:193-214` |
| pause / resume | admin | `src/admin/control.rs:677-740` |

`durable_schedule_run` (M:183-199), unique `(scheduleKey, localOccurrence)`
(M:195). Statuses: `materializing` (inserted and changed to `started` in the
same transaction; never committed), `started`, `queued`, `skipped`,
`coalesced`.

| from | to | actor | code |
|---|---|---|---|
| (none) | materializing → started | materializer | `src/runtime/schedule_materializer.rs:379-420` |
| (none) | queued | materializer (QueueOne, active run) | `src/runtime/schedule_materializer.rs:348-371` |
| (none) | skipped / coalesced | materializer | `src/runtime/schedule_materializer.rs:374-392` |
| queued | started | materializer promotion | `src/runtime/schedule_materializer.rs:461-504` |
| (none) | materializing → started, `localOccurrence = manual:{t}` | admin run-now | `src/admin/control.rs:742-820` |
| started (workflowId X) | started (workflowId = successor) | admin restart | `src/admin/control.rs:305-315` |

### 1.8 Workflow event log (`durable_workflow_event`, M:47-63)

Unique `(workflowId, sequence)` and `(workflowId, deliverySequence)`
(M:59-60). Two kinds:

- **Deliverable** (`deliverySequence` non-NULL): `started`, `continued`,
  `activity_succeeded`, `child_succeeded`, `child_failed`, `timer_fired`,
  `approval_resolved`, `approval_expired`. Only these reach workflow code
  (`src/runtime/coordinator.rs:139-149`, `901-915`).
- **History** (`deliverySequence` NULL): everything else (lease recovery,
  scheduling, dead-letter, operator events).

`sequence` is `MAX(sequence)+1` from a consistent read
(`src/persistence/events.rs:25-38`); `deliverySequence` is
`deliveredEventSequence + 1` read from the locked workflow row (or
`delivered + 1` of the event just consumed for `continued`).

### 1.9 Topic lock (`durable_topic_lock`, M:201-207)

One row per topic holding `maxConcurrency`. Rows are inserted once per
registry (`INSERT IGNORE`) and must match the registered cap
(`src/registry.rs:590-643`). The rows are used only as a mutex for claims.

---

## 2. Actors and their atomic steps

### 2.1 Supervisor (`src/runtime/supervisor.rs`)

One runtime spawns these tasks (`728-789`): activity-execution collector,
coordinator (one sequential loop), health (read-only), timer, approval-expiry,
one task per schedule key, and the activity dispatcher. Any task that returns
`Err` or panics is restarted after `restart_backoff` (`881-910`). The restart
counter per task kind is **cumulative for the process lifetime**; when it
exceeds `max_task_restarts` (default 8, `55`), the whole runtime cancels
itself (`853-866`). A collector failure cancels immediately (`849-852`).
Startup checks readiness and topic caps before any claim (`174-198`).

### 2.2 Coordinator (`run_task` Coordinator, `src/runtime/supervisor.rs:922-939`)

Per iteration: `activate_one` (`src/runtime/coordinator.rs:111-118`). If no
claim, sleep `idle_delay` (2 s). Any error returned by T-C2 or T-C3 other than
the handled definition errors ends the task (G1).

**T-C1 claim** (`src/runtime/coordinator.rs:228-393`), one transaction:
1. `now` ← DB.
2. Expired-lease recovery: page through `status=running AND leaseExpiresAt <= now`
   (consistent read, 32 per page, cursor on `(leaseExpiresAt, id)`); for each
   id, `FOR UPDATE SKIP LOCKED` and recheck; for the first match, fenced
   `UPDATE ... WHERE status=running AND leaseToken=<old>` → `ready`,
   `availableAt=now`, lease cleared; append history `lease_recovered`
   (`242-321`). At most one recovery per transaction. `activationAttempts`
   is not changed.
3. Ready claim: consistent read of ≤32 ids with `status=ready`,
   `availableAt <= now`, `(kind, version)` in the local registry, ordered
   `(availableAt, id)`; for each, `FOR UPDATE SKIP LOCKED` with the same
   filter; first hit is updated with fence `status=ready` → `running`,
   `leaseToken=uuid4`, `leaseExpiresAt=now+lease_duration` (30 s default)
   (`323-390`).

There is no workflow lease renewal anywhere in the code.

**L-C1 activation (no transaction, no lock)** (`136-204`): read the lowest
deliverable event with `deliverySequence > deliveredEventSequence`
(`src/persistence/events.rs:10-23`); error if none. Run
`WorkflowRegistry::step_stored(input, state, event)` (user code, no timeout).
Validate that a `RunChild`/`RunActivity` target is registered locally and the
activity topic matches.

**T-C2 commit** (`src/runtime/coordinator.rs:395-408`, `499-875`), one
transaction. `now` ← DB. Every branch includes the fenced update
`WHERE id AND status=running AND leaseToken=claim` (`19-26`), which sets
`deliveredEventSequence = consumed event`, the new `stateJson`,
`stateVersion+1`, clears the lease, resets `activationAttempts`:
- `Continue`: → `ready`; append deliverable `continued` with
  `delivery = consumed+1`. After `max_consecutive_continuations` (16) in a row,
  `availableAt = now + continuation_delay` and the streak resets (`512-560`).
- `Complete`: → `succeeded`, `resultJson`; history; wake parents (child-terminal
  scan `FOR UPDATE`) (`561-591`).
- `SleepUntil`: `commandSequence+1`; → `sleeping`, wait `timer`, `availableAt=wakeAt` (`608-635`).
- `WaitForApproval`: INSERT approval (pending); `commandSequence+1`; → `waiting_approval` (`636-683`).
- `RunActivity`: INSERT activity (pending) **before** the fenced update;
  `commandSequence+1`; → `waiting_activity` (`705-781`).
- `RunChild`: dedup lookup (consistent read) and upsert child row; fenced
  parent update → `waiting_child`; history; if the child already existed,
  lock it `FOR UPDATE` (current read) and, if terminal, wake waiting parents
  in the same transaction (`783-875`).
A fence miss rolls back the whole transaction, including the inserts.

**T-C3 activation failure** (`410-496`), one transaction: lock the row with
the claim fence (`FOR UPDATE`); `attempt = activationAttempts+1`; exhausted iff
`attempt >= min(maxActivationAttempts, config.max_activation_attempts)`;
fenced update → `failed` (+`completedAt`, wake parents) or `ready` with
`availableAt = now + backoff(attempt)`. The deliverable event is not consumed.
Used for handler errors, unregistered child/activity, topic mismatch, and
`DefinitionMismatch`/`InvalidDefinition` from T-C2 (`166-223`).

### 2.3 Activity dispatcher and executions (`src/runtime/supervisor.rs:1033-1132`)

Per iteration: surface a pending execution panic (task error); compute
`available = Σ local topic caps − active executions`; if 0, wait for a change;
otherwise T-W1 and spawn one execution task per claim. Empty sweeps back off
1/2/5/10 s with per-runtime jitter.

**T-W1 claim_batch** (`src/runtime/activity_worker.rs:280-413`), one
transaction:
1. Lock **all** registered topic rows `FOR UPDATE SKIP LOCKED`, ordered by
   topic. If any is missing, return no claims (`299-312`).
2. Sample the local monotonic instant, then `now` ← DB (`314-315`).
3. For each topic:
   a. `reconcile_expired` (`852-974`): consistent read of
      `status=running AND (leaseExpiresAt <= now OR NULL)`; for each: lock
      workflow `FOR UPDATE` (blocking), relock the activity `FOR UPDATE` and
      recheck; fenced `UPDATE WHERE status=running AND attemptCount=k AND leaseToken=t`
      → `pending` (backoff) or `dead_lettered` (if `attemptCount >= maxAttempts`);
      close the attempt `lease_expired`; history; if dead-lettered and the
      workflow waits on it, block the workflow.
   b. `in_flight` = count of `running AND leaseExpiresAt > now` (consistent read).
   c. `wanted = min(cap − in_flight, local capacity, remaining batch)`.
   d. Candidates: consistent read joining workflow on
      `workflow.status=waiting_activity AND workflow.waitReferenceId=activity.id`,
      `activity.status=pending`, `availableAt <= now`, local `(kind, version)`,
      ≤32, ordered `(availableAt, continuation tie-break, id)`.
   e. `claim_locked_candidate` (`415-524`): workflow `FOR UPDATE SKIP LOCKED`
      with the wait recheck; activity `FOR UPDATE SKIP LOCKED` with recheck;
      error (aborts the whole T-W1) if the definition is missing, the attempt
      cap is exceeded, or `leaseDuration <= timeout`; fenced
      `UPDATE WHERE status=pending AND attemptCount=k` → `running`,
      `attemptCount=k+1`, `leaseToken=uuid4`, `leaseExpiresAt=now+leaseDuration`;
      INSERT attempt `k+1`.
4. Local lease deadline = sampled instant + `leaseDuration − 1 ms`
   (`820-829`).

`claim_one` (`178-275`) is the single-topic variant: it locks one topic row
with a blocking `FOR UPDATE` and otherwise follows steps 2–4.

**L-W execution** (`552-704`), local state machine. Inputs: handler future,
heartbeat loop, `timeout` (local timer), runtime cancellation, forced
cancellation, `shutdown_grace` (30 s). Refuses to start if the local lease
deadline already passed (`553-555`).
- Handler returns → outcome from the result (`688-700`).
- Timeout → cancel the handler token; outcome `Retryable(timeout)`; the handler
  may run cleanup up to `shutdown_grace` while heartbeats continue (`644-657`).
- Runtime cancellation → cancel the handler token, start the grace timer
  (`623-629`); if grace elapses first, outcome `Retryable(cancelled)`
  (`630-643`).
- Heartbeat failure (fence miss or local deadline) → cancel the handler token,
  wait at most `min(grace, last confirmed lease deadline)`, then return the
  error **without** T-W3 (`658-687`).
- Forced cancellation → stop heartbeats and return without T-W3 (`605-622`).
Then T-W3 with the outcome.

**T-W2 heartbeat** (`740-818`), every `min(heartbeat_interval, lease/3)`:
separate connection, one transaction: sample local instant, `now` ← DB;
fenced activity `UPDATE WHERE id AND status=running AND attemptCount=k AND leaseToken=t`
sets `leaseExpiresAt=now+leaseDuration`; fenced attempt update
(`finishedAt IS NULL`). Raced against the local deadline; losing the race
drops the in-flight transaction future.

**T-W3 finish** (`706-718`, `1002-1097`), one transaction: lock workflow
`FOR UPDATE`; `now` ← DB; then:
- Success: fenced activity → `succeeded`; close attempt; `wake_workflow`
  requires `status=waiting_activity AND waitReferenceId=activity` (else
  FencedWrite → rollback); append `activity_succeeded` with
  `delivery = deliveredEventSequence+1`; workflow → `ready`.
- Retryable with `attempt < maxAttempts`: fenced → `pending`,
  `availableAt = now + backoff(attempt)`; close attempt; history.
- Permanent, or retryable on the last attempt: fenced → `dead_lettered`; close
  attempt; block workflow (requires the same wait, else rollback).

**T-W4 progress report** (`src/progress.rs:83-150`): lock the activity with
the claim fence; if fewer than 100 events for the attempt, insert the next.

### 2.4 Timer and approval-expiry materializers (`src/runtime/temporal.rs`)

Both loops sample `now` on one connection, then run the transaction on
another, so `now` can be slightly stale (conservative). Poll interval ≤ 60 s
when idle (`src/runtime/supervisor.rs:1148-1150`).

**T-T1 fire timer** (`31-59`): select one `status=sleeping AND waitKind=timer AND availableAt <= now`
ordered `(availableAt, id)` `FOR UPDATE SKIP LOCKED`; require
`waitReferenceId = commandSequence` (else error); append `timer_fired`
(`delivery+1`); fenced `clear_wait` on `(status, waitKind, waitReferenceId)`
→ `ready` (`221-248`).

**T-A1 expire approval** (`82-159`): consistent read of the oldest pending
approval with `expiresAt <= now` (outside the transaction); in a transaction,
lock workflow then approval `FOR UPDATE`; if no longer pending/expired, return;
if the workflow does not wait on it (status `waiting_approval`/`paused`, kind,
version, command) → **error** (task restart); append `approval_expired`;
fenced approval → `expired`; `clear_wait` (paused stays paused).

### 2.5 Schedule materializer (`src/runtime/schedule_materializer.rs`, one task per key)

Per tick: `now` ← DB; T-S1; T-S2. Sleep the poll interval if nothing happened.
Retryable errors (database, pool, serialization) end the task
(`src/runtime/supervisor.rs:1024-1026`, `1170-1178`); others are alerted and
retried next tick.

**T-S1 reconcile state** (`src/schedule.rs:343-415`): `INSERT IGNORE` a
state row with cursor `next_after(now)`; lock it `FOR UPDATE`; persisted
version greater → `NewerPersisted` (T-S2 returns Conflict); equal version with
different fingerprint → Conflict; smaller → overwrite version, fingerprint,
and reset the cursor to `next_after(now)` (`Upgraded`). `Inserted`/`Upgraded`
end the tick.

**T-S2 materialize** (`73-218`), one transaction: lock state `FOR UPDATE`;
require the pinned version and fingerprint; return if paused; `active` = count
of non-terminal workflows joined through `scheduleRunId` for this key, and
`queued` = exists queued run (consistent reads taken after the lock); if
QueueOne and `active=0` and queued, promote the oldest queued run (starts a
workflow). Parse and verify the cursor; plan ≤10,000 due occurrences
(`plan_due_chunk`, `228-269`) and classify them (`271-322`); per occurrence
insert a run row and, for `Start`, call user `start_occurrence` on the same
connection and mark the run `started`; advance the cursor with a fence on the
old cursor, version, and fingerprint (`193-214`).

### 2.6 Application API (`src/store.rs`)

- **T-X1 start** (`95-136`, `313-403`): optional dedup consistent read; upsert
  (`ON DUPLICATE KEY UPDATE id = id + LAST_INSERT_ID(0)`,
  `src/dialect/mysql.rs` `insert_workflow`) plus reload `FOR UPDATE` on a
  dedup conflict; a restart-key collision returns `Conflict`; `started`
  event. Options with both a dedup key and a restart source are rejected
  (`InvalidDefinition`). Can run inside a caller transaction
  (`start_with_conn`).
- **T-X2 start_or_restart_recoverable** (`169-311`): lock the row with
  `(kind, dedupKey)` `FOR UPDATE`; lock the newest row of its lineage
  (`id = root OR rootWorkflowId = root`) `FOR UPDATE`; if the newest is not
  `failed`/`blocked`, return it; else cancel its `dead_lettered` activities,
  move a `blocked` newest to `cancelled` (history
  `workflow_superseded_by_recovery`; no parent wake; wait fields kept), and
  insert a successor with no dedup key, same root, `restartedFromWorkflowId`.
- **T-X3 cancel_with_conn** (`58-93`): inside the caller's transaction; lock
  workflow `FOR UPDATE`; terminal → no-op; else `cancel_locked_workflow`
  (`503-568`): cancel pending/running activities (closing open attempts),
  cancel pending approvals, fenced workflow → `cancelled` (wait and lease
  cleared), history, wake waiting parents with `child_cancelled`.

### 2.7 Admin control (`src/admin/control.rs`)

Each action is one transaction that first locks the workflow `FOR UPDATE`
(schedule actions lock the schedule state row).

- **T-A2 pause** (`59-101`): reject paused/terminal; if waiting on an
  activity that is `running`, close its attempt (`operator_paused`) and move
  it to `pending` with `maxAttempts+1` (`888-938`); fenced workflow → `paused`,
  lease cleared.
- **T-A3 resume** (`103-146`): status derived from the wait
  (`940-1020`); `availableAt=now` only when resuming to `ready`.
- **T-A4 cancel** (`148-180`): terminal → Conflict; else
  `cancel_locked_workflow`.
- **T-A5 restart / correct-and-restart** (`200-344`): source must be
  terminal, paused, or blocked and have no successor; insert successor (same
  schedule run, same root); a non-terminal source is cancelled with its
  activities and approvals (wait fields kept) and its waiting parents get
  `child_superseded`; transfer the schedule run's `workflowId`.
- **T-A6 retry / correct-and-retry activity** (`368-510`): workflow must be
  `blocked` on this `dead_lettered` activity; insert replacement
  (`replacementNumber+1`, same `commandSequence`, same operation key unless
  corrected); fenced workflow → `waiting_activity` on the replacement.
- **T-A7 resolve approval** (`512-658`): approval pending and not expired;
  workflow waits on it; validate decision; append `approval_resolved`
  (`delivery+1`); fenced approval → `resolved`; workflow → `ready`
  (paused stays paused).
- **T-A8 pause/resume schedule** (`677-740`), **T-A9 run now** (`742-820`):
  lock state; run-now inserts a `manual:{t}` run with
  `t = max(last scheduledFor + 1, now)` and starts a workflow. It checks
  neither `pausedAt` nor the overlap policy.

### 2.8 Lock order summary

- Claims: topic row(s) → workflow → activity → attempt insert.
- Finish, reconcile, pause, cancel, retry: workflow → activity → attempt;
  approvals after the workflow.
- `RunActivity`/`WaitForApproval` commit: INSERT new child row → workflow
  (new rows are invisible to others, so no cycle).
- Child terminal: child → every waiting parent (locking scan on
  `waitKind, waitReferenceId, status`; there is no index on
  `waitReferenceId`, so the scan can lock many rows; UNCLEAR how many).
- `RunChild` commit attaching to an existing child: parent → child. This
  inverts the previous line (G9).
- Progress: activity only. Schedules: state → run rows → new workflow rows.

---

## 3. Safety invariants

### Workflow lease and delivery

**S1. At most one valid workflow lease.** `status=running ⇔ leaseToken ≠ NULL ∧ leaseExpiresAt ≠ NULL`,
and a token is issued only on `ready → running`. **ENFORCED**: claim is
`FOR UPDATE SKIP LOCKED` plus fence `status=ready`
(`src/runtime/coordinator.rs:352-386`); every exit from `running` clears the
lease (`297-313`, `446-469`, `521-540`, `562-576`, `612-631`, `661-679`,
`759-777`, `807-825`; `src/admin/control.rs:78-90`; `src/store.rs:518-533`).

**S2. A stale workflow lease cannot commit.** Every coordinator write is
fenced on `status=running ∧ leaseToken = claim token`
(`src/runtime/coordinator.rs:19-26`, `421-430`, `446-451`), and recovery,
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
the follow-on event (`src/runtime/coordinator.rs:499-875`). A failed
activation (T-C3) or a lost fence consumes nothing. **ENFORCED**.

**S5. Delivery-sequence discipline.** Per workflow: (a) delivery sequences
are unique (M:60); (b) every deliverable append uses
`deliveredEventSequence+1` and, in the same transaction, moves the workflow
out of its wait (or appends `continued` while leaving `running`); (c) hence at
most one undelivered deliverable event exists; (d) `status=ready` implies one
exists. **ENFORCED** by construction (append sites:
`src/persistence/workflows.rs:71-86`, `225-268`;
`src/runtime/coordinator.rs:544-559`; `src/runtime/activity_worker.rs:1187-1217`;
`src/runtime/temporal.rs:183-248`; `src/admin/control.rs:591-650`) plus the
unique key. The resume branches that yield `ready` without an event
(`src/admin/control.rs:967`, `1010-1014`) are unreachable on traced paths. A
violation of (d) makes the coordinator fail (`src/runtime/coordinator.rs:145-149`).

**S6. Workflow terminal states are absorbing.** Every write into
`succeeded`/`failed`/`cancelled` is fenced on a non-terminal prior status,
and no write selects a terminal row for a status change. Restarts create new
rows. **ENFORCED** (`src/runtime/coordinator.rs:446-451`, `562`;
`src/store.rs:83-88`, `257-260`, `518-522`; `src/admin/control.rs:158-164`,
`211-217`, `268-272`).

**S7. Wait coupling** (table in §1.1) holds for non-terminal rows. **ENFORCED**
by construction; no DB constraint. Terminal rows cancelled through T-X2 or
T-A5 keep stale wait fields (`src/store.rs:262-269`,
`src/admin/control.rs:273-279`); all readers also filter on `status`.

**S8. Activation attempts are bounded for handler errors.** Each T-C3
increments `activationAttempts`; at `min(maxActivationAttempts, config)` the
workflow fails; any successful commit resets it to 0. **ENFORCED**
(`src/runtime/coordinator.rs:431-434`, `533`). Lease recovery does not
increment it (`296-321`), so crashes and hangs are unbounded (G3).

### Activities

**S9. At most one live lease per activity.** Only `pending → running` issues a
token, fenced on `status=pending ∧ attemptCount=k` under
`FOR UPDATE SKIP LOCKED` (`src/runtime/activity_worker.rs:437-493`); every
exit from `running` clears it. **ENFORCED**.

**S10. A stale activity lease cannot commit, heartbeat, or report progress.**
All such writes are fenced on `status=running ∧ attemptCount=k ∧ leaseToken=t`
(`src/runtime/activity_worker.rs:23-31`; `src/progress.rs:94-106`).
**ENFORCED** (tests `stale_lease_cannot_emit_progress_or_commit_a_result`,
`heartbeat_and_completion_are_fenced_by_attempt_and_token`). The fence does not
test expiry, so a heartbeat can revive an expired but unreconciled lease
(`src/runtime/activity_worker.rs:797-804`; see G7).

**S11. Running ⇔ exactly one open attempt.** `activity.status=running ⇔`
attempt `(id, attemptCount)` exists with `finishedAt IS NULL` and the same
`leaseToken`; each attempt closes exactly once. **ENFORCED**: every exit from
`running` closes the attempt in the same transaction with a
`finishedAt IS NULL` fence (§1.3 table).

**S12. Attempts are monotonic and bounded.** Per activity row,
`attemptCount` only increases by 1 at claim, attempt numbers are contiguous
from 1, and `attemptCount ≤ maxAttempts`. `maxAttempts` only increases (by 1
per operator pause of a running attempt). **ENFORCED**
(`src/runtime/activity_worker.rs:457-466`, `898`, `1045`;
`src/admin/control.rs:915-930`). A pending row at the cap would make T-W1 fail
rather than skip it (G10); no traced path creates one. Retries create new rows
with `attemptCount=0` (`src/admin/control.rs:450`).

**S13. At most one handler executes per activity at any instant.**
**ASSUMED**. It relies on: the local deadline being at or before the DB
expiry (`src/runtime/activity_worker.rs:189-190`, `314-315`, `820-829`);
the process monotonic clock not running slower than the DB clock; the DB
clock not jumping forward; handlers yielding so the dropped future stops; and
no detached tasks inside handlers. Across attempts, activities are
at-least-once: a result committed after the lease was reconciled is rejected
and the activity runs again.

**S14. Activity terminal states.** `succeeded` and `cancelled` are absorbing;
`dead_lettered` can only move to `cancelled`, via T-X2
(`src/store.rs:241-255`). **ENFORCED**.

**S15. Activity/workflow coupling.** `activity.status ∈ {pending, running}` ⇒
its workflow waits on it (`waiting_activity`, or `paused` with the activity
`pending`); `workflow.status=blocked` ⇒ it waits on a `dead_lettered`
activity. Claims require the wait (`src/runtime/activity_worker.rs:240-241`,
`424-436`); success and dead-letter roll back without it (`1176-1178`,
`1227-1241`); pause converts `running` to `pending` (`src/admin/control.rs:888-938`).
**ENFORCED** by construction.

**S16. Activity success is delivered at most once.** Success, attempt close,
event append, and workflow wake are one transaction under the workflow lock,
and the wake clears the wait. **ENFORCED**.

**S17. Topic concurrency cap at claim time.** When T-W1/`claim_one` commits,
`|{running ∧ leaseExpiresAt > now}|` on the topic ≤ `maxConcurrency`. Claims on
a topic serialize on its lock row, and reconcile runs first in the same
transaction. **ENFORCED**, with one exception by one (G7).

### Deduplication and lineage

**S18. Workflow deduplication.** At most one row per `(kind, deduplicationKey)`
(M:33); a duplicate start returns the original without mutating it
(`src/persistence/workflows.rs` `insert_started`). **ENFORCED**.

**S19. One successor per source; one live generation per recovery root.** At
most one row per `restartedFromWorkflowId` (M:34; admin pre-check
`src/admin/control.rs:218-228`). A colliding start returns `Conflict` and
leaves the caller's transaction usable (test
`second_restart_of_a_source_conflicts_and_leaves_the_caller_transaction_usable`). T-X2 locks the original and the newest
generation and restarts only a `failed`/`blocked` newest, so concurrent
recoveries and admin retries leave one live generation. **ENFORCED** (test
`recoverable_start_fences_dead_letter_retry_and_races_to_one_live_generation`).

### Approvals and timers

**S20. An approval resolves at most once, and resolution excludes expiry.**
All exits from `pending` are fenced on `status='pending'` under the workflow
lock; resolve rejects `expiresAt <= now`; expiry requires `expiresAt <= now`.
**ENFORCED** (`src/admin/control.rs:533-556`, `615-629`;
`src/runtime/temporal.rs:101-154`; test
`approval_resolution_and_expiry_have_one_transactional_winner`).

**S21. A pending approval is awaited by its workflow**
(`waiting_approval` or `paused`, `waitReferenceId` = approval, same kind,
version, and command). **ENFORCED** by construction (created with the wait;
every path that ends the wait resolves, expires, or cancels it). If violated,
T-A1 fails the task on every tick (`src/runtime/temporal.rs:120-139`) and the
restart budget runs out.

**S22. A timer fires at most once per command and never while paused.**
**ENFORCED** (`src/runtime/temporal.rs:35-56`, `221-248`; test
`timers_wake_once_at_the_exact_command_and_preserve_pause`).

### Children and cancellation

**S23. A child outcome reaches each waiting parent at most once, and never a
parent that stopped waiting.** The wake is fenced on
`(status, waitKind, waitReferenceId)` and clears the wait
(`src/persistence/workflows.rs:192-199`, `253-268`). **ENFORCED**.

**S24. A child outcome reaches each parent that waits on it when the child
becomes terminal, for every terminal transition except T-X2 supersession.**
Covers the attach race: a parent that attaches to an existing child locks
the child and wakes itself if it is already terminal
(`src/runtime/coordinator.rs:835-873`). **ENFORCED** in that scope; T-X2
violates it (G2).

**S25. Cancellation is atomic for the workflow's own work.** One transaction
cancels the workflow, its pending/running activities (closing attempts), its
pending approvals, and wakes waiting parents (`src/store.rs:503-568`).
`cancel_with_conn` is idempotent on terminal workflows; admin cancel returns
Conflict. **ENFORCED**.

### Schedules

**S26. Each schedule occurrence materializes at most once.** Unique
`(scheduleKey, localOccurrence)` (M:195), the state row lock, and the cursor
fence (`src/runtime/schedule_materializer.rs:116-128`, `193-214`).
**ENFORCED** (test `run_latest_records_backlog_starts_one_and_is_multi_instance_exactly_once`).
Run-now uses `manual:{t}` with `t` strictly increasing per key under the same
lock.

**S27. Between version upgrades, the cursor strictly increases in local time
and every occurrence before it has exactly one run row.** A failed tick rolls
back its rows and cursor together. **ENFORCED**. An upgrade resets the cursor
without run rows for the skipped span (`src/schedule.rs:402-412`; G5).

**S28. Misfire policies.** `Skip`: start iff `dueAt + grace ≥ now`;
`RunLatest`: only the latest runnable occurrence of the whole backlog starts,
earlier ones are `coalesced`; `CatchUp{n}`: only the latest `n` runnable
occurrences start; DST-gap occurrences are `skipped(dst_gap)`; chunking
(10,000) keeps these global (`src/runtime/schedule_materializer.rs:228-322`).
**ENFORCED** (tests in `tests/schedule_materialization.rs` and the unit
test at `src/runtime/schedule_materializer.rs:519-585`).

**S29. Overlap policies for materializer runs.** `SkipIfActive`: no start
while any run's workflow of the key is non-terminal (blocked and paused count
as active); `QueueOne`: at most one `queued` run, promoted only when
`active=0`. Evaluated under the state lock with a snapshot taken after the
lock. **ENFORCED**. T-A9 (run-now) bypasses both overlap and pause (G12).

**S30. Schedule definition pinning.** T-S2 runs only when the persisted
`(version, fingerprint)` equals the local definition; versions never
decrease; same-version drift is rejected (`src/schedule.rs:387-401`).
**ENFORCED**.

### Flows (`src/flow.rs`)

**S31. Replay is structurally deterministic.** Each await at position `i`
replays journal entry `i` only if `(stepKind, stepVersion)` match; otherwise
the flow fails closed. A delivered result must carry
`commandSequence = journal length + 1`. A completed flow must have consumed
the whole journal. **ENFORCED** (`252-282`, `341-349`, `385-459`).

**S32. Flow code between awaits is deterministic in its arguments.** Replay
does not compare payloads or inputs. **ASSUMED**.

**S33. A journaled step never re-executes its activity.** Replayed positions
never create commands (`159-166`); the journal is persisted atomically with
event consumption (S4). Only the first unjournaled step can emit a command,
and its activity is at-least-once. **ENFORCED**.

### Idempotency, limits, fencing by operators

**S34. `operationKey` idempotency.** The engine stores the key and passes it
to the handler (`src/definition.rs:140-142`); it has no uniqueness constraint
and deduplicates nothing. Replacements reuse the key; corrections derive
`durable:activity:{root}:correction:{n}` (`src/admin/control.rs:413-419`);
the flow auto-key `wf:{workflowId}:step:{n}` (`src/flow.rs:284-287`) changes
across restart generations. **ASSUMED** (handler and provider).

**S35. Payload limits** for values built through the typed constructors:
workflow input/state ≤ 256 KiB (`src/store.rs:119-121`,
`src/registry.rs:692-723`, `src/transition.rs:205-216`); activity payload
≤ 256 KiB (`src/transition.rs:38-42`); activity and workflow output ≤ 64 KiB
(`src/registry.rs:338`, `724-726`, `src/transition.rs:146`, `266`); approval
request/decision and temporal event metadata ≤ 16 KiB (`src/registry.rs:714-718`,
`122`, `src/runtime/temporal.rs:191-195`, `src/admin/control.rs:586-590`);
error category ≤ 64 bytes and message ≤ 2 KiB by truncation (`src/error.rs:89-121`);
progress ≤ 100 events per attempt and description ≤ 2 KiB (`src/progress.rs:115`,
`159-163`, M:138-139); keys ≤ 191 characters (`src/store.rs:491-501`,
`src/transition.rs:55-62`). **ENFORCED** in that scope. Not bounded:
`activity_succeeded`/`child_succeeded` metadata (up to 64 KiB output plus
envelope, which exceeds the 16 KiB metadata constant). `ActivityCommand` and
`ChildWorkflowCommand` implement `Deserialize`, so a serde-built command skips
constructor checks; the coordinator does not re-check them (G10).

**S36. Operator pause fences in-flight work.** Pausing a running workflow
invalidates the coordinator's token; pausing while an activity runs closes its
attempt, returns it to `pending`, and invalidates the worker token, with
`maxAttempts+1` so the paused attempt does not consume the budget.
**ENFORCED** (tests `pause_fences_a_workflow_transition_claimed_before_the_operator_action`,
`pausing_the_final_activity_attempt_preserves_one_execution_attempt`).

### Rejected candidates

- **"Cancellation eventually stops all descendants."** Rejected.
  `cancel_locked_workflow` cancels only the workflow's own activities and
  approvals (`src/store.rs:516-517`). Child workflows keep running;
  `parentWorkflowId` is never read. A later child outcome is dropped because
  the parent no longer waits (S23). The same holds for restart supersession.
- **"Child results are delivered exactly once."** Holds as S23 + S24 only;
  T-X2 breaks the at-least-once half (G2).
- **"Operation keys are idempotent."** The engine does not enforce this (S34).

---

## 4. Liveness properties

Global fairness assumptions used below:
- **F1** The DB is eventually available, and DB time advances.
- **F2** At least one runtime stays up and has not self-cancelled (G1, G3
  threaten this).
- **F3** The runtime serving an entity has its exact `(kind, version)` and
  topic registered (claims filter on local definitions).
- **F4** Handlers and `step` return, or yield to cancellation, in finite time.

**L1. A ready workflow with `availableAt ≤ now` is eventually claimed.**
Needs F1–F3 and a coordinator that polls. Order is `(availableAt, id)`;
continuation streaks yield after 16 (`src/runtime/coordinator.rs:513-520`).
**ENFORCED**.

**L2. An expired workflow lease is eventually recovered.** Needs F1 and any
coordinator that polls (recovery runs before the definition filter,
`src/runtime/coordinator.rs:242-327`). **ENFORCED**.

**L3. A claimed workflow eventually leaves `running`.** Needs F4 for `step`,
or L2 after the lease expires. **ASSUMED** (G3: no timeout around `step`).

**L4. A pending activity whose workflow waits on it is eventually claimed.**
Needs F1–F3, `availableAt ≤ now`, free topic capacity, and a T-W1 sweep that
acquires every registered topic row (all-or-nothing,
`src/runtime/activity_worker.rs:310-312`). System-wide progress holds because a
sweep that skips a lock implies another sweep holds it. **ENFORCED**.

**L5. An expired activity lease is eventually reconciled.** Needs a dispatcher
for that topic with local capacity > 0 (T-W1 returns early otherwise,
`src/runtime/activity_worker.rs:285-287`). **ENFORCED**.

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
terminal** (same transaction), except under G2. Not guaranteed if the child
never terminates (G8). **ENFORCED**.

**L12. Cancellation, pause, or restart stops an in-flight activity handler
within `heartbeat interval + min(shutdown_grace, remaining lease)`**, through
the heartbeat fence (`src/runtime/activity_worker.rs:658-687`). Cooperative
(F4). **ENFORCED**.

**L13. Graceful shutdown finishes within `deadline + forced_shutdown_timeout`**
(`src/runtime/supervisor.rs:259-301`). Leases left behind are recovered by
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
  the read and the lock (G7). Under REPEATABLE READ (a caller's `*_with_conn`
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
  (`src/persistence/mod.rs:62-69`). The process wall clock is not used
  (`persistence::now_millis`, `src/persistence/mod.rs:58-60`, is only public
  API). Tests confirm session time is honoured (`tests/database_time.rs`).
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
| activity execution | lease expires → reconcile consumes the attempt (may dead-letter and block) |
| claims returned by T-W1 but not yet spawned (dispatcher error at `src/runtime/supervisor.rs:1120-1129`) | same as an activity execution: attempt consumed without running |
| T-W2 future dropped after its COMMIT was sent | lease extended with no executor (liveness delay only) |
| any transaction | rolled back, or committed with the error lost |

- Panics: a panic in `step` fails the coordinator task; a panic in a handler
  is collected and fails the dispatcher task. Both count toward the restart
  budget (`src/runtime/supervisor.rs:853-866`); the runtime is fail-stop by
  design (test `panicking_worker_is_reported_restarted_and_recovered_to_dead_letter`).

### 5.4 Shutdown

- Graceful (`RuntimeHandle::shutdown`): loops stop at their next check; an
  activation in progress runs to completion; executions get the cancellation
  token and `shutdown_grace`. A handler that finishes in the grace window
  records its real outcome. Otherwise the outcome is `Retryable(cancelled)`,
  which dead-letters the activity if it was the last attempt
  (`src/runtime/activity_worker.rs:630-643`, `1045-1046`).
- Forced (deadline elapsed): executions stop heartbeating and return without
  T-W3 (`605-608`); leases expire and are recovered elsewhere.
- Abort (forced timeout elapsed, or `RuntimeHandle` dropped): the supervisor
  task is aborted, which drops all tasks at their current await
  (`src/runtime/supervisor.rs:242-248`, `279-281`).

### 5.5 User code contracts (ASSUMED)

- `step` is deterministic in `(input, state, event)` and side-effect free (S3).
- `start_occurrence` starts exactly one workflow on the given connection with
  `schedule_run_id` set, and does not commit it separately.
- `cancel_with_conn` and `start_with_conn` run inside the caller's
  transaction; the caller commits.
- Handlers honour the cancellation token and do not spawn detached work.

---

## 6. Suspected gaps

Status: G4 is closed for library transactions (P4, READ COMMITTED).
G1, G2, G3, G6, G8, G10 and G11, and the model findings N1 and N2
(`spec/README.md`), are confirmed by ignored tests in `tests/gaps.rs`.

Each item below was traced from the code; the status line above records
which ones a test has since confirmed or closed.

**G1. Benign races and transient DB errors use up a restart budget that never
resets, which stops the runtime.** `activate_claim_inner` returns
`FencedWrite` and database errors from T-C2 and T-C3
(`src/runtime/coordinator.rs:217`, `410-430`); the coordinator task
propagates them (`src/runtime/supervisor.rs:935`); the restart counter is
cumulative (`853-866`). Interleaving: coordinator claims W (T-C1); operator
pauses or cancels W (T-A2/T-A4; both accept `running`,
`src/admin/control.rs:69`, `158`); T-C2 misses the fence → task error. Nine
such events in one process lifetime (default budget 8) cancel the whole
runtime. Other triggers: lease-recovery races with slow steps, InnoDB
deadlocks (G9), duplicate-key aborts (G4), and claim-batch errors in the
dispatcher (`src/runtime/supervisor.rs:1104-1106`). The test
`pause_fences_a_workflow_transition_claimed_before_the_operator_action`
confirms that `activate_claim` returns `Err(FencedWrite)`.

**G2. Recoverable start strands parents of a superseded blocked child.**
T-X2 moves a `blocked` newest generation to `cancelled` without calling
`wake_waiting_parents_on_child_terminal` (`src/store.rs:256-293`). Interleaving:
parent P runs `child_with_key(C, "k")` → child row C (kind CK, key k) →
P `waiting_child` on C; C's activity dead-letters → C `blocked`; the
application calls `start_or_restart_recoverable(CK, key "k")` → C
`cancelled`, successor C' with no dedup key and no parent link. P waits on C
forever; resuming P after a pause fails with Conflict
(`src/admin/control.rs:988-991`). A later `child_with_key(..., "k")` resolves
to C and fails at once.

**G3. A poison-pill `step` stops runtimes.** Lease recovery does not count
activation attempts (`src/runtime/coordinator.rs:296-321`). A `step` that
panics fails the coordinator task, the row is recovered after 30 s, claimed
again, and panics again; each runtime that claims it uses up its restart
budget. A `step` that never returns blocks that runtime's only coordinator
loop (`src/runtime/supervisor.rs:931-938`; no timeout around
`src/runtime/coordinator.rs:152-164`), and after lease expiry the next
runtime that claims it also blocks.

**G4. Stale snapshot for `sequence` causes spurious aborts.** **Closed** for
library transactions by READ COMMITTED (§5.1): `next_event_sequence` after the
row lock sees every committed append (test
`g4_child_completion_sees_a_parent_pause_committed_after_its_first_read`, which
fails with the duplicate key under REPEATABLE READ). It remains possible in a
caller's REPEATABLE READ `*_with_conn` transaction. Original interleaving:
child C's T-C2 `Complete` locks C and appends history, so its first consistent
read (`next_event_sequence(C)`, `src/persistence/events.rs:29-33`) fixes
snapshot S. Then an operator pauses parent P and commits (history event
`k+1` on P). Then C's wake locks P (current read: paused, still waiting on C)
and computes `next_event_sequence(P)` from S = `k+1` → duplicate
`uq_durable_workflow_event_sequence` → T-C2 rolls back → G1. C runs `step`
again after lease expiry. Safety holds through the unique key. The same
pattern applies to any transaction whose first consistent read happens before
it locks the workflow it appends to.

**G5. A schedule upgrade during a DST fall-back hour can re-target an
already-materialized occurrence.** T-S1 `Upgraded` resets the cursor to
`next_after(now)` computed on naive local time (`src/schedule.rs:93-111`,
`359`, `402-412`). If `now` is in the second pass of a repeated hour, the next
local occurrence (e.g. `01:30`) may already have a run row from the first
pass. Every T-S2 then fails on `uq_durable_schedule_run_occurrence`
(`src/runtime/schedule_materializer.rs:379-392`), a retryable database error →
task error each tick (`src/runtime/supervisor.rs:1024-1026`) → G1. The cursor
never advances because each tick rolls back. Separately, every upgrade drops
the unmaterialized span between the old cursor and `now` without run rows.

**G6. A concurrent child dedup race skips the version check.** `insert_child`
checks the version only on its consistent-read pre-check
(`src/store.rs:423-438`); the upsert conflict path returns the winner's id
without comparing versions (`src/persistence/workflows.rs:43-70`). Two
parents that start key k at versions 1 and 2 at the same moment can both wait
on the v1 child. The v2 parent's flow replay then fails closed
(`src/flow.rs:272-277`) and the parent fails after its activation attempts.

**G7. The topic cap can be exceeded by one through heartbeat revival.**
Interleaving: activity A's lease expires in DB time while its worker gives up
locally and drops a heartbeat whose COMMIT was already sent. T-W1 starts its
snapshot with the reconcile candidate read (A expired). The heartbeat commits
(`leaseExpiresAt = now+L`; no expiry check, `src/runtime/activity_worker.rs:797-804`).
Reconcile relocks A (current read: live) and skips it (`878-894`). The
`in_flight` count still uses the snapshot (`319-325`) and misses A. T-W1
claims up to the cap, so live leases = cap + 1. Under S13's clock assumption
A has no executor, so real concurrency stays within the cap; A is reconciled
later and loses an attempt without running.

**G8. Domain keys can form wait cycles.** `child_with_key` can resolve to the
calling workflow or an ancestor (same kind and key). `commit_child` then
waits on a non-terminal row (`src/runtime/coordinator.rs:807-873`). There is
no cycle detection. Liveness only.

**G9. Lock-order inversion between a child's terminal commit and a parent
attaching to it.** A child's terminal transaction locks the child, then scans
waiting parents `FOR UPDATE` (`src/persistence/workflows.rs:156-167`). A
parent's `RunChild` commit on an existing child locks the parent, then the
child (`src/runtime/coordinator.rs:807-840`). With a domain-keyed child that
completes while a parent attaches, InnoDB can deadlock and abort one side →
G1, and the aborted step runs again after lease expiry. Safety holds.

**G10. One invalid row stops all activity claims.** `claim_locked_candidate`
returns an error, not a skip, for a missing definition, an attempt cap
overrun, or `leaseDuration <= timeout` (`src/runtime/activity_worker.rs:451-472`).
The error aborts the whole T-W1 across all topics and ends the dispatcher
task (G1); the same row is selected again next sweep. No traced engine path
creates such a row, but a `Deserialize`-built `ActivityCommand` or a manual
edit can.

**G11. Parent cancellation does not reach descendants** (specification gap).
See the rejected candidate in §3. Child workflows and their external side
effects continue after the parent is cancelled or superseded.

**G12. Run-now ignores the schedule pause and the overlap policy**
(`src/admin/control.rs:742-820`). This may be intended as an operator
override; the tests do not cover a paused or active schedule. Intent is
UNCLEAR.

---

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
- `∀a: act[a].status = running ⇔ attempt[(a, act[a].attemptCount)].open` (S11)
- `∀a: act[a].attemptCount ≤ act[a].maxAttempts` (S12)
- `∀a: act[a].status ∈ {pending, running} ⇒ wf[act[a].wf].waitRef = a` (S15)
- `∀w: |{e ∈ events[w] : e.deliverySeq > wf[w].delivered}| ≤ 1` (S5)
- `∀w: wf[w].status = ready ⇒ ∃e ∈ events[w]: e.deliverySeq = wf[w].delivered + 1` (S5)
- terminal-absorbing as an action property (S6, S14, S20)
- `∀t: |{a : act[a].topic = t ∧ status = running ∧ leaseExp > now}| ≤ topicCap[t]`
  checked only at T-W1 commits (S17; G7 shows it can fail between them)
- at most one committed `schedRun` per `(key, localKey)` (S26)
