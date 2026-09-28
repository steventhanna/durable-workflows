# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Before 1.0, minor versions may change the storage schema.

## [Unreleased]

Initial public release, to be tagged 0.1.0, of `durable-workflows` and
`durable-workflows-macros`, extracted from an engine that has run in
production on MySQL since August 2026.

### Added

- Journaled workflows written as `async fn` with `#[durable_flow]`, and
  derive macros for workflows, activities and schedules (`DurableWorkflow`,
  `DurableActivity`, `DurableSchedule`).
- Activities with per-activity attempts, timeouts, leases with fencing
  tokens, and fixed or exponential backoff with jitter; retryable and
  permanent errors are distinct.
- Topics with a per-topic concurrency cap shared by every process.
- Child workflows, timers and approvals with expiry.
- Time-zone aware cron schedules with misfire (skip / catch up) and overlap
  policies.
- Idempotent starts with deduplication keys, activity operation keys, and
  recoverable restarts of failed or blocked workflows.
- Admin query, control (pause, resume, cancel, restart, retry, approvals,
  schedule pause/resume/run-now) and metrics
  services; a health scanner with a pluggable alert sink; readiness
  reporting; graceful shutdown.
- `DurableError::is_transient`: whether the database aborted the
  transaction with a deadlock, serialization failure or lock wait timeout
  that a retry of the whole transaction can clear.
- Two database backends, one per build, selected with mutually exclusive
  cargo features: `postgres` (default; PostgreSQL 14+) and `mysql`
  (MySQL 8.0.16+ and 8.4, InnoDB). CI runs the full suite on MySQL 8.0 and
  8.4 and on Postgres 14 and 17.
- `DurableConnection` and `DurablePool` aliases for the backend's
  diesel-async connection and bb8 pool.
- A `migrations` module: the baseline SQL for each backend ships in the
  crate, as `MIGRATIONS` (Diesel `EmbeddedMigrations`), `BASELINE_UP_SQL`,
  and `apply(database_url)`, which records versions in
  `__diesel_schema_migrations`.
- A test-only `fake-clock` feature that lets tests override the database
  clock. Never enable it in production.
- `docs/INVARIANTS.md` (protocol invariants), a Quint model of the core
  protocol in `spec/`, and reproduction tests for the known gaps in
  `tests/gaps.rs`.
- `CoordinatorConfig::step_timeout` (default 30 s, the default lease;
  must be non-zero): the longest a workflow `step` may run before the
  activation fails.
- `RuntimeConfig::restart_window` (default 10 minutes; must be non-zero):
  `max_task_restarts` now counts restarts of one task within this window,
  not over the process lifetime.
- `MAX_RETRY_DELAY_SECS` (`i64::MAX / 2_000` seconds): the largest retry
  delay `RetryPolicy::fixed` and `RetryPolicy::exponential` accept.
- Visibility for benign activation outcomes (G1): `activate_one` counts each
  activation it skips by `observability::BenignActivationKind` (`FenceMiss`,
  `Transient`) in process-local `observability::ActivationCounters`
  (`get(kind)`), read through `RuntimeHandle::activation_counters` or
  `WorkflowCoordinator::activation_counters`. More than
  `RuntimeConfig::max_transient_activation_errors` (default 10; must be
  non-zero) transient errors within
  `RuntimeConfig::transient_activation_error_window` (default 5 minutes; must
  be non-zero) add `HealthAlert::TransientActivationErrors` to the next health
  report and its `HealthAlertSink` call. `HealthAlert` is now
  `#[non_exhaustive]`.

### Changed

Compared with the production-internal version it was extracted from:

- The test-only `fake-clock` feature is a compile error in a build without
  debug assertions (the `release` profile), so it cannot reach a release
  binary. Tests build in the dev/test profiles and are not affected.

- Removed the public host-clock function `persistence::now_millis()`.
  Persisted times and due/expiry comparisons use the database clock
  (`persistence::database_now_millis(&mut connection)`); a caller that
  writes a time into a durable table must read that clock, not the host's.
- New persisted activity status `cancelling`: an application cancel or
  operator pause of a running activity moves it to `cancelling` instead of
  `cancelled`/`pending`; it keeps its lease, open attempt and topic slot
  until its handler stops or its lease expires, then settles to `cancelled`
  (terminal workflow) or `pending` (paused). Every process must run the new
  code before such a row can appear: an older worker neither renews nor
  settles it (it recovers only through lease expiry on a new process).
  `ActivityWorker::heartbeat` returns `FencedWrite` for a revoked claim.

- `WorkflowStatus` and `ActivityStatus` are `#[non_exhaustive]`. A
  downstream exhaustive `match` on either needs a wildcard arm (breaking),
  so new statuses can ship in a minor release.
- More public enums are `#[non_exhaustive]` (breaking for a downstream
  exhaustive `match`; add a `_` arm): the errors `DurableError`,
  `ActivityError`, `WfError`, `WorkflowDispatchError` and
  `ActivityDispatchError`, and `WorkflowTransition`, `StoredTransition`,
  `BackoffPolicy`, `MisfirePolicy`, `OverlapPolicy`,
  `LocalTimeDisposition`, `ScheduleStateReconcileOutcome`,
  `ProgressSeverity`, `ProgressReportOutcome`, `admin::ScheduleHealthIssue`
  and `admin::TimelineEntry`. Building their values is unchanged.
  `WorkflowEvent` and `BackendKind` stay exhaustive: a new event or backend
  is meant to break every `match` that must handle it.
- `RuntimeConfig`, `CoordinatorConfig`, `WorkerConfig` and
  `observability::HealthScannerConfig` are `#[non_exhaustive]` and have a
  `with_<field>` setter for every field, so fields can be added in a minor
  release. A struct expression outside the crate, including
  `RuntimeConfig { idle_delay, ..RuntimeConfig::default() }`, no longer
  compiles (E0639). Migration: start from `default()` and chain setters,
  `RuntimeConfig::default().with_idle_delay(Duration::from_millis(150))`.
  Fields stay public for reading and assignment. Bounds are still checked
  where the config is used (`DurableRuntime::new`,
  `WorkflowCoordinator::new`, `ActivityWorker::new`, `HealthScanner::new`).
- The remaining string-typed status and kind columns of the public row
  types are enums, like `WorkflowStatus`: `WorkflowRow::wait_kind` and
  `NewWorkflowRow::wait_kind` are `Option<persistence::WaitKind>`,
  `ApprovalRow::status` and `NewApprovalRow::status` are
  `persistence::ApprovalStatus`, `ScheduleRunRow::status` and
  `NewScheduleRunRow::status` are `persistence::ScheduleRunStatus`, and
  `ActivityAttemptRow::outcome` and `NewActivityAttemptRow::outcome` are
  `Option<persistence::AttemptOutcome>` (breaking for code that reads or
  builds these rows with strings; `as_str()` gives the old text). All four
  are `#[non_exhaustive]`. The persisted text and the admin view types are
  unchanged. Loading a row whose column holds a value outside the enum is
  an `InvalidState` error instead of a string the engine never wrote.
- Every transaction the library opens runs at READ COMMITTED on both
  backends. This closes gap G4 for library transactions. MySQL with binary
  logging needs `binlog_format=ROW` (the 8.x default). The `*_with_conn`
  methods run in the caller's transaction at the caller's isolation level.
- The schema is one baseline migration per backend with snake_case column
  names.
- Keys (deduplication, topic, schedule, operation) compare by exact bytes on
  both backends: MySQL tables use `utf8mb4_bin`, and topic metrics no longer
  merge topics whose names differ only in case or accents.
- A start that collides on the restart key (`uq_durable_workflow_restart`)
  returns `DurableError::Conflict` and leaves the caller's transaction
  usable (it returned `InvalidState`).
- Cancelling a workflow cancels every generation of the child workflows it
  owns (the child and its recovery or restart successors), and theirs, in
  the same transaction (G11): `DurableStore::cancel_with_conn`,
  `AdminControlService::cancel_workflow`, and an admin restart that
  supersedes a paused or blocked source. A child is owned when the flow
  started it with `WfCtx::child` (key `child:{parent}:{command}`); a child
  started with `WfCtx::child_with_key` may be shared and keeps running. The
  child's history records `workflow_cancelled` with the reason
  `parent workflow {id} cancelled: {reason}`, and its running activities are
  revoked to `cancelling` like any cancel. The cascade locks parent before
  child, so it can deadlock with a child that finishes at the same moment;
  the database aborts one side, and the error `is_transient()`: retry it.
- `StartOptions::restarted_from_workflow_id` and `StartOptions::root_workflow_id`
  are crate-private (N3): only a recoverable start and the admin restart set
  a restart source, so an application can no longer start a "successor" of a
  live workflow or take a failed workflow's restart key. Use
  `start_or_restart_recoverable` or `AdminControlService::restart_workflow`.
  Struct-update syntax (`StartOptions { .., ..StartOptions::default() }`) no
  longer compiles outside the crate; use the new builders
  `StartOptions::with_schedule_run_id` and `StartOptions::with_available_at`.
- `ScheduleHandler::start_occurrence` takes `&mut DurableConnection`. It must
  return every error and must not continue after a failed statement
  (Postgres aborts the transaction); use `connection.transaction(..)` for a
  savepoint.
- `RetryPolicy::fixed` rejects a `delay_secs`, and
  `RetryPolicy::exponential` a `max_secs`, above `MAX_RETRY_DELAY_SECS`
  with `InvalidDefinition`, so a delay with +100% jitter fits the database's
  millisecond range. `#[derive(DurableActivity)]` rejects the same values at
  compile time.
- The trace-checking interface is version 5 (`spec/README.md`, "Changes in
  v5"): `TW1_Claim` records the rows the claim quarantined, and `TW1_Error`
  is gone; `TX2_RecoverableStart`'s newest generation follows the restart
  chain and re-points waiting parents (G2, N1), and `TC2_RunChild` attaches
  to the newest generation of a keyed child (D4) and never to the caller or
  an ancestor (G8).
- Breaking: a `WorkflowCoordinator` holds at most one outstanding workflow
  claim, enforced by the type system. `claim_one` and `activate_one` take
  `&mut self`, and `claim_one` returns a `WorkflowClaim<'_, C>` that borrows
  the coordinator until it is activated or dropped, so a second `claim_one`
  while a claim is alive does not compile. `WorkflowClaim::activate(self)`
  replaces `WorkflowCoordinator::activate_claim(claim)`. This matches the
  model's one-claim-per-runtime rule (`TC1_Claim`). Use one coordinator per
  worker id; the trace checker maps a worker id to one model runtime.
- `AdminControlService::run_schedule_now` can return `Conflict` when the
  schedule's overlap policy is `SkipIfActive` or `QueueOne` and a run of the
  schedule is active (G12). It still ignores the pause.

### Removed

- `persistence::connection_last_insert_id`.

### Fixed

- G1: a coordinator commit that loses its fence (an operator paused or
  cancelled the workflow during its step, or another runtime recovered it)
  or hits a transient database error (deadlock, serialization failure, lock
  wait timeout) is logged by `activate_one` and no longer ends the
  coordinator task or uses up the runtime's restart budget.
  `WorkflowClaim::activate` still returns the error. The
  activity dispatcher retries after a transient `claim_batch` error, and the
  restart budget counts within `RuntimeConfig::restart_window`.
- G3: a `step` that panics or exceeds `step_timeout` is an activation
  failure, so the workflow fails after its activation attempts instead of
  stopping every runtime that claims it. A `panic = "abort"` build still
  aborts.
- G9: a parent that attaches to an existing child workflow locks the child
  before itself, the order the child's completion uses, so the two no
  longer deadlock.
- N4: a stale coordinator committing `RunActivity` or `WaitForApproval` for
  a workflow another runtime already recovered and advanced now gets
  `FencedWrite` instead of a duplicate-key database error.
- G10: one activity row the claim cannot run no longer makes
  `claim_batch`/`claim_one` fail on every topic and end the dispatcher. A
  row without a local definition is skipped; a pending row past its attempt
  cap or with invalid timeout/lease bounds is quarantined in the same
  transaction: dead-lettered with `last_error_category = "invalid_row"`,
  history `activity_quarantined`, and its workflow blocked. An operator
  `retry_activity` replaces it with a row built from the registered
  definition.
- Retry jitter no longer wraps: a jittered delay above `u64::MAX` seconds
  (reachable through a stored or `from_validated` policy) saturates at
  `u64::MAX` instead of wrapping to a short delay, so delays stay within the
  jitter bounds and never shrink as attempts grow.
- G2: `start_or_restart_recoverable` on a blocked child no longer strands
  the parents waiting on it. When the successor runs the same workflow
  version, each waiting parent (`waiting_child`, or `paused` on the child)
  now waits on the successor (history `child_wait_reattached` with
  `{from, to}`) and receives its outcome; otherwise the parents are woken
  with `child_failed` (category `child_superseded`), as after an operator
  restart.
- N1: `start_or_restart_recoverable` finds the newest generation by
  following `restarted_from_workflow_id` from the keyed row, so a recovery
  of a keyed child no longer supersedes or returns a different child of the
  same kind in the same tree.
- D4: `child_with_key` on a key whose row was superseded by a recoverable
  start attaches to the newest generation instead of the cancelled row.
- G6: a child deduplication hit found by the insert's conflict (a concurrent
  parent inserted the key first) is version-checked like one found by the
  pre-read, so a parent can no longer wait on a child of another definition
  version; the commit fails with `DefinitionMismatch`, an activation failure.
- G8: `child_with_key` resolving to the calling workflow or one of its
  ancestors is an activation failure (`InvalidDefinition`: "child key {k}
  resolves to workflow {id}, which is the caller or an ancestor") instead of
  a wait that never ends. Wait cycles through keyed workflows that are not
  ancestors of each other are still not detected.
- N2: an application cancel or operator pause no longer frees the topic
  concurrency slot while the revoked handler still runs, so a cap-1 topic
  can no longer run two handlers. The row is `cancelling` until the worker's
  heartbeat learns of the revoke, cancels the handler and settles it (or the
  lease expires); a pause then resume does not claim the next attempt until
  the old one settles.
- N5: an activity whose handler ignored cancellation past its timeout and
  `shutdown_grace` could not be finished while the handler was inside a
  progress report: the report's transaction held the activity row, the
  finish waited on it, and nothing polled the handler again. The row stayed
  `running` until its lease expired (MySQL lock wait timeout) or the
  executor hung (Postgres). The executor now drops the handler future, which
  rolls back its transaction, before it finishes the attempt.
- G5: `ScheduleCalendar::next_after` is strictly later than its argument as an
  instant. A schedule upgraded (or first reconciled) in the second pass of a
  daylight-saving fall-back hour no longer points its cursor at an occurrence
  the first pass already ran, which made every later tick fail on the
  occurrence's unique key. An occurrence in a repeated hour fires once, at
  the earlier pass. A version upgrade sets the cursor to the new calendar's
  first occurrence after now that is strictly after the last materialized
  occurrence, so it never re-targets an occurrence that already ran, also
  when the upgrade changes the timezone; a new earlier slot still runs (daily
  08:00 changed to 07:00 at 05:00 runs today at 07:00).
- G12: admin run-now respects the schedule's overlap policy (see Changed).
- G11: cancelling or superseding a parent no longer leaves the children it
  owns running (see Changed).
