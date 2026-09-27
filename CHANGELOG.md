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

### Changed

Compared with the production-internal version it was extracted from:

- `WorkflowStatus` and `ActivityStatus` are `#[non_exhaustive]`. A
  downstream exhaustive `match` on either needs a wildcard arm (breaking),
  so new statuses can ship in a minor release.
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

### Known issues

The confirmed protocol gaps G11 and N2 have
ignored reproduction tests in `durable-workflows/tests/gaps.rs` and reproduce
on both backends. See the "Known issues" section of the README and
`docs/INVARIANTS.md` §6.
