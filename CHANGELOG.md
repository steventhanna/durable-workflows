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

### Changed

Compared with the production-internal version it was extracted from:

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
- `StartOptions` with both a deduplication key and a restart source
  (`restarted_from_workflow_id`) is rejected with `InvalidDefinition`.
- `ScheduleHandler::start_occurrence` takes `&mut DurableConnection`. It must
  return every error and must not continue after a failed statement
  (Postgres aborts the transaction); use `connection.transaction(..)` for a
  savepoint.

### Removed

- `persistence::connection_last_insert_id`.

### Known issues

The confirmed protocol gaps G1, G2, G3, G6, G8, G10, G11, N1 and N2 have
ignored reproduction tests in `durable-workflows/tests/gaps.rs` and reproduce
on both backends. See the "Known issues" section of the README and
`docs/INVARIANTS.md` §6.
