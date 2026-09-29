# Design: MySQL 8 + Postgres 14+ as equal backends

Status: accepted (2026-09-24). Owner decisions: exact-byte key comparison on
every backend; snake_case columns; MySQL + Postgres only (SQLite later,
single-process).

`CRATE` = `durable-workflows/` (the engine crate). Line numbers refer to the
code as copied from the production app, before phase P1.

Facts verified in diesel-async 0.9.2 that this design depends on:

- `AsyncConnection::transaction` and
  `AsyncPgConnection::build_transaction().read_committed().run(..)` take the
  same `AsyncFnOnce(&mut Conn) -> Result<R, E>` closure (`src/lib.rs` `AsyncConnection::transaction`,
  `src/pg/transaction_builder.rs` `TransactionBuilder::run`).
- The MySQL connection sets `client_found_rows(true)` (`src/mysql/mod.rs` `establish_connection_inner`),
  so affected-row counts are *matched* rows on both backends. Every
  `changed == 1` fence keeps its meaning on Postgres.
- `set_instrumentation` exists on both connections.
- diesel-async has no `MultiConnection`. Its `mysql`, `postgres`, `bb8`,
  `migrations`, `async-connection-wrapper` features are independent.

## 1. Backend abstraction: one backend per build

Options considered:

- **(A) Engine generic over a sealed `Backend` trait.** ~200 diesel queries in
  13 files would need `QueryFragment<DB>`, `FromSqlRow`, `LoadQuery` + `Send`
  bounds per query; boxed queries with `.or_filter` need
  `BoxableExpression<T, DB>`. diesel-async only satisfies the `Send` future
  bounds for concrete connections. Containing the bounds means per-backend
  concrete query bodies, i.e. (B) in disguise. Rejected.
- **(B) Runtime enum dispatch.** A MultiConnection-style synthetic backend is
  ~1,500 lines of glue and forbids exactly the backend-specific DSL we need
  (`on_conflict`, `returning`, `for_update`). A macro that expands each body
  per variant compiles every query twice, forces both drivers on every user,
  and makes every engine file macro-shaped. Rejected.
- **(C) Mutually exclusive cargo features `mysql` / `postgres`.** Chosen.

Why (C): the engine is ~90% diesel queries and (C) compiles them once against
a concrete backend. The public API stays non-generic: `DurableConnection` *is*
the user's `AsyncPgConnection` / `AsyncMysqlConnection`, so `*_with_conn` and
`ScheduleHandler::start_occurrence` take the user's own connection with no
adapter. The seam is 8 dialect functions + 2 type aliases, and it is also the
door for a later `sqlite` feature. One driver, one monomorphization.

Cost: the features are not additive. Two crates in one build that pick
different backends fail with a `compile_error!` naming the fix.

```toml
[features]
default = ["postgres"]
mysql    = ["diesel/mysql",    "diesel-async/mysql"]
postgres = ["diesel/postgres", "diesel-async/postgres"]
fake-clock = []          # test-only DB clock override; never enable in production

[package.metadata.docs.rs]
no-default-features = true
features = ["postgres"]
```

`CRATE/src/dialect/mod.rs`:

```rust
#[cfg(all(feature = "mysql", feature = "postgres"))]
compile_error!("durable-workflows: enable exactly one of `mysql` or `postgres` (use `default-features = false`)");
#[cfg(not(any(feature = "mysql", feature = "postgres")))]
compile_error!("durable-workflows: enable the `mysql` or `postgres` feature");
```

## 2. Public API after the change

```rust
// CRATE/src/lib.rs
#[cfg(feature = "mysql")]    pub type DurableConnection = diesel_async::AsyncMysqlConnection;
#[cfg(feature = "postgres")] pub type DurableConnection = diesel_async::AsyncPgConnection;
pub type DurablePool = diesel_async::pooled_connection::bb8::Pool<DurableConnection>;

#[cfg(feature = "mysql")]    pub(crate) type Db = diesel::mysql::Mysql;
#[cfg(feature = "postgres")] pub(crate) type Db = diesel::pg::Pg;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind { Mysql, Postgres }
pub const BACKEND: BackendKind;            // cfg'd constant

pub mod schema;                            // public: users join against these tables
#[doc(hidden)] pub mod persistence;        // row structs + database_now_millis; unstable
pub mod migrations;                        // see §4
mod dialect;                               // private
```

`DurableStore`, `DurableRuntime<C>`, `AdminQueryService`,
`AdminControlService<C>`, `ReadinessReport::query`,
`TopicRegistry::seed_locks`, `persistence::*` keep their shapes with
`AsyncMysqlConnection` replaced by `DurableConnection`.

`ScheduleHandler::start_occurrence(context, connection: &mut DurableConnection,
schedule_run_id, scheduled_for)`. Trait docs gain: the handler must return
every error and must not continue after a failed statement; to recover from a
failed statement it wraps that step in `connection.transaction(..)`
(a savepoint).

Contract on every `_with_conn`: runs inside the caller's transaction at the
caller's isolation level. Correct under READ COMMITTED on both backends and
under REPEATABLE READ on MySQL. On Postgres under REPEATABLE READ or
SERIALIZABLE it may fail with a serialization error or `Conflict`; the caller
retries the whole transaction.

Behavior changes:

- A start that collides on `uq_durable_workflow_restart` returns
  `DurableError::Conflict` on both backends (MySQL returned `InvalidState`,
  `persistence/workflows.rs` `insert_started`).
- `StartOptions` cannot carry both a deduplication key and a restart
  source: both live in one private enum (`store.rs` `StartLineage`). This
  replaced a runtime `InvalidDefinition` check in `validate_options`.
- `persistence::connection_last_insert_id` is deleted.
- `TopicLockRow.max_concurrency` / `NewTopicLockRow.max_concurrency` become
  `i32`; `TopicMetrics.max_concurrency` stays `u32`.

### The dialect seam (`CRATE/src/dialect/{mod,mysql,postgres}.rs`)

Named persistence operations with concrete row types, never generic query
combinators; that keeps diesel bounds out of the seam.

```rust
pub(crate) enum WorkflowInsert { Inserted(i64), DeduplicationConflict }

/// Library-owned transaction, pinned to READ COMMITTED on both backends.
/// Copy the closure bounds from diesel-async-0.9.2/src/lib.rs `AsyncConnection::transaction`. Its
/// `AsyncFunc` helper trait is private, so the crate keeps a local copy
/// (`dialect::TransactionCallback`) with the same blanket impl.
pub(crate) async fn transaction<R, E, F>(conn: &mut DurableConnection, callback: F) -> Result<R, E>;

pub(crate) async fn now_millis(conn: &mut DurableConnection) -> Result<i64, DurableError>;
pub(crate) async fn insert_workflow(conn: &mut DurableConnection, row: NewWorkflowRow) -> Result<WorkflowInsert, DurableError>;
pub(crate) async fn insert_activity(conn: &mut DurableConnection, row: NewActivityRow) -> Result<i64, DurableError>;
pub(crate) async fn insert_approval(conn: &mut DurableConnection, row: NewApprovalRow) -> Result<i64, DurableError>;
pub(crate) async fn insert_schedule_run(conn: &mut DurableConnection, row: NewScheduleRunRow) -> Result<i64, DurableError>;
pub(crate) async fn insert_schedule_state_if_absent(conn: &mut DurableConnection, row: NewScheduleStateRow) -> Result<bool, DurableError>;
pub(crate) async fn insert_topic_locks_if_absent(conn: &mut DurableConnection, rows: &[NewTopicLockRow]) -> Result<(), DurableError>;
// shared, in mod.rs:
pub(crate) fn is_unique_violation(error: &diesel::result::Error) -> bool;
```

## 3. Per-backend SQL

### (a) New-row ids — 5 sites

`runtime/coordinator.rs` `commit_wait_transition` (approval), `commit_activity` (activity),
`admin/control.rs` `AdminControlService::retry` (replacement activity), `run_schedule_now` (manual
schedule run), `runtime/schedule_materializer.rs` `materialize_occurrence` (schedule run).
Each becomes `dialect::insert_<table>(connection, row)`.

- MySQL: `insert_into(t).values(row).execute(c)` then
  `select(last_insert_id()).get_result::<i64>(c)` (the `define_sql_function!`
  from `workflows.rs` `last_insert_id` moves into `dialect/mysql.rs`).
- Postgres: `insert_into(t).values(row).returning(t::id).get_result::<i64>(c)`.

### (b) Insert-or-find workflow — `persistence/workflows.rs` `insert_started`

Defined behavior on both backends:

- Dedup hit on `uq_durable_workflow_deduplication (kind, deduplication_key)`
  → `(existing_id, inserted=false)`, existing row untouched (S18).
- Collision on `uq_durable_workflow_restart` →
  `Err(Conflict("workflow {id} already has a successor"))` (S19); the caller's
  transaction stays usable.
- Disjoint by construction: a start's `StartLineage` holds one key or the
  other, and
  `insert_child` never sets a restart id.

```
match dialect::insert_workflow(conn, row).await? {
    Inserted(id) => { append `started` event (seq 1, delivery 1); Ok((id, true)) }
    DeduplicationConflict => match find_by_deduplication_key_for_update(conn, kind, key).await? {
        Some(row) => Ok((row.id, false)),
        None => Err(Conflict("duplicate start raced with an uncommitted insert outside READ COMMITTED; retry")),
    }
}
```

- MySQL: keep `INSERT .. ON DUPLICATE KEY UPDATE id = id + LAST_INSERT_ID(0)`
  then `SELECT LAST_INSERT_ID()`. `>0` → `Inserted`. `0` with no dedup key →
  only the restart key can have fired → `Conflict`. `0` with a key →
  `DeduplicationConflict`.
- Postgres: `.on_conflict((kind, deduplication_key)).do_nothing()
  .returning(id).get_result::<i64>(c).optional()`: `Some` → `Inserted`, `None`
  → `DeduplicationConflict`. A restart-index violation raises
  `unique_violation` → map to `Conflict` via `is_unique_violation`. Because a
  failed statement aborts a Postgres transaction, wrap the INSERT in a
  savepoint (nested `conn.transaction`) **only when
  `restarted_from_workflow_id.is_some()`**.
- NULL keys are distinct in unique indexes on both. Do not use
  `NULLS NOT DISTINCT`.

### (c) Insert-if-absent — `registry.rs` `TopicRegistry::seed_locks_once`, `schedule.rs` `ScheduleRegistry::reconcile_state`

Existing rows are never modified; only a duplicate primary key is ignored;
return value means "inserted exactly this row".

- MySQL: keep `insert_or_ignore_into` (`== 1` means inserted). Do not switch
  to `ON DUPLICATE KEY UPDATE k = k`: with `client_found_rows` it reports 1 for
  a no-op. The IGNORE caveat is bounded: every column is Rust-validated first
  and both call sites read the row back and compare it.
- Postgres: `insert_into(t).values(..).on_conflict(<pk>).do_nothing()`.

### (d) Database clock — `persistence/mod.rs` `MysqlNowMillis`, `database_now_millis`, 34 call sites

Call-time semantics are required (several sites sample after waiting on a
lock). Postgres `now()` is transaction-start and is forbidden.

- MySQL: `select(dsl::sql::<Timestamp>("UTC_TIMESTAMP(3)"))`.
- Postgres: `select(dsl::sql::<Timestamp>("clock_timestamp() AT TIME ZONE 'UTC'"))`.
- Both: `.and_utc().timestamp_millis()`. Delete `MysqlNowMillis`.

Test clock (`fake-clock` feature), one helper
`support::freeze_database_clock(&mut DurableConnection, millis)` replacing the
raw `SET TIMESTAMP` in `tests/database_time_mysql.rs` `database_clock_uses_mysql_session_time`,
`activity_execution_mysql.rs` `activity_claim_eligibility_uses_database_time_when_process_clock_differs`, `admin_metrics_mysql.rs` `current_topic_metrics_use_mysql_time`:

- MySQL: `SET TIMESTAMP = {secs}.{millis:03}` (production expression unchanged).
- Postgres, only under `fake-clock`, the expression becomes
  `COALESCE(to_timestamp(NULLIF(current_setting('durable.fake_now_millis', true), '')::bigint / 1000.0) AT TIME ZONE 'UTC', clock_timestamp() AT TIME ZONE 'UTC')`,
  set with `SELECT set_config('durable.fake_now_millis', '{millis}', false)`.

### (e) `for_update()` / `skip_locked()` — 31 sites

No change. Audit in P7: no locking site uses `DISTINCT`/`GROUP BY`/aggregates;
Postgres locks only returned rows after `ORDER BY`/`LIMIT`, which matches the
page-then-relock pattern (`coordinator.rs` `WorkflowCoordinator::claim_one`).

### (f) Isolation: READ COMMITTED on both, pinned by the library

- MySQL: `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` before
  `conn.transaction(callback)`.
- Postgres: `conn.build_transaction().read_committed().run(callback)`.

READ COMMITTED on MySQL closes G4 (`next_event_sequence` after the row lock
sees every committed append), narrows G7, and removes gap-lock deadlocks
(`coordinator.rs` `WorkflowCoordinator::claim_one`). No invariant relies on snapshot staleness; all
rest on `FOR UPDATE` + fences + unique keys. Requirement: MySQL with binary
logging needs `binlog_format=ROW` (the 8.x default).

Applies to every pool-owned transaction (`coordinator.rs`, `activity_worker.rs`,
`temporal.rs`, `schedule_materializer.rs`, `schedule.rs`, `admin/control.rs`,
`progress.rs`, and the pool paths of `DurableStore::start` /
`start_or_restart_recoverable`). `_with_conn` APIs never set isolation.

### (g) Postgres aborts a transaction on any failed statement

User code in `ScheduleHandler::start_occurrence` runs inside the materializer
transaction (`schedule_materializer.rs` `materialize_occurrence`, `promote_queued`,
`control.rs` `run_schedule_now`). Engine behavior is unchanged (any error rolls back the
tick); the contract in §2 makes handler behavior identical on both backends.

### (h)–(k)

- (h) The idempotent index migration is obsolete with a fresh baseline.
- (j) `VARCHAR(191)` stays on both backends so the Rust limit constants stay
  single-source.
- (k) Keep the spawned bb8 checkout in `activity_worker.rs` `heartbeat_once`; reword the
  comment to be backend-neutral.

## 4. Schema and migrations

Migrations live inside the crate so `cargo publish` packages them:

```
CRATE/migrations/mysql/2026-09-24-000000_durable_baseline/{up,down}.sql
CRATE/migrations/postgres/2026-09-24-000000_durable_baseline/{up,down}.sql
```

The three copied MySQL migrations are replaced by the baseline (M1 + M2 + M3
merged, snake_case):

- MySQL: every table `ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_bin` (exact-byte comparison for every column).
  `max_concurrency INT NOT NULL CHECK (max_concurrency > 0)`.
- Postgres: `id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY`;
  `LONGTEXT` → `TEXT`; `KEY` → `CREATE INDEX`; `UNIQUE KEY` →
  `CONSTRAINT .. UNIQUE`; FKs and CHECKs verbatim; no collation clause.
- All existing indexes carry over. Addition: `idx_durable_workflow_wait
  (wait_kind, wait_reference_id, status)` for the parent-wake scan
  (`workflows.rs` `wake_waiting_parents_on_child_terminal`, INVARIANTS §2.8).
- `down.sql` drops in reverse FK order.

`schema.rs` stays single-source: remove all `#[sql_name]`, normalize
`Longtext`/`Char`/`Varchar` to `Text`, `Unsigned<Integer>` → `Integer`.
`check_for_backend(Db)` in `persistence/models.rs`. `OCTET_LENGTH`
declarations (`admin/query.rs` `octet_length_longtext`, `octet_length_nullable_longtext`; `admin/metrics.rs` `approval_decision_octet_length`) use `Text`.

Users apply migrations by (1) copying the SQL into their own diesel migration
tree, (2) `durable_workflows::migrations::MIGRATIONS` (`EmbeddedMigrations`),
or (3) `durable_workflows::migrations::apply(url)` (spawn_blocking +
`AsyncConnectionWrapper` + `run_pending_migrations`). The test fixture uses
`migrations::BASELINE_UP_SQL` via `batch_execute`.

## 5. Testing

One suite, compiled per backend; the backend is the cargo feature, never the
URL. The fixture panics if the URL scheme does not match
`durable_workflows::BACKEND`.

- Postgres fixture: `CREATE DATABASE "dwt_.." TEMPLATE template0` behind a
  process-wide `tokio::sync::Mutex`; stale sweep via `pg_database`; drop with
  `DROP DATABASE .. WITH (FORCE)` from a server connection.
- `git mv` the `*_mysql.rs` test files to drop the suffix.
- `admin_controls_mysql.rs` `assert_activity_claim_lock_order`: `START TRANSACTION` → `BEGIN`.
- `workflow_activation_mysql.rs` `paused_expiry_probe_pool`: quote-agnostic match
  (`contains("ORDER BY") && contains("lease_expires_at")`).
- `application_cancellation/timeout_cleanup.rs` `heartbeat_failure_limits_cleanup_to_the_last_confirmed_lease`: manager type →
  `DurableConnection`.
- Replace the unicode_ci aliasing tests (`admin_metrics_mysql.rs` `topic_metrics_resolve_unicode_ci_topic_spellings`,
  `topic_metrics_dead_letter_count_accumulates_collation_aliases`) with `topic_metrics_treat_spellings_as_distinct_topics`.

CI: lint per backend (`--no-default-features --features <b>,fake-clock`);
`test-mysql` on 8.0/8.4; `test-postgres` on 14/17 with
`-c max_connections=300`; separate `rust-cache` keys per backend.

## 6. Phases

Every phase ends with the full MySQL suite green.

| Phase | Goal | Acceptance |
|---|---|---|
| P1 | Fresh MySQL baseline: snake_case, `utf8mb4_bin`, `max_concurrency` `INT`/`i32`; drop `#[sql_name]` | `rg sql_name src/schema.rs` = 0; `rg Unsigned src` = 0; suite green |
| P2 | Remove collation-alias machinery (`admin/metrics.rs` `resolve_topic_aliases`, `TopicAliasRow`, alias indirection); distinct-spellings test; INVARIANTS updated | `rg "unicode_ci\|JSON_TABLE\|sql_query" src` = 0; green |
| P3a | `dialect` module + aliases; persistence layer through it; §3b insert semantics; generic `string_status!` `FromSql`/`ToSql` | `AsyncMysqlConnection` only in `lib.rs` + `dialect/mysql.rs` for persistence; tests: restart collision → `Conflict` with caller txn usable; both keys → `InvalidDefinition`; green |
| P3b | Finish alias swap in runtime/admin; typed inserts at the 5 id sites; `into_boxed::<Db>()` | `rg "mysql::Mysql\|AsyncMysqlConnection\|connection_last_insert_id" src` only in `lib.rs` + `dialect/mysql.rs`; green |
| P4 | Pin READ COMMITTED via `dialect::transaction`; G4 regression test; isolation probe; docs/comments | green on 8.0/8.4; G4 test green |
| P5 | Features, `dialect/postgres.rs`, Postgres baseline, `migrations` module, CI lint per feature | `cargo check --lib` passes for both features; MySQL green |
| P6 | Backend-neutral test suite and fixture; `test-postgres` job (temporarily `continue-on-error`) | MySQL green; Postgres `--no-run` compiles |
| P7 | Postgres green on 14 and 17 | all DB jobs green, no `continue-on-error` |
| P8 | Docs and release hygiene (README backend selection, migrations, contracts, `fake-clock` warning, `binlog_format=ROW`, CHANGELOG) | `cargo doc -D warnings` per feature |

## 7. Invariant risk review (Postgres)

| Invariant / gap | Holds on Postgres? | Why |
|---|---|---|
| S1, S9 | Yes | Same `SKIP LOCKED` + fence. |
| S2, S10, S11, S16, S20, S23, S36 | Yes | Matched-row counts on both. |
| S4 | Yes | Errors propagate; PG abort-on-error is stricter. |
| S5 | Yes, stronger | RC `MAX(sequence)+1` after the row lock sees committed appends. |
| S6, S14 | Yes | Fences on prior status. |
| S12, S15, S17 | Yes | Topic row lock serializes claims; G7 narrows. |
| S13 | Unchanged ASSUMED | Wall clocks on both. |
| S18 | Yes | `ON CONFLICT DO NOTHING` + locking reload; returns `Conflict`, never a wrong id. |
| S19 | Yes, improved | `Conflict` on both; savepoint keeps caller txn usable. |
| S21, S22, S24 | Yes | Same locking scans and fences. |
| S25 | Yes under RC | Caller retries under RR/SERIALIZABLE (documented). |
| S26–S30 | Yes | State row lock, unique occurrence key, cursor fence. |
| S31–S35 | Yes / untouched | DB-independent or enforced by CHECK/length on both. |
| L1–L13 | Yes | L9: a swallowed handler error fails the tick at the next statement (`25P02`). |
| G4 | Closed on both | READ COMMITTED. |
| G7 | Narrowed | Smaller window. |
| G1, G5, G6, G8–G12 | Unchanged | Logic-level; not addressed by this design. |
