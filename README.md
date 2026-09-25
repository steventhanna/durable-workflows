# durable-workflows

Durable workflows for Rust that live in the database you already run.

`durable-workflows` is an embedded durable-execution engine. Workflows,
activities, timers, approvals and cron schedules are rows in your own SQL
database, driven by a runtime inside your process. There is no separate
server to operate: if your app can reach its database, it can run durable
work.

It has run in production on MySQL since August 2026, carrying payments,
notifications, document delivery and scheduled jobs.

> **Status:** pre-1.0. MySQL (8.0.16+ and 8.4) and PostgreSQL (14+) are both
> supported, and CI runs the full suite on MySQL 8.0 and 8.4 and on Postgres
> 14 and 17. The storage schema may still change before 1.0. See
> [Known issues](#known-issues) for the confirmed protocol gaps.

## Features

- **Journaled flows.** Write a workflow as a plain `async fn` with
  `#[durable_flow]`. Completed steps are journaled, so a crash or redeploy
  resumes where it stopped and never re-runs a finished activity.
- **Activities with real retry semantics.** Per-activity attempts, timeouts,
  leases, and fixed or exponential backoff with jitter. Retryable and
  permanent errors are distinct.
- **Leases with fencing.** Workers claim work with a lease token; a worker
  whose lease expired cannot commit a stale result.
- **Topics.** Group activities by topic and cap concurrency per topic, across
  every process that runs the runtime.
- **Child workflows, timers and approvals.** Start sub-workflows, sleep until
  a point in time, or wait for a human decision with an expiry.
- **Cron schedules.** Time-zone aware schedules with explicit misfire
  (skip / catch up) and overlap policies.
- **Idempotent starts.** Deduplication keys for workflows and operation keys
  for activities.
- **Operations.** Admin query, control (cancel, retry, restart) and metrics
  services; health scanning with a pluggable alert sink; graceful shutdown.

## Quick start

```toml
[dependencies]
durable-workflows = "0.1"
diesel-async = { version = "0.9", features = ["postgres", "bb8"] }
serde = { version = "1", features = ["derive"] }
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
```

### Choosing a backend

Each build targets one database, picked with a cargo feature. The default is
`postgres`. For MySQL, turn the default off:

```toml
durable-workflows = { version = "0.1", default-features = false, features = ["mysql"] }
diesel-async = { version = "0.9", features = ["mysql", "bb8"] }
```

The two features are mutually exclusive; enabling both (or neither) is a
compile error. One compiled backend keeps the API non-generic:
`durable_workflows::DurableConnection` *is* the backend's diesel-async
connection (`AsyncPgConnection` or `AsyncMysqlConnection`), so you pass your
own connection to the `*_with_conn` methods and to
`ScheduleHandler::start_occurrence` with no adapter, and the same code builds
a pool for either backend:

```rust,ignore
let manager = AsyncDieselConnectionManager::<durable_workflows::DurableConnection>::new(url);
```

The `fake-clock` feature lets the test suite override the database clock. It
is for tests only: **never enable `fake-clock` in production.**

### Creating the tables

The schema ships with the crate as plain Diesel migrations in
[`durable-workflows/migrations/postgres/`](durable-workflows/migrations/postgres)
and [`durable-workflows/migrations/mysql/`](durable-workflows/migrations/mysql).
Apply them in one of three ways:

1. Copy the SQL files into your own Diesel migrations directory.
2. Run `durable_workflows::migrations::MIGRATIONS` (a
   `diesel_migrations::EmbeddedMigrations`) with your own migration harness.
3. Call `durable_workflows::migrations::apply(&database_url).await`, which
   applies any pending durable migrations and returns their versions.

Options 2 and 3 record the applied versions in Diesel's
`__diesel_schema_migrations` table, so they combine with your application's
own Diesel migrations.

### Database requirements

- **MySQL 8.0.16+ or 8.4, InnoDB.** The library runs its own transactions at
  READ COMMITTED, so with binary logging on the server needs
  `binlog_format=ROW` (the 8.x default). The tables use `utf8mb4_bin`, so keys
  compare by exact bytes.
- **PostgreSQL 14+.** Keys compare by exact bytes under any deterministic
  collation (the default).

### Writing a workflow

```rust
use durable_workflows::{
    durable_flow, ActivityContext, ActivityError, ActivityHandler, WfCtx, WfError,
};

struct App {
    greeting: String,
}

#[derive(Clone, Copy)]
enum Topics {
    Email,
}

impl durable_workflows::ActivityTopic for Topics {
    fn key(self) -> &'static str {
        "email"
    }
    fn max_concurrency(self) -> u32 {
        4
    }
}

#[derive(serde::Serialize, serde::Deserialize, durable_workflows::DurableActivity)]
#[activity(
    kind = "send_greeting",
    version = 1,
    topic = Topics::Email,
    max_attempts = 5,
    timeout_secs = 30,
    lease_secs = 60,
    backoff = exponential(initial_secs = 1, max_secs = 60, jitter_percent = 20),
)]
struct SendGreeting {
    name: String,
}

#[async_trait::async_trait]
impl ActivityHandler for SendGreeting {
    type Context = App;
    type Output = String;

    async fn execute(&self, ctx: ActivityContext<'_, App>) -> Result<String, ActivityError> {
        Ok(format!("{}, {}!", ctx.application().greeting, self.name))
    }
}

#[durable_flow(kind = "greet", version = 1)]
async fn greet_flow(ctx: &mut WfCtx<'_, App>, name: String) -> Result<String, WfError> {
    ctx.run(&SendGreeting { name }).await
}
```

Register the definitions, spawn the runtime, and start a workflow. See
[`examples/quickstart.rs`](durable-workflows/examples/quickstart.rs) for the
complete program.

## How it works

Each process runs a `DurableRuntime`. It supervises a small set of loops:

- a **workflow coordinator** that claims ready workflows and applies their
  next transition;
- **activity workers** per topic that claim due activities under a lease and
  record each attempt;
- **materializers** that turn due timers, expiring approvals and cron
  occurrences into workflow events;
- a **health scanner** that reports stuck or failing work.

All coordination goes through the database. Any number of processes can run
the runtime against the same tables.

## Contracts

- **`*_with_conn` runs in your transaction.** `DurableStore::start_with_conn`,
  `cancel_with_conn` and the other `*_with_conn` methods run inside the
  caller's transaction at the caller's isolation level, and the caller
  commits. They are correct under READ COMMITTED on both backends and under
  REPEATABLE READ on MySQL. On Postgres under REPEATABLE READ or SERIALIZABLE
  they can fail with a serialization error or `DurableError::Conflict`; retry
  the whole transaction. The methods without `_with_conn` open their own
  transaction, pinned to READ COMMITTED.
- **`ScheduleHandler::start_occurrence` must return every error.** It runs
  inside the schedule materializer's transaction on the library's
  connection. Do not run more statements after one fails: Postgres aborts the
  whole transaction on any failed statement. To recover from a statement that
  may fail, run that step in `connection.transaction(..)` (a savepoint). Any
  error rolls back the whole materializer tick.
- **Keys compare by exact bytes.** Deduplication keys, topic keys, schedule
  keys and operation keys are case- and accent-sensitive on both backends:
  `email` and `Email` are different topics.
- **`fake-clock` is test-only.** Never enable it in production.

## Verification

The safety and liveness properties the engine relies on are written down in
[`docs/INVARIANTS.md`](docs/INVARIANTS.md). [`spec/`](spec) holds a Quint
model of the core protocol (claims, leases, activity attempts, child
workflows, cancellation) that is checked by simulation and bounded model
checking. [`durable-workflows/tests/gaps.rs`](durable-workflows/tests/gaps.rs)
has reproduction tests for most of the suspected gaps in INVARIANTS §6 and for two
more (N1, N2) found by the model. A test that confirms its gap is `#[ignore]`d
with the reason, so the suite stays green; a fix removes the `#[ignore]`. A
test that refuted its gap (G4, closed by READ COMMITTED) stays as a
regression test. G5, G7, G9 and G12 are not reproduced by a test yet.

Trace checking ([`docs/TRACE_CHECKING.md`](docs/TRACE_CHECKING.md)) records
every engine transaction of the test suites and replays the traces through
the Quint model, on MySQL and Postgres in CI; the gap tests double as its
self-test, since each must show its invariant violation.

## Known issues

These gaps are confirmed by ignored tests in
[`tests/gaps.rs`](durable-workflows/tests/gaps.rs). Details and interleavings
are in [INVARIANTS §6](docs/INVARIANTS.md#6-suspected-gaps) and
[`spec/README.md`](spec/README.md).

- **G1** — A benign race (such as an operator pausing a workflow during its
  step) makes the coordinator return `FencedWrite`, which uses up the
  runtime's restart budget. The budget never resets, so repeated races stop
  the runtime.
- **G2** — A recoverable start on a blocked keyed child cancels that child
  but does not wake its parent, which stays `waiting_child` forever.
- **G3** — A `step` that panics never counts an activation attempt; after
  lease recovery it panics again, without limit, in every runtime that
  claims it.
- **G6** — Under a concurrent child deduplication race, a parent can wait on
  a child of a different definition version; the parent then fails on replay.
- **G8** — `child_with_key` can resolve to the calling workflow (or an
  ancestor), which then waits on itself forever.
- **G10** — One invalid activity row (for example a lease no longer than its
  timeout) makes the activity claim fail on every topic, not only its own.
- **G11** — Cancelling a parent workflow does not cancel its children; they
  keep running.
- **N1** — A recoverable start on a child key can cancel and restart a
  different sibling child in the same tree, or return an unrelated sibling's
  id.
- **N2** — An application cancel or operator pause frees the topic
  concurrency slot while the cancelled handler is still running, so a cap-1
  topic can briefly run two handlers.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
