# CLAUDE.md

Guidance for Claude Code (and other agents) working in this repository.

## Project

A durable workflow engine for Rust on MySQL or Postgres (Diesel 2 +
diesel-async + bb8, tokio). Workspace: `durable-workflows/` (engine),
`durable-workflows-macros/` (derives), `tools/durable-trace/` (trace checker,
not published). Start with `docs/ARCHITECTURE.md` for how the system works.
The behavior is specified in `docs/INVARIANTS.md` and modeled
in Quint under `spec/`; `docs/TRACE_CHECKING.md` explains how recorded test
runs are replayed through the model.

## Rely on the compiler first

Every invariant should be enforced by the strongest mechanism available.
When you add or change an invariant, or fix a bug, first ask whether the type
system can make the violation fail to compile. In order of preference:

1. **Unrepresentable.** Model the domain so the bad state has no value: an
   enum instead of a combination of `Option` fields, a newtype instead of a
   raw `i64`/`String`, a parsed type instead of a validated one (a schedule's
   local occurrence key is a `LocalOccurrence`, the one parser and formatter
   of the persisted text, ordered like it; a `RetryPolicy` has a private
   field and only checked constructors and a checked decode, so a policy
   outside the retry bounds has no value outside the crate:
   `retry_policy_unchecked_constructor` and `retry_policy_field_private`
   compile-fail cases), an outcome
   enum instead of a flag (a lease renewal returns `Renewed::Held` or
   `Renewed::Revoked`; a revoked execution finishes as
   `ExecutionOutcome::Revoked`), one enum for fields that exclude each other
   (a start's deduplication key and its engine-set restart lineage are the
   private `StartLineage` in `StartOptions`, so a start with both has no
   value; `start_options_dedup_key_field_private` compile-fail case).
2. **Uncompilable.** Make the wrong use a compile error: a borrow guard
   (`WorkflowCoordinator::claim_one(&mut self)` returns a `WorkflowClaim<'_, C>`,
   so a second claim while one is alive is E0499), a method that consumes
   `self` for a one-shot transition (`claim.activate()`), a function that
   takes an enum of only the variants it handles instead of returning an
   error for the others (the coordinator's `commit_wait_transition` takes a
   `WaitTransition`, not a `StoredTransition` it would have to reject
   `Continue`/`Complete` from), a proof token with a
   private constructor that a function must receive before it may act (a
   `tx::Locked<'tx, Row>` row-lock witness: `insert_activity` takes the
   `Locked<ClaimFence>` that `lock_fence` returns, N4; `commit_child`'s parent
   update takes the `ChildStart` whose `Existing` arm is the locked child,
   G9), a SQL type per id column (each id column in `schema.rs` has its own
   type from `ids::sql_types`, and only the matching id newtype is
   `AsExpression`/`FromSql` for it, so comparing `durable_workflow::id`
   with an `ActivityId` is E0277: `id_swapped_in_query` compile-fail case;
   `wait_reference_id` stays `BigInt` and is compared through
   `ids::untyped_id`), an exhaustive `match` that forces every new variant
   to be decided.
3. **Checked at the boundary.** A constructor or parser that returns `Result`
   (e.g. `RetryPolicy::fixed`), or a transition that does
   (`ScheduleCursor::advance_to` rejects a cursor that does not move forward
   in local time, and `ScheduleCursor::upgrade` places the cursor strictly
   after a `MaterializedFloor` read from the database, S27), plus a test.
4. **Checked by the model.** The Quint model, directed tests and trace
   checking. These back up the types; they do not replace them.

A bug fix that stops at level 3 or 4 should say in its commit or PR why the
type system cannot express the rule.

### Rules that keep type-level guarantees sound

- A proof token, witness or guard is sound only while its constructor is
  private to one module. Never add a `pub` or `pub(crate)` constructor, a
  `Default`, a `Clone`, or a `From` conversion to one without a reason in the
  review.
- Status enums (`WorkflowStatus`, `ActivityStatus`, and every status or kind
  stored as a string) are matched exhaustively inside the crate: no `_ =>`
  arm, no hand-written list of statuses. Put membership in one exhaustive
  method or constant on the enum and use it everywhere, so adding a variant
  is a compile error at each place that must decide. Today (in
  `src/persistence/mod.rs`): `ActivityStatus::holds_slot` / `SLOT_HOLDERS`
  (topic cap, in-flight counts), `holds_lease` / `LEASE_HOLDERS` (lease
  reconciliation, stale-lease health), `is_terminal` / `NON_TERMINAL`
  (cancellation, readiness); `WorkflowStatus::is_terminal` / `TERMINAL`,
  `awaits_child` / `CHILD_WAITERS`, `awaits_approval` / `APPROVAL_WAITERS`,
  `awaits_dead_letter` / `DEAD_LETTER_WAITERS`, `is_restartable`,
  `is_start_recoverable`. `WaitKind`, `ApprovalStatus`,
  `ScheduleRunStatus` and `AttemptOutcome` are the same kind of enum (no
  status sets yet; add one the same way when code needs membership). Each
  constant is checked against its predicate at compile time (`status_set_matches_predicate!`); SQL filters use the
  constant (`status.eq_any(ActivityStatus::SLOT_HOLDERS)`), Rust code the
  predicate.
- Every write of `durable_schedule_state.next_local_occurrence` goes through
  `ScheduleCursor` (`src/schedule/cursor.rs`): `initial` for a new row,
  `advance_to` for a tick, `upgrade` (with a `MaterializedFloor` loaded under
  the state lock) for a version upgrade. The cursor stays strictly after the
  last materialized occurrence. These types are `pub(crate)`, so their checks
  are unit tests in that module, not trybuild cases.
- A workflow's wait is read through `WorkflowRow::wait()` (the one parser,
  `persistence::Wait::parse`, of `wait_kind` + `wait_reference_id` into a
  `Wait` whose kind carries its reference) and written through the
  `persistence::WaitColumns` changeset fragment (`on(wait)` or
  `cleared()`), never by setting one wait column alone.
- Every `durable_activity` update that clears the lease sets the
  `persistence::LeaseCleared` changeset fragment instead of listing the three
  lease columns (S1, S9).
- Public enums and config structs that may grow are `#[non_exhaustive]`
  (the persisted status and kind enums `WorkflowStatus`, `ActivityStatus`,
  `WaitKind`, `ApprovalStatus`, `ScheduleRunStatus`, `AttemptOutcome`; the
  error enums `DurableError`, `ActivityError`, `WfError`,
  `WorkflowDispatchError`, `ActivityDispatchError`; `WorkflowTransition`,
  `StoredTransition`, `BackoffPolicy`, `MisfirePolicy`, `OverlapPolicy`,
  `LocalTimeDisposition`, `ScheduleStateReconcileOutcome`,
  `ProgressSeverity`, `ProgressReportOutcome`, `HealthAlert`,
  `BenignActivationKind`, `ScheduleHealthIssue`, `TimelineEntry`; the
  compile-fail case `activity_status_match_non_exhaustive` shows a
  downstream exhaustive `match` is E0004). Two public enums stay exhaustive
  on purpose: `WorkflowEvent`, because a workflow handler must decide every
  event (a new event must break its `match`, not fall into a `_` arm that
  drops it), and `BackendKind`, because code that picks backend SQL (tests,
  `tools/durable-trace`) must decide a new backend. Public config structs
  (`RuntimeConfig`, `CoordinatorConfig`, `WorkerConfig`,
  `HealthScannerConfig`) are `#[non_exhaustive]` with `Default` and a
  `with_<field>` setter per field; add a setter with each new field
  (`config_struct_literal` compile-fail case: a struct expression outside
  the crate is E0639).
- Never persist or compare host wall-clock time with a database timestamp.
  Persisted times and due/expiry comparisons use the database clock; process
  time (`tokio::time::Instant`) is only for local deadlines, timeouts and
  sleeps. Keep the two in different types.
- Ids crossing a function boundary use the id newtypes in `src/ids.rs`, not
  `i64`. A new id column in `schema.rs` uses its id's SQL type, not
  `BigInt`; the id's `FromSql` checks positivity through `Id::new`, so a
  stored id that is not positive is a decode error, never a panic.
- `#[doc(hidden)] pub` escape hatches (such as `RetryPolicy::from_checked`)
  exist only for the derive macros, which must validate the same bounds at
  expansion time. Make the hatch a `const fn` that checks the bounds too
  and have the macro emit it in a `const { .. }` block, so a drift between
  the two is a compile error (E0080; `#[track_caller]` keeps the error in
  the caller's code; `retry_policy_out_of_bounds_const` compile-fail case).
- A value type with bounds that is persisted checks them on decode too:
  `#[serde(try_from = "Stored..")]` through the same check the constructors
  use (`RetryPolicy`), and a stored value that fails to decode routes to an
  existing error path (a stored retry policy: G10 quarantine at claim),
  never a panic or a default.
- Guards and tokens that should not be dropped unused are `#[must_use]`.
  Rust types are affine, not linear: document what happens when one is
  dropped.
- Every compile-time guarantee has a compile-fail case under
  `durable-workflows/tests/ui/fail/` (trybuild; regenerate `.stderr` with
  `TRYBUILD=overwrite` and read it before committing). A `pub(crate)` type
  that trybuild cannot name has a documented manual probe instead (the lock
  witnesses: `src/tx.rs` module docs).
- Types cannot prove what the database contains. Keep the SQL fences
  (lease token and status filters); a type-level rule adds to them.

### Transaction callbacks and lock witnesses

`src/tx.rs` holds the transaction scope and the row-lock witnesses. The
N4 and G9 lock orders, and every helper documented as "the caller holds
the row `FOR UPDATE`", depend on them.

- A transaction callback (`dialect::transaction`, `tx::caller_transaction`)
  has one argument, `Tx<'r>`, destructured in the closure head:
  `async move |Tx { connection, scope }| { .. }`, or `Tx { connection, .. }`
  when the body takes no lock. `TxScope<'tx>` is the transaction's brand;
  only `tx::enter` makes one.
- A helper that needs a lock taken earlier in the transaction takes the
  witness `Locked<'tx, &Row>` (a `Copy` token; `locked.as_ref()`) next to
  `connection: &mut DurableConnection`. Locks are taken through
  `tx::lock_optional` / `lock_first` / `lock_by_update`, or a wrapper of
  them (`lock_fence`, `lock_workflow_by_id`, `lock_workflow`,
  `lock_schedule_state`), which need the `scope`.
- `Locked::new` stays private to `tx.rs`. No `Clone`, `Default` or `From`
  on `Locked<'tx, Row>` other than the `Row: Copy` impl.
- Compiler limit 1: never put `&mut Tx<'_>`, `&mut T<'tx>` or any
  `&'a mut X<'b>` in an `async fn` signature that runs inside a transaction
  callback. rustc cannot prove the future `Send` under the callback's
  higher-ranked lifetime (rust-lang/rust#102211, #110338). Pass the
  connection and the witness as separate parameters.
- Compiler limit 2: a helper generic over a Diesel query type (a
  `LoadQuery<'q, ..>` bound) is written
  `fn .. -> impl Future<Output = ..> + Send + 'conn { async move { .. } }`,
  never `async fn`, for the same reason.
- The witness types are `pub(crate)`, so trybuild cannot reach them. Their
  compile-fail check is the list of manual probes in the `src/tx.rs` module
  docs. Run it when you change `tx.rs` or a signature that takes a witness.
- A witness proves that a lock was taken in this transaction. It does not
  prove which row the caller meant, that the row is fresh, or what the
  database contains. Keep the SQL fences.

## Commands

The `mysql` and `postgres` features are mutually exclusive; `postgres` is the
default. Always name the backend and add `fake-clock` for tests:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --no-default-features \
    --features durable-workflows/mysql,durable-workflows/fake-clock -- -D warnings
cargo clippy --workspace --all-targets --no-default-features \
    --features durable-workflows/postgres,durable-workflows/fake-clock -- -D warnings
# also both with durable-workflows/trace-model added

DURABLE_WORKFLOWS_TEST_DATABASE_URL=mysql://root:durable@127.0.0.1:33306/mysql \
  cargo test --workspace --all-targets --no-default-features \
    --features durable-workflows/mysql,durable-workflows/fake-clock
DURABLE_WORKFLOWS_TEST_DATABASE_URL=postgres://postgres:durable@127.0.0.1:55432/postgres \
  cargo test --workspace --all-targets --no-default-features \
    --features durable-workflows/postgres,durable-workflows/fake-clock

cd spec && ./check.sh                                   # Quint typecheck, tests, simulation
scripts/trace-pipeline.sh mysql && scripts/trace-pipeline.sh postgres
```

- `fake-clock` and `trace-model` are test-only: a build without
  `debug_assertions` (e.g. `--release`) that enables either is a
  `compile_error!` (`src/dialect/mod.rs`). Never build tests in release
  with them.
- Run tests at full parallelism; never `--test-threads=1`. Each test creates
  its own `dwt_*` database.
- `trace-pipeline.sh` drops every `dwt_*` database on its server: never run it
  at the same time as another test run on that server, or alongside
  `check.sh` (CPU contention breaks timing-sensitive tests).
- Pipe long cargo output through `tail` or a `grep` for
  `error|warning|FAILED|panicked|test result`.

## Changing behavior

Use the `verify-invariants` skill (`.claude/skills/verify-invariants/`) for
every behavior change and before every push; it runs
`scripts/verify-invariants.sh` (`--full` before a push or release) and says how
to classify a failure as a code, recorder or model problem.

- Name the invariant in `docs/INVARIANTS.md` that a change relies on or
  changes, and update that document, the Quint model (`spec/durable.qnt`,
  `spec/durable_tests.qnt`) and `CHANGELOG.md` with it.
- A fix for a known gap removes the `#[ignore]` from its reproduction test in
  `durable-workflows/tests/gaps.rs`, removes its entry from
  `spec/traces/gaps.yaml`, and inverts its Quint scenario to assert the fixed
  behavior.
- Refresh `spec/traces/expected.json` with `--update-baseline` only when a
  change moves traces on purpose, and check the diff. CI fails when `pass`
  drops or an exclusion count rises; a trace that flips by timing keeps its
  exclusion key and the lower `pass` count.
- A change to the recorded interface bumps `TRACE_IFACE_VERSION` in
  `spec/durable.qnt` and `IFACE_VERSION` in `tools/durable-trace/src/gen.rs`,
  and lists the change in `spec/README.md`.

## Test integrity

If a test fails, fix the code, not the test. If a test looks wrong (a stale
expectation, a timing assumption, a premise the fix removes), stop and ask
before you edit, rewrite or delete it.
