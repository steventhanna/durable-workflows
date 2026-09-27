# CLAUDE.md

Guidance for Claude Code (and other agents) working in this repository.

## Project

A durable workflow engine for Rust on MySQL or Postgres (Diesel 2 +
diesel-async + bb8, tokio). Workspace: `durable-workflows/` (engine),
`durable-workflows-macros/` (derives), `tools/durable-trace/` (trace checker,
not published). The behavior is specified in `docs/INVARIANTS.md` and modeled
in Quint under `spec/`; `docs/TRACE_CHECKING.md` explains how recorded test
runs are replayed through the model.

## Rely on the compiler first

Every invariant should be enforced by the strongest mechanism available.
When you add or change an invariant, or fix a bug, first ask whether the type
system can make the violation fail to compile. In order of preference:

1. **Unrepresentable.** Model the domain so the bad state has no value: an
   enum instead of a combination of `Option` fields, a newtype instead of a
   raw `i64`/`String`, a parsed type instead of a validated one, an outcome
   enum instead of a flag (a lease renewal returns `Renewed::Held` or
   `Renewed::Revoked`; a revoked execution finishes as
   `ExecutionOutcome::Revoked`).
2. **Uncompilable.** Make the wrong use a compile error: a borrow guard
   (`WorkflowCoordinator::claim_one(&mut self)` returns a `WorkflowClaim<'_, C>`,
   so a second claim while one is alive is E0499), a method that consumes
   `self` for a one-shot transition (`claim.activate()`), a proof token with a
   private constructor that a function must receive before it may act, an
   exhaustive `match` that forces every new variant to be decided.
3. **Checked at the boundary.** A constructor or parser that returns `Result`
   (e.g. `RetryPolicy::fixed`), plus a test.
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
  `is_start_recoverable`. Each constant is checked against its predicate at
  compile time (`status_set_matches_predicate!`); SQL filters use the
  constant (`status.eq_any(ActivityStatus::SLOT_HOLDERS)`), Rust code the
  predicate.
- Every `durable_activity` update that clears the lease sets the
  `persistence::LeaseCleared` changeset fragment instead of listing the three
  lease columns (S1, S9).
- Public enums and config structs that may grow are `#[non_exhaustive]`
  (`WorkflowStatus`, `ActivityStatus`; the compile-fail case
  `activity_status_match_non_exhaustive` shows a downstream exhaustive
  `match` is E0004).
- Never persist or compare host wall-clock time with a database timestamp.
  Persisted times and due/expiry comparisons use the database clock; process
  time (`tokio::time::Instant`) is only for local deadlines, timeouts and
  sleeps. Keep the two in different types.
- Ids crossing a function boundary use the id newtypes in `src/ids.rs`, not
  `i64`.
- `#[doc(hidden)] pub` escape hatches (such as `RetryPolicy::from_validated`)
  exist only for the derive macros, which must validate the same bounds at
  expansion time.
- Guards and tokens that should not be dropped unused are `#[must_use]`.
  Rust types are affine, not linear: document what happens when one is
  dropped.
- Every compile-time guarantee has a compile-fail case under
  `durable-workflows/tests/ui/fail/` (trybuild; regenerate `.stderr` with
  `TRYBUILD=overwrite` and read it before committing).
- Types cannot prove what the database contains. Keep the SQL fences
  (lease token and status filters); a type-level rule adds to them.

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
