//! Transaction scope and row-lock witnesses (CLAUDE.md, "Rely on the compiler
//! first"; INVARIANTS §2.8).
//!
//! - [`Tx`] is what a transaction callback receives: the connection plus a
//!   [`TxScope`] token branded with the callback's lifetime. Only [`enter`]
//!   builds one, inside a transaction that `dialect::transaction` or
//!   [`caller_transaction`] opened. Callbacks destructure it
//!   (`async move |Tx { connection, scope }| ...`) and keep passing the plain
//!   `&mut DurableConnection` to helpers, as before.
//! - [`Locked`] proves that its row was read `FOR UPDATE` (or changed by a
//!   one-row fenced `UPDATE`) in the transaction of its brand `'tx`. Only the
//!   `lock_*` functions in this module build one; they need the scope token,
//!   and the token exists only inside the callback.
//!
//! A helper whose correctness needs a row lock taken earlier in the same
//! transaction takes `Locked<'tx, &Row>` (a `Copy` token) or
//! `&Locked<'tx, Row>` next to `&mut DurableConnection`. Calling it before the
//! lock does not compile. N4 (`dialect::insert_activity`/`insert_approval`
//! take the claim fence) and G9 (`commit_child`'s parent update takes the
//! `ChildStart`, whose `Existing` arm holds the locked child) are enforced
//! this way.
//!
//! Do not put `&mut Tx<'tx>` (or any `&mut T<'tx>`) in an `async fn`
//! signature that runs inside the callback: rustc cannot prove such futures
//! `Send` under the callback's higher-ranked lifetime
//! (rust-lang/rust#102211, #110338). Keep the connection and the witness as
//! separate parameters.
//!
//! Limits: a witness proves that the lock was taken, not that the row is
//! fresh or which row the caller meant; types cannot prove database state, so
//! the SQL fences stay. Rust is affine: dropping a witness releases nothing
//! (the lock is held to commit either way).
//!
//! # Compile-fail check (manual)
//!
//! These types are `pub(crate)`, so trybuild cases and doctests (both
//! compile as other crates) cannot name them. Instead, apply each probe
//! below as a temporary edit, confirm that
//! `cargo check -p durable-workflows --lib --no-default-features --features mysql,fake-clock`
//! fails with the listed error, and revert. Do this when you change this
//! module or a signature that takes a witness.
//!
//! 1. N4: in `coordinator::commit_activity`, move `insert_activity` above
//!    `lock_fence`: E0425 (`fence` not found).
//! 2. N4: in `admin_retry`, pass a locked activity row
//!    (`tx::lock_first(.., durable_activity::table..)`) as the parent of
//!    `insert_activity`: E0277 (`ActivityRow: CommandParent` not satisfied).
//! 3. G9: in `coordinator::commit_child`, move `set_child_wait` above
//!    `insert_child`: E0425 (`outcome` not found).
//! 4. G9: in `DurableStore::insert_child`, build `ChildStart::Existing` from
//!    `find_workflow_by_id` (an unlocked read): E0308 (expected
//!    `Locked<'_, WorkflowRow>`, found `WorkflowRow`).
//! 5. Brand: return `lock_workflow(..)` from one `dialect::transaction` and
//!    use it in the next: "lifetime may not live long enough" (no code).
//! 6. In `activity_worker::finish_on_connection`, call `settle_revoked` with
//!    the claim's unlocked copy `&claim.row`: E0308 (expected
//!    `Locked<'_, &ActivityRow>`, found `&ActivityRow`).

use std::{future::Future, marker::PhantomData, ops::Deref};

use diesel::{
    dsl::Limit, query_builder::QueryFragment, query_dsl::methods::LimitDsl, OptionalExtension,
};
use diesel_async::{
    methods::{ExecuteDsl, LoadQuery},
    AsyncConnection, RunQueryDsl,
};

use crate::{dialect::TransactionCallback, Db, DurableConnection, DurableError, WorkflowId};

/// Brand of one transaction callback. Invariant in `'tx`, `Copy`, no public
/// constructor: it exists only inside the callback that [`enter`] runs.
#[derive(Clone, Copy)]
pub(crate) struct TxScope<'tx>(PhantomData<fn(&'tx ()) -> &'tx ()>);

/// The argument of a transaction callback.
pub(crate) struct Tx<'tx> {
    pub(crate) connection: &'tx mut DurableConnection,
    pub(crate) scope: TxScope<'tx>,
}

/// Runs `callback` inside the transaction the caller opened on `connection`
/// (the only constructor of [`Tx`]), within the trace scope.
pub(crate) async fn enter<R, E, F>(connection: &mut DurableConnection, callback: F) -> Result<R, E>
where
    for<'r> F: AsyncFnOnce(Tx<'r>) -> Result<R, E>
        + TransactionCallback<Tx<'r>, Result<R, E>, Fut: Send>
        + Send,
    E: Send,
    R: Send,
{
    crate::trace::scoped(connection, async move |connection| {
        callback(Tx {
            connection,
            scope: TxScope(PhantomData),
        })
        .await
    })
    .await
}

/// Opens a transaction on `connection` at the caller's isolation level (a
/// savepoint inside an open one) and runs `callback` in it: for the
/// `*_with_conn` entry points, which run in the caller's transaction.
pub(crate) async fn caller_transaction<R, E, F>(
    connection: &mut DurableConnection,
    callback: F,
) -> Result<R, E>
where
    for<'r> F: AsyncFnOnce(Tx<'r>) -> Result<R, E>
        + TransactionCallback<Tx<'r>, Result<R, E>, Fut: Send>
        + Send,
    E: From<diesel::result::Error> + Send,
    R: Send,
{
    connection
        .transaction(async move |connection| enter(connection, callback).await)
        .await
}

/// Proof that `Row` was locked in the transaction `'tx`; see the module docs.
#[must_use = "a lock witness proves an order; pass it to the function that needs the lock"]
pub(crate) struct Locked<'tx, Row> {
    row: Row,
    scope: TxScope<'tx>,
}

impl<'tx, Row> Locked<'tx, Row> {
    fn new(scope: TxScope<'tx>, row: Row) -> Self {
        Self { row, scope }
    }

    /// The scope this lock belongs to, for locking further rows.
    pub(crate) fn scope(&self) -> TxScope<'tx> {
        self.scope
    }

    /// A `Copy` witness borrowing the row: the form helpers take by value.
    pub(crate) fn as_ref(&self) -> Locked<'tx, &Row> {
        Locked {
            row: &self.row,
            scope: self.scope,
        }
    }

    /// The locked row, giving up the witness.
    pub(crate) fn into_row(self) -> Row {
        self.row
    }
}

impl<'tx, 'a, Row> Locked<'tx, &'a Row> {
    /// The borrowed row with its own lifetime, free of the token.
    pub(crate) fn row(self) -> &'a Row {
        self.row
    }
}

/// A locked workflow row a new command row (activity, approval) may be
/// inserted under (N4): the claim fence on the coordinator's path, the whole
/// row on an operator's.
pub(crate) trait CommandParent {
    fn workflow_id(&self) -> WorkflowId;
}

/// The claimed workflow row, locked under its lease fence (T-C2, N4).
#[derive(diesel::Queryable)]
pub(crate) struct ClaimFence {
    workflow_id: WorkflowId,
}

impl CommandParent for ClaimFence {
    fn workflow_id(&self) -> WorkflowId {
        self.workflow_id
    }
}

impl CommandParent for crate::persistence::WorkflowRow {
    fn workflow_id(&self) -> WorkflowId {
        self.id
    }
}

impl<'tx, Row: Copy> Clone for Locked<'tx, Row> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'tx, Row: Copy> Copy for Locked<'tx, Row> {}

impl<Row> Deref for Locked<'_, Row> {
    type Target = Row;

    fn deref(&self) -> &Row {
        &self.row
    }
}

/// Runs a `FOR UPDATE` read and returns its row, if any, as a witness.
///
/// The `lock_*` functions return `impl Future + Send` (as diesel-async's
/// query methods do) instead of being `async fn`: a generic `async fn` with
/// a query lifetime cannot be proven `Send` under the transaction callback's
/// higher-ranked lifetime.
pub(crate) fn lock_optional<'conn, 'tx, 'q, Q, U>(
    connection: &'conn mut DurableConnection,
    scope: TxScope<'tx>,
    query: Q,
) -> impl Future<Output = Result<Option<Locked<'tx, U>>, diesel::result::Error>> + Send + 'conn
where
    Q: LimitDsl + Send + 'conn,
    Limit<Q>: LoadQuery<'q, DurableConnection, U> + QueryFragment<Db> + Send + 'q,
    U: Send + 'static,
    'q: 'conn,
    'tx: 'conn,
{
    async move {
        let query = query.limit(1);
        debug_assert_locks(&query);
        Ok(query
            .get_result::<U>(connection)
            .await
            .optional()?
            .map(|row| Locked::new(scope, row)))
    }
}

/// [`lock_optional`] for a row that must exist (`NotFound` otherwise).
pub(crate) fn lock_first<'conn, 'tx, 'q, Q, U>(
    connection: &'conn mut DurableConnection,
    scope: TxScope<'tx>,
    query: Q,
) -> impl Future<Output = Result<Locked<'tx, U>, diesel::result::Error>> + Send + 'conn
where
    Q: LimitDsl + Send + 'conn,
    Limit<Q>: LoadQuery<'q, DurableConnection, U> + QueryFragment<Db> + Send + 'q,
    U: Send + 'static,
    'q: 'conn,
    'tx: 'conn,
{
    async move {
        let query = query.limit(1);
        debug_assert_locks(&query);
        Ok(Locked::new(scope, query.get_result::<U>(connection).await?))
    }
}

/// Runs a fenced one-row `UPDATE`, which holds the row's lock to commit, and
/// returns `row` (the caller's copy of the row) as its witness. Zero or
/// several changed rows is `FencedWrite`. Weaker than a read lock: the witness
/// holds the caller's pre-update copy.
// Not `async fn`: see `lock_optional` (higher-ranked Send).
#[allow(clippy::manual_async_fn)]
pub(crate) fn lock_by_update<'conn, 'tx, S, T>(
    connection: &'conn mut DurableConnection,
    scope: TxScope<'tx>,
    update: S,
    row: T,
) -> impl Future<Output = Result<Locked<'tx, T>, DurableError>> + Send + 'conn
where
    S: ExecuteDsl<DurableConnection> + QueryFragment<Db> + Send + 'conn,
    T: Send + 'conn,
    'tx: 'conn,
{
    async move {
        debug_assert!(
            diesel::debug_query::<Db, _>(&update)
                .to_string()
                .starts_with("UPDATE"),
            "lock_by_update needs an UPDATE statement"
        );
        let changed = update.execute(connection).await?;
        if changed == 1 {
            Ok(Locked::new(scope, row))
        } else {
            Err(DurableError::FencedWrite)
        }
    }
}

fn debug_assert_locks<Q: QueryFragment<Db>>(query: &Q) {
    debug_assert!(
        diesel::debug_query::<Db, _>(query)
            .to_string()
            .contains(" FOR UPDATE"),
        "a lock witness needs a FOR UPDATE read"
    );
}
