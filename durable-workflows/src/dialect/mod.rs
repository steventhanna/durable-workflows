//! Backend-specific SQL behind named persistence operations.
//!
//! Each backend module exposes the same functions with concrete row types, so
//! the rest of the crate never names a backend.

#[cfg(all(feature = "mysql", feature = "postgres"))]
compile_error!(
    "durable-workflows: enable exactly one of `mysql` or `postgres` (use `default-features = false`)"
);
#[cfg(not(any(feature = "mysql", feature = "postgres")))]
compile_error!("durable-workflows: enable the `mysql` or `postgres` feature");
// `fake-clock` lets a session override the database clock. `debug_assertions`
// is on in the dev and test profiles and off in release, and unlike
// `cfg(test)` it is set for integration tests and downstream crates, so a
// release build that enables the feature fails. Turning debug assertions on
// in a release profile is an explicit opt-in that this does not catch.
#[cfg(all(feature = "fake-clock", not(debug_assertions)))]
compile_error!(
    "durable-workflows: the test-only `fake-clock` feature must not reach a release build \
     (it lets a session override the database clock)"
);
// `trace-model` is test-only for the same reason: it installs triggers on the
// durable tables and writes a trace row in every engine transaction.
#[cfg(all(feature = "trace-model", not(debug_assertions)))]
compile_error!(
    "durable-workflows: the test-only `trace-model` feature must not reach a release build \
     (it records every engine transaction into trace tables)"
);

#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "mysql")]
pub(crate) use mysql::*;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
pub(crate) use postgres::*;

pub(crate) enum WorkflowInsert {
    Inserted(crate::WorkflowId),
    DeduplicationConflict,
}

/// Local copy of diesel-async's `AsyncFunc` callback bound
/// (diesel-async-0.9.2/src/transaction_manager.rs:36-48), which diesel-async
/// does not export.
pub(crate) trait TransactionCallback<T, R>:
    AsyncFnOnce(T) -> R + FnOnce(T) -> <Self as TransactionCallback<T, R>>::Fut
{
    type Fut: std::future::Future<Output = R>;
}

impl<F, T, Fut, R> TransactionCallback<T, R> for F
where
    F: AsyncFnOnce(T) -> R + FnOnce(T) -> Fut,
    Fut: std::future::Future<Output = R>,
{
    type Fut = Fut;
}

// Only Postgres reports restart-key collisions as unique violations.
#[cfg_attr(feature = "mysql", allow(dead_code))]
pub(crate) fn is_unique_violation(error: &diesel::result::Error) -> bool {
    matches!(
        error,
        diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _)
    )
}
