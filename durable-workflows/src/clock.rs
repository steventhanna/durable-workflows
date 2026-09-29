//! The database clock. Persisted times and due/expiry comparisons use the
//! database's clock, read by [`crate::persistence::database_now_millis`] as a
//! [`DbMillis`]. Process time (`tokio::time::Instant`) is only for local
//! deadlines, timeouts and sleeps, and does not convert to a `DbMillis`.
//!
//! A function that compares with or writes a timestamp column takes a
//! `DbMillis`, so a raw `i64` (for example a host wall-clock stamp) is a type
//! error at the call:
//!
//! ```compile_fail,E0308
//! use durable_workflows::TimerMaterializer;
//!
//! async fn wake(timers: &TimerMaterializer, host_now: i64) {
//!     let _ = timers.wake_one(host_now).await;
//! }
//! ```
//!
//! Durations are added through named methods, never `+`:
//!
//! ```compile_fail,E0369
//! fn later(now: durable_workflows::DbMillis) {
//!     let _ = now + 1;
//! }
//! ```

use std::fmt;
use std::time::Duration;

use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::expression::AsExpression;
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::BigInt;

use crate::{Db, DurableError};

/// The SQL type of persisted database timestamps. Storage stays `BIGINT`.
pub mod sql_types {
    #[derive(
        Debug, Clone, Copy, Default, diesel::sql_types::SqlType, diesel::query_builder::QueryId,
    )]
    #[diesel(postgres_type(oid = 20, array_oid = 1016))]
    #[diesel(mysql_type(name = "LongLong"))]
    pub struct DbMillis;

    impl diesel::sql_types::SqlOrd for DbMillis {}
}

/// Epoch milliseconds on the database clock.
///
/// Values come from [`crate::persistence::database_now_millis`], from a
/// named method on another `DbMillis`, or from
/// [`DbMillis::from_database_millis`] for a value the caller read from a
/// timestamp column. Public APIs that take an absolute time convert it at
/// their storage boundary. Those requested times do not replace a database
/// clock sample for due or lease checks. There is no `From<i64>`, no `Default`
/// and no arithmetic operator, so a host-clock value never becomes one by
/// accident.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    AsExpression,
    FromSqlRow,
    serde::Serialize,
)]
#[diesel(sql_type = sql_types::DbMillis)]
#[serde(transparent)]
pub struct DbMillis(i64);

impl DbMillis {
    /// Convert a caller's or schedule's requested absolute time at the storage
    /// boundary. This value is a target, not a sampled database clock time.
    pub(crate) const fn from_requested_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// A value read from a database timestamp column (or a test fixture for
    /// one). Never pass a host wall-clock stamp: that is the mix this type
    /// exists to prevent.
    pub const fn from_database_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// The persisted epoch milliseconds.
    pub const fn get(self) -> i64 {
        self.0
    }

    /// `self + duration`, or an error when the sum leaves the database range.
    pub fn plus(self, duration: Duration) -> Result<Self, DurableError> {
        let millis = i64::try_from(duration.as_millis()).map_err(|_| {
            DurableError::InvalidDefinition("duration exceeds the database range".to_string())
        })?;
        self.plus_millis(millis)
    }

    /// `self + millis`, or `None` when the sum overflows.
    pub const fn checked_plus_millis(self, millis: i64) -> Option<Self> {
        match self.0.checked_add(millis) {
            Some(sum) => Some(Self(sum)),
            None => None,
        }
    }

    /// `self + millis`, or an error when the sum overflows.
    pub fn plus_millis(self, millis: i64) -> Result<Self, DurableError> {
        self.checked_plus_millis(millis).ok_or_else(|| {
            DurableError::InvalidDefinition("database timestamp overflow".to_string())
        })
    }

    /// `self + millis`, clamped to the `i64` range.
    pub const fn saturating_plus_millis(self, millis: i64) -> Self {
        Self(self.0.saturating_add(millis))
    }

    /// `self - millis`, clamped to the `i64` range.
    pub const fn saturating_minus_millis(self, millis: i64) -> Self {
        Self(self.0.saturating_sub(millis))
    }

    /// Milliseconds from `earlier` to `self`, clamped to the `i64` range
    /// (negative when `earlier` is later).
    pub const fn millis_since(self, earlier: Self) -> i64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl fmt::Display for DbMillis {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromSql<sql_types::DbMillis, Db> for DbMillis {
    fn from_sql(
        bytes: <Db as diesel::backend::Backend>::RawValue<'_>,
    ) -> deserialize::Result<Self> {
        let value = <i64 as FromSql<BigInt, Db>>::from_sql(bytes)?;
        Ok(Self(value))
    }
}

impl ToSql<sql_types::DbMillis, Db> for DbMillis {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Db>) -> serialize::Result {
        <i64 as ToSql<BigInt, Db>>::to_sql(&self.0, out)
    }
}
