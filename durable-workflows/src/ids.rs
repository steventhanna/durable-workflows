//! Typed ids. Each id has its own SQL type in [`sql_types`], and the schema
//! declares every id column with it, so a query that compares a column with
//! the wrong kind of id does not compile:
//!
//! ```compile_fail,E0277
//! use diesel::prelude::*;
//! use durable_workflows::{schema::durable_workflow, ActivityId};
//!
//! fn find(activity_id: ActivityId) {
//!     let _ = durable_workflow::table.find(activity_id);
//! }
//! ```
//!
//! A row read from the database is checked on decode: an id that is not
//! positive is a deserialization error, never a panic.

use std::fmt;

use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::expression::AsExpression;
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::BigInt;
use serde::{Deserialize, Serialize};

use crate::{Db, DurableError};

/// SQL types of the id columns. Each is a `BIGINT` on the wire (signed on
/// both backends), distinct only to the type checker.
pub mod sql_types {
    macro_rules! define_id_sql_type {
        ($name:ident) => {
            #[doc = concat!("SQL type of a `", stringify!($name), "` column (a `BIGINT`).")]
            #[derive(
                Debug,
                Clone,
                Copy,
                Default,
                diesel::sql_types::SqlType,
                diesel::query_builder::QueryId,
            )]
            #[diesel(postgres_type(oid = 20, array_oid = 1016))]
            #[diesel(mysql_type(name = "LongLong"))]
            pub struct $name;

            impl diesel::sql_types::SqlOrd for $name {}
        };
    }

    define_id_sql_type!(WorkflowId);
    define_id_sql_type!(ActivityId);
    define_id_sql_type!(ApprovalId);
    define_id_sql_type!(ScheduleRunId);
}

macro_rules! define_id {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            Serialize,
            Deserialize,
            utoipa::ToSchema,
            AsExpression,
            FromSqlRow,
        )]
        #[serde(transparent)]
        #[diesel(sql_type = sql_types::$name)]
        pub struct $name(i64);

        impl FromSql<sql_types::$name, Db> for $name {
            fn from_sql(
                bytes: <Db as diesel::backend::Backend>::RawValue<'_>,
            ) -> deserialize::Result<Self> {
                let value = <i64 as FromSql<BigInt, Db>>::from_sql(bytes)?;
                Ok(Self::new(value)?)
            }
        }

        impl ToSql<sql_types::$name, Db> for $name {
            fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Db>) -> serialize::Result {
                <i64 as ToSql<BigInt, Db>>::to_sql(&self.0, out)
            }
        }

        impl $name {
            pub fn new(value: i64) -> Result<Self, DurableError> {
                if value <= 0 {
                    return Err(DurableError::InvalidId(value));
                }
                Ok(Self(value))
            }

            pub fn get(self) -> i64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

define_id!(WorkflowId);
define_id!(ActivityId);
define_id!(ApprovalId);
define_id!(ScheduleRunId);

/// Marks the id SQL types for [`untyped_id`].
pub(crate) trait IdSqlType: diesel::sql_types::SingleValue {}

impl IdSqlType for sql_types::WorkflowId {}
impl IdSqlType for sql_types::ActivityId {}
impl IdSqlType for sql_types::ApprovalId {}
impl IdSqlType for sql_types::ScheduleRunId {}

/// An id column read as a plain `BIGINT`, for a comparison with
/// `durable_workflow.wait_reference_id`, the one column that may name a
/// workflow, an activity or an approval. The caller filters on `wait_kind`
/// too; `persistence::Wait::parse` is the typed reader of that pair.
pub(crate) fn untyped_id<E>(expression: E) -> UntypedId<E>
where
    E: diesel::Expression,
    E::SqlType: IdSqlType,
{
    UntypedId(expression)
}

#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId, diesel::expression::ValidGrouping)]
pub(crate) struct UntypedId<E>(E);

impl<E> diesel::Expression for UntypedId<E>
where
    E: diesel::Expression,
    E::SqlType: IdSqlType,
{
    type SqlType = BigInt;
}

impl<E, DB> diesel::query_builder::QueryFragment<DB> for UntypedId<E>
where
    DB: diesel::backend::Backend,
    E: diesel::query_builder::QueryFragment<DB>,
{
    fn walk_ast<'b>(
        &'b self,
        pass: diesel::query_builder::AstPass<'_, 'b, DB>,
    ) -> diesel::QueryResult<()> {
        self.0.walk_ast(pass)
    }
}

impl<E, QS> diesel::AppearsOnTable<QS> for UntypedId<E>
where
    E: diesel::AppearsOnTable<QS>,
    Self: diesel::Expression,
{
}

impl<E, QS> diesel::SelectableExpression<QS> for UntypedId<E>
where
    E: diesel::SelectableExpression<QS>,
    Self: diesel::AppearsOnTable<QS>,
{
}
