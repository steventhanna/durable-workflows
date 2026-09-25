mod activities;
mod events;
mod models;
mod workflows;

pub use models::{
    ActivityAttemptRow, ActivityRow, ApprovalRow, NewActivityAttemptRow, NewActivityRow,
    NewApprovalRow, NewProgressEventRow, NewScheduleRunRow, NewScheduleStateRow, NewTopicLockRow,
    NewWorkflowEventRow, NewWorkflowRow, ProgressEventRow, ScheduleRunRow, ScheduleStateRow,
    TopicLockRow, WorkflowEventRow, WorkflowRow,
};

use crate::{DurableConnection, DurableError};

pub(crate) use events::{append_event, next_delivery_event, next_event_sequence};
pub use workflows::find_workflow_by_id;
pub(crate) use workflows::{
    find_by_deduplication_key, find_workflow_by_id_for_update, insert_started,
    wake_waiting_parents_on_child_terminal,
};

pub fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub async fn database_now_millis(connection: &mut DurableConnection) -> Result<i64, DurableError> {
    let now = crate::dialect::now_millis(connection).await?;
    crate::trace::sample_now(now);
    Ok(now)
}

macro_rules! string_status {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq,
            diesel::AsExpression, diesel::FromSqlRow,
            serde::Serialize, serde::Deserialize,
        )]
        #[diesel(sql_type = diesel::sql_types::Text)]
        pub enum $name {
            $(#[serde(rename = $value)] $variant),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl TryFrom<&str> for $name {
            type Error = DurableError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(DurableError::InvalidState(format!(
                        "unknown {} status {value}",
                        stringify!($name)
                    ))),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl<DB> diesel::deserialize::FromSql<diesel::sql_types::Text, DB> for $name
        where
            DB: diesel::backend::Backend,
            String: diesel::deserialize::FromSql<diesel::sql_types::Text, DB>,
        {
            fn from_sql(value: DB::RawValue<'_>) -> diesel::deserialize::Result<Self> {
                let value = <String as diesel::deserialize::FromSql<diesel::sql_types::Text, DB>>::from_sql(value)?;
                Ok(Self::try_from(value.as_str())?)
            }
        }

        impl<DB> diesel::serialize::ToSql<diesel::sql_types::Text, DB> for $name
        where
            DB: diesel::backend::Backend,
            str: diesel::serialize::ToSql<diesel::sql_types::Text, DB>,
        {
            fn to_sql<'b>(
                &'b self,
                output: &mut diesel::serialize::Output<'b, '_, DB>,
            ) -> diesel::serialize::Result {
                <str as diesel::serialize::ToSql<diesel::sql_types::Text, DB>>::to_sql(
                    self.as_str(),
                    output,
                )
            }
        }
    };
}

string_status!(WorkflowStatus {
    Ready => "ready",
    Running => "running",
    WaitingActivity => "waiting_activity",
    WaitingChild => "waiting_child",
    Sleeping => "sleeping",
    WaitingApproval => "waiting_approval",
    Paused => "paused",
    Blocked => "blocked",
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
});

string_status!(ActivityStatus {
    Pending => "pending",
    Running => "running",
    Succeeded => "succeeded",
    DeadLettered => "dead_lettered",
    Cancelled => "cancelled",
});
pub use activities::find_activity_by_id;
