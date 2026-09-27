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
pub(crate) use models::LeaseCleared;
pub use workflows::find_workflow_by_id;
pub(crate) use workflows::{
    find_by_deduplication_key, find_workflow_by_id_for_update, insert_started,
    wake_waiting_parents_on_child_terminal, StartedInsert,
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
        #[non_exhaustive]
        pub enum $name {
            $(#[serde(rename = $value)] $variant),+
        }

        impl $name {
            /// Every variant, generated from the declaration, so the status-set
            /// checks below cannot miss one.
            const ALL: &'static [Self] = &[$(Self::$variant),+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }

            /// `true` when `set` lists `status`. `const` so a status-set
            /// constant can be checked against its predicate at compile time.
            const fn set_contains(set: &[Self], status: Self) -> bool {
                let mut index = 0;
                while index < set.len() {
                    if set[index] as usize == status as usize {
                        return true;
                    }
                    index += 1;
                }
                false
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

/// Fails the build unless the status-set constant `$set` lists exactly the
/// statuses for which the exhaustive predicate `$predicate` is `true`.
macro_rules! status_set_matches_predicate {
    ($name:ident :: $set:ident == $predicate:ident) => {
        const _: () = {
            let mut index = 0;
            while index < $name::ALL.len() {
                let status = $name::ALL[index];
                assert!(
                    $name::set_contains(&$name::$set, status) == status.$predicate(),
                    concat!(
                        stringify!($name),
                        "::",
                        stringify!($set),
                        " disagrees with ",
                        stringify!($predicate)
                    )
                );
                index += 1;
            }
        };
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
    Cancelling => "cancelling",
    Succeeded => "succeeded",
    DeadLettered => "dead_lettered",
    Cancelled => "cancelled",
});

// Status membership lives in the exhaustive predicates and constants below;
// code elsewhere uses them instead of listing statuses, so adding a variant
// is a compile error at each predicate that must decide (and each constant
// is checked against its predicate at compile time).

impl WorkflowStatus {
    /// Statuses a workflow never leaves.
    pub(crate) const TERMINAL: [Self; 3] = [Self::Succeeded, Self::Failed, Self::Cancelled];
    /// Statuses in which a workflow may hold a `child` wait: waiting, or
    /// paused while waiting (resume restores `WaitingChild`).
    pub(crate) const CHILD_WAITERS: [Self; 2] = [Self::WaitingChild, Self::Paused];
    /// Statuses in which a workflow may hold an `approval` wait.
    pub(crate) const APPROVAL_WAITERS: [Self; 2] = [Self::WaitingApproval, Self::Paused];
    /// Statuses in which a workflow may still wait on a dead-lettered
    /// activity that an operator can retry: blocked, or paused while blocked
    /// (resume restores `Blocked`).
    pub(crate) const DEAD_LETTER_WAITERS: [Self; 2] = [Self::Blocked, Self::Paused];

    pub(crate) const fn is_terminal(self) -> bool {
        match self {
            Self::Succeeded | Self::Failed | Self::Cancelled => true,
            Self::Ready
            | Self::Running
            | Self::WaitingActivity
            | Self::WaitingChild
            | Self::Sleeping
            | Self::WaitingApproval
            | Self::Paused
            | Self::Blocked => false,
        }
    }

    pub(crate) const fn awaits_child(self) -> bool {
        match self {
            Self::WaitingChild | Self::Paused => true,
            Self::Ready
            | Self::Running
            | Self::WaitingActivity
            | Self::Sleeping
            | Self::WaitingApproval
            | Self::Blocked
            | Self::Succeeded
            | Self::Failed
            | Self::Cancelled => false,
        }
    }

    pub(crate) const fn awaits_approval(self) -> bool {
        match self {
            Self::WaitingApproval | Self::Paused => true,
            Self::Ready
            | Self::Running
            | Self::WaitingActivity
            | Self::WaitingChild
            | Self::Sleeping
            | Self::Blocked
            | Self::Succeeded
            | Self::Failed
            | Self::Cancelled => false,
        }
    }

    pub(crate) const fn awaits_dead_letter(self) -> bool {
        match self {
            Self::Blocked | Self::Paused => true,
            Self::Ready
            | Self::Running
            | Self::WaitingActivity
            | Self::WaitingChild
            | Self::Sleeping
            | Self::WaitingApproval
            | Self::Succeeded
            | Self::Failed
            | Self::Cancelled => false,
        }
    }

    /// Source statuses an operator restart accepts: terminal, paused or
    /// blocked (N3).
    pub(crate) const fn is_restartable(self) -> bool {
        match self {
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Paused | Self::Blocked => true,
            Self::Ready
            | Self::Running
            | Self::WaitingActivity
            | Self::WaitingChild
            | Self::Sleeping
            | Self::WaitingApproval => false,
        }
    }

    /// Statuses of the newest generation from which
    /// `start_or_restart_recoverable` starts a new generation instead of
    /// returning the existing one.
    pub(crate) const fn is_start_recoverable(self) -> bool {
        match self {
            Self::Failed | Self::Blocked => true,
            Self::Ready
            | Self::Running
            | Self::WaitingActivity
            | Self::WaitingChild
            | Self::Sleeping
            | Self::WaitingApproval
            | Self::Paused
            | Self::Succeeded
            | Self::Cancelled => false,
        }
    }
}

status_set_matches_predicate!(WorkflowStatus::TERMINAL == is_terminal);
status_set_matches_predicate!(WorkflowStatus::CHILD_WAITERS == awaits_child);
status_set_matches_predicate!(WorkflowStatus::APPROVAL_WAITERS == awaits_approval);
status_set_matches_predicate!(WorkflowStatus::DEAD_LETTER_WAITERS == awaits_dead_letter);

impl ActivityStatus {
    /// Statuses that count against the topic concurrency cap (they own an
    /// open attempt). A `cancelling` row keeps its slot until its revoked
    /// handler stops (N2).
    pub(crate) const SLOT_HOLDERS: [Self; 2] = [Self::Running, Self::Cancelling];
    /// Statuses whose row carries a lease that heartbeats renew and that
    /// lease-expiry reconciliation recovers.
    pub(crate) const LEASE_HOLDERS: [Self; 2] = [Self::Running, Self::Cancelling];
    /// Statuses a row may still leave: the activity can still run, and
    /// workflow cancellation must settle it.
    pub(crate) const NON_TERMINAL: [Self; 3] = [Self::Pending, Self::Running, Self::Cancelling];

    pub(crate) const fn holds_slot(self) -> bool {
        match self {
            Self::Running | Self::Cancelling => true,
            Self::Pending | Self::Succeeded | Self::DeadLettered | Self::Cancelled => false,
        }
    }

    pub(crate) const fn holds_lease(self) -> bool {
        match self {
            Self::Running | Self::Cancelling => true,
            Self::Pending | Self::Succeeded | Self::DeadLettered | Self::Cancelled => false,
        }
    }

    /// Statuses a row never leaves. A dead-lettered row is terminal: an
    /// operator retry inserts a new row.
    pub(crate) const fn is_terminal(self) -> bool {
        match self {
            Self::Succeeded | Self::DeadLettered | Self::Cancelled => true,
            Self::Pending | Self::Running | Self::Cancelling => false,
        }
    }

    const fn is_non_terminal(self) -> bool {
        !self.is_terminal()
    }
}

status_set_matches_predicate!(ActivityStatus::SLOT_HOLDERS == holds_slot);
status_set_matches_predicate!(ActivityStatus::LEASE_HOLDERS == holds_lease);
status_set_matches_predicate!(ActivityStatus::NON_TERMINAL == is_non_terminal);

pub use activities::find_activity_by_id;
