use diesel::OptionalExtension;
use diesel_async::{AsyncConnection, RunQueryDsl};

use super::{is_unique_violation, TransactionCallback, WorkflowInsert};
use crate::error::Transience;
use crate::tx::{CommandParent, Committed, Locked, Tx};
use crate::{
    persistence::{
        NewActivityRow, NewApprovalRow, NewScheduleRunRow, NewScheduleStateRow, NewTopicLockRow,
        NewWorkflowRow,
    },
    schema::{
        durable_activity, durable_approval, durable_schedule_run, durable_schedule_state,
        durable_topic_lock, durable_workflow,
    },
    ActivityId, ApprovalId, DurableConnection, DurableError, ScheduleRunId, WorkflowId,
};

// `now()` is the transaction start time; callers sample the clock after
// waiting on row locks, so the clock must be read at call time.
#[cfg(not(feature = "fake-clock"))]
const NOW_SQL: &str = "clock_timestamp() AT TIME ZONE 'UTC'";

// Tests freeze the clock per session with
// `SELECT set_config('durable.fake_now_millis', '<millis>', false)`.
#[cfg(feature = "fake-clock")]
const NOW_SQL: &str = "COALESCE(\
     to_timestamp(NULLIF(current_setting('durable.fake_now_millis', true), '')::bigint / 1000.0) \
     AT TIME ZONE 'UTC', \
     clock_timestamp() AT TIME ZONE 'UTC')";

/// Library-owned transaction, pinned to READ COMMITTED.
pub(crate) async fn transaction<'a, 'conn, R, E, F>(
    connection: &'conn mut DurableConnection,
    callback: F,
) -> Result<R, E>
where
    for<'r> F: AsyncFnOnce(Tx<'r>) -> Result<Committed<R>, E>
        + TransactionCallback<Tx<'r>, Result<Committed<R>, E>, Fut: Send>
        + Send
        + 'a,
    E: From<diesel::result::Error> + Send + 'a,
    R: Send + 'a,
    'a: 'conn,
{
    connection
        .build_transaction()
        .read_committed()
        .run(async move |connection| crate::tx::enter(connection, callback).await)
        .await
}

pub(crate) async fn now_millis(connection: &mut DurableConnection) -> Result<i64, DurableError> {
    let now = diesel::select(diesel::dsl::sql::<diesel::sql_types::Timestamp>(NOW_SQL))
        .get_result::<chrono::NaiveDateTime>(connection)
        .await?;
    Ok(now.and_utc().timestamp_millis())
}

pub(crate) async fn insert_workflow(
    connection: &mut DurableConnection,
    row: NewWorkflowRow,
) -> Result<WorkflowInsert, DurableError> {
    let restarted_from_workflow_id = row.restarted_from_workflow_id;
    // Only the restart key can raise a unique violation here, and a failed
    // statement aborts the whole Postgres transaction, so that insert runs
    // in a savepoint to keep the caller's transaction usable.
    let inserted = if restarted_from_workflow_id.is_some() {
        connection
            .transaction(async move |connection| insert_workflow_row(connection, row).await)
            .await
    } else {
        insert_workflow_row(connection, row).await
    };
    match inserted {
        Ok(Some(id)) => Ok(WorkflowInsert::Inserted(id)),
        Ok(None) => Ok(WorkflowInsert::DeduplicationConflict),
        Err(error) if is_unique_violation(&error) => {
            Err(restart_conflict(restarted_from_workflow_id))
        }
        Err(error) => Err(error.into()),
    }
}

async fn insert_workflow_row(
    connection: &mut DurableConnection,
    row: NewWorkflowRow,
) -> Result<Option<WorkflowId>, diesel::result::Error> {
    diesel::insert_into(durable_workflow::table)
        .values(row)
        .on_conflict((durable_workflow::kind, durable_workflow::deduplication_key))
        .do_nothing()
        .returning(durable_workflow::id)
        .get_result::<WorkflowId>(connection)
        .await
        .optional()
}

fn restart_conflict(restarted_from_workflow_id: Option<WorkflowId>) -> DurableError {
    match restarted_from_workflow_id {
        Some(id) => DurableError::Conflict(format!("workflow {id} already has a successor")),
        None => DurableError::Conflict(
            "workflow insert conflicted without a deduplication or restart key".to_string(),
        ),
    }
}

/// Takes the locked parent workflow, so the insert cannot run before the
/// fence lock (N4): a stale claim then gets `FencedWrite`, not a duplicate key.
pub(crate) async fn insert_activity<'tx, P: CommandParent + Sync>(
    connection: &mut DurableConnection,
    parent: Locked<'tx, &P>,
    row: NewActivityRow,
) -> Result<ActivityId, DurableError> {
    debug_assert_eq!(parent.row().workflow_id(), row.workflow_id);
    Ok(diesel::insert_into(durable_activity::table)
        .values(row)
        .returning(durable_activity::id)
        .get_result::<ActivityId>(connection)
        .await?)
}

/// Takes the locked parent workflow, so the insert cannot run before the
/// fence lock (N4): a stale claim then gets `FencedWrite`, not a duplicate key.
pub(crate) async fn insert_approval<'tx, P: CommandParent + Sync>(
    connection: &mut DurableConnection,
    parent: Locked<'tx, &P>,
    row: NewApprovalRow,
) -> Result<ApprovalId, DurableError> {
    debug_assert_eq!(parent.row().workflow_id(), row.workflow_id);
    Ok(diesel::insert_into(durable_approval::table)
        .values(row)
        .returning(durable_approval::id)
        .get_result::<ApprovalId>(connection)
        .await?)
}

pub(crate) async fn insert_schedule_run(
    connection: &mut DurableConnection,
    row: NewScheduleRunRow,
) -> Result<ScheduleRunId, DurableError> {
    Ok(diesel::insert_into(durable_schedule_run::table)
        .values(row)
        .returning(durable_schedule_run::id)
        .get_result::<ScheduleRunId>(connection)
        .await?)
}

/// Returns whether exactly this row was inserted; an existing row is never
/// modified.
pub(crate) async fn insert_schedule_state_if_absent(
    connection: &mut DurableConnection,
    row: NewScheduleStateRow,
) -> Result<bool, DurableError> {
    let inserted = diesel::insert_into(durable_schedule_state::table)
        .values(row)
        .on_conflict(durable_schedule_state::schedule_key)
        .do_nothing()
        .execute(connection)
        .await?;
    Ok(inserted == 1)
}

pub(crate) async fn insert_topic_locks_if_absent(
    connection: &mut DurableConnection,
    rows: &[NewTopicLockRow],
) -> Result<(), DurableError> {
    diesel::insert_into(durable_topic_lock::table)
        .values(rows)
        .on_conflict(durable_topic_lock::topic)
        .do_nothing()
        .execute(connection)
        .await?;
    Ok(())
}

/// Classifies a Postgres error for [`DurableError::transience`].
/// diesel-async maps only a few SQLSTATEs to a `DatabaseErrorKind` and does
/// not expose the code, so the rest are matched by the server message (the
/// server's `lc_messages` must be English, the default). An error without a
/// SQLSTATE (I/O, a closed connection, a failed connect) arrives as
/// `UnableToSendCommand` with the tokio-postgres `Display` text.
///
/// - `RolledBack`: a serialization failure (40001), deadlock (40P01) or
///   lock timeout (55P03).
/// - `Unavailable`: a lost or refused connection; a server that is starting,
///   stopping or in recovery (57P01, 57P02, 57P03); a read-only former
///   primary after a failover (25006); too many connections (53300).
pub(crate) fn database_error_transience(
    kind: &diesel::result::DatabaseErrorKind,
    message: &str,
) -> Option<Transience> {
    use diesel::result::DatabaseErrorKind;

    // tokio-postgres `Kind::Io`, `Closed`, `Connect` and `Timeout`.
    const CLIENT_UNAVAILABLE_PREFIXES: [&str; 4] = [
        "error communicating with the server",
        "connection closed",
        "error connecting to server",
        "timeout waiting for server",
    ];
    const SERVER_UNAVAILABLE_PREFIXES: [&str; 5] = [
        "terminating connection due to administrator command",
        "terminating connection because of crash of another server process",
        "the database system is",
        "sorry, too many clients already",
        "remaining connection slots are reserved",
    ];
    match kind {
        DatabaseErrorKind::SerializationFailure => Some(Transience::RolledBack),
        DatabaseErrorKind::ClosedConnection | DatabaseErrorKind::ReadOnlyTransaction => {
            Some(Transience::Unavailable)
        }
        DatabaseErrorKind::UnableToSendCommand => CLIENT_UNAVAILABLE_PREFIXES
            .iter()
            .any(|prefix| message.starts_with(prefix))
            .then_some(Transience::Unavailable),
        _ if message == "deadlock detected"
            || message == "canceling statement due to lock timeout" =>
        {
            Some(Transience::RolledBack)
        }
        _ => SERVER_UNAVAILABLE_PREFIXES
            .iter()
            .any(|prefix| message.starts_with(prefix))
            .then_some(Transience::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use diesel::result::DatabaseErrorKind::{self, UnableToSendCommand, Unknown};

    use super::*;

    fn classify(kind: DatabaseErrorKind, message: &str) -> Option<Transience> {
        database_error_transience(&kind, message)
    }

    #[test]
    fn rolled_back_transactions_are_transient() {
        assert_eq!(
            classify(
                DatabaseErrorKind::SerializationFailure,
                "could not serialize access due to concurrent update"
            ),
            Some(Transience::RolledBack)
        );
        for message in [
            "deadlock detected",
            "canceling statement due to lock timeout",
        ] {
            assert_eq!(classify(Unknown, message), Some(Transience::RolledBack));
        }
    }

    #[test]
    fn an_unreachable_or_failing_over_server_is_unavailable() {
        // tokio-postgres errors without a SQLSTATE.
        for message in [
            "error connecting to server: Connection refused (os error 111)",
            "error communicating with the server: Connection reset by peer (os error 104)",
            "connection closed",
            "timeout waiting for server",
        ] {
            assert_eq!(
                classify(UnableToSendCommand, message),
                Some(Transience::Unavailable),
                "{message}"
            );
        }
        for message in [
            "terminating connection due to administrator command",
            "the database system is starting up",
            "the database system is shutting down",
            "the database system is in recovery mode",
            "sorry, too many clients already",
        ] {
            assert_eq!(
                classify(Unknown, message),
                Some(Transience::Unavailable),
                "{message}"
            );
        }
        assert_eq!(
            classify(
                DatabaseErrorKind::ReadOnlyTransaction,
                "cannot execute UPDATE in a read-only transaction"
            ),
            Some(Transience::Unavailable)
        );
    }

    #[test]
    fn other_errors_are_not_transient() {
        for message in [
            "relation \"durable_approval\" does not exist",
            "password authentication failed for user \"durable\"",
        ] {
            assert_eq!(classify(Unknown, message), None, "{message}");
        }
        assert_eq!(
            classify(UnableToSendCommand, "error performing TLS handshake"),
            None
        );
        assert_eq!(
            classify(
                DatabaseErrorKind::UniqueViolation,
                "duplicate key value violates unique constraint"
            ),
            None
        );
    }
}
