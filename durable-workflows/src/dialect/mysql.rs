use diesel::ExpressionMethods;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};

use super::{TransactionCallback, WorkflowInsert};
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

diesel::define_sql_function! {
    fn last_insert_id() -> diesel::sql_types::Bigint;
}

/// Library-owned transaction, pinned to READ COMMITTED.
///
/// Only for a connection the library checked out from its pool: the
/// `SET TRANSACTION` applies to the next transaction and is an error inside
/// an open one.
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
        .batch_execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .await?;
    connection
        .transaction(async move |connection| crate::tx::enter(connection, callback).await)
        .await
}

pub(crate) async fn now_millis(connection: &mut DurableConnection) -> Result<i64, DurableError> {
    // MySQL requires the fractional precision to be a literal, so Diesel's
    // bind-parameter SQL-function macro cannot represent UTC_TIMESTAMP(3).
    let now = diesel::select(diesel::dsl::sql::<diesel::sql_types::Timestamp>(
        "UTC_TIMESTAMP(3)",
    ))
    .get_result::<chrono::NaiveDateTime>(connection)
    .await?;
    Ok(now.and_utc().timestamp_millis())
}

pub(crate) async fn insert_workflow(
    connection: &mut DurableConnection,
    row: NewWorkflowRow,
) -> Result<WorkflowInsert, DurableError> {
    let has_deduplication_key = row.deduplication_key.is_some();
    let restarted_from_workflow_id = row.restarted_from_workflow_id;
    // The duplicate branch zeroes the connection-local insert ID so a
    // conflict is distinguishable from an insert without failing the
    // statement, which keeps the caller's transaction usable.
    diesel::insert_into(durable_workflow::table)
        .values(row)
        .on_conflict(diesel::dsl::DuplicatedKeys)
        .do_update()
        .set(
            durable_workflow::id.eq(diesel::dsl::sql::<crate::ids::sql_types::WorkflowId>(
                "`id` + LAST_INSERT_ID(0)",
            )),
        )
        .execute(connection)
        .await?;
    let last_id = diesel::select(last_insert_id())
        .get_result::<i64>(connection)
        .await?;
    if last_id > 0 {
        return Ok(WorkflowInsert::Inserted(WorkflowId::new(last_id)?));
    }
    if has_deduplication_key {
        return Ok(WorkflowInsert::DeduplicationConflict);
    }
    // Without a deduplication key only the restart key can have collided.
    Err(restart_conflict(restarted_from_workflow_id))
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
    diesel::insert_into(durable_activity::table)
        .values(row)
        .execute(connection)
        .await?;
    ActivityId::new(connection_last_insert_id(connection).await?)
}

/// Takes the locked parent workflow, so the insert cannot run before the
/// fence lock (N4): a stale claim then gets `FencedWrite`, not a duplicate key.
pub(crate) async fn insert_approval<'tx, P: CommandParent + Sync>(
    connection: &mut DurableConnection,
    parent: Locked<'tx, &P>,
    row: NewApprovalRow,
) -> Result<ApprovalId, DurableError> {
    debug_assert_eq!(parent.row().workflow_id(), row.workflow_id);
    diesel::insert_into(durable_approval::table)
        .values(row)
        .execute(connection)
        .await?;
    ApprovalId::new(connection_last_insert_id(connection).await?)
}

pub(crate) async fn insert_schedule_run(
    connection: &mut DurableConnection,
    row: NewScheduleRunRow,
) -> Result<ScheduleRunId, DurableError> {
    diesel::insert_into(durable_schedule_run::table)
        .values(row)
        .execute(connection)
        .await?;
    ScheduleRunId::new(connection_last_insert_id(connection).await?)
}

/// Returns whether exactly this row was inserted; an existing row is never
/// modified.
pub(crate) async fn insert_schedule_state_if_absent(
    connection: &mut DurableConnection,
    row: NewScheduleStateRow,
) -> Result<bool, DurableError> {
    let inserted = diesel::insert_or_ignore_into(durable_schedule_state::table)
        .values(row)
        .execute(connection)
        .await?;
    Ok(inserted == 1)
}

pub(crate) async fn insert_topic_locks_if_absent(
    connection: &mut DurableConnection,
    rows: &[NewTopicLockRow],
) -> Result<(), DurableError> {
    diesel::insert_or_ignore_into(durable_topic_lock::table)
        .values(rows)
        .execute(connection)
        .await?;
    Ok(())
}

async fn connection_last_insert_id(
    connection: &mut DurableConnection,
) -> Result<i64, DurableError> {
    Ok(diesel::select(last_insert_id())
        .get_result::<i64>(connection)
        .await?)
}

/// Classifies a MySQL error for [`DurableError::transience`]. diesel-async
/// keeps only the server message of a server error (no error code), and the
/// `Display` text of a client I/O or driver error, so both are matched by
/// message (the server's `lc_messages` must be English, the default).
///
/// - `RolledBack`: a deadlock (1213), lock wait timeout (1205) or
///   serialization failure.
/// - `Unavailable`: a lost or refused connection (mysql_async I/O errors
///   except TLS, "Connection to the server is closed"); a server that is
///   shutting down (1053, 1077) or read-only after a failover (1290, 1792);
///   a proxy that reports a lost server ("server has gone away", "Lost
///   connection"); Vitess while a tablet restarts or reparents ("primary is
///   not serving", `code = Unavailable`, "connection refused").
pub(crate) fn database_error_transience(
    kind: &diesel::result::DatabaseErrorKind,
    message: &str,
) -> Option<Transience> {
    use diesel::result::DatabaseErrorKind;

    const ROLLED_BACK_PREFIXES: [&str; 2] = [
        "Deadlock found when trying to get lock",
        "Lock wait timeout exceeded",
    ];
    // Matched case-insensitively: proxies reword the client texts.
    const UNAVAILABLE_MARKERS: [&str; 10] = [
        "connection to the server is closed",
        "server shutdown in progress",
        "normal shutdown",
        "is running with the --read-only option",
        "is running with the --super-read-only option",
        "server has gone away",
        "lost connection to mysql server",
        "primary is not serving",
        "code = unavailable",
        "connection refused",
    ];
    match kind {
        DatabaseErrorKind::SerializationFailure => return Some(Transience::RolledBack),
        DatabaseErrorKind::ClosedConnection
        | DatabaseErrorKind::UnableToSendCommand
        | DatabaseErrorKind::ReadOnlyTransaction => return Some(Transience::Unavailable),
        _ => {}
    }
    if ROLLED_BACK_PREFIXES
        .iter()
        .any(|prefix| message.starts_with(prefix))
    {
        return Some(Transience::RolledBack);
    }
    let io_error = message.starts_with("Input/output error: ") && !message.contains("TLS error");
    let lowercase = message.to_ascii_lowercase();
    let unavailable = io_error
        || UNAVAILABLE_MARKERS
            .iter()
            .any(|marker| lowercase.contains(marker));
    unavailable.then_some(Transience::Unavailable)
}

#[cfg(test)]
mod tests {
    use diesel::result::DatabaseErrorKind::{self, Unknown};

    use super::*;

    fn classify(kind: DatabaseErrorKind, message: &str) -> Option<Transience> {
        database_error_transience(&kind, message)
    }

    #[test]
    fn rolled_back_transactions_are_transient() {
        for message in [
            "Deadlock found when trying to get lock; try restarting transaction",
            "Lock wait timeout exceeded; try restarting transaction",
        ] {
            assert_eq!(classify(Unknown, message), Some(Transience::RolledBack));
        }
    }

    #[test]
    fn an_unreachable_or_failing_over_server_is_unavailable() {
        for message in [
            // Issue #4: a PlanetScale tablet restart, then its reparent.
            "target: leasing.-.primary: vttablet: rpc error: code = Unavailable desc = \
             connection error: desc = \"transport: Error while dialing: dial tcp \
             10.0.0.1:15991: connect: connection refused\"",
            "primary is not serving, there may be a reparent operation in progress",
            // mysql_async client errors, as diesel-async formats them.
            "Input/output error: Input/output error: Connection refused (os error 111)",
            "Input/output error: Input/output error: Connection reset by peer (os error 104)",
            "Driver error: `Connection to the server is closed.'",
            "MySQL server has gone away",
            "Lost connection to MySQL server during query",
            "Server shutdown in progress",
            "The MySQL server is running with the --super-read-only option so it cannot \
             execute this statement",
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
                "Cannot execute statement in a READ ONLY transaction."
            ),
            Some(Transience::Unavailable)
        );
    }

    #[test]
    fn other_errors_are_not_transient() {
        for message in [
            "Table 'dwt_1.durable_approval' doesn't exist",
            "Access denied for user 'durable'@'10.0.0.1' (using password: YES)",
            "Input/output error: TLS error: `invalid peer certificate'",
            "Unknown column 'nope' in 'field list'",
        ] {
            assert_eq!(classify(Unknown, message), None, "{message}");
        }
        assert_eq!(
            classify(
                DatabaseErrorKind::UniqueViolation,
                "Duplicate entry '1' for key 'PRIMARY'"
            ),
            None
        );
    }
}
