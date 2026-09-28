use diesel::ExpressionMethods;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};

use super::{TransactionCallback, WorkflowInsert};
use crate::tx::{CommandParent, Locked, Tx};
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
    for<'r> F: AsyncFnOnce(Tx<'r>) -> Result<R, E>
        + TransactionCallback<Tx<'r>, Result<R, E>, Fut: Send>
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

/// A deadlock (1213) or lock wait timeout (1205): the transaction was rolled
/// back and retrying it later is safe. diesel-async maps neither code to a
/// `DatabaseErrorKind` and keeps only the server message, so the message is
/// matched (the server's `lc_messages` must be English, the default).
pub(crate) fn is_transient_error(error: &DurableError) -> bool {
    let DurableError::Database(diesel::result::Error::DatabaseError(kind, info)) = error else {
        return false;
    };
    matches!(
        kind,
        diesel::result::DatabaseErrorKind::SerializationFailure
    ) || {
        let message = info.message();
        message.starts_with("Deadlock found when trying to get lock")
            || message.starts_with("Lock wait timeout exceeded")
    }
}
