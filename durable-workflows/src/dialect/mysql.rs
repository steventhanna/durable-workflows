use diesel::ExpressionMethods;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};

use super::{TransactionCallback, WorkflowInsert};
use crate::{
    persistence::{
        NewActivityRow, NewApprovalRow, NewScheduleRunRow, NewScheduleStateRow, NewTopicLockRow,
        NewWorkflowRow,
    },
    schema::{
        durable_activity, durable_approval, durable_schedule_run, durable_schedule_state,
        durable_topic_lock, durable_workflow,
    },
    DurableConnection, DurableError,
};

diesel::define_sql_function! {
    fn last_insert_id() -> diesel::sql_types::Bigint;
}

diesel::define_sql_function! {
    #[sql_name = "LAST_INSERT_ID"]
    fn set_last_insert_id(value: diesel::sql_types::Bigint) -> diesel::sql_types::Bigint;
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
    for<'r> F: AsyncFnOnce(&'r mut DurableConnection) -> Result<R, E>
        + TransactionCallback<&'r mut DurableConnection, Result<R, E>, Fut: Send>
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
        .transaction(async move |connection| crate::trace::scoped(connection, callback).await)
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
        .set(durable_workflow::id.eq(durable_workflow::id + set_last_insert_id(0_i64)))
        .execute(connection)
        .await?;
    let last_id = diesel::select(last_insert_id())
        .get_result::<i64>(connection)
        .await?;
    if last_id > 0 {
        return Ok(WorkflowInsert::Inserted(last_id));
    }
    if has_deduplication_key {
        return Ok(WorkflowInsert::DeduplicationConflict);
    }
    // Without a deduplication key only the restart key can have collided.
    Err(restart_conflict(restarted_from_workflow_id))
}

fn restart_conflict(restarted_from_workflow_id: Option<i64>) -> DurableError {
    match restarted_from_workflow_id {
        Some(id) => DurableError::Conflict(format!("workflow {id} already has a successor")),
        None => DurableError::Conflict(
            "workflow insert conflicted without a deduplication or restart key".to_string(),
        ),
    }
}

pub(crate) async fn insert_activity(
    connection: &mut DurableConnection,
    row: NewActivityRow,
) -> Result<i64, DurableError> {
    diesel::insert_into(durable_activity::table)
        .values(row)
        .execute(connection)
        .await?;
    connection_last_insert_id(connection).await
}

pub(crate) async fn insert_approval(
    connection: &mut DurableConnection,
    row: NewApprovalRow,
) -> Result<i64, DurableError> {
    diesel::insert_into(durable_approval::table)
        .values(row)
        .execute(connection)
        .await?;
    connection_last_insert_id(connection).await
}

pub(crate) async fn insert_schedule_run(
    connection: &mut DurableConnection,
    row: NewScheduleRunRow,
) -> Result<i64, DurableError> {
    diesel::insert_into(durable_schedule_run::table)
        .values(row)
        .execute(connection)
        .await?;
    connection_last_insert_id(connection).await
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
