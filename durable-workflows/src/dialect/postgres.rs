use diesel::OptionalExtension;
use diesel_async::{AsyncConnection, RunQueryDsl};

use super::{is_unique_violation, TransactionCallback, WorkflowInsert};
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
    for<'r> F: AsyncFnOnce(&'r mut DurableConnection) -> Result<R, E>
        + TransactionCallback<&'r mut DurableConnection, Result<R, E>, Fut: Send>
        + Send
        + 'a,
    E: From<diesel::result::Error> + Send + 'a,
    R: Send + 'a,
    'a: 'conn,
{
    connection
        .build_transaction()
        .read_committed()
        .run(async move |connection| crate::trace::scoped(connection, callback).await)
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
) -> Result<Option<i64>, diesel::result::Error> {
    diesel::insert_into(durable_workflow::table)
        .values(row)
        .on_conflict((durable_workflow::kind, durable_workflow::deduplication_key))
        .do_nothing()
        .returning(durable_workflow::id)
        .get_result::<i64>(connection)
        .await
        .optional()
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
    Ok(diesel::insert_into(durable_activity::table)
        .values(row)
        .returning(durable_activity::id)
        .get_result::<i64>(connection)
        .await?)
}

pub(crate) async fn insert_approval(
    connection: &mut DurableConnection,
    row: NewApprovalRow,
) -> Result<i64, DurableError> {
    Ok(diesel::insert_into(durable_approval::table)
        .values(row)
        .returning(durable_approval::id)
        .get_result::<i64>(connection)
        .await?)
}

pub(crate) async fn insert_schedule_run(
    connection: &mut DurableConnection,
    row: NewScheduleRunRow,
) -> Result<i64, DurableError> {
    Ok(diesel::insert_into(durable_schedule_run::table)
        .values(row)
        .returning(durable_schedule_run::id)
        .get_result::<i64>(connection)
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
