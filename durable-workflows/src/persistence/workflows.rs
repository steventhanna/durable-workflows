use diesel::{
    BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
};
use diesel_async::RunQueryDsl;

use crate::{
    dialect::{self, WorkflowInsert},
    persistence::{self, NewWorkflowEventRow, NewWorkflowRow, WorkflowRow, WorkflowStatus},
    schema::{durable_workflow, durable_workflow_event},
    ChildResult, DurableConnection, DurableError, WorkflowEvent, WorkflowId,
};

pub(crate) async fn find_by_deduplication_key(
    connection: &mut DurableConnection,
    kind: &str,
    key: &str,
) -> Result<Option<WorkflowRow>, DurableError> {
    Ok(durable_workflow::table
        .filter(durable_workflow::kind.eq(kind))
        .filter(durable_workflow::deduplication_key.eq(key))
        .select(WorkflowRow::as_select())
        .first(connection)
        .await
        .optional()?)
}

pub(crate) async fn insert_started(
    connection: &mut DurableConnection,
    workflow: NewWorkflowRow,
) -> Result<(i64, bool), DurableError> {
    let kind = workflow.kind.clone();
    let deduplication_key = workflow.deduplication_key.clone();
    match dialect::insert_workflow(connection, workflow).await? {
        WorkflowInsert::Inserted(id) => {
            diesel::insert_into(durable_workflow_event::table)
                .values(NewWorkflowEventRow {
                    workflow_id: id,
                    sequence: 1,
                    delivery_sequence: Some(1),
                    event_type: "started".to_string(),
                    metadata_json: None,
                    actor_type: Some("system".to_string()),
                    actor_id: None,
                    reason: None,
                    created_at: crate::persistence::database_now_millis(connection).await?,
                })
                .execute(connection)
                .await?;
            crate::trace::touch_wf(id);
            crate::trace::touch_event(id, 1, "started");
            Ok((id, true))
        }
        WorkflowInsert::DeduplicationConflict => {
            let key = deduplication_key.as_deref().ok_or_else(|| {
                DurableError::InvalidState(
                    "workflow deduplication conflict without a deduplication key".to_string(),
                )
            })?;
            match find_by_deduplication_key_for_update(connection, &kind, key).await? {
                Some(row) => Ok((row.id, false)),
                None => Err(DurableError::Conflict(
                    "duplicate start raced with an uncommitted insert outside READ COMMITTED; retry"
                        .to_string(),
                )),
            }
        }
    }
}

async fn find_by_deduplication_key_for_update(
    connection: &mut DurableConnection,
    kind: &str,
    key: &str,
) -> Result<Option<WorkflowRow>, DurableError> {
    Ok(durable_workflow::table
        .filter(durable_workflow::kind.eq(kind))
        .filter(durable_workflow::deduplication_key.eq(key))
        .for_update()
        .select(WorkflowRow::as_select())
        .first(connection)
        .await
        .optional()?)
}

pub async fn find_workflow_by_id(
    connection: &mut DurableConnection,
    workflow_id: crate::WorkflowId,
) -> Result<WorkflowRow, DurableError> {
    Ok(durable_workflow::table
        .find(workflow_id.get())
        .select(WorkflowRow::as_select())
        .first(connection)
        .await?)
}

/// Current-read lock of a workflow row for use inside an open transaction.
///
/// An unlocked read can miss a concurrent terminal commit (one still in flight,
/// or under REPEATABLE READ one that committed after the snapshot). Callers
/// that establish a parent wait on a deduplicated child must lock that child
/// before rechecking its status.
pub async fn find_workflow_by_id_for_update(
    connection: &mut DurableConnection,
    workflow_id: crate::WorkflowId,
) -> Result<WorkflowRow, DurableError> {
    Ok(durable_workflow::table
        .find(workflow_id.get())
        .for_update()
        .select(WorkflowRow::as_select())
        .first(connection)
        .await?)
}

/// Delivers a terminal child outcome to every parent awaiting that child.
///
/// Parents are discovered by `wait_reference_id` rather than the child's
/// `parent_workflow_id`, so a domain-keyed child that multiple parents await
/// wakes each waiter. A parent that no longer waits on this child (for
/// example after an operator cancelled or restarted it) is skipped, so a
/// child's own terminal commit can never be blocked by parent state.
pub(crate) async fn wake_waiting_parents_on_child_terminal(
    connection: &mut DurableConnection,
    child_workflow_id: i64,
    child_kind: &str,
    child_version: i32,
    outcome: Result<String, (String, String)>,
    now: i64,
) -> Result<(), DurableError> {
    let parents = durable_workflow::table
        .filter(durable_workflow::wait_kind.eq("child"))
        .filter(durable_workflow::wait_reference_id.eq(child_workflow_id))
        .filter(
            durable_workflow::status
                .eq(WorkflowStatus::WaitingChild)
                .or(durable_workflow::status.eq(WorkflowStatus::Paused)),
        )
        .for_update()
        .select(WorkflowRow::as_select())
        .load::<WorkflowRow>(connection)
        .await?;
    for parent in parents {
        wake_loaded_parent_on_child_terminal(
            connection,
            parent,
            child_workflow_id,
            child_kind,
            child_version,
            &outcome,
            now,
        )
        .await?;
    }
    Ok(())
}

async fn wake_loaded_parent_on_child_terminal(
    connection: &mut DurableConnection,
    parent: WorkflowRow,
    child_workflow_id: i64,
    child_kind: &str,
    child_version: i32,
    outcome: &Result<String, (String, String)>,
    now: i64,
) -> Result<(), DurableError> {
    if !matches!(
        parent.status,
        WorkflowStatus::WaitingChild | WorkflowStatus::Paused
    ) || parent.wait_kind.as_deref() != Some("child")
        || parent.wait_reference_id != Some(child_workflow_id)
    {
        return Ok(());
    }
    let command_sequence = u32::try_from(parent.command_sequence).map_err(|_| {
        DurableError::InvalidState(format!(
            "workflow {} has a negative command sequence",
            parent.id
        ))
    })?;
    let (event_type, event) = match outcome {
        Ok(output_json) => (
            "child_succeeded",
            WorkflowEvent::ChildSucceeded {
                command_sequence,
                result: ChildResult::new(child_kind, child_version, output_json.clone())?,
            },
        ),
        Err((category, message)) => (
            "child_failed",
            WorkflowEvent::ChildFailed {
                command_sequence,
                kind: child_kind.to_string(),
                version: child_version,
                category: category.clone(),
                message: message.clone(),
            },
        ),
    };
    let delivery_sequence = parent
        .delivered_event_sequence
        .checked_add(1)
        .ok_or_else(|| {
            DurableError::InvalidState("workflow delivery sequence overflow".to_string())
        })?;
    let parent_id = WorkflowId::new(parent.id)?;
    let sequence = persistence::next_event_sequence(connection, parent_id).await?;
    crate::trace::touch_wf(parent.id);
    // The woken event's reference (the child); the event itself is touched on append.
    crate::trace::note(
        "woke",
        || serde_json::json!({ parent.id.to_string(): child_workflow_id }),
    );
    persistence::append_event(
        connection,
        NewWorkflowEventRow {
            workflow_id: parent.id,
            sequence,
            delivery_sequence: Some(delivery_sequence),
            event_type: event_type.to_string(),
            metadata_json: Some(serde_json::to_string(&event)?),
            actor_type: Some("system".to_string()),
            actor_id: None,
            reason: None,
            created_at: now,
        },
    )
    .await?;
    let next_status = if parent.status == WorkflowStatus::Paused {
        WorkflowStatus::Paused
    } else {
        WorkflowStatus::Ready
    };
    let changed = diesel::update(
        durable_workflow::table
            .find(parent.id)
            .filter(durable_workflow::status.eq(&parent.status))
            .filter(durable_workflow::wait_kind.eq(parent.wait_kind.clone()))
            .filter(durable_workflow::wait_reference_id.eq(parent.wait_reference_id)),
    )
    .set((
        durable_workflow::status.eq(next_status),
        durable_workflow::wait_kind.eq(None::<String>),
        durable_workflow::wait_reference_id.eq(None::<i64>),
        durable_workflow::available_at.eq(now),
        durable_workflow::updated_at.eq(now),
    ))
    .execute(connection)
    .await?;
    if changed == 1 {
        Ok(())
    } else {
        Err(DurableError::FencedWrite)
    }
}
