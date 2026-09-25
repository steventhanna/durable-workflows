use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;

use crate::{
    persistence::{NewWorkflowEventRow, WorkflowEventRow},
    schema::durable_workflow_event,
    DurableConnection, DurableError, WorkflowId,
};

pub(crate) async fn next_delivery_event(
    connection: &mut DurableConnection,
    workflow_id: WorkflowId,
    delivered_sequence: i32,
) -> Result<Option<WorkflowEventRow>, DurableError> {
    Ok(durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id.get()))
        .filter(durable_workflow_event::delivery_sequence.gt(delivered_sequence))
        .order(durable_workflow_event::delivery_sequence.asc())
        .select(WorkflowEventRow::as_select())
        .first(connection)
        .await
        .optional()?)
}

pub(crate) async fn next_event_sequence(
    connection: &mut DurableConnection,
    workflow_id: WorkflowId,
) -> Result<i32, DurableError> {
    let current = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id.get()))
        .select(diesel::dsl::max(durable_workflow_event::sequence))
        .get_result::<Option<i32>>(connection)
        .await?;
    current
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| DurableError::InvalidState("workflow event sequence overflow".to_string()))
}

pub(crate) async fn append_event(
    connection: &mut DurableConnection,
    event: NewWorkflowEventRow,
) -> Result<(), DurableError> {
    if let Some(delivery_sequence) = event.delivery_sequence {
        crate::trace::touch_event(event.workflow_id, delivery_sequence, &event.event_type);
    }
    diesel::insert_into(durable_workflow_event::table)
        .values(event)
        .execute(connection)
        .await?;
    Ok(())
}
