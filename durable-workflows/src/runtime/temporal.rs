use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;

use crate::{
    error::ensure_size,
    persistence::{self, ApprovalRow, NewWorkflowEventRow, WorkflowRow, WorkflowStatus},
    schema::{durable_approval, durable_workflow},
    ApprovalId, DurableError, DurablePool, WorkflowEvent, WorkflowId, MAX_EVENT_METADATA_BYTES,
};

pub struct TimerMaterializer {
    pool: DurablePool,
}

impl TimerMaterializer {
    pub fn new(pool: DurablePool) -> Self {
        Self { pool }
    }

    pub async fn wake_one(&self, now: i64) -> Result<Option<WorkflowId>, DurableError> {
        self.materialize_one(now).await
    }

    pub async fn materialize_next(&self) -> Result<Option<WorkflowId>, DurableError> {
        let mut connection = self.pool.get().await?;
        let now = persistence::database_now_millis(&mut connection).await?;
        drop(connection);
        self.materialize_one(now).await
    }

    pub async fn materialize_one(&self, now: i64) -> Result<Option<WorkflowId>, DurableError> {
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            let Some(workflow) = durable_workflow::table
                .filter(durable_workflow::status.eq(WorkflowStatus::Sleeping))
                .filter(durable_workflow::wait_kind.eq("timer"))
                .filter(durable_workflow::available_at.le(now))
                .order((
                    durable_workflow::available_at.asc(),
                    durable_workflow::id.asc(),
                ))
                .for_update()
                .skip_locked()
                .select(WorkflowRow::as_select())
                .first::<WorkflowRow>(connection)
                .await
                .optional()?
            else {
                return Ok(None);
            };
            let command_sequence = wait_command_sequence(&workflow, "timer")?;
            crate::trace::declare_unmodeled("timer_fired", true);
            crate::trace::touch_wf(workflow.id);
            let event = WorkflowEvent::TimerFired { command_sequence };
            append_delivery_event(connection, &workflow, "timer_fired", &event, now).await?;
            clear_wait(connection, &workflow, now).await?;
            Ok(Some(WorkflowId::new(workflow.id)?))
        })
        .await
    }
}

pub struct ApprovalExpiryMaterializer {
    pool: DurablePool,
}

impl ApprovalExpiryMaterializer {
    pub fn new(pool: DurablePool) -> Self {
        Self { pool }
    }

    pub async fn expire_one(&self, now: i64) -> Result<Option<ApprovalId>, DurableError> {
        self.materialize_one(now).await
    }

    pub async fn expire_next(&self) -> Result<Option<ApprovalId>, DurableError> {
        let mut connection = self.pool.get().await?;
        let now = persistence::database_now_millis(&mut connection).await?;
        drop(connection);
        self.materialize_one(now).await
    }

    pub async fn materialize_one(&self, now: i64) -> Result<Option<ApprovalId>, DurableError> {
        let mut connection = self.pool.get().await?;
        let candidate = durable_approval::table
            .filter(durable_approval::status.eq("pending"))
            .filter(durable_approval::expires_at.le(now))
            .order((
                durable_approval::expires_at.asc(),
                durable_approval::id.asc(),
            ))
            .select((durable_approval::id, durable_approval::workflow_id))
            .first::<(i64, i64)>(&mut connection)
            .await
            .optional()?;
        let Some((approval_id, workflow_id)) = candidate else {
            return Ok(None);
        };

        crate::dialect::transaction(&mut connection, async move |connection| {
            let workflow = durable_workflow::table
                .find(workflow_id)
                .for_update()
                .select(WorkflowRow::as_select())
                .first::<WorkflowRow>(connection)
                .await?;
            let approval = durable_approval::table
                .find(approval_id)
                .for_update()
                .select(ApprovalRow::as_select())
                .first::<ApprovalRow>(connection)
                .await?;
            if approval.status != "pending"
                || approval
                    .expires_at
                    .is_none_or(|expires_at| expires_at > now)
            {
                return Ok(None);
            }
            if !matches!(
                workflow.status,
                WorkflowStatus::WaitingApproval | WorkflowStatus::Paused
            ) || workflow.wait_kind.as_deref() != Some("approval")
                || workflow.wait_reference_id != Some(approval.id)
                || workflow.kind != approval.kind
                || workflow.version != approval.version
            {
                return Err(DurableError::InvalidState(format!(
                    "expired approval {} no longer matches workflow {}",
                    approval.id, workflow.id
                )));
            }
            let command_sequence = wait_command_sequence(&workflow, "approval")?;
            if i64::from(command_sequence) != i64::from(approval.command_sequence) {
                return Err(DurableError::InvalidState(format!(
                    "approval {} command sequence does not match workflow {}",
                    approval.id, workflow.id
                )));
            }
            let event = WorkflowEvent::ApprovalExpired { command_sequence };
            crate::trace::declare_unmodeled("approval_expired", true);
            crate::trace::touch_wf(workflow.id);
            append_delivery_event(connection, &workflow, "approval_expired", &event, now).await?;
            let changed = diesel::update(
                durable_approval::table
                    .find(approval.id)
                    .filter(durable_approval::status.eq("pending")),
            )
            .set((
                durable_approval::status.eq("expired"),
                durable_approval::resolved_at.eq(Some(now)),
            ))
            .execute(connection)
            .await?;
            ensure_single_change(changed)?;
            clear_wait(connection, &workflow, now).await?;
            Ok(Some(ApprovalId::new(approval.id)?))
        })
        .await
    }
}

fn wait_command_sequence(workflow: &WorkflowRow, kind: &str) -> Result<u32, DurableError> {
    let reference = workflow.wait_reference_id.ok_or_else(|| {
        DurableError::InvalidState(format!(
            "workflow {} {kind} wait has no reference",
            workflow.id
        ))
    })?;
    if kind == "timer" && reference != i64::from(workflow.command_sequence) {
        return Err(DurableError::InvalidState(format!(
            "workflow {} timer reference does not match its command sequence",
            workflow.id
        )));
    }
    u32::try_from(workflow.command_sequence).map_err(|_| {
        DurableError::InvalidState(format!(
            "workflow {} has a negative command sequence",
            workflow.id
        ))
    })
}

async fn append_delivery_event(
    connection: &mut crate::DurableConnection,
    workflow: &WorkflowRow,
    event_type: &str,
    event: &WorkflowEvent,
    now: i64,
) -> Result<(), DurableError> {
    let metadata_json = serde_json::to_string(event)?;
    ensure_size(
        "temporal workflow event",
        &metadata_json,
        MAX_EVENT_METADATA_BYTES,
    )?;
    let delivery_sequence = workflow
        .delivered_event_sequence
        .checked_add(1)
        .ok_or_else(|| {
            DurableError::InvalidState("workflow delivery sequence overflow".to_string())
        })?;
    let workflow_id = WorkflowId::new(workflow.id)?;
    let sequence = persistence::next_event_sequence(connection, workflow_id).await?;
    persistence::append_event(
        connection,
        NewWorkflowEventRow {
            workflow_id: workflow.id,
            sequence,
            delivery_sequence: Some(delivery_sequence),
            event_type: event_type.to_string(),
            metadata_json: Some(metadata_json),
            actor_type: Some("system".to_string()),
            actor_id: None,
            reason: None,
            created_at: now,
        },
    )
    .await
}

async fn clear_wait(
    connection: &mut crate::DurableConnection,
    workflow: &WorkflowRow,
    now: i64,
) -> Result<(), DurableError> {
    let next_status = if workflow.status == WorkflowStatus::Paused {
        WorkflowStatus::Paused
    } else {
        WorkflowStatus::Ready
    };
    let changed = diesel::update(
        durable_workflow::table
            .find(workflow.id)
            .filter(durable_workflow::status.eq(&workflow.status))
            .filter(durable_workflow::wait_kind.eq(workflow.wait_kind.clone()))
            .filter(durable_workflow::wait_reference_id.eq(workflow.wait_reference_id)),
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
    ensure_single_change(changed)
}

fn ensure_single_change(changed: usize) -> Result<(), DurableError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(DurableError::FencedWrite)
    }
}
