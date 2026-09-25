use std::{sync::Arc, time::Duration};

use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;

use crate::{
    admin::{
        ActivityRetryOutcome, ApprovalResolutionOutcome, Operator, ScheduleControlOutcome,
        ScheduleRegistry, ScheduleRunNowOutcome, WorkflowControlOutcome, WorkflowRestartOutcome,
    },
    error::ensure_size,
    persistence::{
        self, ActivityRow, ActivityStatus, ApprovalRow, NewActivityRow, NewScheduleRunRow,
        NewWorkflowEventRow, ScheduleStateRow, WorkflowRow, WorkflowStatus,
    },
    schema::{
        durable_activity, durable_approval, durable_schedule_run, durable_schedule_state,
        durable_workflow,
    },
    store::{cancel_activities, cancel_approvals, close_attempt},
    ActivityId, ActivityRegistry, ApprovalId, ApprovalResult, DurableError, DurablePool,
    DurableStore, ScheduleRunId, StartOptions, WorkflowEvent, WorkflowId, WorkflowRegistry,
    MAX_EVENT_METADATA_BYTES,
};

#[derive(Clone)]
pub struct AdminControlService<C> {
    pool: DurablePool,
    workflows: Arc<WorkflowRegistry<C>>,
    activities: Arc<ActivityRegistry<C>>,
    context: Option<Arc<C>>,
    schedules: Option<Arc<ScheduleRegistry<C>>>,
}

impl<C> AdminControlService<C>
where
    C: Send + Sync + 'static,
{
    pub fn new(
        pool: DurablePool,
        workflows: Arc<WorkflowRegistry<C>>,
        activities: Arc<ActivityRegistry<C>>,
    ) -> Self {
        Self {
            pool,
            workflows,
            activities,
            context: None,
            schedules: None,
        }
    }

    pub fn with_schedules(mut self, context: Arc<C>, schedules: Arc<ScheduleRegistry<C>>) -> Self {
        self.context = Some(context);
        self.schedules = Some(schedules);
        self
    }

    pub async fn pause_workflow(
        &self,
        workflow_id: WorkflowId,
        operator: &Operator,
    ) -> Result<WorkflowControlOutcome, DurableError> {
        let operator = operator.clone();
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            let workflow = lock_workflow(connection, workflow_id).await?;
            if workflow.status == WorkflowStatus::Paused || is_terminal(workflow.status) {
                return Err(conflict(workflow_id, "cannot be paused", workflow.status));
            }
            crate::trace::declare(|| {
                crate::trace::Action::new(
                    "AdminPause",
                    serde_json::json!({ "workflow_id": workflow.id }),
                )
            });
            crate::trace::touch_wf(workflow.id);
            let now = persistence::database_now_millis(connection).await?;
            if workflow.wait_kind.as_deref() == Some("activity") {
                if let Some(activity_id) = workflow.wait_reference_id {
                    pause_activity(connection, activity_id, &operator, now).await?;
                }
            }
            let changed = diesel::update(
                durable_workflow::table
                    .find(workflow.id)
                    .filter(durable_workflow::status.eq(&workflow.status)),
            )
            .set((
                durable_workflow::status.eq(WorkflowStatus::Paused),
                durable_workflow::lease_owner.eq(None::<String>),
                durable_workflow::lease_token.eq(None::<String>),
                durable_workflow::lease_expires_at.eq(None::<i64>),
                durable_workflow::updated_at.eq(now),
            ))
            .execute(connection)
            .await?;
            ensure_changed(changed)?;
            append_operator_event(connection, workflow_id, "workflow_paused", &operator, now)
                .await?;
            Ok(WorkflowControlOutcome {
                workflow_id,
                status: "paused".to_string(),
            })
        })
        .await
    }

    pub async fn resume_workflow(
        &self,
        workflow_id: WorkflowId,
        operator: &Operator,
    ) -> Result<WorkflowControlOutcome, DurableError> {
        let operator = operator.clone();
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            let workflow = lock_workflow(connection, workflow_id).await?;
            if workflow.status != WorkflowStatus::Paused {
                return Err(conflict(workflow_id, "is not paused", workflow.status));
            }
            crate::trace::declare(|| {
                crate::trace::Action::new(
                    "AdminResume",
                    serde_json::json!({ "workflow_id": workflow.id }),
                )
            });
            crate::trace::touch_wf(workflow.id);
            let now = persistence::database_now_millis(connection).await?;
            let status = resume_status(connection, &workflow).await?;
            let changed = diesel::update(
                durable_workflow::table
                    .find(workflow.id)
                    .filter(durable_workflow::status.eq(WorkflowStatus::Paused)),
            )
            .set((
                durable_workflow::status.eq(status),
                durable_workflow::available_at.eq(if status == WorkflowStatus::Ready {
                    now
                } else {
                    workflow.available_at
                }),
                durable_workflow::lease_owner.eq(None::<String>),
                durable_workflow::lease_token.eq(None::<String>),
                durable_workflow::lease_expires_at.eq(None::<i64>),
                durable_workflow::updated_at.eq(now),
            ))
            .execute(connection)
            .await?;
            ensure_changed(changed)?;
            append_operator_event(connection, workflow_id, "workflow_resumed", &operator, now)
                .await?;
            Ok(WorkflowControlOutcome {
                workflow_id,
                status: status.to_string(),
            })
        })
        .await
    }

    pub async fn cancel_workflow(
        &self,
        workflow_id: WorkflowId,
        operator: &Operator,
    ) -> Result<WorkflowControlOutcome, DurableError> {
        let operator = operator.clone();
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            let workflow = lock_workflow(connection, workflow_id).await?;
            if is_terminal(workflow.status) {
                return Err(conflict(
                    workflow_id,
                    "is already terminal",
                    workflow.status,
                ));
            }
            let now = persistence::database_now_millis(connection).await?;
            crate::trace::declare(|| {
                crate::trace::Action::new(
                    "AdminCancel",
                    serde_json::json!({ "workflow_id": workflow.id }),
                )
            });
            crate::store::cancel_locked_workflow(
                connection,
                &workflow,
                operator.reason(),
                Some(operator.actor_id()),
                now,
            )
            .await?;
            Ok(WorkflowControlOutcome {
                workflow_id,
                status: "cancelled".to_string(),
            })
        })
        .await
    }

    pub async fn restart_workflow(
        &self,
        workflow_id: WorkflowId,
        operator: &Operator,
    ) -> Result<WorkflowRestartOutcome, DurableError> {
        self.restart(workflow_id, None, operator.clone()).await
    }

    pub async fn correct_and_restart_workflow(
        &self,
        workflow_id: WorkflowId,
        input_json: &str,
        operator: &Operator,
    ) -> Result<WorkflowRestartOutcome, DurableError> {
        self.restart(workflow_id, Some(input_json.to_string()), operator.clone())
            .await
    }

    async fn restart(
        &self,
        workflow_id: WorkflowId,
        correction: Option<String>,
        operator: Operator,
    ) -> Result<WorkflowRestartOutcome, DurableError> {
        let workflows = self.workflows.clone();
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            crate::trace::declare_unmodeled("admin_restart", true);
            let source = lock_workflow(connection, workflow_id).await?;
            if !is_restartable(source.status) {
                return Err(conflict(
                    workflow_id,
                    "must be paused, blocked, or terminal before restart",
                    source.status,
                ));
            }
            let existing_restart = durable_workflow::table
                .filter(durable_workflow::restarted_from_workflow_id.eq(Some(source.id)))
                .select(durable_workflow::id)
                .first::<i64>(connection)
                .await
                .optional()?;
            if existing_restart.is_some() {
                return Err(DurableError::Conflict(format!(
                    "workflow {workflow_id} already has a restart"
                )));
            }
            let prepared = match correction.as_deref() {
                Some(input) => workflows.prepare_start_current(&source.kind, input)?,
                None => workflows.prepare_start_exact(
                    &source.kind,
                    source.version,
                    &source.input_json,
                )?,
            };
            let kind = prepared.kind().to_string();
            let version = prepared.version();
            let root = WorkflowId::new(source.root_workflow_id.unwrap_or(source.id))?;
            let schedule_run_id = source.schedule_run_id.map(ScheduleRunId::new).transpose()?;
            let outcome = DurableStore::start_prepared_with_conn(
                connection,
                prepared,
                StartOptions {
                    schedule_run_id,
                    root_workflow_id: Some(root),
                    restarted_from_workflow_id: Some(workflow_id),
                    ..StartOptions::default()
                },
            )
            .await?;
            if !outcome.inserted {
                return Err(DurableError::InvalidState(
                    "workflow restart unexpectedly deduplicated".to_string(),
                ));
            }
            let now = persistence::database_now_millis(connection).await?;
            if !is_terminal(source.status) {
                cancel_activities(
                    connection,
                    source.id,
                    operator.reason(),
                    "operator_cancelled",
                    now,
                )
                .await?;
                cancel_approvals(connection, source.id, operator.reason(), now).await?;
                let changed = diesel::update(
                    durable_workflow::table
                        .find(source.id)
                        .filter(durable_workflow::status.eq(&source.status)),
                )
                .set((
                    durable_workflow::status.eq(WorkflowStatus::Cancelled),
                    durable_workflow::lease_owner.eq(None::<String>),
                    durable_workflow::lease_token.eq(None::<String>),
                    durable_workflow::lease_expires_at.eq(None::<i64>),
                    durable_workflow::updated_at.eq(now),
                    durable_workflow::completed_at.eq(Some(now)),
                ))
                .execute(connection)
                .await?;
                ensure_changed(changed)?;
                append_operator_event(
                    connection,
                    workflow_id,
                    "workflow_superseded_by_restart",
                    &operator,
                    now,
                )
                .await?;
                persistence::wake_waiting_parents_on_child_terminal(
                    connection,
                    source.id,
                    &source.kind,
                    source.version,
                    Err((
                        "child_superseded".to_string(),
                        operator.reason().to_string(),
                    )),
                    now,
                )
                .await?;
            }
            if let Some(schedule_run_id) = schedule_run_id {
                let changed = diesel::update(
                    durable_schedule_run::table
                        .find(schedule_run_id.get())
                        .filter(durable_schedule_run::workflow_id.eq(Some(source.id))),
                )
                .set(durable_schedule_run::workflow_id.eq(Some(outcome.workflow_id.get())))
                .execute(connection)
                .await?;
                ensure_changed(changed)?;
            }
            append_operator_event(
                connection,
                workflow_id,
                if correction.is_some() {
                    "workflow_corrected_and_restarted"
                } else {
                    "workflow_restarted"
                },
                &operator,
                now,
            )
            .await?;
            append_operator_event(
                connection,
                outcome.workflow_id,
                "workflow_restart_created",
                &operator,
                now,
            )
            .await?;
            Ok(WorkflowRestartOutcome {
                source_workflow_id: workflow_id,
                workflow_id: outcome.workflow_id,
                kind,
                version,
            })
        })
        .await
    }

    pub async fn retry_activity(
        &self,
        activity_id: ActivityId,
        operator: &Operator,
    ) -> Result<ActivityRetryOutcome, DurableError> {
        self.retry(activity_id, None, operator.clone()).await
    }

    pub async fn correct_and_retry_activity(
        &self,
        activity_id: ActivityId,
        payload_json: &str,
        operator: &Operator,
    ) -> Result<ActivityRetryOutcome, DurableError> {
        self.retry(
            activity_id,
            Some(payload_json.to_string()),
            operator.clone(),
        )
        .await
    }

    async fn retry(
        &self,
        activity_id: ActivityId,
        correction: Option<String>,
        operator: Operator,
    ) -> Result<ActivityRetryOutcome, DurableError> {
        let mut connection = self.pool.get().await?;
        let workflow_id = durable_activity::table
            .find(activity_id.get())
            .select(durable_activity::workflow_id)
            .first::<i64>(&mut connection)
            .await
            .optional()?
            .map(WorkflowId::new)
            .transpose()?
            .ok_or_else(|| not_found("activity", activity_id))?;
        let activities = self.activities.clone();
        crate::dialect::transaction(&mut connection, async move |connection| {
            crate::trace::declare_unmodeled("admin_retry", true);
                    let workflow = lock_workflow(connection, workflow_id).await?;
                    let source = durable_activity::table
                        .find(activity_id.get())
                        .for_update()
                        .select(ActivityRow::as_select())
                        .first::<ActivityRow>(connection)
                        .await
                        .optional()?
                        .ok_or_else(|| not_found("activity", activity_id))?;
                    if workflow.status != WorkflowStatus::Blocked
                        || workflow.wait_reference_id != Some(source.id)
                        || source.status != ActivityStatus::DeadLettered
                    {
                        return Err(DurableError::Conflict(format!(
                            "activity {activity_id} is not the dead-lettered activity blocking workflow {workflow_id}"
                        )));
                    }
                    let replacement_number = source
                        .replacement_number
                        .checked_add(1)
                        .ok_or_else(|| {
                            DurableError::InvalidState(
                                "activity replacement number overflow".to_string(),
                            )
                        })?;
                    let root_id = source.root_activity_id.unwrap_or(source.id);
                    let operation_key = if correction.is_some() {
                        Some(format!(
                            "durable:activity:{root_id}:correction:{replacement_number}"
                        ))
                    } else {
                        source.operation_key.clone()
                    };
                    let command = match correction.as_deref() {
                        Some(payload) => activities.prepare_command_current(
                            &source.kind,
                            payload,
                            operation_key.clone(),
                        )?,
                        None => activities.prepare_command_exact(
                            &source.kind,
                            source.version,
                            &source.payload_json,
                            operation_key.clone(),
                        )?,
                    };
                    let now = persistence::database_now_millis(connection).await?;
                    let inserted_id = crate::dialect::insert_activity(
                        connection,
                        NewActivityRow {
                            workflow_id: source.workflow_id,
                            command_sequence: source.command_sequence,
                            replacement_number,
                            kind: command.kind().to_string(),
                            version: command.version(),
                            topic: command.topic().to_string(),
                            payload_json: command.payload_json().to_string(),
                            status: ActivityStatus::Pending,
                            available_at: now,
                            max_attempts: i32::try_from(command.max_attempts()).map_err(|_| {
                                DurableError::InvalidDefinition(
                                    "activity attempts exceed the database integer range".to_string(),
                                )
                            })?,
                            attempt_count: 0,
                            timeout_millis: duration_millis(command.timeout())?,
                            lease_duration_millis: duration_millis(command.lease_duration())?,
                            retry_policy_json: serde_json::to_string(&command.retry_policy())?,
                            operation_key: operation_key.clone(),
                            provider_result_json: None,
                            last_error_category: None,
                            last_error_message: None,
                            lease_owner: None,
                            lease_token: None,
                            lease_expires_at: None,
                            root_activity_id: Some(root_id),
                            replaces_activity_id: Some(source.id),
                            created_at: now,
                            updated_at: now,
                            completed_at: None,
                        },
                    )
                    .await?;
                    let replacement_id = ActivityId::new(inserted_id)?;
                    let changed = diesel::update(
                        durable_workflow::table
                            .find(workflow.id)
                            .filter(durable_workflow::status.eq(WorkflowStatus::Blocked))
                            .filter(durable_workflow::wait_reference_id.eq(Some(source.id))),
                    )
                    .set((
                        durable_workflow::status.eq(WorkflowStatus::WaitingActivity),
                        durable_workflow::wait_kind.eq(Some("activity".to_string())),
                        durable_workflow::wait_reference_id.eq(Some(replacement_id.get())),
                        durable_workflow::available_at.eq(now),
                        durable_workflow::error_category.eq(None::<String>),
                        durable_workflow::error_message.eq(None::<String>),
                        durable_workflow::updated_at.eq(now),
                    ))
                    .execute(connection)
                    .await?;
                    ensure_changed(changed)?;
                    append_operator_event(
                        connection,
                        workflow_id,
                        if correction.is_some() {
                            "activity_corrected_and_retried"
                        } else {
                            "activity_retried"
                        },
                        &operator,
                        now,
                    )
                    .await?;
                    Ok(ActivityRetryOutcome {
                        source_activity_id: activity_id,
                        activity_id: replacement_id,
                        kind: command.kind().to_string(),
                        version: command.version(),
                        operation_key,
                    })
            })
            .await
    }

    pub async fn resolve_approval(
        &self,
        approval_id: ApprovalId,
        decision_json: &str,
        operator: &Operator,
    ) -> Result<ApprovalResolutionOutcome, DurableError> {
        let actor_id = operator_actor_id(operator)?;
        let operator = operator.clone();
        let decision_json = decision_json.to_string();
        let workflows = self.workflows.clone();
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            crate::trace::declare_unmodeled("admin_resolve_approval", true);
            let workflow_id = durable_approval::table
                .find(approval_id.get())
                .select(durable_approval::workflow_id)
                .first::<i64>(connection)
                .await
                .optional()?
                .ok_or_else(|| not_found("approval", approval_id))?;
            let workflow_id = WorkflowId::new(workflow_id)?;
            let workflow = lock_workflow(connection, workflow_id).await?;
            let approval = durable_approval::table
                .find(approval_id.get())
                .for_update()
                .select(ApprovalRow::as_select())
                .first::<ApprovalRow>(connection)
                .await
                .optional()?
                .ok_or_else(|| not_found("approval", approval_id))?;
            if approval.status != "pending" {
                return Err(DurableError::Conflict(format!(
                    "approval {approval_id} is already {}",
                    approval.status
                )));
            }
            let now = persistence::database_now_millis(connection).await?;
            if approval
                .expires_at
                .is_some_and(|expires_at| expires_at <= now)
            {
                return Err(DurableError::Conflict(format!(
                    "approval {approval_id} has expired"
                )));
            }
            if !matches!(
                workflow.status,
                WorkflowStatus::WaitingApproval | WorkflowStatus::Paused
            ) || workflow.wait_kind.as_deref() != Some("approval")
                || workflow.wait_reference_id != Some(approval.id)
                || workflow.kind != approval.kind
                || workflow.version != approval.version
            {
                return Err(DurableError::Conflict(format!(
                    "approval {approval_id} no longer matches workflow {workflow_id}"
                )));
            }
            let decision = workflows.validate_approval_exact(
                &workflow.kind,
                workflow.version,
                &decision_json,
            )?;
            let command_sequence = u32::try_from(approval.command_sequence).map_err(|_| {
                DurableError::InvalidState("negative approval command sequence".to_string())
            })?;
            let event = WorkflowEvent::ApprovalResolved {
                command_sequence,
                result: ApprovalResult {
                    kind: workflow.kind.clone(),
                    version: workflow.version,
                    payload_json: decision.clone(),
                },
            };
            let metadata_json = serde_json::to_string(&event)?;
            ensure_size(
                "approval resolution event",
                &metadata_json,
                MAX_EVENT_METADATA_BYTES,
            )?;
            let delivery_sequence = workflow
                .delivered_event_sequence
                .checked_add(1)
                .ok_or_else(|| {
                    DurableError::InvalidState("workflow delivery sequence overflow".to_string())
                })?;
            let sequence = persistence::next_event_sequence(connection, workflow_id).await?;
            persistence::append_event(
                connection,
                NewWorkflowEventRow {
                    workflow_id: workflow.id,
                    sequence,
                    delivery_sequence: Some(delivery_sequence),
                    event_type: "approval_resolved".to_string(),
                    metadata_json: Some(metadata_json),
                    actor_type: Some("operator".to_string()),
                    actor_id: Some(operator.actor_id().to_string()),
                    reason: Some(operator.reason().to_string()),
                    created_at: now,
                },
            )
            .await?;
            let changed = diesel::update(
                durable_approval::table
                    .find(approval.id)
                    .filter(durable_approval::status.eq("pending")),
            )
            .set((
                durable_approval::status.eq("resolved"),
                durable_approval::decision_payload_json.eq(Some(decision)),
                durable_approval::decided_by.eq(Some(actor_id)),
                durable_approval::operator_reason.eq(Some(operator.reason().to_string())),
                durable_approval::resolved_at.eq(Some(now)),
            ))
            .execute(connection)
            .await?;
            ensure_changed(changed)?;
            let next_status = if workflow.status == WorkflowStatus::Paused {
                WorkflowStatus::Paused
            } else {
                WorkflowStatus::Ready
            };
            let changed = diesel::update(
                durable_workflow::table
                    .find(workflow.id)
                    .filter(durable_workflow::status.eq(&workflow.status))
                    .filter(durable_workflow::wait_reference_id.eq(Some(approval.id))),
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
            ensure_changed(changed)?;
            Ok(ApprovalResolutionOutcome {
                approval_id,
                workflow_id,
                status: "resolved".to_string(),
            })
        })
        .await
    }

    pub async fn pause_schedule(
        &self,
        schedule_key: &str,
        operator: &Operator,
    ) -> Result<ScheduleControlOutcome, DurableError> {
        self.set_schedule_paused(schedule_key, true, operator).await
    }

    pub async fn resume_schedule(
        &self,
        schedule_key: &str,
        operator: &Operator,
    ) -> Result<ScheduleControlOutcome, DurableError> {
        self.set_schedule_paused(schedule_key, false, operator)
            .await
    }

    async fn set_schedule_paused(
        &self,
        schedule_key: &str,
        paused: bool,
        operator: &Operator,
    ) -> Result<ScheduleControlOutcome, DurableError> {
        let schedules = self.schedule_registry()?;
        let definition = schedules
            .get(schedule_key)
            .cloned()
            .ok_or_else(|| not_found("schedule definition", schedule_key))?;
        let actor_id = operator_actor_id(operator)?;
        let operator = operator.clone();
        let schedule_key = schedule_key.to_string();
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            crate::trace::declare_unmodeled("admin_schedule_pause", false);
            let state = lock_schedule_state(connection, &schedule_key).await?;
            validate_schedule_state(&state, &definition)?;
            if paused == state.paused_at.is_some() {
                return Err(DurableError::Conflict(format!(
                    "schedule {schedule_key} is already {}",
                    if paused { "paused" } else { "active" }
                )));
            }
            let now = persistence::database_now_millis(connection).await?;
            let changed = if paused {
                diesel::update(
                    durable_schedule_state::table
                        .find(&schedule_key)
                        .filter(durable_schedule_state::paused_at.is_null()),
                )
                .set((
                    durable_schedule_state::paused_at.eq(Some(now)),
                    durable_schedule_state::paused_by.eq(Some(actor_id)),
                    durable_schedule_state::pause_reason.eq(Some(operator.reason().to_string())),
                    durable_schedule_state::updated_at.eq(now),
                ))
                .execute(connection)
                .await?
            } else {
                diesel::update(
                    durable_schedule_state::table
                        .find(&schedule_key)
                        .filter(durable_schedule_state::paused_at.is_not_null()),
                )
                .set((
                    durable_schedule_state::paused_at.eq(None::<i64>),
                    durable_schedule_state::paused_by.eq(None::<i32>),
                    durable_schedule_state::pause_reason.eq(None::<String>),
                    durable_schedule_state::updated_at.eq(now),
                ))
                .execute(connection)
                .await?
            };
            ensure_changed(changed)?;
            Ok(ScheduleControlOutcome {
                schedule_key,
                paused,
            })
        })
        .await
    }

    pub async fn run_schedule_now(
        &self,
        schedule_key: &str,
        operator: &Operator,
    ) -> Result<ScheduleRunNowOutcome, DurableError> {
        let schedules = self.schedule_registry()?;
        let context = self.context.clone().ok_or_else(|| {
            DurableError::InvalidDefinition(
                "admin schedule controls require an application context".to_string(),
            )
        })?;
        let definition = schedules
            .get(schedule_key)
            .cloned()
            .ok_or_else(|| not_found("schedule definition", schedule_key))?;
        let actor_id = operator_actor_id(operator)?;
        let operator = operator.clone();
        let schedule_key = schedule_key.to_string();
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            crate::trace::declare_unmodeled("admin_run_schedule_now", true);
            let state = lock_schedule_state(connection, &schedule_key).await?;
            validate_schedule_state(&state, &definition)?;
            let now = persistence::database_now_millis(connection).await?;
            let last_scheduled = durable_schedule_run::table
                .filter(durable_schedule_run::schedule_key.eq(&schedule_key))
                .select(diesel::dsl::max(durable_schedule_run::scheduled_for))
                .get_result::<Option<i64>>(connection)
                .await?;
            let scheduled_for = last_scheduled
                .and_then(|last| last.checked_add(1))
                .map_or(now, |next| next.max(now));
            let schedule_run_id = ScheduleRunId::new(
                crate::dialect::insert_schedule_run(
                    connection,
                    NewScheduleRunRow {
                        schedule_key: schedule_key.clone(),
                        local_occurrence: format!("manual:{scheduled_for}"),
                        scheduled_for,
                        materialized_at: now,
                        status: "materializing".to_string(),
                        reason: Some(operator.reason().to_string()),
                        actor_id: Some(actor_id),
                        workflow_id: None,
                        created_at: now,
                    },
                )
                .await?,
            )?;
            let workflow_id = schedules
                .start_occurrence(
                    &schedule_key,
                    context.as_ref(),
                    connection,
                    schedule_run_id,
                    scheduled_for,
                )
                .await?;
            let changed = diesel::update(
                durable_schedule_run::table
                    .find(schedule_run_id.get())
                    .filter(durable_schedule_run::status.eq("materializing")),
            )
            .set((
                durable_schedule_run::status.eq("started"),
                durable_schedule_run::workflow_id.eq(Some(workflow_id.get())),
            ))
            .execute(connection)
            .await?;
            ensure_changed(changed)?;
            append_operator_event(connection, workflow_id, "schedule_run_now", &operator, now)
                .await?;
            Ok(ScheduleRunNowOutcome {
                schedule_run_id,
                workflow_id,
                scheduled_for,
            })
        })
        .await
    }

    fn schedule_registry(&self) -> Result<Arc<ScheduleRegistry<C>>, DurableError> {
        self.schedules.clone().ok_or_else(|| {
            DurableError::InvalidDefinition(
                "admin schedule controls require a schedule registry".to_string(),
            )
        })
    }
}

async fn lock_workflow(
    connection: &mut crate::DurableConnection,
    workflow_id: WorkflowId,
) -> Result<WorkflowRow, DurableError> {
    durable_workflow::table
        .find(workflow_id.get())
        .for_update()
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(connection)
        .await
        .optional()?
        .ok_or_else(|| not_found("workflow", workflow_id))
}

async fn lock_schedule_state(
    connection: &mut crate::DurableConnection,
    schedule_key: &str,
) -> Result<ScheduleStateRow, DurableError> {
    durable_schedule_state::table
        .find(schedule_key)
        .for_update()
        .select(ScheduleStateRow::as_select())
        .first::<ScheduleStateRow>(connection)
        .await
        .optional()?
        .ok_or_else(|| not_found("schedule state", schedule_key))
}

fn validate_schedule_state(
    state: &ScheduleStateRow,
    definition: &super::ScheduleDefinitionMetadata,
) -> Result<(), DurableError> {
    if state.definition_version == definition.version
        && state.definition_fingerprint == definition.fingerprint
    {
        return Ok(());
    }
    Err(DurableError::Conflict(format!(
        "schedule {} persisted definition does not match registered v{}",
        state.schedule_key, definition.version
    )))
}

fn operator_actor_id(operator: &Operator) -> Result<i32, DurableError> {
    let actor_id = operator.actor_id().parse::<i32>().map_err(|_| {
        DurableError::InvalidDefinition(
            "schedule and approval controls require a numeric operator actor ID".to_string(),
        )
    })?;
    if actor_id <= 0 {
        return Err(DurableError::InvalidDefinition(
            "schedule and approval controls require a positive operator actor ID".to_string(),
        ));
    }
    Ok(actor_id)
}

async fn pause_activity(
    connection: &mut crate::DurableConnection,
    activity_id: i64,
    operator: &Operator,
    now: i64,
) -> Result<(), DurableError> {
    let activity = durable_activity::table
        .find(activity_id)
        .for_update()
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(connection)
        .await
        .optional()?;
    let Some(activity) = activity else {
        return Ok(());
    };
    if activity.status != ActivityStatus::Running {
        return Ok(());
    }
    crate::trace::touch_act(activity.id);
    crate::trace::note("paused_activity", || serde_json::json!(activity.id));
    close_attempt(
        connection,
        &activity,
        "operator_paused",
        operator.reason(),
        now,
    )
    .await?;
    let max_attempts = activity.max_attempts.checked_add(1).ok_or_else(|| {
        DurableError::InvalidState(
            "activity attempt limit overflow after operator pause".to_string(),
        )
    })?;
    let changed = diesel::update(
        durable_activity::table
            .find(activity.id)
            .filter(durable_activity::status.eq(ActivityStatus::Running))
            .filter(durable_activity::lease_token.eq(activity.lease_token)),
    )
    .set((
        durable_activity::status.eq(ActivityStatus::Pending),
        durable_activity::available_at.eq(now),
        durable_activity::max_attempts.eq(max_attempts),
        durable_activity::lease_owner.eq(None::<String>),
        durable_activity::lease_token.eq(None::<String>),
        durable_activity::lease_expires_at.eq(None::<i64>),
        durable_activity::updated_at.eq(now),
    ))
    .execute(connection)
    .await?;
    ensure_changed(changed)
}

async fn resume_status(
    connection: &mut crate::DurableConnection,
    workflow: &WorkflowRow,
) -> Result<WorkflowStatus, DurableError> {
    match workflow.wait_kind.as_deref() {
        None => Ok(WorkflowStatus::Ready),
        Some("timer") => Ok(WorkflowStatus::Sleeping),
        Some("activity") => {
            let activity_id = workflow.wait_reference_id.ok_or_else(|| {
                DurableError::InvalidState("paused activity wait has no reference".to_string())
            })?;
            let status = durable_activity::table
                .find(activity_id)
                .select(durable_activity::status)
                .first::<ActivityStatus>(connection)
                .await
                .optional()?
                .ok_or_else(|| {
                    DurableError::InvalidState(
                        "paused activity wait references a missing activity".to_string(),
                    )
                })?;
            match status {
                ActivityStatus::Pending | ActivityStatus::Running => {
                    Ok(WorkflowStatus::WaitingActivity)
                }
                ActivityStatus::DeadLettered => Ok(WorkflowStatus::Blocked),
                ActivityStatus::Succeeded => Ok(WorkflowStatus::Ready),
                value => Err(DurableError::Conflict(format!(
                    "paused workflow references activity in {value} state"
                ))),
            }
        }
        Some("child") => {
            let child_id = workflow.wait_reference_id.ok_or_else(|| {
                DurableError::InvalidState("paused child wait has no reference".to_string())
            })?;
            let status = durable_workflow::table
                .find(child_id)
                .select(durable_workflow::status)
                .first::<WorkflowStatus>(connection)
                .await
                .optional()?
                .ok_or_else(|| {
                    DurableError::InvalidState(
                        "paused child wait references a missing workflow".to_string(),
                    )
                })?;
            match status {
                WorkflowStatus::Succeeded | WorkflowStatus::Failed | WorkflowStatus::Cancelled => Err(DurableError::Conflict(format!(
                    "paused workflow references terminal child workflow {child_id} whose outcome was never delivered"
                ))),
                _ => Ok(WorkflowStatus::WaitingChild),
            }
        }
        Some("approval") => {
            let approval_id = workflow.wait_reference_id.ok_or_else(|| {
                DurableError::InvalidState("paused approval wait has no reference".to_string())
            })?;
            let status = durable_approval::table
                .find(approval_id)
                .select(durable_approval::status)
                .first::<String>(connection)
                .await
                .optional()?
                .ok_or_else(|| {
                    DurableError::InvalidState(
                        "paused approval wait references a missing approval".to_string(),
                    )
                })?;
            Ok(if status == "pending" {
                WorkflowStatus::WaitingApproval
            } else {
                WorkflowStatus::Ready
            })
        }
        Some(value) => Err(DurableError::InvalidState(format!(
            "paused workflow has unsupported wait kind {value}"
        ))),
    }
}

async fn append_operator_event(
    connection: &mut crate::DurableConnection,
    workflow_id: WorkflowId,
    event_type: &str,
    operator: &Operator,
    now: i64,
) -> Result<(), DurableError> {
    let sequence = persistence::next_event_sequence(connection, workflow_id).await?;
    persistence::append_event(
        connection,
        NewWorkflowEventRow {
            workflow_id: workflow_id.get(),
            sequence,
            delivery_sequence: None,
            event_type: event_type.to_string(),
            metadata_json: None,
            actor_type: Some("operator".to_string()),
            actor_id: Some(operator.actor_id().to_string()),
            reason: Some(operator.reason().to_string()),
            created_at: now,
        },
    )
    .await
}

fn is_terminal(status: WorkflowStatus) -> bool {
    matches!(
        status,
        WorkflowStatus::Succeeded | WorkflowStatus::Failed | WorkflowStatus::Cancelled
    )
}

fn is_restartable(status: WorkflowStatus) -> bool {
    is_terminal(status) || matches!(status, WorkflowStatus::Blocked | WorkflowStatus::Paused)
}

fn ensure_changed(changed: usize) -> Result<(), DurableError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(DurableError::FencedWrite)
    }
}

fn conflict(workflow_id: WorkflowId, action: &str, status: WorkflowStatus) -> DurableError {
    DurableError::Conflict(format!(
        "workflow {workflow_id} {action} while in {status} state"
    ))
}

fn not_found(resource: &'static str, identifier: impl ToString) -> DurableError {
    DurableError::NotFound {
        resource,
        identifier: identifier.to_string(),
    }
}

fn duration_millis(duration: Duration) -> Result<i64, DurableError> {
    i64::try_from(duration.as_millis()).map_err(|_| {
        DurableError::InvalidDefinition("duration exceeds the database range".to_string())
    })
}
