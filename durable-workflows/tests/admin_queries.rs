mod support;

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use durable_workflows::{
    admin::{
        ActivityDetailRequest, ActivityListFilter, AdminQueryService, PageRequest, TimelineEntry,
        WorkflowListFilter,
    },
    persistence::{NewActivityAttemptRow, NewActivityRow, NewApprovalRow, NewProgressEventRow},
    schema::{
        durable_activity, durable_activity_attempt, durable_approval, durable_progress_event,
        durable_workflow, durable_workflow_event,
    },
    DurableStore, DurableWorkflow, RetryPolicy, StartOptions, WorkflowContext, WorkflowError,
    WorkflowEvent, WorkflowHandler, WorkflowTransition,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct AdminWorkflow {
    secret: String,
}

impl DurableWorkflow for AdminWorkflow {
    const KIND: &'static str = "admin_query_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for AdminWorkflow {
    type Context = ();
    type State = String;
    type Approval = bool;
    type Output = String;

    fn initial_state(&self) -> Self::State {
        self.secret.clone()
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<WorkflowTransition<Self::State, Self::Approval, Self::Output>, WorkflowError> {
        Ok(WorkflowTransition::Complete { output: state })
    }
}

async fn seed_workflows(pool: &durable_workflows::DurablePool) -> Vec<i64> {
    let store = DurableStore::new(pool.clone());
    let mut ids = Vec::new();
    for index in 1..=3 {
        let outcome = store
            .start(
                &AdminWorkflow {
                    secret: format!("customer-secret-{index}"),
                },
                StartOptions::default(),
            )
            .await
            .expect("workflow starts");
        ids.push(outcome.workflow_id.get());
    }
    let mut connection = pool.get().await.expect("test connection");
    for id in &ids {
        diesel::update(durable_workflow::table.find(id))
            .set((
                durable_workflow::created_at.eq(10_000_i64),
                durable_workflow::updated_at.eq(10_000_i64),
            ))
            .execute(&mut connection)
            .await
            .expect("normalize timestamps");
    }
    diesel::update(durable_workflow::table.find(ids[1]))
        .set((
            durable_workflow::status.eq("waiting_approval"),
            durable_workflow::wait_kind.eq(Some("approval".to_string())),
        ))
        .execute(&mut connection)
        .await
        .expect("waiting approval");
    ids
}

async fn seed_activity(pool: &durable_workflows::DurablePool, workflow_id: i64) -> i64 {
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_activity::table)
        .values(NewActivityRow {
            workflow_id,
            command_sequence: 1,
            replacement_number: 0,
            kind: "admin_query_activity".to_string(),
            version: 1,
            topic: "external".to_string(),
            payload_json: r#"{"secret":"activity-customer-secret"}"#.to_string(),
            status: durable_workflows::persistence::ActivityStatus::try_from("dead_lettered")
                .expect("valid fixture status"),
            available_at: 11_000,
            max_attempts: 2,
            attempt_count: 1,
            timeout_millis: 1_000,
            lease_duration_millis: 2_000,
            retry_policy_json: serde_json::to_string(&RetryPolicy::fixed(1).expect("retry policy"))
                .expect("retry JSON"),
            operation_key: Some("provider-operation-7".to_string()),
            provider_result_json: Some(r#"{"secret":"provider-customer-secret"}"#.to_string()),
            last_error_category: Some("provider".to_string()),
            last_error_message: Some("bounded failure".to_string()),
            lease_owner: None,
            lease_token: None,
            lease_expires_at: None,
            root_activity_id: None,
            replaces_activity_id: None,
            created_at: 11_000,
            updated_at: 12_000,
            completed_at: Some(12_000),
        })
        .execute(&mut connection)
        .await
        .expect("activity insert");
    let activity_id = durable_activity::table
        .select(durable_activity::id)
        .order(durable_activity::id.desc())
        .first::<i64>(&mut connection)
        .await
        .expect("activity ID");
    diesel::insert_into(durable_activity_attempt::table)
        .values(NewActivityAttemptRow {
            activity_id,
            attempt_number: 1,
            worker_id: "worker-1".to_string(),
            lease_token: "00000000-0000-0000-0000-000000000001".to_string(),
            started_at: 11_100,
            heartbeat_at: 11_500,
            finished_at: Some(12_000),
            outcome: Some("dead_lettered".to_string()),
            error_category: Some("provider".to_string()),
            error_message: Some("bounded failure".to_string()),
            provider_result_json: Some(r#"{"secret":"attempt-customer-secret"}"#.to_string()),
        })
        .execute(&mut connection)
        .await
        .expect("attempt insert");
    diesel::insert_into(durable_progress_event::table)
        .values(NewProgressEventRow {
            activity_id,
            attempt_number: 1,
            sequence: 1,
            code: "uploading".to_string(),
            description: "Uploaded one bounded batch".to_string(),
            description_bytes: 26,
            completed_units: Some(1),
            total_units: Some(2),
            severity: "info".to_string(),
            metadata_json: Some(r#"{"secret":"progress-metadata"}"#.to_string()),
            created_at: 11_400,
        })
        .execute(&mut connection)
        .await
        .expect("progress insert");
    activity_id
}

#[tokio::test]
async fn workflow_pages_are_stable_filterable_and_redacted() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let ids = seed_workflows(&pool).await;
    let service = AdminQueryService::new(pool.clone());
    let first = service
        .list_workflows(WorkflowListFilter {
            page: PageRequest {
                cursor: None,
                limit: Some(2),
            },
            ..WorkflowListFilter::default()
        })
        .await
        .expect("first page");
    assert_eq!(
        first
            .items
            .iter()
            .map(|workflow| workflow.id.get())
            .collect::<Vec<_>>(),
        vec![ids[2], ids[1]]
    );
    let second = service
        .list_workflows(WorkflowListFilter {
            page: PageRequest {
                cursor: first.next_cursor,
                limit: Some(2),
            },
            ..WorkflowListFilter::default()
        })
        .await
        .expect("second page");
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].id.get(), ids[0]);

    let waiting = service
        .list_workflows(WorkflowListFilter {
            waiting_approval: Some(true),
            ..WorkflowListFilter::default()
        })
        .await
        .expect("approval filter");
    assert_eq!(waiting.items.len(), 1);
    assert_eq!(waiting.items[0].id.get(), ids[1]);

    let detail = service
        .get_workflow(waiting.items[0].id)
        .await
        .expect("workflow detail");
    let json = serde_json::to_string(&detail).expect("detail JSON");
    assert!(detail.input.present);
    assert!(detail.input.bytes > 0);
    assert_eq!(
        detail.input.value,
        Some(serde_json::json!({ "secret": "customer-secret-2" }))
    );
    assert!(json.contains("customer-secret-2"));
    assert!(detail.state.value.is_some());
    // List responses stay redacted; only the detail GET exposes payload bodies.
    let list_json = serde_json::to_string(&waiting).expect("list JSON");
    assert!(!list_json.contains("customer-secret"));
    assert!(!list_json.contains("\"value\""));

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn activity_detail_exposes_attempts_progress_and_payload_values() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = seed_workflows(&pool).await[0];
    let activity_id = seed_activity(&pool, workflow_id).await;
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_activity_attempt::table)
        .values(NewActivityAttemptRow {
            activity_id,
            attempt_number: 2,
            worker_id: "worker-2".to_string(),
            lease_token: "00000000-0000-0000-0000-000000000002".to_string(),
            started_at: 12_100,
            heartbeat_at: 12_500,
            finished_at: Some(13_000),
            outcome: Some("dead_lettered".to_string()),
            error_category: Some("provider".to_string()),
            error_message: Some("bounded failure".to_string()),
            provider_result_json: None,
        })
        .execute(&mut connection)
        .await
        .expect("second attempt");
    diesel::insert_into(durable_progress_event::table)
        .values(NewProgressEventRow {
            activity_id,
            attempt_number: 2,
            sequence: 1,
            code: "retrying".to_string(),
            description: "Retried bounded batch".to_string(),
            description_bytes: 21,
            completed_units: Some(2),
            total_units: Some(2),
            severity: "info".to_string(),
            metadata_json: None,
            created_at: 12_400,
        })
        .execute(&mut connection)
        .await
        .expect("second progress");
    drop(connection);
    let service = AdminQueryService::new(pool.clone());
    let page = service
        .list_activities(ActivityListFilter {
            workflow_id: Some(durable_workflows::WorkflowId::new(workflow_id).expect("workflow")),
            status: Some("dead_lettered".to_string()),
            ..ActivityListFilter::default()
        })
        .await
        .expect("activities");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id.get(), activity_id);
    assert_eq!(
        page.items[0].operation_key.as_deref(),
        Some("provider-operation-7")
    );

    let detail = service
        .get_activity(
            page.items[0].id,
            ActivityDetailRequest {
                attempt_limit: Some(1),
                progress_limit: Some(1),
                ..ActivityDetailRequest::default()
            },
        )
        .await
        .expect("activity detail");
    assert_eq!(detail.attempts.items.len(), 1);
    assert!(detail.attempts.next_cursor.is_some());
    assert_eq!(detail.progress.items.len(), 1);
    assert!(detail.progress.next_cursor.is_some());
    assert_eq!(detail.progress.items[0].code, "uploading");
    let next = service
        .get_activity(
            page.items[0].id,
            ActivityDetailRequest {
                attempt_cursor: detail.attempts.next_cursor.clone(),
                attempt_limit: Some(1),
                progress_cursor: detail.progress.next_cursor.clone(),
                progress_limit: Some(1),
            },
        )
        .await
        .expect("next activity detail page");
    assert_eq!(next.attempts.items[0].attempt_number, 2);
    assert_eq!(next.progress.items[0].code, "retrying");
    assert!(next.attempts.next_cursor.is_none());
    assert!(next.progress.next_cursor.is_none());
    assert_eq!(
        detail.activity.payload.value,
        Some(serde_json::json!({ "secret": "activity-customer-secret" }))
    );
    assert_eq!(
        detail.activity.provider_result.value,
        Some(serde_json::json!({ "secret": "provider-customer-secret" }))
    );
    assert_eq!(
        detail.attempts.items[0].provider_result.value,
        Some(serde_json::json!({ "secret": "attempt-customer-secret" }))
    );
    let json = serde_json::to_string(&detail).expect("detail JSON");
    assert!(json.contains("activity-customer-secret"));
    assert!(json.contains("provider-customer-secret"));
    assert!(json.contains("attempt-customer-secret"));
    // Progress metadata remains redacted; only description/code are operator-facing.
    assert!(!json.contains("progress-metadata"));
    assert!(json.contains("Uploaded one bounded batch"));
    let list_json = serde_json::to_string(&page).expect("list JSON");
    assert!(!list_json.contains("activity-customer-secret"));
    assert!(!list_json.contains("\"value\""));

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn invalid_ranges_and_missing_records_return_typed_errors() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let service = AdminQueryService::new(pool.clone());
    assert!(matches!(
        service
            .list_workflows(WorkflowListFilter {
                created_after: Some(10),
                created_before: Some(10),
                ..WorkflowListFilter::default()
            })
            .await,
        Err(durable_workflows::DurableError::InvalidDefinition(_))
    ));
    assert!(matches!(
        service
            .get_workflow(durable_workflows::WorkflowId::new(999).expect("workflow"))
            .await,
        Err(durable_workflows::DurableError::NotFound { .. })
    ));

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn workflow_timeline_is_stable_across_sources_and_redacted() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = seed_workflows(&pool).await[0];
    let activity_id = seed_activity(&pool, workflow_id).await;
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_approval::table)
        .values(NewApprovalRow {
            workflow_id,
            command_sequence: 2,
            kind: "medical_review".to_string(),
            version: 1,
            prompt_metadata_json: r#"{"secret":"approval-prompt-secret"}"#.to_string(),
            validation_schema_json: r#"{"type":"boolean"}"#.to_string(),
            validation_version: 1,
            status: "approved".to_string(),
            requested_at: 20_000,
            expires_at: None,
            decision_payload_json: Some(r#"{"secret":"approval-decision-secret"}"#.to_string()),
            decided_by: Some(7),
            operator_reason: Some("Reviewed by operations".to_string()),
            resolved_at: Some(20_100),
        })
        .execute(&mut connection)
        .await
        .expect("approval insert");
    diesel::update(
        durable_workflow_event::table.filter(durable_workflow_event::workflow_id.eq(workflow_id)),
    )
    .set(durable_workflow_event::created_at.eq(20_000_i64))
    .execute(&mut connection)
    .await
    .expect("event timestamp");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::created_at.eq(20_000_i64))
        .execute(&mut connection)
        .await
        .expect("activity timestamp");
    diesel::update(
        durable_activity_attempt::table
            .filter(durable_activity_attempt::activity_id.eq(activity_id)),
    )
    .set(durable_activity_attempt::started_at.eq(20_000_i64))
    .execute(&mut connection)
    .await
    .expect("attempt timestamp");
    diesel::update(
        durable_progress_event::table.filter(durable_progress_event::activity_id.eq(activity_id)),
    )
    .set(durable_progress_event::created_at.eq(20_000_i64))
    .execute(&mut connection)
    .await
    .expect("progress timestamp");
    drop(connection);

    let service = AdminQueryService::new(pool.clone());
    let mut entries = Vec::new();
    let mut cursor = None;
    loop {
        let page = service
            .workflow_timeline(
                durable_workflows::WorkflowId::new(workflow_id).expect("workflow"),
                PageRequest {
                    cursor,
                    limit: Some(2),
                },
            )
            .await
            .expect("timeline page");
        entries.extend(page.items);
        match page.next_cursor {
            Some(next_cursor) => cursor = Some(next_cursor),
            None => break,
        }
    }
    let entry_types = entries
        .iter()
        .map(|entry| match entry {
            TimelineEntry::WorkflowEvent { .. } => "workflowEvent",
            TimelineEntry::Activity(_) => "activity",
            TimelineEntry::ActivityAttempt(_) => "activityAttempt",
            TimelineEntry::Progress(_) => "progress",
            TimelineEntry::Approval(_) => "approval",
        })
        .collect::<Vec<_>>();
    assert_eq!(
        entry_types,
        vec![
            "workflowEvent",
            "activity",
            "activityAttempt",
            "progress",
            "approval"
        ]
    );
    let json = serde_json::to_string(&entries).expect("timeline JSON");
    assert!(!json.contains("customer-secret"));
    assert!(!json.contains("approval-prompt-secret"));
    assert!(!json.contains("approval-decision-secret"));
    assert!(json.contains("Reviewed by operations"));

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}
