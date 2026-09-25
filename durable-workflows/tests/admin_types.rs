use durable_workflows::{
    admin::{
        decode_cursor, encode_cursor, ActivitySummary, AdminPage, CursorPosition, JsonFieldSummary,
        Operator, PageRequest, WorkflowSummary, MAX_ADMIN_PAGE_SIZE,
    },
    ActivityId, DurableError, WorkflowId,
};

#[test]
fn cursor_round_trips_and_is_collection_scoped() {
    let position = CursorPosition {
        timestamp: 1_721_000_000_000,
        tie_breaker: "42".to_string(),
    };
    let cursor = encode_cursor("workflows", &position).expect("cursor encodes");
    assert_eq!(
        decode_cursor("workflows", &cursor).expect("cursor decodes"),
        position
    );
    assert!(matches!(
        decode_cursor("activities", &cursor),
        Err(DurableError::InvalidCursor(_))
    ));
}

#[test]
fn malformed_or_extended_cursors_are_rejected() {
    assert!(matches!(
        decode_cursor("workflows", "not-base64!"),
        Err(DurableError::InvalidCursor(_))
    ));
    let payload = base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        br#"{"version":1,"scope":"workflows","timestamp":1,"tie_breaker":"1","extra":true}"#,
    );
    assert!(matches!(
        decode_cursor("workflows", &payload),
        Err(DurableError::InvalidCursor(_))
    ));
}

#[test]
fn page_limits_are_explicitly_bounded() {
    assert_eq!(
        PageRequest::default()
            .bounded_limit(MAX_ADMIN_PAGE_SIZE)
            .expect("default limit"),
        50
    );
    assert!(PageRequest {
        cursor: None,
        limit: Some(0)
    }
    .bounded_limit(MAX_ADMIN_PAGE_SIZE)
    .is_err());
    assert!(PageRequest {
        cursor: None,
        limit: Some(MAX_ADMIN_PAGE_SIZE + 1)
    }
    .bounded_limit(MAX_ADMIN_PAGE_SIZE)
    .is_err());
}

#[test]
fn operator_requires_an_actor_and_bounded_trimmed_reason() {
    let operator = Operator::new("17", "  retry after provider review  ").expect("operator");
    assert_eq!(operator.actor_id(), "17");
    assert_eq!(operator.reason(), "retry after provider review");
    assert!(Operator::new("", "reason").is_err());
    assert!(Operator::new("17", "   ").is_err());
    assert!(Operator::new("17", "x".repeat(2_049)).is_err());
}

#[test]
fn admin_list_json_exposes_only_redacted_payload_summaries() {
    let secret = "account-secret-value";
    let workflow = WorkflowSummary {
        id: WorkflowId::new(1).expect("workflow ID"),
        kind: "test_workflow".to_string(),
        version: 1,
        status: "ready".to_string(),
        wait_kind: None,
        schedule_run_id: None,
        root_workflow_id: None,
        restarted_from_workflow_id: None,
        input: JsonFieldSummary::from_required(secret),
        state: JsonFieldSummary::from_required(secret),
        result: JsonFieldSummary::from_optional(None),
        error_category: None,
        error_message: None,
        created_at: 1,
        updated_at: 1,
        completed_at: None,
    };
    let activity = ActivitySummary {
        id: ActivityId::new(2).expect("activity ID"),
        workflow_id: workflow.id,
        kind: "test_activity".to_string(),
        version: 1,
        topic: "external".to_string(),
        status: "pending".to_string(),
        replacement_number: 0,
        attempt_count: 0,
        max_attempts: 3,
        operation_key: Some("provider-ref-1".to_string()),
        root_activity_id: None,
        replaces_activity_id: None,
        payload: JsonFieldSummary::from_required(secret),
        provider_result: JsonFieldSummary::from_optional(Some(secret)),
        error_category: None,
        error_message: None,
        available_at: 1,
        created_at: 1,
        updated_at: 1,
        completed_at: None,
    };
    let json = serde_json::to_string(&AdminPage {
        items: vec![(workflow, activity)],
        next_cursor: None,
    })
    .expect("admin response serializes");
    assert!(!json.contains(secret));
    assert!(!json.contains("\"value\""));
    assert!(!json.contains("inputJson"));
    assert!(!json.contains("stateJson"));
    assert!(!json.contains("payloadJson"));
    assert!(!json.contains("providerResultJson"));
    assert!(json.contains("provider-ref-1"));
}

#[test]
fn admin_detail_json_includes_parsed_payload_values() {
    let summary = JsonFieldSummary::from_required_json(r#"{"secret":"account-secret-value"}"#)
        .expect("valid JSON summary");
    let json = serde_json::to_value(&summary).expect("summary serializes");
    assert_eq!(
        json,
        serde_json::json!({
            "present": true,
            "bytes": 33,
            "value": { "secret": "account-secret-value" }
        })
    );
}
