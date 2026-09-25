mod support;

use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::{
    persistence::{ActivityRow, ActivityStatus, WorkflowRow, WorkflowStatus},
    schema::{durable_activity, durable_workflow},
};

#[tokio::test]
async fn persisted_workflow_row_rejects_unknown_status() {
    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };

    diesel::insert_into(durable_workflow::table)
        .values((
            durable_workflow::id.eq(1_i64),
            durable_workflow::kind.eq("status_test"),
            durable_workflow::version.eq(1),
            durable_workflow::input_json.eq("null"),
            durable_workflow::state_json.eq("null"),
            durable_workflow::status.eq("unknown_workflow_status"),
            durable_workflow::available_at.eq(0_i64),
            durable_workflow::max_activation_attempts.eq(1),
            durable_workflow::created_at.eq(0_i64),
            durable_workflow::updated_at.eq(0_i64),
        ))
        .execute(&mut connection)
        .await
        .expect("insert malformed workflow fixture");

    let workflow = durable_workflow::table
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await;
    assert!(
        matches!(
            workflow,
            Err(diesel::result::Error::DeserializationError(_))
        ),
        "unknown workflow status must fail decoding"
    );
}

#[tokio::test]
async fn persisted_activity_row_rejects_unknown_status() {
    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };

    diesel::insert_into(durable_workflow::table)
        .values((
            durable_workflow::id.eq(1_i64),
            durable_workflow::kind.eq("status_test"),
            durable_workflow::version.eq(1),
            durable_workflow::input_json.eq("null"),
            durable_workflow::state_json.eq("null"),
            durable_workflow::status.eq("ready"),
            durable_workflow::available_at.eq(0_i64),
            durable_workflow::max_activation_attempts.eq(1),
            durable_workflow::created_at.eq(0_i64),
            durable_workflow::updated_at.eq(0_i64),
        ))
        .execute(&mut connection)
        .await
        .expect("insert workflow fixture");

    diesel::insert_into(durable_activity::table)
        .values((
            durable_activity::workflow_id.eq(1_i64),
            durable_activity::command_sequence.eq(1),
            durable_activity::kind.eq("status_test"),
            durable_activity::version.eq(1),
            durable_activity::topic.eq("status_test"),
            durable_activity::payload_json.eq("null"),
            durable_activity::status.eq("unknown_activity_status"),
            durable_activity::available_at.eq(0_i64),
            durable_activity::max_attempts.eq(1),
            durable_activity::timeout_millis.eq(1_i64),
            durable_activity::lease_duration_millis.eq(1_i64),
            durable_activity::retry_policy_json.eq("{}"),
            durable_activity::created_at.eq(0_i64),
            durable_activity::updated_at.eq(0_i64),
        ))
        .execute(&mut connection)
        .await
        .expect("insert malformed activity fixture");

    let activity = durable_activity::table
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await;
    assert!(
        matches!(
            activity,
            Err(diesel::result::Error::DeserializationError(_))
        ),
        "unknown activity status must fail decoding"
    );
}

#[tokio::test]
async fn persisted_statuses_preserve_database_and_json_spellings() {
    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };
    diesel::insert_into(durable_workflow::table)
        .values((
            durable_workflow::id.eq(1_i64),
            durable_workflow::kind.eq("status_test"),
            durable_workflow::version.eq(1),
            durable_workflow::input_json.eq("null"),
            durable_workflow::state_json.eq("null"),
            durable_workflow::status.eq("ready"),
            durable_workflow::available_at.eq(0_i64),
            durable_workflow::max_activation_attempts.eq(1),
            durable_workflow::created_at.eq(0_i64),
            durable_workflow::updated_at.eq(0_i64),
        ))
        .execute(&mut connection)
        .await
        .expect("insert workflow fixture");

    diesel::insert_into(durable_activity::table)
        .values((
            durable_activity::workflow_id.eq(1_i64),
            durable_activity::command_sequence.eq(1),
            durable_activity::kind.eq("status_test"),
            durable_activity::version.eq(1),
            durable_activity::topic.eq("status_test"),
            durable_activity::payload_json.eq("null"),
            durable_activity::status.eq("pending"),
            durable_activity::available_at.eq(0_i64),
            durable_activity::max_attempts.eq(1),
            durable_activity::timeout_millis.eq(1_i64),
            durable_activity::lease_duration_millis.eq(1_i64),
            durable_activity::retry_policy_json.eq("{}"),
            durable_activity::created_at.eq(0_i64),
            durable_activity::updated_at.eq(0_i64),
        ))
        .execute(&mut connection)
        .await
        .expect("insert activity fixture");

    for (status, spelling) in [
        (WorkflowStatus::Ready, "ready"),
        (WorkflowStatus::Running, "running"),
        (WorkflowStatus::WaitingActivity, "waiting_activity"),
        (WorkflowStatus::WaitingChild, "waiting_child"),
        (WorkflowStatus::Sleeping, "sleeping"),
        (WorkflowStatus::WaitingApproval, "waiting_approval"),
        (WorkflowStatus::Paused, "paused"),
        (WorkflowStatus::Blocked, "blocked"),
        (WorkflowStatus::Succeeded, "succeeded"),
        (WorkflowStatus::Failed, "failed"),
        (WorkflowStatus::Cancelled, "cancelled"),
    ] {
        diesel::update(durable_workflow::table)
            .set(durable_workflow::status.eq(status))
            .execute(&mut connection)
            .await
            .expect("persist typed status");
        let row = durable_workflow::table
            .select(WorkflowRow::as_select())
            .first::<WorkflowRow>(&mut connection)
            .await
            .expect("decode typed status");
        assert_eq!(row.status, status);
        let stored = durable_workflow::table
            .select(durable_workflow::status)
            .first::<String>(&mut connection)
            .await
            .expect("read stored spelling");
        assert_eq!(stored, spelling);
        assert_eq!(status.to_string(), spelling);
        let json = serde_json::json!(spelling);
        assert_eq!(
            serde_json::to_value(status).expect("serialize status"),
            json
        );
        assert_eq!(
            serde_json::from_value::<WorkflowStatus>(json).expect("deserialize status"),
            status
        );
    }
    for (status, spelling) in [
        (ActivityStatus::Pending, "pending"),
        (ActivityStatus::Running, "running"),
        (ActivityStatus::Succeeded, "succeeded"),
        (ActivityStatus::DeadLettered, "dead_lettered"),
        (ActivityStatus::Cancelled, "cancelled"),
    ] {
        diesel::update(durable_activity::table)
            .set(durable_activity::status.eq(status))
            .execute(&mut connection)
            .await
            .expect("persist typed status");
        let row = durable_activity::table
            .select(ActivityRow::as_select())
            .first::<ActivityRow>(&mut connection)
            .await
            .expect("decode typed status");
        assert_eq!(row.status, status);
        let stored = durable_activity::table
            .select(durable_activity::status)
            .first::<String>(&mut connection)
            .await
            .expect("read stored spelling");
        assert_eq!(stored, spelling);
        assert_eq!(status.to_string(), spelling);
        let json = serde_json::json!(spelling);
        assert_eq!(
            serde_json::to_value(status).expect("serialize status"),
            json
        );
        assert_eq!(
            serde_json::from_value::<ActivityStatus>(json).expect("deserialize status"),
            status
        );
    }
}
