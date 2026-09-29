diesel::table! {
    use diesel::sql_types::*;
    use crate::sql_types::*;

    durable_workflow (id) {
        id -> WorkflowId,
        kind -> Text,
        version -> Integer,
        input_json -> Text,
        state_json -> Text,
        state_version -> Integer,
        status -> Text,
        result_json -> Nullable<Text>,
        error_category -> Nullable<Text>,
        error_message -> Nullable<Text>,
        wait_kind -> Nullable<Text>,
        wait_reference_id -> Nullable<Bigint>,
        available_at -> DbMillis,
        activation_attempts -> Integer,
        max_activation_attempts -> Integer,
        consecutive_continuations -> Integer,
        lease_owner -> Nullable<Text>,
        lease_token -> Nullable<Text>,
        lease_expires_at -> Nullable<DbMillis>,
        deduplication_key -> Nullable<Text>,
        schedule_run_id -> Nullable<ScheduleRunId>,
        root_workflow_id -> Nullable<WorkflowId>,
        restarted_from_workflow_id -> Nullable<WorkflowId>,
        parent_workflow_id -> Nullable<WorkflowId>,
        parent_command_sequence -> Nullable<Integer>,
        command_sequence -> Integer,
        delivered_event_sequence -> Integer,
        created_at -> DbMillis,
        updated_at -> DbMillis,
        completed_at -> Nullable<DbMillis>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::sql_types::*;

    durable_workflow_event (id) {
        id -> Bigint,
        workflow_id -> WorkflowId,
        sequence -> Integer,
        delivery_sequence -> Nullable<Integer>,
        event_type -> Text,
        metadata_json -> Nullable<Text>,
        actor_type -> Nullable<Text>,
        actor_id -> Nullable<Text>,
        reason -> Nullable<Text>,
        created_at -> DbMillis,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::sql_types::*;

    durable_activity (id) {
        id -> ActivityId,
        workflow_id -> WorkflowId,
        command_sequence -> Integer,
        replacement_number -> Integer,
        kind -> Text,
        version -> Integer,
        topic -> Text,
        payload_json -> Text,
        status -> Text,
        available_at -> DbMillis,
        max_attempts -> Integer,
        attempt_count -> Integer,
        timeout_millis -> Bigint,
        lease_duration_millis -> Bigint,
        retry_policy_json -> Text,
        operation_key -> Nullable<Text>,
        provider_result_json -> Nullable<Text>,
        last_error_category -> Nullable<Text>,
        last_error_message -> Nullable<Text>,
        lease_owner -> Nullable<Text>,
        lease_token -> Nullable<Text>,
        lease_expires_at -> Nullable<DbMillis>,
        root_activity_id -> Nullable<ActivityId>,
        replaces_activity_id -> Nullable<ActivityId>,
        created_at -> DbMillis,
        updated_at -> DbMillis,
        completed_at -> Nullable<DbMillis>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::sql_types::*;

    durable_activity_attempt (activity_id, attempt_number) {
        activity_id -> ActivityId,
        attempt_number -> Integer,
        worker_id -> Text,
        lease_token -> Text,
        started_at -> DbMillis,
        heartbeat_at -> DbMillis,
        finished_at -> Nullable<DbMillis>,
        outcome -> Nullable<Text>,
        error_category -> Nullable<Text>,
        error_message -> Nullable<Text>,
        provider_result_json -> Nullable<Text>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::sql_types::*;

    durable_progress_event (activity_id, attempt_number, sequence) {
        activity_id -> ActivityId,
        attempt_number -> Integer,
        sequence -> Integer,
        code -> Text,
        description -> Text,
        description_bytes -> Integer,
        completed_units -> Nullable<Bigint>,
        total_units -> Nullable<Bigint>,
        severity -> Text,
        metadata_json -> Nullable<Text>,
        created_at -> DbMillis,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::sql_types::*;

    durable_approval (id) {
        id -> ApprovalId,
        workflow_id -> WorkflowId,
        command_sequence -> Integer,
        kind -> Text,
        version -> Integer,
        prompt_metadata_json -> Text,
        validation_schema_json -> Text,
        validation_version -> Integer,
        status -> Text,
        requested_at -> DbMillis,
        expires_at -> Nullable<DbMillis>,
        decision_payload_json -> Nullable<Text>,
        decided_by -> Nullable<Integer>,
        operator_reason -> Nullable<Text>,
        resolved_at -> Nullable<DbMillis>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::sql_types::DbMillis;

    durable_schedule_state (schedule_key) {
        schedule_key -> Text,
        definition_fingerprint -> Text,
        definition_version -> Integer,
        next_local_occurrence -> Text,
        next_occurrence_at -> DbMillis,
        last_materialized_at -> Nullable<DbMillis>,
        paused_at -> Nullable<DbMillis>,
        paused_by -> Nullable<Integer>,
        pause_reason -> Nullable<Text>,
        created_at -> DbMillis,
        updated_at -> DbMillis,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::sql_types::*;

    durable_schedule_run (id) {
        id -> ScheduleRunId,
        schedule_key -> Text,
        local_occurrence -> Text,
        scheduled_for -> DbMillis,
        materialized_at -> DbMillis,
        status -> Text,
        reason -> Nullable<Text>,
        actor_id -> Nullable<Integer>,
        workflow_id -> Nullable<WorkflowId>,
        created_at -> DbMillis,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::sql_types::DbMillis;

    durable_topic_lock (topic) {
        topic -> Text,
        max_concurrency -> Integer,
        updated_at -> DbMillis,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    durable_workflow,
    durable_workflow_event,
    durable_activity,
    durable_activity_attempt,
    durable_progress_event,
    durable_approval,
    durable_schedule_state,
    durable_schedule_run,
    durable_topic_lock,
);
