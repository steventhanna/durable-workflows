diesel::table! {
    durable_workflow (id) {
        id -> Bigint,
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
        available_at -> Bigint,
        activation_attempts -> Integer,
        max_activation_attempts -> Integer,
        consecutive_continuations -> Integer,
        lease_owner -> Nullable<Text>,
        lease_token -> Nullable<Text>,
        lease_expires_at -> Nullable<Bigint>,
        deduplication_key -> Nullable<Text>,
        schedule_run_id -> Nullable<Bigint>,
        root_workflow_id -> Nullable<Bigint>,
        restarted_from_workflow_id -> Nullable<Bigint>,
        parent_workflow_id -> Nullable<Bigint>,
        parent_command_sequence -> Nullable<Integer>,
        command_sequence -> Integer,
        delivered_event_sequence -> Integer,
        created_at -> Bigint,
        updated_at -> Bigint,
        completed_at -> Nullable<Bigint>,
    }
}

diesel::table! {
    durable_workflow_event (id) {
        id -> Bigint,
        workflow_id -> Bigint,
        sequence -> Integer,
        delivery_sequence -> Nullable<Integer>,
        event_type -> Text,
        metadata_json -> Nullable<Text>,
        actor_type -> Nullable<Text>,
        actor_id -> Nullable<Text>,
        reason -> Nullable<Text>,
        created_at -> Bigint,
    }
}

diesel::table! {
    durable_activity (id) {
        id -> Bigint,
        workflow_id -> Bigint,
        command_sequence -> Integer,
        replacement_number -> Integer,
        kind -> Text,
        version -> Integer,
        topic -> Text,
        payload_json -> Text,
        status -> Text,
        available_at -> Bigint,
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
        lease_expires_at -> Nullable<Bigint>,
        root_activity_id -> Nullable<Bigint>,
        replaces_activity_id -> Nullable<Bigint>,
        created_at -> Bigint,
        updated_at -> Bigint,
        completed_at -> Nullable<Bigint>,
    }
}

diesel::table! {
    durable_activity_attempt (activity_id, attempt_number) {
        activity_id -> Bigint,
        attempt_number -> Integer,
        worker_id -> Text,
        lease_token -> Text,
        started_at -> Bigint,
        heartbeat_at -> Bigint,
        finished_at -> Nullable<Bigint>,
        outcome -> Nullable<Text>,
        error_category -> Nullable<Text>,
        error_message -> Nullable<Text>,
        provider_result_json -> Nullable<Text>,
    }
}

diesel::table! {
    durable_progress_event (activity_id, attempt_number, sequence) {
        activity_id -> Bigint,
        attempt_number -> Integer,
        sequence -> Integer,
        code -> Text,
        description -> Text,
        description_bytes -> Integer,
        completed_units -> Nullable<Bigint>,
        total_units -> Nullable<Bigint>,
        severity -> Text,
        metadata_json -> Nullable<Text>,
        created_at -> Bigint,
    }
}

diesel::table! {
    durable_approval (id) {
        id -> Bigint,
        workflow_id -> Bigint,
        command_sequence -> Integer,
        kind -> Text,
        version -> Integer,
        prompt_metadata_json -> Text,
        validation_schema_json -> Text,
        validation_version -> Integer,
        status -> Text,
        requested_at -> Bigint,
        expires_at -> Nullable<Bigint>,
        decision_payload_json -> Nullable<Text>,
        decided_by -> Nullable<Integer>,
        operator_reason -> Nullable<Text>,
        resolved_at -> Nullable<Bigint>,
    }
}

diesel::table! {
    durable_schedule_state (schedule_key) {
        schedule_key -> Text,
        definition_fingerprint -> Text,
        definition_version -> Integer,
        next_local_occurrence -> Text,
        next_occurrence_at -> Bigint,
        last_materialized_at -> Nullable<Bigint>,
        paused_at -> Nullable<Bigint>,
        paused_by -> Nullable<Integer>,
        pause_reason -> Nullable<Text>,
        created_at -> Bigint,
        updated_at -> Bigint,
    }
}

diesel::table! {
    durable_schedule_run (id) {
        id -> Bigint,
        schedule_key -> Text,
        local_occurrence -> Text,
        scheduled_for -> Bigint,
        materialized_at -> Bigint,
        status -> Text,
        reason -> Nullable<Text>,
        actor_id -> Nullable<Integer>,
        workflow_id -> Nullable<Bigint>,
        created_at -> Bigint,
    }
}

diesel::table! {
    durable_topic_lock (topic) {
        topic -> Text,
        max_concurrency -> Integer,
        updated_at -> Bigint,
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
