use std::time::Duration;

use durable_workflows::{
    ActivityCommand, ActivityError, ActivityResult, ActivityTopic, BackoffPolicy, DurableActivity,
    DurableWorkflow, RetryPolicy, WorkflowError, WorkflowId,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
struct ExampleWorkflow {
    value: i32,
}

impl DurableWorkflow for ExampleWorkflow {
    const KIND: &'static str = "example";
    const VERSION: i32 = 1;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Topics {
    External,
}

impl ActivityTopic for Topics {
    fn key(self) -> &'static str {
        match self {
            Self::External => "example_topic",
        }
    }

    fn max_concurrency(self) -> u32 {
        match self {
            Self::External => 3,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
struct ExampleActivity {
    value: i32,
}

impl DurableActivity for ExampleActivity {
    type Topic = Topics;

    const KIND: &'static str = "example_activity";
    const VERSION: i32 = 2;
    const MAX_ATTEMPTS: u32 = 8;
    const TIMEOUT: Duration = Duration::from_secs(120);
    const LEASE_DURATION: Duration = Duration::from_secs(180);

    fn topic() -> Self::Topic {
        Topics::External
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::exponential(5, 300, 20).expect("test policy is valid")
    }
}

#[test]
fn definition_metadata_is_stable_and_typed() {
    assert_eq!(ExampleWorkflow::KIND, "example");
    assert_eq!(ExampleWorkflow::VERSION, 1);
    assert_eq!(ExampleActivity::KIND, "example_activity");
    assert_eq!(ExampleActivity::VERSION, 2);
    assert_eq!(ExampleActivity::topic(), Topics::External);
    assert_eq!(ExampleActivity::topic().key(), "example_topic");
    assert_eq!(ExampleActivity::topic().max_concurrency(), 3);
}

#[test]
fn activity_command_preserves_exact_definition_and_policy() {
    let command = ActivityCommand::new(&ExampleActivity { value: 7 }, Some("op-7".into()))
        .expect("command is valid");

    assert_eq!(command.kind(), "example_activity");
    assert_eq!(command.version(), 2);
    assert_eq!(command.topic(), "example_topic");
    assert_eq!(command.operation_key(), Some("op-7"));
    assert_eq!(command.payload_json(), r#"{"value":7}"#);
    assert_eq!(command.max_attempts(), 8);
    assert_eq!(command.timeout(), Duration::from_secs(120));
    assert_eq!(command.lease_duration(), Duration::from_secs(180));
}

#[test]
fn exponential_backoff_stays_inside_configured_jitter_bounds() {
    let policy = RetryPolicy::exponential(5, 300, 20).expect("valid policy");

    assert_eq!(
        policy.delay_for_attempt(1, 0).unwrap(),
        Duration::from_secs(4)
    );
    assert_eq!(
        policy.delay_for_attempt(1, 50).unwrap(),
        Duration::from_secs(5)
    );
    assert_eq!(
        policy.delay_for_attempt(1, 100).unwrap(),
        Duration::from_secs(6)
    );
    assert_eq!(
        policy.delay_for_attempt(10, 50).unwrap(),
        Duration::from_secs(300)
    );
    assert_eq!(
        policy.backoff(),
        BackoffPolicy::Exponential {
            initial_secs: 5,
            max_secs: 300,
            jitter_percent: 20,
        }
    );
}

#[test]
fn activity_result_rejects_definition_mismatch_before_decoding() {
    let result = ActivityResult::new("another_activity", 2, r#"{"accepted":true}"#.into())
        .expect("stored result is valid");

    let error = result
        .decode_for::<ExampleActivity, serde_json::Value>()
        .expect_err("kind mismatch must fail");
    assert!(error.to_string().contains("another_activity"));
    assert!(error.to_string().contains("example_activity"));
}

#[test]
fn typed_ids_round_trip_and_display_without_cross_type_conversion() {
    let workflow_id = WorkflowId::new(42).expect("positive database id");

    assert_eq!(workflow_id.get(), 42);
    assert_eq!(workflow_id.to_string(), "42");
    assert_eq!(serde_json::to_string(&workflow_id).unwrap(), "42");
    assert_eq!(
        serde_json::from_str::<WorkflowId>("42").unwrap(),
        workflow_id
    );
}

#[test]
fn activity_payloads_over_256_kib_are_rejected_before_persistence() {
    #[derive(serde::Serialize)]
    struct OversizedActivity {
        value: String,
    }

    impl DurableActivity for OversizedActivity {
        type Topic = Topics;

        const KIND: &'static str = "oversized";
        const VERSION: i32 = 1;
        const MAX_ATTEMPTS: u32 = 1;
        const TIMEOUT: Duration = Duration::from_secs(1);
        const LEASE_DURATION: Duration = Duration::from_secs(2);

        fn topic() -> Self::Topic {
            Topics::External
        }

        fn retry_policy() -> RetryPolicy {
            RetryPolicy::fixed(1).expect("test policy is valid")
        }
    }

    impl<'de> serde::Deserialize<'de> for OversizedActivity {
        fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            Ok(Self {
                value: String::new(),
            })
        }
    }

    let error = ActivityCommand::new(
        &OversizedActivity {
            value: "x".repeat(256 * 1024),
        },
        None,
    )
    .expect_err("oversized payload must fail");

    assert!(error.to_string().contains("262144"));
}

#[test]
fn handler_errors_are_truncated_on_utf8_boundaries() {
    let category = "é".repeat(40);
    let message = "🦀".repeat(600);

    let workflow_error = WorkflowError::new(&category, &message);
    assert_eq!(workflow_error.category.len(), 64);
    assert_eq!(workflow_error.message.len(), 2_048);
    assert!(workflow_error
        .category
        .is_char_boundary(workflow_error.category.len()));
    assert!(workflow_error
        .message
        .is_char_boundary(workflow_error.message.len()));

    let activity_error = ActivityError::retryable(category, message);
    let ActivityError::Retryable { category, message } = activity_error else {
        panic!("expected a retryable activity error");
    };
    assert_eq!(category.len(), 64);
    assert_eq!(message.len(), 2_048);
}

#[test]
fn manual_activity_definitions_require_a_lease_longer_than_the_timeout() {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct EqualLeaseActivity;

    impl DurableActivity for EqualLeaseActivity {
        type Topic = Topics;

        const KIND: &'static str = "equal_lease";
        const VERSION: i32 = 1;
        const MAX_ATTEMPTS: u32 = 1;
        const TIMEOUT: Duration = Duration::from_secs(30);
        const LEASE_DURATION: Duration = Duration::from_secs(30);

        fn topic() -> Self::Topic {
            Topics::External
        }

        fn retry_policy() -> RetryPolicy {
            RetryPolicy::fixed(1).expect("test policy is valid")
        }
    }

    let error = ActivityCommand::new(&EqualLeaseActivity, None)
        .expect_err("the lease must leave time for timeout handling and fencing");

    assert!(error
        .to_string()
        .contains("lease duration must be greater than its timeout"));
}
