use std::time::Duration;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    error::ensure_size, ActivityTopic, DurableActivity, DurableError, DurableWorkflow, RetryPolicy,
    WorkflowHandler, MAX_EVENT_METADATA_BYTES, MAX_INPUT_STATE_PAYLOAD_BYTES, MAX_OUTPUT_BYTES,
};

/// Matches `durable_activity.operation_key VARCHAR(191)`.
const MAX_OPERATION_KEY_CHARS: usize = 191;

// Zero sorts ready continuation chains ahead of ordinary work without bypassing retry deadlines.
pub(crate) const CONTINUATION_READY_AT_MILLIS: i64 = 0;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityCommand {
    kind: String,
    version: i32,
    topic: String,
    payload_json: String,
    operation_key: Option<String>,
    max_attempts: u32,
    timeout_millis: u64,
    lease_duration_millis: u64,
    retry_policy: RetryPolicy,
    #[serde(default)]
    continuation_priority: bool,
}

impl ActivityCommand {
    pub fn new<A: DurableActivity>(
        activity: &A,
        operation_key: Option<String>,
    ) -> Result<Self, DurableError> {
        let payload_json = serde_json::to_string(activity)?;
        ensure_size(
            "activity payload",
            &payload_json,
            MAX_INPUT_STATE_PAYLOAD_BYTES,
        )?;
        let timeout_millis = duration_millis(A::TIMEOUT)?;
        let lease_duration_millis = duration_millis(A::LEASE_DURATION)?;
        if lease_duration_millis <= timeout_millis {
            return Err(DurableError::InvalidDefinition(
                "activity lease duration must be greater than its timeout".to_string(),
            ));
        }
        if A::MAX_ATTEMPTS == 0 {
            return Err(DurableError::InvalidDefinition(
                "activity max attempts must be greater than zero".to_string(),
            ));
        }
        if let Some(key) = operation_key.as_deref() {
            let key_length = key.chars().count();
            if key_length == 0 || key_length > MAX_OPERATION_KEY_CHARS {
                return Err(DurableError::InvalidDefinition(format!(
                    "activity operation key must contain 1 to {MAX_OPERATION_KEY_CHARS} characters"
                )));
            }
        }

        Ok(Self {
            kind: A::KIND.to_owned(),
            version: A::VERSION,
            topic: A::topic().key().to_owned(),
            payload_json,
            operation_key,
            max_attempts: A::MAX_ATTEMPTS,
            timeout_millis,
            lease_duration_millis,
            retry_policy: A::retry_policy(),
            continuation_priority: false,
        })
    }

    /// Prefer the next page of a finite scan over starting unrelated work.
    /// Retry backoff still applies; fresh scans should leave this disabled.
    pub fn with_continuation_priority(mut self, enabled: bool) -> Self {
        self.continuation_priority = enabled;
        self
    }

    pub fn has_continuation_priority(&self) -> bool {
        self.continuation_priority
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn payload_json(&self) -> &str {
        &self.payload_json
    }

    pub fn operation_key(&self) -> Option<&str> {
        self.operation_key.as_deref()
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_millis)
    }

    pub fn lease_duration(&self) -> Duration {
        Duration::from_millis(self.lease_duration_millis)
    }

    pub fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy
    }
}

fn duration_millis(duration: Duration) -> Result<u64, DurableError> {
    u64::try_from(duration.as_millis()).map_err(|_| {
        DurableError::InvalidDefinition("duration exceeds the durable storage range".to_string())
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityResult {
    kind: String,
    version: i32,
    output_json: String,
}

impl ActivityResult {
    pub fn new(
        kind: impl Into<String>,
        version: i32,
        output_json: String,
    ) -> Result<Self, DurableError> {
        ensure_size("activity output", &output_json, MAX_OUTPUT_BYTES)?;
        Ok(Self {
            kind: kind.into(),
            version,
            output_json,
        })
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn output_json(&self) -> &str {
        &self.output_json
    }

    pub fn decode_for<A, O>(&self) -> Result<O, DurableError>
    where
        A: DurableActivity,
        O: DeserializeOwned,
    {
        ensure_definition::<A>(&self.kind, self.version)?;
        Ok(serde_json::from_str(&self.output_json)?)
    }
}

fn ensure_definition<A: DurableActivity>(kind: &str, version: i32) -> Result<(), DurableError> {
    if kind == A::KIND && version == A::VERSION {
        return Ok(());
    }
    Err(DurableError::DefinitionMismatch {
        actual_kind: kind.to_owned(),
        actual_version: version,
        expected_kind: A::KIND.to_string(),
        expected_version: A::VERSION,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildWorkflowCommand {
    kind: String,
    version: i32,
    input_json: String,
    state_json: String,
    deduplication_key: Option<String>,
}

impl ChildWorkflowCommand {
    pub fn new<W: WorkflowHandler>(workflow: &W) -> Result<Self, DurableError> {
        if W::KIND.is_empty() || W::VERSION <= 0 {
            return Err(DurableError::InvalidDefinition(
                "child workflow kind must be non-empty and version must be positive".to_string(),
            ));
        }
        let input_json = serde_json::to_string(workflow)?;
        ensure_size(
            "child workflow input",
            &input_json,
            MAX_INPUT_STATE_PAYLOAD_BYTES,
        )?;
        let state_json = serde_json::to_string(&workflow.initial_state())?;
        ensure_size(
            "child workflow state",
            &state_json,
            MAX_INPUT_STATE_PAYLOAD_BYTES,
        )?;
        Ok(Self {
            kind: W::KIND.to_owned(),
            version: W::VERSION,
            input_json,
            state_json,
            deduplication_key: None,
        })
    }

    pub fn with_deduplication_key(mut self, key: impl Into<String>) -> Self {
        self.deduplication_key = Some(key.into());
        self
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn input_json(&self) -> &str {
        &self.input_json
    }

    pub fn state_json(&self) -> &str {
        &self.state_json
    }

    pub fn deduplication_key(&self) -> Option<&str> {
        self.deduplication_key.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildResult {
    kind: String,
    version: i32,
    output_json: String,
}

impl ChildResult {
    pub fn new(
        kind: impl Into<String>,
        version: i32,
        output_json: String,
    ) -> Result<Self, DurableError> {
        ensure_size("child workflow output", &output_json, MAX_OUTPUT_BYTES)?;
        Ok(Self {
            kind: kind.into(),
            version,
            output_json,
        })
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn output_json(&self) -> &str {
        &self.output_json
    }

    pub fn decode_for<W, O>(&self) -> Result<O, DurableError>
    where
        W: DurableWorkflow,
        O: DeserializeOwned,
    {
        if self.kind != W::KIND || self.version != W::VERSION {
            return Err(DurableError::DefinitionMismatch {
                actual_kind: self.kind.clone(),
                actual_version: self.version,
                expected_kind: W::KIND.to_string(),
                expected_version: W::VERSION,
            });
        }
        Ok(serde_json::from_str(&self.output_json)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRequest {
    pub kind: String,
    pub version: i32,
    pub payload_json: String,
}

impl ApprovalRequest {
    pub fn new<T: Serialize>(
        kind: impl Into<String>,
        version: i32,
        value: &T,
    ) -> Result<Self, DurableError> {
        let payload_json = serde_json::to_string(value)?;
        ensure_size("approval request", &payload_json, MAX_EVENT_METADATA_BYTES)?;
        Ok(Self {
            kind: kind.into(),
            version,
            payload_json,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalResult {
    pub kind: String,
    pub version: i32,
    pub payload_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "camelCase")]
pub enum WorkflowEvent {
    Started,
    ActivitySucceeded {
        command_sequence: u32,
        result: ActivityResult,
    },
    ChildSucceeded {
        command_sequence: u32,
        result: ChildResult,
    },
    ChildFailed {
        command_sequence: u32,
        kind: String,
        version: i32,
        category: String,
        message: String,
    },
    TimerFired {
        command_sequence: u32,
    },
    ApprovalResolved {
        command_sequence: u32,
        result: ApprovalResult,
    },
    ApprovalExpired {
        command_sequence: u32,
    },
    Continued,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum WorkflowTransition<S, A, O> {
    RunActivity {
        state: S,
        activity: ActivityCommand,
    },
    RunChild {
        state: S,
        child: ChildWorkflowCommand,
    },
    SleepUntil {
        state: S,
        wake_at_millis: i64,
    },
    WaitForApproval {
        state: S,
        approval: A,
        expires_at_millis: Option<i64>,
    },
    Continue {
        state: S,
    },
    Complete {
        output: O,
    },
}
