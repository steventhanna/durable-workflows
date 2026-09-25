use std::time::Duration;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    ActivityError, ActivityId, ProgressReporter, RetryPolicy, WorkflowError, WorkflowEvent,
    WorkflowTransition,
};

pub trait DurableWorkflow: Serialize + DeserializeOwned + Send + Sync + 'static {
    const KIND: &'static str;
    const VERSION: i32;
}

pub trait DurableActivity: Serialize + DeserializeOwned + Send + Sync + 'static {
    type Topic: ActivityTopic;

    const KIND: &'static str;
    const VERSION: i32;
    const MAX_ATTEMPTS: u32;
    const TIMEOUT: Duration;
    const LEASE_DURATION: Duration;

    fn topic() -> Self::Topic;
    fn retry_policy() -> RetryPolicy;
}

/// Logical activity queue. `key()` must be lowercase ASCII matching
/// `[a-z0-9][a-z0-9._-]*`.
pub trait ActivityTopic: Copy + Send + Sync + 'static {
    fn key(self) -> &'static str;
    fn max_concurrency(self) -> u32;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DefinitionKey {
    pub kind: String,
    pub version: i32,
}

impl DefinitionKey {
    pub fn new(kind: impl Into<String>, version: i32) -> Self {
        Self {
            kind: kind.into(),
            version,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WorkflowContext<'a, C> {
    application: &'a C,
    workflow_id: Option<crate::WorkflowId>,
}

impl<'a, C> WorkflowContext<'a, C> {
    pub fn new(application: &'a C) -> Self {
        Self {
            application,
            workflow_id: None,
        }
    }

    pub fn with_workflow_id(mut self, workflow_id: crate::WorkflowId) -> Self {
        self.workflow_id = Some(workflow_id);
        self
    }

    pub fn application(&self) -> &'a C {
        self.application
    }

    pub fn workflow_id(&self) -> Option<crate::WorkflowId> {
        self.workflow_id
    }
}

#[derive(Debug, Clone)]
pub struct ActivityContext<'a, C> {
    application: &'a C,
    activity_id: Option<ActivityId>,
    attempt_number: Option<u32>,
    lease_token: Option<&'a str>,
    operation_key: Option<&'a str>,
    cancellation: Option<tokio_util::sync::CancellationToken>,
    progress: Option<ProgressReporter>,
}

impl<'a, C> ActivityContext<'a, C> {
    pub fn new(application: &'a C) -> Self {
        Self {
            application,
            activity_id: None,
            attempt_number: None,
            lease_token: None,
            operation_key: None,
            cancellation: None,
            progress: None,
        }
    }

    pub(crate) fn for_execution(
        application: &'a C,
        activity_id: ActivityId,
        attempt_number: u32,
        lease_token: &'a str,
        operation_key: Option<&'a str>,
        cancellation: tokio_util::sync::CancellationToken,
        progress: ProgressReporter,
    ) -> Self {
        Self {
            application,
            activity_id: Some(activity_id),
            attempt_number: Some(attempt_number),
            lease_token: Some(lease_token),
            operation_key,
            cancellation: Some(cancellation),
            progress: Some(progress),
        }
    }

    pub fn application(&self) -> &'a C {
        self.application
    }

    pub fn activity_id(&self) -> Option<ActivityId> {
        self.activity_id
    }

    pub fn attempt_number(&self) -> Option<u32> {
        self.attempt_number
    }

    pub fn lease_token(&self) -> Option<&str> {
        self.lease_token
    }

    pub fn operation_key(&self) -> Option<&str> {
        self.operation_key
    }

    pub fn cancellation_token(&self) -> Option<&tokio_util::sync::CancellationToken> {
        self.cancellation.as_ref()
    }

    pub fn progress_reporter(&self) -> Option<&ProgressReporter> {
        self.progress.as_ref()
    }
}

#[async_trait]
pub trait WorkflowHandler: DurableWorkflow {
    type Context: Send + Sync + 'static;
    type State: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Approval: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Output: Serialize + DeserializeOwned + Send + Sync + 'static;

    fn initial_state(&self) -> Self::State;

    async fn step(
        &self,
        context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        event: WorkflowEvent,
    ) -> Result<WorkflowTransition<Self::State, Self::Approval, Self::Output>, WorkflowError>;
}

#[async_trait]
pub trait ActivityHandler: DurableActivity {
    type Context: Send + Sync + 'static;
    type Output: Serialize + DeserializeOwned + Send + Sync + 'static;

    async fn execute(
        &self,
        context: ActivityContext<'_, Self::Context>,
    ) -> Result<Self::Output, ActivityError>;
}
