use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::ensure_size,
    persistence::{NewTopicLockRow, TopicLockRow},
    schema::durable_topic_lock,
    ActivityContext, ActivityError, ActivityHandler, ActivityTopic, DefinitionKey,
    DurableConnection, DurableError, WorkflowContext, WorkflowError, WorkflowEvent,
    WorkflowHandler, WorkflowTransition, MAX_EVENT_METADATA_BYTES, MAX_INPUT_STATE_PAYLOAD_BYTES,
    MAX_OUTPUT_BYTES,
};

#[derive(Debug, Clone, PartialEq)]
pub enum StoredTransition {
    RunActivity {
        state_json: String,
        activity: crate::ActivityCommand,
    },
    RunChild {
        state_json: String,
        child: crate::ChildWorkflowCommand,
    },
    SleepUntil {
        state_json: String,
        wake_at_millis: i64,
    },
    WaitForApproval {
        state_json: String,
        approval_json: String,
        expires_at_millis: Option<i64>,
    },
    Continue {
        state_json: String,
    },
    Complete {
        output_json: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum WorkflowDispatchError {
    #[error("workflow handler failed: {0}")]
    Handler(#[from] WorkflowError),
    #[error("workflow dispatch serialization failed: {0}")]
    Durable(#[from] DurableError),
}

#[derive(Debug, thiserror::Error)]
pub enum ActivityDispatchError {
    #[error("activity handler failed: {0}")]
    Handler(#[from] ActivityError),
    #[error("activity dispatch serialization failed: {0}")]
    Durable(#[from] DurableError),
}

#[async_trait]
trait ErasedWorkflowHandler<C>: Send + Sync {
    async fn step_stored(
        &self,
        context: &C,
        workflow_id: Option<crate::WorkflowId>,
        input_json: &str,
        state_json: &str,
        event: WorkflowEvent,
    ) -> Result<StoredTransition, WorkflowDispatchError>;

    fn prepare_start(&self, input_json: &str) -> Result<PreparedWorkflowStart, DurableError>;

    fn validate_approval(&self, decision_json: &str) -> Result<String, DurableError>;
}

struct WorkflowAdapter<W>(PhantomData<W>);

#[async_trait]
impl<C, W> ErasedWorkflowHandler<C> for WorkflowAdapter<W>
where
    C: Send + Sync + 'static,
    W: WorkflowHandler<Context = C>,
{
    async fn step_stored(
        &self,
        context: &C,
        workflow_id: Option<crate::WorkflowId>,
        input_json: &str,
        state_json: &str,
        event: WorkflowEvent,
    ) -> Result<StoredTransition, WorkflowDispatchError> {
        let workflow: W = deserialize(input_json)?;
        let state: W::State = deserialize(state_json)?;
        let mut context = WorkflowContext::new(context);
        if let Some(workflow_id) = workflow_id {
            context = context.with_workflow_id(workflow_id);
        }
        let transition = workflow.step(context, state, event).await?;
        store_transition(transition)
    }

    fn prepare_start(&self, input_json: &str) -> Result<PreparedWorkflowStart, DurableError> {
        let workflow: W = deserialize_request("workflow input", input_json)?;
        Ok(PreparedWorkflowStart {
            kind: W::KIND.to_string(),
            version: W::VERSION,
            input_json: serialize_bounded(
                "workflow input",
                &workflow,
                MAX_INPUT_STATE_PAYLOAD_BYTES,
            )?,
            state_json: serialize_bounded(
                "workflow state",
                &workflow.initial_state(),
                MAX_INPUT_STATE_PAYLOAD_BYTES,
            )?,
        })
    }

    fn validate_approval(&self, decision_json: &str) -> Result<String, DurableError> {
        let decision: W::Approval = deserialize_request("approval decision", decision_json)?;
        serialize_bounded("approval decision", &decision, MAX_EVENT_METADATA_BYTES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedWorkflowStart {
    kind: String,
    version: i32,
    input_json: String,
    state_json: String,
}

impl PreparedWorkflowStart {
    pub fn from_workflow<W, S>(workflow: &W, state: &S) -> Result<Self, DurableError>
    where
        W: crate::DurableWorkflow + Serialize,
        S: Serialize,
    {
        validate_definition(W::KIND, W::VERSION)?;
        Ok(Self {
            kind: W::KIND.to_string(),
            version: W::VERSION,
            input_json: serialize_bounded(
                "workflow input",
                workflow,
                MAX_INPUT_STATE_PAYLOAD_BYTES,
            )?,
            state_json: serialize_bounded("workflow state", state, MAX_INPUT_STATE_PAYLOAD_BYTES)?,
        })
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
}

pub struct WorkflowRegistry<C> {
    definitions: HashMap<DefinitionKey, Arc<dyn ErasedWorkflowHandler<C>>>,
}

impl<C> Default for WorkflowRegistry<C> {
    fn default() -> Self {
        Self {
            definitions: HashMap::new(),
        }
    }
}

impl<C> WorkflowRegistry<C>
where
    C: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<W>(&mut self) -> Result<(), DurableError>
    where
        W: WorkflowHandler<Context = C>,
    {
        validate_definition(W::KIND, W::VERSION)?;
        let key = DefinitionKey::new(W::KIND, W::VERSION);
        if self.definitions.contains_key(&key) {
            return Err(DurableError::DuplicateDefinition {
                kind: key.kind,
                version: key.version,
            });
        }
        self.definitions
            .insert(key, Arc::new(WorkflowAdapter::<W>(PhantomData)));
        Ok(())
    }

    pub fn contains(&self, kind: &str, version: i32) -> bool {
        self.definitions
            .contains_key(&DefinitionKey::new(kind, version))
    }

    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }

    pub fn definition_keys(&self) -> Vec<(String, i32)> {
        let mut keys: Vec<_> = self
            .definitions
            .keys()
            .map(|key| (key.kind.clone(), key.version))
            .collect();
        keys.sort();
        keys
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn step_stored(
        &self,
        kind: &str,
        version: i32,
        context: &C,
        workflow_id: Option<crate::WorkflowId>,
        input_json: &str,
        state_json: &str,
        event: WorkflowEvent,
    ) -> Result<StoredTransition, WorkflowDispatchError> {
        let definition = self
            .definitions
            .get(&DefinitionKey::new(kind, version))
            .ok_or_else(|| DurableError::MissingDefinition {
                kind: kind.to_owned(),
                version,
            })?;
        definition
            .step_stored(context, workflow_id, input_json, state_json, event)
            .await
    }

    pub fn current_version(&self, kind: &str) -> Option<i32> {
        self.definitions
            .keys()
            .filter(|key| key.kind == kind)
            .map(|key| key.version)
            .max()
    }

    pub fn prepare_start_exact(
        &self,
        kind: &str,
        version: i32,
        input_json: &str,
    ) -> Result<PreparedWorkflowStart, DurableError> {
        self.workflow_definition(kind, version)?
            .prepare_start(input_json)
    }

    pub fn prepare_start_current(
        &self,
        kind: &str,
        input_json: &str,
    ) -> Result<PreparedWorkflowStart, DurableError> {
        let version =
            self.current_version(kind)
                .ok_or_else(|| DurableError::MissingCurrentDefinition {
                    kind: kind.to_string(),
                })?;
        self.prepare_start_exact(kind, version, input_json)
    }

    pub fn validate_approval_exact(
        &self,
        kind: &str,
        version: i32,
        decision_json: &str,
    ) -> Result<String, DurableError> {
        self.workflow_definition(kind, version)?
            .validate_approval(decision_json)
    }

    fn workflow_definition(
        &self,
        kind: &str,
        version: i32,
    ) -> Result<&Arc<dyn ErasedWorkflowHandler<C>>, DurableError> {
        self.definitions
            .get(&DefinitionKey::new(kind, version))
            .ok_or_else(|| DurableError::MissingDefinition {
                kind: kind.to_owned(),
                version,
            })
    }
}

#[async_trait]
trait ErasedActivityHandler<C>: Send + Sync {
    async fn execute_stored(
        &self,
        context: ActivityContext<'_, C>,
        payload_json: &str,
    ) -> Result<String, ActivityDispatchError>;

    fn prepare_command(
        &self,
        payload_json: &str,
        operation_key: Option<String>,
    ) -> Result<crate::ActivityCommand, DurableError>;
}

struct ActivityAdapter<A>(PhantomData<A>);

#[async_trait]
impl<C, A> ErasedActivityHandler<C> for ActivityAdapter<A>
where
    C: Send + Sync + 'static,
    A: ActivityHandler<Context = C>,
{
    async fn execute_stored(
        &self,
        context: ActivityContext<'_, C>,
        payload_json: &str,
    ) -> Result<String, ActivityDispatchError> {
        let activity: A = deserialize(payload_json)?;
        let output = ActivityHandler::execute(&activity, context).await?;
        serialize_bounded("activity output", &output, MAX_OUTPUT_BYTES).map_err(Into::into)
    }

    fn prepare_command(
        &self,
        payload_json: &str,
        operation_key: Option<String>,
    ) -> Result<crate::ActivityCommand, DurableError> {
        let activity: A = deserialize_request("activity payload", payload_json)?;
        crate::ActivityCommand::new(&activity, operation_key)
    }
}

pub struct ActivityRegistry<C> {
    definitions: HashMap<DefinitionKey, Arc<dyn ErasedActivityHandler<C>>>,
    topics: HashMap<DefinitionKey, String>,
}

impl<C> Default for ActivityRegistry<C> {
    fn default() -> Self {
        Self {
            definitions: HashMap::new(),
            topics: HashMap::new(),
        }
    }
}

impl<C> ActivityRegistry<C>
where
    C: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<A>(&mut self) -> Result<(), DurableError>
    where
        A: ActivityHandler<Context = C>,
    {
        validate_definition(A::KIND, A::VERSION)?;
        let key = DefinitionKey::new(A::KIND, A::VERSION);
        if self.definitions.contains_key(&key) {
            return Err(DurableError::DuplicateDefinition {
                kind: key.kind,
                version: key.version,
            });
        }
        self.topics.insert(key.clone(), A::topic().key().to_owned());
        self.definitions
            .insert(key, Arc::new(ActivityAdapter::<A>(PhantomData)));
        Ok(())
    }

    pub fn contains(&self, kind: &str, version: i32) -> bool {
        self.definitions
            .contains_key(&DefinitionKey::new(kind, version))
    }

    pub fn topic_for(&self, kind: &str, version: i32) -> Option<&str> {
        self.topics
            .get(&DefinitionKey::new(kind, version))
            .map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }

    pub fn definition_keys(&self) -> Vec<(String, i32)> {
        let mut keys: Vec<_> = self
            .definitions
            .keys()
            .map(|key| (key.kind.clone(), key.version))
            .collect();
        keys.sort();
        keys
    }

    pub fn required_topics(&self) -> Vec<String> {
        let mut topics: Vec<_> = self.topics.values().cloned().collect();
        topics.sort();
        topics.dedup();
        topics
    }

    pub async fn execute_stored(
        &self,
        kind: &str,
        version: i32,
        context: &C,
        payload_json: &str,
    ) -> Result<String, ActivityDispatchError> {
        let definition = self
            .definitions
            .get(&DefinitionKey::new(kind, version))
            .ok_or_else(|| DurableError::MissingDefinition {
                kind: kind.to_owned(),
                version,
            })?;
        definition
            .execute_stored(ActivityContext::new(context), payload_json)
            .await
    }

    pub fn current_version(&self, kind: &str) -> Option<i32> {
        self.definitions
            .keys()
            .filter(|key| key.kind == kind)
            .map(|key| key.version)
            .max()
    }

    pub fn prepare_command_exact(
        &self,
        kind: &str,
        version: i32,
        payload_json: &str,
        operation_key: Option<String>,
    ) -> Result<crate::ActivityCommand, DurableError> {
        self.activity_definition(kind, version)?
            .prepare_command(payload_json, operation_key)
    }

    pub fn prepare_command_current(
        &self,
        kind: &str,
        payload_json: &str,
        operation_key: Option<String>,
    ) -> Result<crate::ActivityCommand, DurableError> {
        let version =
            self.current_version(kind)
                .ok_or_else(|| DurableError::MissingCurrentDefinition {
                    kind: kind.to_string(),
                })?;
        self.prepare_command_exact(kind, version, payload_json, operation_key)
    }

    fn activity_definition(
        &self,
        kind: &str,
        version: i32,
    ) -> Result<&Arc<dyn ErasedActivityHandler<C>>, DurableError> {
        self.definitions
            .get(&DefinitionKey::new(kind, version))
            .ok_or_else(|| DurableError::MissingDefinition {
                kind: kind.to_owned(),
                version,
            })
    }

    pub(crate) async fn execute_claimed(
        &self,
        kind: &str,
        version: i32,
        context: ActivityContext<'_, C>,
        payload_json: &str,
    ) -> Result<String, ActivityDispatchError> {
        let definition = self
            .definitions
            .get(&DefinitionKey::new(kind, version))
            .ok_or_else(|| DurableError::MissingDefinition {
                kind: kind.to_owned(),
                version,
            })?;
        definition.execute_stored(context, payload_json).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDefinition {
    pub key: String,
    pub max_concurrency: u32,
}

#[derive(Debug, Default)]
pub struct TopicRegistry {
    definitions: HashMap<String, TopicDefinition>,
    seeded: tokio::sync::OnceCell<()>,
}

impl TopicRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T: ActivityTopic>(&mut self, topic: T) -> Result<(), DurableError> {
        let key = topic.key();
        let max_concurrency = topic.max_concurrency();
        if key.is_empty() {
            return Err(DurableError::InvalidDefinition(
                "activity topic key cannot be empty".to_string(),
            ));
        }
        if !is_canonical_topic_key(key) {
            return Err(DurableError::InvalidDefinition(format!(
                "activity topic {key} must match [a-z0-9][a-z0-9._-]*"
            )));
        }
        if max_concurrency == 0 {
            return Err(DurableError::InvalidDefinition(format!(
                "activity topic {key} must allow at least one concurrent activity"
            )));
        }
        if let Some(existing) = self.definitions.get(key) {
            if existing.max_concurrency == max_concurrency {
                return Ok(());
            }
            return Err(DurableError::InvalidDefinition(format!(
                "activity topic {key} has conflicting concurrency limits {} and {max_concurrency}",
                existing.max_concurrency
            )));
        }
        self.definitions.insert(
            key.to_owned(),
            TopicDefinition {
                key: key.to_owned(),
                max_concurrency,
            },
        );
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&TopicDefinition> {
        self.definitions.get(key)
    }

    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }

    pub fn definitions(&self) -> Vec<TopicDefinition> {
        let mut definitions: Vec<_> = self.definitions.values().cloned().collect();
        definitions.sort_by(|left, right| left.key.cmp(&right.key));
        definitions
    }

    /// Ensures `durable_topic_lock` rows exist for every registered topic and
    /// validates their persisted concurrency limits, doing the actual
    /// insert/select/validate work at most once per `TopicRegistry` instance.
    /// `claim_one` calls this on every poll from every topic worker, so
    /// without the `OnceCell` guard this ran an `INSERT IGNORE` touching all
    /// topic rows plus a bulk `SELECT` on every single poll iteration across
    /// every worker — a steady source of row-lock contention on a table that
    /// only ever changes on redeploy.
    pub async fn seed_locks(&self, connection: &mut DurableConnection) -> Result<(), DurableError> {
        self.seeded
            .get_or_try_init(|| self.seed_locks_once(connection))
            .await?;
        Ok(())
    }

    async fn seed_locks_once(
        &self,
        connection: &mut DurableConnection,
    ) -> Result<(), DurableError> {
        let timestamp = crate::persistence::database_now_millis(connection).await?;
        let mut rows = self
            .definitions
            .values()
            .map(|definition| {
                let max_concurrency = i32::try_from(definition.max_concurrency).map_err(|_| {
                    DurableError::InvalidDefinition(format!(
                        "activity topic {} concurrency limit {} exceeds {}",
                        definition.key,
                        definition.max_concurrency,
                        i32::MAX
                    ))
                })?;
                Ok(NewTopicLockRow {
                    topic: definition.key.clone(),
                    max_concurrency,
                    updated_at: timestamp,
                })
            })
            .collect::<Result<Vec<_>, DurableError>>()?;
        rows.sort_by(|left, right| left.topic.cmp(&right.topic));
        if rows.is_empty() {
            return Ok(());
        }
        crate::dialect::insert_topic_locks_if_absent(connection, &rows).await?;
        let topics: Vec<_> = rows.iter().map(|row| row.topic.clone()).collect();
        let persisted = durable_topic_lock::table
            .filter(durable_topic_lock::topic.eq_any(topics))
            .select(TopicLockRow::as_select())
            .load::<TopicLockRow>(connection)
            .await?;
        for row in persisted {
            let definition = self.definitions.get(&row.topic).ok_or_else(|| {
                DurableError::InvalidState(format!(
                    "persisted topic {} was not registered",
                    row.topic
                ))
            })?;
            let persisted_limit = u32::try_from(row.max_concurrency).map_err(|_| {
                DurableError::InvalidState(format!(
                    "activity topic {} has invalid persisted concurrency limit {}",
                    row.topic, row.max_concurrency
                ))
            })?;
            if persisted_limit != definition.max_concurrency {
                return Err(DurableError::InvalidDefinition(format!(
                    "activity topic {} persisted concurrency limit {} does not match registered limit {}",
                    row.topic, row.max_concurrency, definition.max_concurrency
                )));
            }
        }
        Ok(())
    }
}

fn validate_definition(kind: &str, version: i32) -> Result<(), DurableError> {
    if kind.is_empty() {
        return Err(DurableError::InvalidDefinition(
            "durable definition kind cannot be empty".to_string(),
        ));
    }
    if version <= 0 {
        return Err(DurableError::InvalidDefinition(format!(
            "durable definition {kind} version must be greater than zero"
        )));
    }
    Ok(())
}

fn deserialize<T: DeserializeOwned>(json: &str) -> Result<T, DurableError> {
    Ok(serde_json::from_str(json)?)
}

fn deserialize_request<T: DeserializeOwned>(
    field: &'static str,
    json: &str,
) -> Result<T, DurableError> {
    serde_json::from_str(json).map_err(|error| DurableError::InvalidPayload {
        field,
        message: error.to_string(),
    })
}

fn serialize_bounded<T: Serialize>(
    field: &'static str,
    value: &T,
    max_bytes: usize,
) -> Result<String, DurableError> {
    let json = serde_json::to_string(value)?;
    ensure_size(field, &json, max_bytes)?;
    Ok(json)
}

fn store_transition<S, A, O>(
    transition: WorkflowTransition<S, A, O>,
) -> Result<StoredTransition, WorkflowDispatchError>
where
    S: Serialize,
    A: Serialize,
    O: Serialize,
{
    let stored = match transition {
        WorkflowTransition::RunActivity { state, activity } => StoredTransition::RunActivity {
            state_json: serialize_bounded("workflow state", &state, MAX_INPUT_STATE_PAYLOAD_BYTES)?,
            activity,
        },
        WorkflowTransition::RunChild { state, child } => StoredTransition::RunChild {
            state_json: serialize_bounded("workflow state", &state, MAX_INPUT_STATE_PAYLOAD_BYTES)?,
            child,
        },
        WorkflowTransition::SleepUntil {
            state,
            wake_at_millis,
        } => StoredTransition::SleepUntil {
            state_json: serialize_bounded("workflow state", &state, MAX_INPUT_STATE_PAYLOAD_BYTES)?,
            wake_at_millis,
        },
        WorkflowTransition::WaitForApproval {
            state,
            approval,
            expires_at_millis,
        } => StoredTransition::WaitForApproval {
            state_json: serialize_bounded("workflow state", &state, MAX_INPUT_STATE_PAYLOAD_BYTES)?,
            approval_json: serialize_bounded(
                "approval request",
                &approval,
                MAX_EVENT_METADATA_BYTES,
            )?,
            expires_at_millis,
        },
        WorkflowTransition::Continue { state } => StoredTransition::Continue {
            state_json: serialize_bounded("workflow state", &state, MAX_INPUT_STATE_PAYLOAD_BYTES)?,
        },
        WorkflowTransition::Complete { output } => StoredTransition::Complete {
            output_json: serialize_bounded("workflow output", &output, MAX_OUTPUT_BYTES)?,
        },
    };
    Ok(stored)
}

fn is_canonical_topic_key(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some('a'..='z' | '0'..='9'))
        && chars.all(|ch| matches!(ch, 'a'..='z' | '0'..='9' | '.' | '_' | '-'))
}

#[macro_export]
macro_rules! register_durable_workflows {
    ($context:ty; $($workflow:ty),* $(,)?) => {{
        let mut registry = $crate::WorkflowRegistry::<$context>::new();
        (|| -> Result<_, $crate::DurableError> {
            $(registry.register::<$workflow>()?;)*
            Ok(registry)
        })()
    }};
}

#[macro_export]
macro_rules! register_durable_activities {
    ($context:ty; $($activity:ty),* $(,)?) => {{
        let mut registry = $crate::ActivityRegistry::<$context>::new();
        (|| -> Result<_, $crate::DurableError> {
            $(registry.register::<$activity>()?;)*
            Ok(registry)
        })()
    }};
}

#[macro_export]
macro_rules! register_durable_topics {
    ($($topic:expr),* $(,)?) => {{
        let mut registry = $crate::TopicRegistry::new();
        (|| -> Result<_, $crate::DurableError> {
            $(registry.register($topic)?;)*
            Ok(registry)
        })()
    }};
}

#[cfg(test)]
mod tests {
    use super::is_canonical_topic_key;

    #[test]
    fn canonical_topic_keys_are_lowercase_ascii() {
        assert!(is_canonical_topic_key("provider"));
        assert!(is_canonical_topic_key("pdf_rendering"));
        assert!(is_canonical_topic_key("a"));
        assert!(is_canonical_topic_key("9fax"));
        assert!(!is_canonical_topic_key(""));
        assert!(!is_canonical_topic_key("PROVIDER"));
        assert!(!is_canonical_topic_key("café"));
        assert!(!is_canonical_topic_key("straße"));
        assert!(!is_canonical_topic_key("_leading"));
        assert!(!is_canonical_topic_key("has space"));
    }
}
