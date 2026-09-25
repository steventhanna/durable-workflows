use diesel::{
    BoolExpressionMethods, ExpressionMethods, JoinOnDsl, NullableExpressionMethods, QueryDsl,
};
use diesel_async::RunQueryDsl;

use crate::{
    persistence::{ActivityStatus, WorkflowStatus},
    schema::{durable_activity, durable_workflow},
    ActivityRegistry, DefinitionKey, DurableConnection, DurableError, TopicRegistry,
    WorkflowRegistry,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadinessReport {
    missing_workflows: Vec<(String, i32)>,
    missing_activities: Vec<(String, i32)>,
    missing_topics: Vec<String>,
}

impl ReadinessReport {
    pub fn compare<C>(
        workflows: &WorkflowRegistry<C>,
        activities: &ActivityRegistry<C>,
        required_workflows: Vec<(String, i32)>,
        required_activities: Vec<(String, i32)>,
    ) -> Self
    where
        C: Send + Sync + 'static,
    {
        let mut missing_workflows: Vec<_> = required_workflows
            .into_iter()
            .filter(|(kind, version)| !workflows.contains(kind, *version))
            .collect();
        let mut missing_activities: Vec<_> = required_activities
            .into_iter()
            .filter(|(kind, version)| !activities.contains(kind, *version))
            .collect();
        missing_workflows.sort();
        missing_workflows.dedup();
        missing_activities.sort();
        missing_activities.dedup();
        Self {
            missing_workflows,
            missing_activities,
            missing_topics: Vec::new(),
        }
    }

    pub async fn query<C>(
        connection: &mut DurableConnection,
        workflows: &WorkflowRegistry<C>,
        activities: &ActivityRegistry<C>,
        topics: &TopicRegistry,
    ) -> Result<Self, DurableError>
    where
        C: Send + Sync + 'static,
    {
        let required_workflows = durable_workflow::table
            .filter(durable_workflow::status.ne_all([
                WorkflowStatus::Succeeded,
                WorkflowStatus::Failed,
                WorkflowStatus::Cancelled,
            ]))
            .select((durable_workflow::kind, durable_workflow::version))
            .distinct()
            .load::<(String, i32)>(connection)
            .await?;
        let mut required_activities = durable_activity::table
            .filter(
                durable_activity::status.eq_any([ActivityStatus::Pending, ActivityStatus::Running]),
            )
            .select((durable_activity::kind, durable_activity::version))
            .distinct()
            .load::<(String, i32)>(connection)
            .await?;
        let recoverable_dead_letters = durable_activity::table
            .inner_join(
                durable_workflow::table.on(durable_workflow::id
                    .eq(durable_activity::workflow_id)
                    .and(durable_workflow::wait_reference_id.eq(durable_activity::id.nullable()))),
            )
            .filter(durable_activity::status.eq(ActivityStatus::DeadLettered))
            .filter(
                durable_workflow::status.eq_any([WorkflowStatus::Blocked, WorkflowStatus::Paused]),
            )
            .filter(durable_workflow::wait_kind.eq("activity"))
            .select((durable_activity::kind, durable_activity::version))
            .distinct()
            .load::<(String, i32)>(connection)
            .await?;
        required_activities.extend(recoverable_dead_letters);
        let mut stored_topics = durable_activity::table
            .filter(
                durable_activity::status.eq_any([ActivityStatus::Pending, ActivityStatus::Running]),
            )
            .select(durable_activity::topic)
            .distinct()
            .load::<String>(connection)
            .await?;
        let recoverable_topics = durable_activity::table
            .inner_join(
                durable_workflow::table.on(durable_workflow::id
                    .eq(durable_activity::workflow_id)
                    .and(durable_workflow::wait_reference_id.eq(durable_activity::id.nullable()))),
            )
            .filter(durable_activity::status.eq(ActivityStatus::DeadLettered))
            .filter(
                durable_workflow::status.eq_any([WorkflowStatus::Blocked, WorkflowStatus::Paused]),
            )
            .filter(durable_workflow::wait_kind.eq("activity"))
            .select(durable_activity::topic)
            .distinct()
            .load::<String>(connection)
            .await?;
        stored_topics.extend(recoverable_topics);
        let mut report = Self::compare(
            workflows,
            activities,
            required_workflows,
            required_activities,
        );
        let mut required_topics = activities.required_topics();
        required_topics.extend(stored_topics);
        report.missing_topics = required_topics
            .into_iter()
            .filter(|topic| topics.get(topic).is_none())
            .collect();
        report.missing_topics.sort();
        report.missing_topics.dedup();
        Ok(report)
    }

    pub fn missing_workflows(&self) -> &[(String, i32)] {
        &self.missing_workflows
    }

    pub fn missing_activities(&self) -> &[(String, i32)] {
        &self.missing_activities
    }

    pub fn missing_topics(&self) -> &[String] {
        &self.missing_topics
    }

    pub fn ensure_ready(&self) -> Result<(), DurableError> {
        if self.missing_workflows.is_empty()
            && self.missing_activities.is_empty()
            && self.missing_topics.is_empty()
        {
            return Ok(());
        }
        Err(DurableError::MissingDefinitions {
            workflows: self
                .missing_workflows
                .iter()
                .map(|(kind, version)| DefinitionKey::new(kind, *version))
                .collect(),
            activities: self
                .missing_activities
                .iter()
                .map(|(kind, version)| DefinitionKey::new(kind, *version))
                .collect(),
            topics: self.missing_topics.clone(),
        })
    }
}
