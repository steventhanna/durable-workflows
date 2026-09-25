use std::collections::{BTreeSet, HashMap, HashSet};

use diesel::{
    dsl::{count_star, min, not},
    BoolExpressionMethods, ExpressionMethods, JoinOnDsl, NullableExpressionMethods,
    OptionalExtension, QueryDsl, SelectableHelper,
};
use diesel_async::{AsyncConnection, RunQueryDsl};

use crate::{
    admin::{
        decode_cursor, encode_cursor, AdminPage, ApprovalListFilter, ApprovalSummary,
        CursorPosition, HourlyThroughputBucket, JsonFieldSummary, PageRequest, ScheduleHealthIssue,
        ScheduleRegistry, ScheduleRunSummary, ScheduleStateSummary, ScheduleSummary, TopicMetrics,
        MAX_ADMIN_PAGE_SIZE,
    },
    persistence::ScheduleStateRow,
    persistence::{ActivityStatus, WorkflowStatus},
    schema::{
        durable_activity, durable_activity_attempt, durable_approval, durable_schedule_run,
        durable_schedule_state, durable_topic_lock, durable_workflow,
    },
    ApprovalId, DurableError, ScheduleRunId, TopicDefinition, TopicRegistry, WorkflowId,
};

use super::cursor::scoped_cursor_scope;
use super::AdminQueryService;

const HOUR_MILLIS: i64 = 3_600_000;
const HOURLY_BUCKET_COUNT: usize = 24;
const MAX_RECENT_SCHEDULE_RUNS: u32 = 20;
/// MySQL prepared statements cap placeholders at 65,535. Each resolved-root
/// lookup binds the chunk twice (`root_activity_id` OR `id`).
const RESOLVED_ROOT_IN_CHUNK: usize = 10_000;

// Postgres returns int4 and MySQL returns BIGINT; the call site CASTs to BIGINT.
diesel::define_sql_function! {
    #[sql_name = "OCTET_LENGTH"]
    fn approval_decision_octet_length(value: diesel::sql_types::Nullable<diesel::sql_types::Text>) -> diesel::sql_types::Nullable<diesel::sql_types::Integer>;
}

#[derive(diesel::Queryable)]
struct ScheduleRunProjection {
    id: i64,
    schedule_key: String,
    local_occurrence: String,
    scheduled_for: i64,
    materialized_at: i64,
    status: String,
    reason: Option<String>,
    actor_id: Option<i32>,
    workflow_id: Option<i64>,
    created_at: i64,
    workflow_status: Option<WorkflowStatus>,
    workflow_completed_at: Option<i64>,
}

#[derive(diesel::Queryable)]
struct ApprovalListProjection {
    id: i64,
    workflow_id: i64,
    kind: String,
    version: i32,
    status: String,
    decision_bytes: Option<i64>,
    decided_by: Option<i32>,
    operator_reason: Option<String>,
    requested_at: i64,
    expires_at: Option<i64>,
    resolved_at: Option<i64>,
}

impl AdminQueryService {
    pub async fn list_topic_metrics(
        &self,
        topics: &TopicRegistry,
        page: PageRequest,
    ) -> Result<AdminPage<TopicMetrics>, DurableError> {
        let mut connection = self.pool.get().await?;
        let now = crate::persistence::database_now_millis(&mut connection).await?;
        drop(connection);
        self.list_topic_metrics_at(topics, page, now).await
    }

    pub async fn list_topic_metrics_at(
        &self,
        topics: &TopicRegistry,
        page: PageRequest,
        now: i64,
    ) -> Result<AdminPage<TopicMetrics>, DurableError> {
        validate_now(now)?;
        let limit = page.bounded_limit(MAX_ADMIN_PAGE_SIZE)?;
        let after = page
            .cursor
            .as_deref()
            .map(|cursor| decode_string_cursor("topics", cursor))
            .transpose()?;
        let mut definitions = topics.definitions();
        if let Some(after) = after.as_deref() {
            definitions.retain(|definition| definition.key.as_str() > after);
        }
        let has_more = definitions.len() > limit as usize;
        definitions.truncate(limit as usize);
        let next_cursor = if has_more {
            definitions
                .last()
                .map(|definition| encode_string_cursor("topics", &definition.key))
                .transpose()?
        } else {
            None
        };
        let items = self.load_topic_metrics(&definitions, now).await?;
        Ok(AdminPage { items, next_cursor })
    }

    async fn load_topic_metrics(
        &self,
        definitions: &[TopicDefinition],
        now: i64,
    ) -> Result<Vec<TopicMetrics>, DurableError> {
        if definitions.is_empty() {
            return Ok(Vec::new());
        }
        let keys = definitions
            .iter()
            .map(|definition| definition.key.as_str())
            .collect::<Vec<_>>();
        let mut connection = self.pool.get().await?;
        let persisted_cap_rows = durable_topic_lock::table
            .filter(durable_topic_lock::topic.eq_any(&keys))
            .select((
                durable_topic_lock::topic,
                durable_topic_lock::max_concurrency,
            ))
            .load::<(String, i32)>(&mut connection)
            .await?;
        let status_rows = durable_activity::table
            .filter(durable_activity::topic.eq_any(&keys))
            .filter(
                durable_activity::status
                    .eq(ActivityStatus::Running)
                    .and(durable_activity::lease_expires_at.gt(now)),
            )
            .group_by((durable_activity::topic, durable_activity::status))
            .select((
                durable_activity::topic,
                durable_activity::status,
                count_star(),
            ))
            .load::<(String, ActivityStatus, i64)>(&mut connection)
            .await?;

        // A dead-lettered activity's own `status` never changes, even once it's
        // resolved. It can resolve two ways: the workflow moves on without it
        // and reaches a terminal status (e.g. a best-effort notification the
        // workflow doesn't wait on), or an operator retries it and the
        // replacement in the same `root_activity_id` chain succeeds while the
        // workflow goes on to other, still-open work (see the health scanner
        // in observability.rs, which resolves the same chain). Counting either
        // case forever would make this metric a permanent false positive.
        // Terminal workflows are excluded in SQL so historical resolved rows
        // never enter the candidate set (or the successor `IN` lookup).
        let dead_letter_candidates = durable_activity::table
            .inner_join(
                durable_workflow::table.on(durable_workflow::id.eq(durable_activity::workflow_id)),
            )
            .filter(durable_activity::topic.eq_any(&keys))
            .filter(durable_activity::status.eq(ActivityStatus::DeadLettered))
            .filter(not(durable_workflow::status.eq_any([
                WorkflowStatus::Succeeded,
                WorkflowStatus::Failed,
                WorkflowStatus::Cancelled,
            ])))
            .select((
                durable_activity::topic,
                durable_activity::id,
                durable_activity::root_activity_id,
            ))
            .load::<(String, i64, Option<i64>)>(&mut connection)
            .await?;
        let dead_letter_rows = if dead_letter_candidates.is_empty() {
            Vec::new()
        } else {
            let mut candidate_roots = dead_letter_candidates
                .iter()
                .map(|(_, id, root_activity_id)| root_activity_id.unwrap_or(*id))
                .collect::<Vec<_>>();
            candidate_roots.sort_unstable();
            candidate_roots.dedup();
            let resolved_roots =
                load_resolved_activity_roots(&mut connection, &candidate_roots).await?;

            let mut counts: HashMap<String, i64> = HashMap::new();
            for (topic, id, root_activity_id) in dead_letter_candidates {
                let effective_root = root_activity_id.unwrap_or(id);
                if resolved_roots.contains(&effective_root) {
                    continue;
                }
                *counts.entry(topic).or_insert(0) += 1;
            }
            counts.into_iter().collect::<Vec<_>>()
        };

        // Match claim eligibility: only count activities whose parent is
        // waiting on that exact activity (paused/detached parents are excluded).
        let ready_rows = durable_activity::table
            .inner_join(
                durable_workflow::table.on(durable_workflow::id.eq(durable_activity::workflow_id)),
            )
            .filter(durable_activity::topic.eq_any(&keys))
            .filter(durable_activity::status.eq(ActivityStatus::Pending))
            .filter(durable_activity::available_at.le(now))
            .filter(durable_workflow::status.eq(WorkflowStatus::WaitingActivity))
            .filter(durable_workflow::wait_reference_id.eq(durable_activity::id.nullable()))
            .group_by(durable_activity::topic)
            .select((
                durable_activity::topic,
                count_star(),
                min(diesel::dsl::case_when(
                    durable_activity::available_at
                        .eq(crate::transition::CONTINUATION_READY_AT_MILLIS),
                    durable_activity::created_at,
                )
                .otherwise(durable_activity::available_at)),
            ))
            .load::<(String, i64, Option<i64>)>(&mut connection)
            .await?;
        let retry_rows = durable_activity::table
            .inner_join(
                durable_workflow::table.on(durable_workflow::id.eq(durable_activity::workflow_id)),
            )
            .filter(durable_activity::topic.eq_any(&keys))
            .filter(durable_activity::status.eq(ActivityStatus::Pending))
            .filter(durable_activity::available_at.gt(now))
            .filter(durable_activity::attempt_count.gt(0))
            .filter(durable_workflow::status.eq(WorkflowStatus::WaitingActivity))
            .filter(durable_workflow::wait_reference_id.eq(durable_activity::id.nullable()))
            .group_by(durable_activity::topic)
            .select((durable_activity::topic, count_star()))
            .load::<(String, i64)>(&mut connection)
            .await?;
        let oldest_active_rows = durable_activity::table
            .inner_join(
                durable_activity_attempt::table.on(durable_activity_attempt::activity_id
                    .eq(durable_activity::id)
                    .and(
                        durable_activity_attempt::attempt_number
                            .eq(durable_activity::attempt_count),
                    )),
            )
            .filter(durable_activity::topic.eq_any(&keys))
            .filter(durable_activity::status.eq(ActivityStatus::Running))
            .filter(durable_activity::lease_expires_at.gt(now))
            .group_by(durable_activity::topic)
            .select((
                durable_activity::topic,
                min(durable_activity_attempt::started_at),
            ))
            .load::<(String, Option<i64>)>(&mut connection)
            .await?;

        let window = hourly_window(now);
        // Diesel cannot GROUP BY a MySQL DIV/hour expression together with
        // `topic`. One grouped COUNT per hour keeps the result bounded to
        // one row per requested topic instead of one row per completion.
        let mut hourly_rows = Vec::with_capacity(HOURLY_BUCKET_COUNT);
        for index in 0..HOURLY_BUCKET_COUNT {
            let (starts_at, ends_at) = hour_bucket_bounds(window, index);
            hourly_rows.push(
                durable_activity::table
                    .filter(durable_activity::topic.eq_any(&keys))
                    .filter(durable_activity::completed_at.ge(starts_at))
                    .filter(durable_activity::completed_at.lt(ends_at))
                    .group_by(durable_activity::topic)
                    .select((durable_activity::topic, count_star()))
                    .load::<(String, i64)>(&mut connection)
                    .await?,
            );
        }

        let mut persisted_caps = HashMap::new();
        for (topic, cap) in persisted_cap_rows {
            let cap = u32::try_from(cap).map_err(|_| {
                DurableError::InvalidState(format!(
                    "activity topic {topic} has invalid persisted concurrency limit {cap}"
                ))
            })?;
            persisted_caps.insert(topic, cap);
        }

        let mut active_counts = HashMap::new();
        for (topic, status, count) in status_rows {
            let count = nonnegative_count(count)?;
            if status == ActivityStatus::Running {
                active_counts.insert(topic, count);
            }
        }

        let mut dead_letter_counts = HashMap::new();
        for (topic, count) in dead_letter_rows {
            dead_letter_counts.insert(topic, nonnegative_count(count)?);
        }

        let mut ready_counts = HashMap::new();
        let mut oldest_ready_by_topic = HashMap::new();
        for (topic, count, oldest_ready) in ready_rows {
            ready_counts.insert(topic.clone(), nonnegative_count(count)?);
            oldest_ready_by_topic.insert(topic, oldest_ready);
        }

        let mut retry_counts = HashMap::new();
        for (topic, count) in retry_rows {
            retry_counts.insert(topic, nonnegative_count(count)?);
        }

        let mut oldest_active_by_topic = HashMap::new();
        for (topic, oldest_active) in oldest_active_rows {
            oldest_active_by_topic.insert(topic, oldest_active);
        }

        let mut hourly_by_topic = definitions
            .iter()
            .map(|definition| (definition.key.clone(), empty_hourly_throughput(window)))
            .collect::<HashMap<_, _>>();
        for (index, rows) in hourly_rows.into_iter().enumerate() {
            for (topic, count) in rows {
                if let Some(buckets) = hourly_by_topic.get_mut(&topic) {
                    buckets[index].completed_count = nonnegative_count(count)?;
                }
            }
        }

        Ok(definitions
            .iter()
            .map(|definition| {
                let persisted_max_concurrency = persisted_caps.get(&definition.key).copied();
                let max_concurrency =
                    persisted_max_concurrency.unwrap_or(definition.max_concurrency);
                let active_count = active_counts.get(&definition.key).copied().unwrap_or(0);
                TopicMetrics {
                    topic: definition.key.clone(),
                    max_concurrency,
                    configured_max_concurrency: definition.max_concurrency,
                    configuration_mismatch: persisted_max_concurrency
                        != Some(definition.max_concurrency),
                    active_count,
                    available_capacity: max_concurrency
                        .saturating_sub(u32::try_from(active_count).unwrap_or(u32::MAX)),
                    ready_count: ready_counts.get(&definition.key).copied().unwrap_or(0),
                    retry_scheduled_count: retry_counts.get(&definition.key).copied().unwrap_or(0),
                    dead_letter_count: dead_letter_counts
                        .get(&definition.key)
                        .copied()
                        .unwrap_or(0),
                    oldest_ready_age_millis: age(
                        now,
                        oldest_ready_by_topic
                            .get(&definition.key)
                            .copied()
                            .flatten(),
                    ),
                    oldest_active_age_millis: age(
                        now,
                        oldest_active_by_topic
                            .get(&definition.key)
                            .copied()
                            .flatten(),
                    ),
                    hourly_throughput: hourly_by_topic
                        .remove(&definition.key)
                        .unwrap_or_else(|| empty_hourly_throughput(window)),
                    captured_at: now,
                }
            })
            .collect())
    }

    pub async fn list_schedules<C>(
        &self,
        schedules: &ScheduleRegistry<C>,
        page: PageRequest,
        recent_run_limit: u32,
    ) -> Result<AdminPage<ScheduleSummary>, DurableError>
    where
        C: Send + Sync + 'static,
    {
        if recent_run_limit == 0 || recent_run_limit > MAX_RECENT_SCHEDULE_RUNS {
            return Err(DurableError::InvalidDefinition(format!(
                "recent schedule run limit must contain 1 to {MAX_RECENT_SCHEDULE_RUNS} entries"
            )));
        }
        let limit = page.bounded_limit(MAX_ADMIN_PAGE_SIZE)?;
        let after = page
            .cursor
            .as_deref()
            .map(|cursor| decode_string_cursor("schedules", cursor))
            .transpose()?;
        let mut keys = BTreeSet::new();
        for definition in schedules.definitions() {
            if after
                .as_deref()
                .is_none_or(|after| definition.key.as_str() > after)
            {
                keys.insert(definition.key);
            }
        }
        let mut state_query = durable_schedule_state::table.into_boxed::<crate::Db>();
        if let Some(after) = after.as_deref() {
            state_query = state_query.filter(durable_schedule_state::schedule_key.gt(after));
        }
        let mut connection = self.pool.get().await?;
        let persisted_keys = state_query
            .select(durable_schedule_state::schedule_key)
            .order(durable_schedule_state::schedule_key.asc())
            .limit(i64::from(limit) + 1)
            .load::<String>(&mut connection)
            .await?;
        keys.extend(persisted_keys);
        let mut keys = keys.into_iter().collect::<Vec<_>>();
        let has_more = keys.len() > limit as usize;
        keys.truncate(limit as usize);
        let next_cursor = if has_more {
            keys.last()
                .map(|key| encode_string_cursor("schedules", key))
                .transpose()?
        } else {
            None
        };
        let state_rows = if keys.is_empty() {
            Vec::new()
        } else {
            durable_schedule_state::table
                .filter(durable_schedule_state::schedule_key.eq_any(&keys))
                .select(ScheduleStateRow::as_select())
                .load::<ScheduleStateRow>(&mut connection)
                .await?
        };
        drop(connection);
        let state_by_key = state_rows
            .into_iter()
            .map(|row| (row.schedule_key.clone(), row))
            .collect::<HashMap<_, _>>();
        let definitions = schedules
            .definitions()
            .into_iter()
            .map(|definition| (definition.key.clone(), definition))
            .collect::<HashMap<_, _>>();
        let mut items = Vec::with_capacity(keys.len());
        for key in keys {
            let definition = definitions.get(&key);
            let state = state_by_key.get(&key);
            let mut health_issues = Vec::new();
            match (definition, state) {
                (Some(_), None) => health_issues.push(ScheduleHealthIssue::MissingState),
                (None, Some(_)) => health_issues.push(ScheduleHealthIssue::UnregisteredState),
                (Some(definition), Some(state))
                    if definition.version != state.definition_version
                        || definition.fingerprint != state.definition_fingerprint =>
                {
                    health_issues.push(ScheduleHealthIssue::DefinitionMismatch {
                        registered_version: definition.version,
                        persisted_version: state.definition_version,
                    });
                }
                _ => {}
            }
            let recent_runs = self
                .load_schedule_runs(&key, None, recent_run_limit)
                .await?;
            let last_run = recent_runs.as_slice().first().cloned();
            let (active_overlap_count, skipped_count, coalesced_count) =
                self.schedule_counts(&key).await?;
            items.push(ScheduleSummary {
                key,
                registered_version: definition.map(|definition| definition.version),
                registered_fingerprint: definition.map(|definition| definition.fingerprint.clone()),
                cron: definition.map(|definition| definition.cron.clone()),
                timezone: definition.map(|definition| definition.timezone.clone()),
                misfire: definition.map(|definition| definition.misfire),
                overlap: definition.map(|definition| definition.overlap),
                misfire_grace_millis: definition.map(|definition| definition.misfire_grace_millis),
                state: state.map(schedule_state_summary),
                active_overlap_count,
                skipped_count,
                coalesced_count,
                last_run,
                recent_runs,
                health_issues,
            });
        }
        Ok(AdminPage { items, next_cursor })
    }

    pub async fn list_schedule_runs(
        &self,
        schedule_key: &str,
        page: PageRequest,
    ) -> Result<AdminPage<ScheduleRunSummary>, DurableError> {
        let limit = page.bounded_limit(MAX_ADMIN_PAGE_SIZE)?;
        let scope = scoped_cursor_scope("schedule_runs", schedule_key);
        let cursor = page
            .cursor
            .as_deref()
            .map(|cursor| decode_numeric_cursor(&scope, cursor))
            .transpose()?;
        let mut connection = self.pool.get().await?;
        let exists = durable_schedule_state::table
            .find(schedule_key)
            .select(durable_schedule_state::schedule_key)
            .first::<String>(&mut connection)
            .await
            .optional()?
            .is_some();
        if !exists {
            return Err(DurableError::NotFound {
                resource: "schedule",
                identifier: schedule_key.to_string(),
            });
        }
        drop(connection);
        let mut rows = self
            .load_schedule_runs(schedule_key, cursor, limit + 1)
            .await?;
        let has_more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let next_cursor = if has_more {
            rows.last()
                .map(|run| {
                    encode_cursor(
                        &scope,
                        &CursorPosition {
                            timestamp: run.scheduled_for,
                            tie_breaker: run.id.get().to_string(),
                        },
                    )
                })
                .transpose()?
        } else {
            None
        };
        Ok(AdminPage {
            items: rows,
            next_cursor,
        })
    }

    async fn load_schedule_runs(
        &self,
        schedule_key: &str,
        cursor: Option<(i64, i64)>,
        limit: u32,
    ) -> Result<Vec<ScheduleRunSummary>, DurableError> {
        let mut query = durable_schedule_run::table
            .left_join(
                durable_workflow::table
                    .on(durable_schedule_run::workflow_id.eq(durable_workflow::id.nullable())),
            )
            .filter(durable_schedule_run::schedule_key.eq(schedule_key))
            .into_boxed::<crate::Db>();
        if let Some((timestamp, id)) = cursor {
            query = query.filter(
                durable_schedule_run::scheduled_for.lt(timestamp).or(
                    durable_schedule_run::scheduled_for
                        .eq(timestamp)
                        .and(durable_schedule_run::id.lt(id)),
                ),
            );
        }
        let mut connection = self.pool.get().await?;
        query
            .order((
                durable_schedule_run::scheduled_for.desc(),
                durable_schedule_run::id.desc(),
            ))
            .limit(i64::from(limit))
            .select((
                durable_schedule_run::id,
                durable_schedule_run::schedule_key,
                durable_schedule_run::local_occurrence,
                durable_schedule_run::scheduled_for,
                durable_schedule_run::materialized_at,
                durable_schedule_run::status,
                durable_schedule_run::reason,
                durable_schedule_run::actor_id,
                durable_schedule_run::workflow_id,
                durable_schedule_run::created_at,
                durable_workflow::status.nullable(),
                durable_workflow::completed_at.nullable(),
            ))
            .load::<ScheduleRunProjection>(&mut connection)
            .await?
            .into_iter()
            .map(schedule_run_summary)
            .collect()
    }

    async fn schedule_counts(&self, schedule_key: &str) -> Result<(u64, u64, u64), DurableError> {
        let mut connection = self.pool.get().await?;
        let active = durable_workflow::table
            .inner_join(
                durable_schedule_run::table
                    .on(durable_workflow::schedule_run_id.eq(durable_schedule_run::id.nullable())),
            )
            .filter(durable_schedule_run::schedule_key.eq(schedule_key))
            .filter(not(durable_workflow::status.eq_any([
                WorkflowStatus::Succeeded,
                WorkflowStatus::Cancelled,
                WorkflowStatus::Failed,
            ])))
            .select(count_star())
            .first::<i64>(&mut connection)
            .await?;
        let skipped = durable_schedule_run::table
            .filter(durable_schedule_run::schedule_key.eq(schedule_key))
            .filter(durable_schedule_run::status.eq("skipped"))
            .select(count_star())
            .first::<i64>(&mut connection)
            .await?;
        let coalesced = durable_schedule_run::table
            .filter(durable_schedule_run::schedule_key.eq(schedule_key))
            .filter(durable_schedule_run::status.eq("coalesced"))
            .select(count_star())
            .first::<i64>(&mut connection)
            .await?;
        Ok((
            nonnegative_count(active)?,
            nonnegative_count(skipped)?,
            nonnegative_count(coalesced)?,
        ))
    }

    pub async fn list_approvals(
        &self,
        filter: ApprovalListFilter,
    ) -> Result<AdminPage<ApprovalSummary>, DurableError> {
        let mut connection = self.pool.get().await?;
        let now = crate::persistence::database_now_millis(&mut connection).await?;
        drop(connection);
        self.list_approvals_at(filter, now).await
    }

    pub async fn list_approvals_at(
        &self,
        filter: ApprovalListFilter,
        now: i64,
    ) -> Result<AdminPage<ApprovalSummary>, DurableError> {
        validate_now(now)?;
        validate_approval_filter(&filter)?;
        let limit = filter.page.bounded_limit(MAX_ADMIN_PAGE_SIZE)?;
        let cursor = filter
            .page
            .cursor
            .as_deref()
            .map(|cursor| decode_numeric_cursor("approvals", cursor))
            .transpose()?;
        let mut query = durable_approval::table.into_boxed::<crate::Db>();
        if let Some(workflow_id) = filter.workflow_id {
            query = query.filter(durable_approval::workflow_id.eq(workflow_id.get()));
        }
        if let Some(kind) = filter.kind {
            query = query.filter(durable_approval::kind.eq(kind));
        }
        if let Some(version) = filter.version {
            query = query.filter(durable_approval::version.eq(version));
        }
        if let Some(status) = filter.status.as_deref() {
            query = match status {
                "expired" => query.filter(
                    durable_approval::status
                        .eq("expired")
                        .or(durable_approval::status
                            .eq("pending")
                            .and(durable_approval::expires_at.le(now))),
                ),
                "pending" => query.filter(
                    durable_approval::status.eq("pending").and(
                        durable_approval::expires_at
                            .is_null()
                            .or(durable_approval::expires_at.gt(now)),
                    ),
                ),
                status => query.filter(durable_approval::status.eq(status)),
            };
        }
        if let Some(after) = filter.requested_after {
            query = query.filter(durable_approval::requested_at.ge(after));
        }
        if let Some(before) = filter.requested_before {
            query = query.filter(durable_approval::requested_at.lt(before));
        }
        if let Some((timestamp, id)) = cursor {
            query = query.filter(
                durable_approval::requested_at
                    .lt(timestamp)
                    .or(durable_approval::requested_at
                        .eq(timestamp)
                        .and(durable_approval::id.lt(id))),
            );
        }
        let mut connection = self.pool.get().await?;
        let mut rows = query
            .order((
                durable_approval::requested_at.desc(),
                durable_approval::id.desc(),
            ))
            .limit(i64::from(limit) + 1)
            .select((
                durable_approval::id,
                durable_approval::workflow_id,
                durable_approval::kind,
                durable_approval::version,
                durable_approval::status,
                approval_decision_octet_length(durable_approval::decision_payload_json)
                    .cast::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>>(),
                durable_approval::decided_by,
                durable_approval::operator_reason,
                durable_approval::requested_at,
                durable_approval::expires_at,
                durable_approval::resolved_at,
            ))
            .load::<ApprovalListProjection>(&mut connection)
            .await?;
        let has_more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let next_cursor = if has_more {
            rows.last()
                .map(|row| {
                    encode_cursor(
                        "approvals",
                        &CursorPosition {
                            timestamp: row.requested_at,
                            tie_breaker: row.id.to_string(),
                        },
                    )
                })
                .transpose()?
        } else {
            None
        };
        Ok(AdminPage {
            items: rows
                .into_iter()
                .map(|row| approval_summary(row, now))
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor,
        })
    }
}

fn schedule_state_summary(row: &ScheduleStateRow) -> ScheduleStateSummary {
    ScheduleStateSummary {
        definition_fingerprint: row.definition_fingerprint.clone(),
        definition_version: row.definition_version,
        next_local_occurrence: row.next_local_occurrence.clone(),
        next_occurrence_at: row.next_occurrence_at,
        last_materialized_at: row.last_materialized_at,
        paused_at: row.paused_at,
        paused_by: row.paused_by,
        pause_reason: row.pause_reason.clone(),
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn schedule_run_summary(row: ScheduleRunProjection) -> Result<ScheduleRunSummary, DurableError> {
    Ok(ScheduleRunSummary {
        id: ScheduleRunId::new(row.id)?,
        schedule_key: row.schedule_key,
        local_occurrence: row.local_occurrence,
        scheduled_for: row.scheduled_for,
        materialized_at: row.materialized_at,
        status: row.status,
        reason: row.reason,
        actor_id: row.actor_id,
        workflow_id: row.workflow_id.map(WorkflowId::new).transpose()?,
        workflow_status: row.workflow_status.map(|status| status.to_string()),
        workflow_completed_at: row.workflow_completed_at,
        created_at: row.created_at,
    })
}

fn approval_summary(
    row: ApprovalListProjection,
    now: i64,
) -> Result<ApprovalSummary, DurableError> {
    let status = if row.status == "pending" && row.expires_at.is_some_and(|expiry| expiry <= now) {
        "expired".to_string()
    } else {
        row.status
    };
    Ok(ApprovalSummary {
        id: ApprovalId::new(row.id)?,
        workflow_id: WorkflowId::new(row.workflow_id)?,
        kind: row.kind,
        version: row.version,
        status,
        decision: json_summary(row.decision_bytes)?,
        decided_by: row.decided_by,
        operator_reason: row.operator_reason,
        requested_at: row.requested_at,
        expires_at: row.expires_at,
        resolved_at: row.resolved_at,
    })
}

fn json_summary(bytes: Option<i64>) -> Result<JsonFieldSummary, DurableError> {
    match bytes {
        Some(bytes) => Ok(JsonFieldSummary {
            present: true,
            bytes: usize::try_from(bytes).map_err(|_| {
                DurableError::InvalidState("negative persisted JSON byte length".to_string())
            })?,
            value: None,
        }),
        None => Ok(JsonFieldSummary {
            present: false,
            bytes: 0,
            value: None,
        }),
    }
}

fn decode_string_cursor(scope: &str, cursor: &str) -> Result<String, DurableError> {
    let position = decode_cursor(scope, cursor)?;
    if position.timestamp != 0 || position.tie_breaker.is_empty() {
        return Err(DurableError::InvalidCursor(
            "string cursor position is invalid".to_string(),
        ));
    }
    Ok(position.tie_breaker)
}

fn encode_string_cursor(scope: &str, value: &str) -> Result<String, DurableError> {
    encode_cursor(
        scope,
        &CursorPosition {
            timestamp: 0,
            tie_breaker: value.to_string(),
        },
    )
}

fn decode_numeric_cursor(scope: &str, cursor: &str) -> Result<(i64, i64), DurableError> {
    let position = decode_cursor(scope, cursor)?;
    let id = position.tie_breaker.parse::<i64>().map_err(|_| {
        DurableError::InvalidCursor("cursor tie breaker is not numeric".to_string())
    })?;
    if id <= 0 {
        return Err(DurableError::InvalidCursor(
            "cursor tie breaker must be positive".to_string(),
        ));
    }
    Ok((position.timestamp, id))
}

#[derive(Clone, Copy)]
struct HourlyWindow {
    first_hour: i64,
    exclusive_end: i64,
}

fn hourly_window(now: i64) -> HourlyWindow {
    let current_hour = hour_bucket_start(now);
    HourlyWindow {
        first_hour: current_hour - (HOURLY_BUCKET_COUNT as i64 - 1) * HOUR_MILLIS,
        exclusive_end: now.saturating_add(1),
    }
}

fn hour_bucket_start(timestamp: i64) -> i64 {
    timestamp.div_euclid(HOUR_MILLIS) * HOUR_MILLIS
}

fn hour_bucket_bounds(window: HourlyWindow, index: usize) -> (i64, i64) {
    let starts_at = window.first_hour + index as i64 * HOUR_MILLIS;
    (
        starts_at,
        starts_at
            .saturating_add(HOUR_MILLIS)
            .min(window.exclusive_end),
    )
}

async fn load_resolved_activity_roots<C>(
    connection: &mut C,
    candidate_roots: &[i64],
) -> Result<HashSet<i64>, DurableError>
where
    C: AsyncConnection<Backend = crate::Db> + Send,
{
    let mut resolved = HashSet::new();
    for chunk in candidate_roots.chunks(RESOLVED_ROOT_IN_CHUNK) {
        resolved.extend(
            durable_activity::table
                .filter(durable_activity::status.eq(ActivityStatus::Succeeded))
                .filter(
                    durable_activity::root_activity_id
                        .eq_any(chunk)
                        .or(durable_activity::id.eq_any(chunk)),
                )
                .select((durable_activity::id, durable_activity::root_activity_id))
                .load::<(i64, Option<i64>)>(connection)
                .await?
                .into_iter()
                .map(|(id, root_activity_id)| root_activity_id.unwrap_or(id)),
        );
    }
    Ok(resolved)
}

fn empty_hourly_throughput(window: HourlyWindow) -> Vec<HourlyThroughputBucket> {
    (0..HOURLY_BUCKET_COUNT)
        .map(|index| HourlyThroughputBucket {
            starts_at: window.first_hour + index as i64 * HOUR_MILLIS,
            completed_count: 0,
        })
        .collect()
}

fn validate_now(now: i64) -> Result<(), DurableError> {
    if now < 0 {
        return Err(DurableError::InvalidDefinition(
            "captured time cannot be negative".to_string(),
        ));
    }
    Ok(())
}

fn validate_approval_filter(filter: &ApprovalListFilter) -> Result<(), DurableError> {
    if filter.version.is_some_and(|version| version <= 0) {
        return Err(DurableError::InvalidDefinition(
            "approval version filter must be positive".to_string(),
        ));
    }
    if matches!(
        (filter.requested_after, filter.requested_before),
        (Some(after), Some(before)) if after >= before
    ) {
        return Err(DurableError::InvalidDefinition(
            "requestedAfter must precede requestedBefore".to_string(),
        ));
    }
    Ok(())
}

fn nonnegative_count(count: i64) -> Result<u64, DurableError> {
    u64::try_from(count)
        .map_err(|_| DurableError::InvalidState("negative aggregate count".to_string()))
}

// Clamp future timestamps to 0: metrics freeze `now` once, then can observe
// rows written with a later DB clock (common for active `started_at`).
fn age(now: i64, timestamp: Option<i64>) -> Option<u64> {
    timestamp.map(|timestamp| u64::try_from(now.saturating_sub(timestamp)).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::{
        age, empty_hourly_throughput, hour_bucket_bounds, hour_bucket_start, hourly_window,
        HOURLY_BUCKET_COUNT, HOUR_MILLIS,
    };

    #[test]
    fn age_is_none_without_timestamp() {
        assert_eq!(age(1_000, None), None);
    }

    #[test]
    fn age_is_delta_when_timestamp_is_in_the_past() {
        assert_eq!(age(1_000, Some(250)), Some(750));
    }

    #[test]
    fn age_clamps_future_timestamps_to_zero() {
        assert_eq!(age(1_000, Some(1_500)), Some(0));
    }

    #[test]
    fn hour_bucket_start_aligns_to_hour_boundaries() {
        assert_eq!(hour_bucket_start(0), 0);
        assert_eq!(hour_bucket_start(HOUR_MILLIS - 1), 0);
        assert_eq!(hour_bucket_start(HOUR_MILLIS), HOUR_MILLIS);
        assert_eq!(hour_bucket_start(100_000_000), 97_200_000);
    }

    #[test]
    fn hourly_window_covers_twenty_four_hours_ending_at_now() {
        let now = 100_000_000;
        let window = hourly_window(now);
        assert_eq!(window.first_hour, 14_400_000);
        assert_eq!(window.exclusive_end, now + 1);
        let buckets = empty_hourly_throughput(window);
        assert_eq!(buckets.len(), HOURLY_BUCKET_COUNT);
        assert_eq!(buckets[0].starts_at, window.first_hour);
        assert_eq!(buckets[23].starts_at, 97_200_000);
        assert!(buckets.iter().all(|bucket| bucket.completed_count == 0));
    }

    #[test]
    fn hour_bucket_bounds_clip_the_current_hour_to_now() {
        let window = hourly_window(100_000_000);
        assert_eq!(
            hour_bucket_bounds(window, 0),
            (window.first_hour, window.first_hour + HOUR_MILLIS)
        );
        assert_eq!(hour_bucket_bounds(window, 23), (97_200_000, 100_000_001));
    }
}
