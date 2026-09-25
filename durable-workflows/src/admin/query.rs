use diesel::{BoolExpressionMethods, ExpressionMethods, JoinOnDsl, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;

use crate::{
    admin::{
        decode_cursor, encode_cursor, ActivityAttemptSummary, ActivityDetail,
        ActivityDetailRequest, ActivityListFilter, ActivitySummary, AdminPage, ApprovalSummary,
        CursorPosition, JsonFieldSummary, PageRequest, ProgressSummary, TimelineEntry,
        WorkflowListFilter, WorkflowSummary, MAX_ADMIN_PAGE_SIZE, MAX_TIMELINE_PAGE_SIZE,
    },
    persistence::{ActivityStatus, WorkflowStatus},
    schema::{
        durable_activity, durable_activity_attempt, durable_approval, durable_progress_event,
        durable_workflow, durable_workflow_event,
    },
    ActivityId, ApprovalId, DurableError, DurablePool, ScheduleRunId, WorkflowId,
};

// Postgres returns int4 and MySQL returns BIGINT; declare the narrower type and
// CAST to BIGINT at each call site so both backends decode the same i64.
diesel::define_sql_function! {
    #[sql_name = "OCTET_LENGTH"]
    fn octet_length_longtext(value: diesel::sql_types::Text) -> diesel::sql_types::Integer;
}

diesel::define_sql_function! {
    #[sql_name = "OCTET_LENGTH"]
    fn octet_length_nullable_longtext(value: diesel::sql_types::Nullable<diesel::sql_types::Text>) -> diesel::sql_types::Nullable<diesel::sql_types::Integer>;
}

#[derive(diesel::Queryable)]
struct WorkflowProjection {
    id: i64,
    kind: String,
    version: i32,
    status: WorkflowStatus,
    wait_kind: Option<String>,
    schedule_run_id: Option<i64>,
    root_workflow_id: Option<i64>,
    restarted_from_workflow_id: Option<i64>,
    input_bytes: i64,
    state_bytes: i64,
    result_bytes: Option<i64>,
    error_category: Option<String>,
    error_message: Option<String>,
    created_at: i64,
    updated_at: i64,
    completed_at: Option<i64>,
}

#[derive(diesel::Queryable)]
struct WorkflowDetailProjection {
    id: i64,
    kind: String,
    version: i32,
    status: WorkflowStatus,
    wait_kind: Option<String>,
    schedule_run_id: Option<i64>,
    root_workflow_id: Option<i64>,
    restarted_from_workflow_id: Option<i64>,
    input_json: String,
    state_json: String,
    result_json: Option<String>,
    error_category: Option<String>,
    error_message: Option<String>,
    created_at: i64,
    updated_at: i64,
    completed_at: Option<i64>,
}

#[derive(diesel::Queryable)]
struct ActivityProjection {
    id: i64,
    workflow_id: i64,
    kind: String,
    version: i32,
    topic: String,
    status: ActivityStatus,
    replacement_number: i32,
    attempt_count: i32,
    max_attempts: i32,
    operation_key: Option<String>,
    root_activity_id: Option<i64>,
    replaces_activity_id: Option<i64>,
    payload_bytes: i64,
    provider_result_bytes: Option<i64>,
    last_error_category: Option<String>,
    last_error_message: Option<String>,
    available_at: i64,
    created_at: i64,
    updated_at: i64,
    completed_at: Option<i64>,
}

#[derive(diesel::Queryable)]
struct ActivityDetailProjection {
    id: i64,
    workflow_id: i64,
    kind: String,
    version: i32,
    topic: String,
    status: ActivityStatus,
    replacement_number: i32,
    attempt_count: i32,
    max_attempts: i32,
    operation_key: Option<String>,
    root_activity_id: Option<i64>,
    replaces_activity_id: Option<i64>,
    payload_json: String,
    provider_result_json: Option<String>,
    last_error_category: Option<String>,
    last_error_message: Option<String>,
    available_at: i64,
    created_at: i64,
    updated_at: i64,
    completed_at: Option<i64>,
}

#[derive(diesel::Queryable)]
struct AttemptProjection {
    activity_id: i64,
    attempt_number: i32,
    worker_id: String,
    started_at: i64,
    heartbeat_at: i64,
    finished_at: Option<i64>,
    outcome: Option<String>,
    error_category: Option<String>,
    error_message: Option<String>,
    provider_result_bytes: Option<i64>,
}

#[derive(diesel::Queryable)]
struct AttemptDetailProjection {
    activity_id: i64,
    attempt_number: i32,
    worker_id: String,
    started_at: i64,
    heartbeat_at: i64,
    finished_at: Option<i64>,
    outcome: Option<String>,
    error_category: Option<String>,
    error_message: Option<String>,
    provider_result_json: Option<String>,
}

#[derive(diesel::Queryable)]
struct ApprovalProjection {
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

struct TimelineEnvelope {
    occurred_at: i64,
    tie_breaker: String,
    entry: TimelineEntry,
}

struct TimelineCursor {
    timestamp: i64,
    rank: u8,
    id: i64,
    attempt_number: i32,
    sequence: i32,
}

macro_rules! workflow_selection {
    () => {
        (
            durable_workflow::id,
            durable_workflow::kind,
            durable_workflow::version,
            durable_workflow::status,
            durable_workflow::wait_kind,
            durable_workflow::schedule_run_id,
            durable_workflow::root_workflow_id,
            durable_workflow::restarted_from_workflow_id,
            octet_length_longtext(durable_workflow::input_json).cast::<diesel::sql_types::BigInt>(),
            octet_length_longtext(durable_workflow::state_json).cast::<diesel::sql_types::BigInt>(),
            octet_length_nullable_longtext(durable_workflow::result_json)
                .cast::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>>(),
            durable_workflow::error_category,
            durable_workflow::error_message,
            durable_workflow::created_at,
            durable_workflow::updated_at,
            durable_workflow::completed_at,
        )
    };
}

macro_rules! workflow_detail_selection {
    () => {
        (
            durable_workflow::id,
            durable_workflow::kind,
            durable_workflow::version,
            durable_workflow::status,
            durable_workflow::wait_kind,
            durable_workflow::schedule_run_id,
            durable_workflow::root_workflow_id,
            durable_workflow::restarted_from_workflow_id,
            durable_workflow::input_json,
            durable_workflow::state_json,
            durable_workflow::result_json,
            durable_workflow::error_category,
            durable_workflow::error_message,
            durable_workflow::created_at,
            durable_workflow::updated_at,
            durable_workflow::completed_at,
        )
    };
}

macro_rules! activity_selection {
    () => {
        (
            durable_activity::id,
            durable_activity::workflow_id,
            durable_activity::kind,
            durable_activity::version,
            durable_activity::topic,
            durable_activity::status,
            durable_activity::replacement_number,
            durable_activity::attempt_count,
            durable_activity::max_attempts,
            durable_activity::operation_key,
            durable_activity::root_activity_id,
            durable_activity::replaces_activity_id,
            octet_length_longtext(durable_activity::payload_json)
                .cast::<diesel::sql_types::BigInt>(),
            octet_length_nullable_longtext(durable_activity::provider_result_json)
                .cast::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>>(),
            durable_activity::last_error_category,
            durable_activity::last_error_message,
            durable_activity::available_at,
            durable_activity::created_at,
            durable_activity::updated_at,
            durable_activity::completed_at,
        )
    };
}

macro_rules! activity_detail_selection {
    () => {
        (
            durable_activity::id,
            durable_activity::workflow_id,
            durable_activity::kind,
            durable_activity::version,
            durable_activity::topic,
            durable_activity::status,
            durable_activity::replacement_number,
            durable_activity::attempt_count,
            durable_activity::max_attempts,
            durable_activity::operation_key,
            durable_activity::root_activity_id,
            durable_activity::replaces_activity_id,
            durable_activity::payload_json,
            durable_activity::provider_result_json,
            durable_activity::last_error_category,
            durable_activity::last_error_message,
            durable_activity::available_at,
            durable_activity::created_at,
            durable_activity::updated_at,
            durable_activity::completed_at,
        )
    };
}

macro_rules! apply_numeric_timeline_cursor {
    ($query:expr, $time:expr, $id:expr, $rank:expr, $cursor:expr $(,)?) => {{
        let mut query = $query;
        let source_rank: u8 = $rank;
        if let Some(cursor) = $cursor {
            query = if source_rank < cursor.rank {
                query.filter($time.gt(cursor.timestamp))
            } else if source_rank > cursor.rank {
                query.filter($time.ge(cursor.timestamp))
            } else {
                query.filter(
                    $time
                        .gt(cursor.timestamp)
                        .or($time.eq(cursor.timestamp).and($id.gt(cursor.id))),
                )
            };
        }
        query
    }};
}

macro_rules! apply_attempt_timeline_cursor {
    ($query:expr, $cursor:expr) => {{
        let mut query = $query;
        if let Some(cursor) = $cursor {
            query = if 2 < cursor.rank {
                query.filter(durable_activity_attempt::started_at.gt(cursor.timestamp))
            } else if 2 > cursor.rank {
                query.filter(durable_activity_attempt::started_at.ge(cursor.timestamp))
            } else {
                query.filter(
                    durable_activity_attempt::started_at
                        .gt(cursor.timestamp)
                        .or(durable_activity_attempt::started_at
                            .eq(cursor.timestamp)
                            .and(
                                durable_activity_attempt::activity_id.gt(cursor.id).or(
                                    durable_activity_attempt::activity_id.eq(cursor.id).and(
                                        durable_activity_attempt::attempt_number
                                            .gt(cursor.attempt_number),
                                    ),
                                ),
                            )),
                )
            };
        }
        query
    }};
}

macro_rules! apply_progress_timeline_cursor {
    ($query:expr, $cursor:expr) => {{
        let mut query = $query;
        if let Some(cursor) = $cursor {
            query = if 3 < cursor.rank {
                query.filter(durable_progress_event::created_at.gt(cursor.timestamp))
            } else if 3 > cursor.rank {
                query.filter(durable_progress_event::created_at.ge(cursor.timestamp))
            } else {
                query.filter(
                    durable_progress_event::created_at.gt(cursor.timestamp).or(
                        durable_progress_event::created_at.eq(cursor.timestamp).and(
                            durable_progress_event::activity_id.gt(cursor.id).or(
                                durable_progress_event::activity_id.eq(cursor.id).and(
                                    durable_progress_event::attempt_number
                                        .gt(cursor.attempt_number)
                                        .or(durable_progress_event::attempt_number
                                            .eq(cursor.attempt_number)
                                            .and(
                                                durable_progress_event::sequence
                                                    .gt(cursor.sequence),
                                            )),
                                ),
                            ),
                        ),
                    ),
                )
            };
        }
        query
    }};
}

#[derive(Clone)]
pub struct AdminQueryService {
    pub(super) pool: DurablePool,
}

impl AdminQueryService {
    pub fn new(pool: DurablePool) -> Self {
        Self { pool }
    }

    pub async fn list_workflows(
        &self,
        filter: WorkflowListFilter,
    ) -> Result<AdminPage<WorkflowSummary>, DurableError> {
        let limit = filter.page.bounded_limit(MAX_ADMIN_PAGE_SIZE)?;
        validate_workflow_filter(&filter)?;
        let cursor = filter
            .page
            .cursor
            .as_deref()
            .map(|cursor| decode_numeric_cursor("workflows", cursor))
            .transpose()?;
        let mut query = durable_workflow::table.into_boxed::<crate::Db>();
        if let Some(kind) = filter.kind {
            query = query.filter(durable_workflow::kind.eq(kind));
        }
        if let Some(version) = filter.version {
            query = query.filter(durable_workflow::version.eq(version));
        }
        if let Some(status) = filter.status {
            query = query.filter(durable_workflow::status.eq(status));
        }
        if let Some(schedule_run_id) = filter.schedule_run_id {
            query = query.filter(durable_workflow::schedule_run_id.eq(schedule_run_id.get()));
        }
        if let Some(root_workflow_id) = filter.root_workflow_id {
            query = query.filter(durable_workflow::root_workflow_id.eq(root_workflow_id.get()));
        }
        if let Some(created_after) = filter.created_after {
            query = query.filter(durable_workflow::created_at.ge(created_after));
        }
        if let Some(created_before) = filter.created_before {
            query = query.filter(durable_workflow::created_at.lt(created_before));
        }
        // Filter on wait_kind so paused approval waits remain visible; status
        // alone excludes paused rows that still hold a pending approval.
        if filter.waiting_approval == Some(true) {
            query = query.filter(durable_workflow::wait_kind.eq("approval"));
        } else if filter.waiting_approval == Some(false) {
            query = query.filter(
                durable_workflow::wait_kind
                    .ne("approval")
                    .or(durable_workflow::wait_kind.is_null()),
            );
        }
        if let Some((timestamp, id)) = cursor {
            query = query.filter(
                durable_workflow::created_at
                    .lt(timestamp)
                    .or(durable_workflow::created_at
                        .eq(timestamp)
                        .and(durable_workflow::id.lt(id))),
            );
        }
        let mut connection = self.pool.get().await?;
        let rows = query
            .order((
                durable_workflow::created_at.desc(),
                durable_workflow::id.desc(),
            ))
            .limit(i64::from(limit) + 1)
            .select(workflow_selection!())
            .load::<WorkflowProjection>(&mut connection)
            .await?;
        page_workflows(rows, limit)
    }

    pub async fn get_workflow(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<WorkflowSummary, DurableError> {
        let mut connection = self.pool.get().await?;
        let row = durable_workflow::table
            .find(workflow_id.get())
            .select(workflow_detail_selection!())
            .first::<WorkflowDetailProjection>(&mut connection)
            .await
            .optional()?
            .ok_or_else(|| not_found("workflow", workflow_id))?;
        workflow_detail_summary(row)
    }

    pub async fn list_activities(
        &self,
        filter: ActivityListFilter,
    ) -> Result<AdminPage<ActivitySummary>, DurableError> {
        let limit = filter.page.bounded_limit(MAX_ADMIN_PAGE_SIZE)?;
        validate_activity_filter(&filter)?;
        let cursor = filter
            .page
            .cursor
            .as_deref()
            .map(|cursor| decode_numeric_cursor("activities", cursor))
            .transpose()?;
        let mut query = durable_activity::table.into_boxed::<crate::Db>();
        if let Some(workflow_id) = filter.workflow_id {
            query = query.filter(durable_activity::workflow_id.eq(workflow_id.get()));
        }
        if let Some(kind) = filter.kind {
            query = query.filter(durable_activity::kind.eq(kind));
        }
        if let Some(version) = filter.version {
            query = query.filter(durable_activity::version.eq(version));
        }
        if let Some(topic) = filter.topic {
            query = query.filter(durable_activity::topic.eq(topic));
        }
        if let Some(status) = filter.status {
            query = query.filter(durable_activity::status.eq(status));
        }
        if let Some(created_after) = filter.created_after {
            query = query.filter(durable_activity::created_at.ge(created_after));
        }
        if let Some(created_before) = filter.created_before {
            query = query.filter(durable_activity::created_at.lt(created_before));
        }
        if let Some((timestamp, id)) = cursor {
            query = query.filter(
                durable_activity::created_at
                    .lt(timestamp)
                    .or(durable_activity::created_at
                        .eq(timestamp)
                        .and(durable_activity::id.lt(id))),
            );
        }
        let mut connection = self.pool.get().await?;
        let rows = query
            .order((
                durable_activity::created_at.desc(),
                durable_activity::id.desc(),
            ))
            .limit(i64::from(limit) + 1)
            .select(activity_selection!())
            .load::<ActivityProjection>(&mut connection)
            .await?;
        page_activities(rows, limit)
    }

    pub async fn get_activity(
        &self,
        activity_id: ActivityId,
        request: ActivityDetailRequest,
    ) -> Result<ActivityDetail, DurableError> {
        let attempt_limit = PageRequest {
            cursor: request.attempt_cursor.clone(),
            limit: request.attempt_limit,
        }
        .bounded_limit(MAX_ADMIN_PAGE_SIZE)?;
        let progress_limit = PageRequest {
            cursor: request.progress_cursor.clone(),
            limit: request.progress_limit,
        }
        .bounded_limit(MAX_ADMIN_PAGE_SIZE)?;
        let attempt_scope = format!("activity_attempts:{}", activity_id.get());
        let attempt_after = request
            .attempt_cursor
            .as_deref()
            .map(|cursor| {
                let (_, attempt) = decode_numeric_cursor(&attempt_scope, cursor)?;
                i32::try_from(attempt).map_err(|_| {
                    DurableError::InvalidCursor(
                        "activity attempt cursor exceeds the integer range".to_string(),
                    )
                })
            })
            .transpose()?;
        let progress_scope = format!("activity_progress:{}", activity_id.get());
        let progress_after = request
            .progress_cursor
            .as_deref()
            .map(|cursor| {
                let (attempt, sequence) = decode_numeric_cursor(&progress_scope, cursor)?;
                Ok::<(i32, i32), DurableError>((
                    i32::try_from(attempt).map_err(|_| {
                        DurableError::InvalidCursor(
                            "progress attempt cursor exceeds the integer range".to_string(),
                        )
                    })?,
                    i32::try_from(sequence).map_err(|_| {
                        DurableError::InvalidCursor(
                            "progress sequence cursor exceeds the integer range".to_string(),
                        )
                    })?,
                ))
            })
            .transpose()?;
        let mut connection = self.pool.get().await?;
        let row = durable_activity::table
            .find(activity_id.get())
            .select(activity_detail_selection!())
            .first::<ActivityDetailProjection>(&mut connection)
            .await
            .optional()?
            .ok_or_else(|| not_found("activity", activity_id))?;
        let mut attempts_query = durable_activity_attempt::table
            .filter(durable_activity_attempt::activity_id.eq(activity_id.get()))
            .into_boxed::<crate::Db>();
        if let Some(attempt_after) = attempt_after {
            attempts_query =
                attempts_query.filter(durable_activity_attempt::attempt_number.gt(attempt_after));
        }
        let mut attempts = attempts_query
            .order(durable_activity_attempt::attempt_number.asc())
            .limit(i64::from(attempt_limit) + 1)
            .select((
                durable_activity_attempt::activity_id,
                durable_activity_attempt::attempt_number,
                durable_activity_attempt::worker_id,
                durable_activity_attempt::started_at,
                durable_activity_attempt::heartbeat_at,
                durable_activity_attempt::finished_at,
                durable_activity_attempt::outcome,
                durable_activity_attempt::error_category,
                durable_activity_attempt::error_message,
                durable_activity_attempt::provider_result_json,
            ))
            .load::<AttemptDetailProjection>(&mut connection)
            .await?;
        let attempts_have_more = attempts.len() > attempt_limit as usize;
        attempts.truncate(attempt_limit as usize);
        let attempts_next_cursor = if attempts_have_more {
            attempts
                .last()
                .map(|attempt| {
                    encode_cursor(
                        &attempt_scope,
                        &CursorPosition {
                            timestamp: 0,
                            tie_breaker: attempt.attempt_number.to_string(),
                        },
                    )
                })
                .transpose()?
        } else {
            None
        };
        let attempts = attempts
            .into_iter()
            .map(attempt_detail_summary)
            .collect::<Result<Vec<_>, _>>()?;
        let mut progress_query = durable_progress_event::table
            .filter(durable_progress_event::activity_id.eq(activity_id.get()))
            .into_boxed::<crate::Db>();
        if let Some((attempt_number, sequence)) = progress_after {
            progress_query = progress_query.filter(
                durable_progress_event::attempt_number
                    .gt(attempt_number)
                    .or(durable_progress_event::attempt_number
                        .eq(attempt_number)
                        .and(durable_progress_event::sequence.gt(sequence))),
            );
        }
        let mut progress = progress_query
            .order((
                durable_progress_event::attempt_number.asc(),
                durable_progress_event::sequence.asc(),
            ))
            .limit(i64::from(progress_limit) + 1)
            .select((
                durable_progress_event::activity_id,
                durable_progress_event::attempt_number,
                durable_progress_event::sequence,
                durable_progress_event::code,
                durable_progress_event::description,
                durable_progress_event::completed_units,
                durable_progress_event::total_units,
                durable_progress_event::severity,
                durable_progress_event::created_at,
            ))
            .load::<(
                i64,
                i32,
                i32,
                String,
                String,
                Option<i64>,
                Option<i64>,
                String,
                i64,
            )>(&mut connection)
            .await?;
        let progress_have_more = progress.len() > progress_limit as usize;
        progress.truncate(progress_limit as usize);
        let progress_next_cursor = if progress_have_more {
            progress
                .last()
                .map(|entry| {
                    encode_cursor(
                        &progress_scope,
                        &CursorPosition {
                            timestamp: i64::from(entry.1),
                            tie_breaker: entry.2.to_string(),
                        },
                    )
                })
                .transpose()?
        } else {
            None
        };
        let progress = progress
            .into_iter()
            .map(progress_summary)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ActivityDetail {
            activity: activity_detail_summary(row)?,
            attempts: AdminPage {
                items: attempts,
                next_cursor: attempts_next_cursor,
            },
            progress: AdminPage {
                items: progress,
                next_cursor: progress_next_cursor,
            },
        })
    }

    pub async fn workflow_timeline(
        &self,
        workflow_id: WorkflowId,
        page: PageRequest,
    ) -> Result<AdminPage<TimelineEntry>, DurableError> {
        let limit = page.bounded_limit(MAX_TIMELINE_PAGE_SIZE)?;
        let scope = format!("workflow_timeline:{}", workflow_id.get());
        let cursor = page
            .cursor
            .as_deref()
            .map(|cursor| decode_timeline_cursor(&scope, cursor))
            .transpose()?;

        let per_source_limit = i64::from(limit) + 1;
        let mut connection = self.pool.get().await?;
        // Existence only — avoid re-loading/parsing input/state/result payloads
        // that get_workflow already returned to the detail controller.
        durable_workflow::table
            .find(workflow_id.get())
            .select(durable_workflow::id)
            .first::<i64>(&mut connection)
            .await
            .optional()?
            .ok_or_else(|| not_found("workflow", workflow_id))?;
        let mut timeline = Vec::new();

        let mut events = durable_workflow_event::table
            .filter(durable_workflow_event::workflow_id.eq(workflow_id.get()))
            .into_boxed::<crate::Db>();
        events = apply_numeric_timeline_cursor!(
            events,
            durable_workflow_event::created_at,
            durable_workflow_event::id,
            0,
            cursor.as_ref(),
        );
        for (id, event_type, actor_type, actor_id, reason, created_at) in events
            .order((
                durable_workflow_event::created_at.asc(),
                durable_workflow_event::id.asc(),
            ))
            .limit(per_source_limit)
            .select((
                durable_workflow_event::id,
                durable_workflow_event::event_type,
                durable_workflow_event::actor_type,
                durable_workflow_event::actor_id,
                durable_workflow_event::reason,
                durable_workflow_event::created_at,
            ))
            .load::<(
                i64,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                i64,
            )>(&mut connection)
            .await?
        {
            timeline.push(TimelineEnvelope {
                occurred_at: created_at,
                tie_breaker: timeline_key(0, &[id]),
                entry: TimelineEntry::WorkflowEvent {
                    event_type,
                    actor_type,
                    actor_id,
                    reason,
                    occurred_at: created_at,
                },
            });
        }

        let mut activities = durable_activity::table
            .filter(durable_activity::workflow_id.eq(workflow_id.get()))
            .into_boxed::<crate::Db>();
        activities = apply_numeric_timeline_cursor!(
            activities,
            durable_activity::created_at,
            durable_activity::id,
            1,
            cursor.as_ref(),
        );
        for row in activities
            .order((
                durable_activity::created_at.asc(),
                durable_activity::id.asc(),
            ))
            .limit(per_source_limit)
            .select(activity_selection!())
            .load::<ActivityProjection>(&mut connection)
            .await?
        {
            let occurred_at = row.created_at;
            let id = row.id;
            timeline.push(TimelineEnvelope {
                occurred_at,
                tie_breaker: timeline_key(1, &[id]),
                entry: TimelineEntry::Activity(activity_summary(row)?),
            });
        }

        let mut attempts = durable_activity_attempt::table
            .inner_join(
                durable_activity::table
                    .on(durable_activity::id.eq(durable_activity_attempt::activity_id)),
            )
            .filter(durable_activity::workflow_id.eq(workflow_id.get()))
            .into_boxed::<crate::Db>();
        attempts = apply_attempt_timeline_cursor!(attempts, cursor.as_ref());
        for row in attempts
            .order((
                durable_activity_attempt::started_at.asc(),
                durable_activity_attempt::activity_id.asc(),
                durable_activity_attempt::attempt_number.asc(),
            ))
            .limit(per_source_limit)
            .select((
                durable_activity_attempt::activity_id,
                durable_activity_attempt::attempt_number,
                durable_activity_attempt::worker_id,
                durable_activity_attempt::started_at,
                durable_activity_attempt::heartbeat_at,
                durable_activity_attempt::finished_at,
                durable_activity_attempt::outcome,
                durable_activity_attempt::error_category,
                durable_activity_attempt::error_message,
                octet_length_nullable_longtext(durable_activity_attempt::provider_result_json)
                    .cast::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>>(),
            ))
            .load::<AttemptProjection>(&mut connection)
            .await?
        {
            let occurred_at = row.started_at;
            let tie_breaker = timeline_key(2, &[row.activity_id, i64::from(row.attempt_number)]);
            timeline.push(TimelineEnvelope {
                occurred_at,
                tie_breaker,
                entry: TimelineEntry::ActivityAttempt(attempt_summary(row)?),
            });
        }

        let mut progress = durable_progress_event::table
            .inner_join(
                durable_activity::table
                    .on(durable_activity::id.eq(durable_progress_event::activity_id)),
            )
            .filter(durable_activity::workflow_id.eq(workflow_id.get()))
            .into_boxed::<crate::Db>();
        progress = apply_progress_timeline_cursor!(progress, cursor.as_ref());
        for row in progress
            .order((
                durable_progress_event::created_at.asc(),
                durable_progress_event::activity_id.asc(),
                durable_progress_event::attempt_number.asc(),
                durable_progress_event::sequence.asc(),
            ))
            .limit(per_source_limit)
            .select((
                durable_progress_event::activity_id,
                durable_progress_event::attempt_number,
                durable_progress_event::sequence,
                durable_progress_event::code,
                durable_progress_event::description,
                durable_progress_event::completed_units,
                durable_progress_event::total_units,
                durable_progress_event::severity,
                durable_progress_event::created_at,
            ))
            .load::<(
                i64,
                i32,
                i32,
                String,
                String,
                Option<i64>,
                Option<i64>,
                String,
                i64,
            )>(&mut connection)
            .await?
        {
            let occurred_at = row.8;
            let tie_breaker = timeline_key(3, &[row.0, i64::from(row.1), i64::from(row.2)]);
            timeline.push(TimelineEnvelope {
                occurred_at,
                tie_breaker,
                entry: TimelineEntry::Progress(progress_summary(row)?),
            });
        }

        let mut approvals = durable_approval::table
            .filter(durable_approval::workflow_id.eq(workflow_id.get()))
            .into_boxed::<crate::Db>();
        approvals = apply_numeric_timeline_cursor!(
            approvals,
            durable_approval::requested_at,
            durable_approval::id,
            4,
            cursor.as_ref(),
        );
        for row in approvals
            .order((
                durable_approval::requested_at.asc(),
                durable_approval::id.asc(),
            ))
            .limit(per_source_limit)
            .select((
                durable_approval::id,
                durable_approval::workflow_id,
                durable_approval::kind,
                durable_approval::version,
                durable_approval::status,
                octet_length_nullable_longtext(durable_approval::decision_payload_json)
                    .cast::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>>(),
                durable_approval::decided_by,
                durable_approval::operator_reason,
                durable_approval::requested_at,
                durable_approval::expires_at,
                durable_approval::resolved_at,
            ))
            .load::<ApprovalProjection>(&mut connection)
            .await?
        {
            let occurred_at = row.requested_at;
            let id = row.id;
            timeline.push(TimelineEnvelope {
                occurred_at,
                tie_breaker: timeline_key(4, &[id]),
                entry: TimelineEntry::Approval(approval_summary(row)?),
            });
        }

        timeline.sort_by(|left, right| {
            (left.occurred_at, &left.tie_breaker).cmp(&(right.occurred_at, &right.tie_breaker))
        });
        let has_more = timeline.len() > limit as usize;
        timeline.truncate(limit as usize);
        let next_cursor = if has_more {
            timeline
                .last()
                .map(|entry| {
                    encode_cursor(
                        &scope,
                        &CursorPosition {
                            timestamp: entry.occurred_at,
                            tie_breaker: entry.tie_breaker.clone(),
                        },
                    )
                })
                .transpose()?
        } else {
            None
        };
        Ok(AdminPage {
            items: timeline.into_iter().map(|entry| entry.entry).collect(),
            next_cursor,
        })
    }
}

fn page_workflows(
    mut rows: Vec<WorkflowProjection>,
    limit: u32,
) -> Result<AdminPage<WorkflowSummary>, DurableError> {
    let has_more = rows.len() > limit as usize;
    if has_more {
        rows.pop();
    }
    let next_cursor = if has_more {
        rows.last()
            .map(|row| {
                encode_cursor(
                    "workflows",
                    &CursorPosition {
                        timestamp: row.created_at,
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
            .map(workflow_summary)
            .collect::<Result<Vec<_>, _>>()?,
        next_cursor,
    })
}

fn page_activities(
    mut rows: Vec<ActivityProjection>,
    limit: u32,
) -> Result<AdminPage<ActivitySummary>, DurableError> {
    let has_more = rows.len() > limit as usize;
    if has_more {
        rows.pop();
    }
    let next_cursor = if has_more {
        rows.last()
            .map(|row| {
                encode_cursor(
                    "activities",
                    &CursorPosition {
                        timestamp: row.created_at,
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
            .map(activity_summary)
            .collect::<Result<Vec<_>, _>>()?,
        next_cursor,
    })
}

fn workflow_summary(row: WorkflowProjection) -> Result<WorkflowSummary, DurableError> {
    Ok(WorkflowSummary {
        id: WorkflowId::new(row.id)?,
        kind: row.kind,
        version: row.version,
        status: row.status.to_string(),
        wait_kind: row.wait_kind,
        schedule_run_id: row.schedule_run_id.map(ScheduleRunId::new).transpose()?,
        root_workflow_id: row.root_workflow_id.map(WorkflowId::new).transpose()?,
        restarted_from_workflow_id: row
            .restarted_from_workflow_id
            .map(WorkflowId::new)
            .transpose()?,
        input: required_json_summary(row.input_bytes)?,
        state: required_json_summary(row.state_bytes)?,
        result: optional_json_summary(row.result_bytes)?,
        error_category: row.error_category,
        error_message: row.error_message,
        created_at: row.created_at,
        updated_at: row.updated_at,
        completed_at: row.completed_at,
    })
}

fn workflow_detail_summary(row: WorkflowDetailProjection) -> Result<WorkflowSummary, DurableError> {
    Ok(WorkflowSummary {
        id: WorkflowId::new(row.id)?,
        kind: row.kind,
        version: row.version,
        status: row.status.to_string(),
        wait_kind: row.wait_kind,
        schedule_run_id: row.schedule_run_id.map(ScheduleRunId::new).transpose()?,
        root_workflow_id: row.root_workflow_id.map(WorkflowId::new).transpose()?,
        restarted_from_workflow_id: row
            .restarted_from_workflow_id
            .map(WorkflowId::new)
            .transpose()?,
        input: JsonFieldSummary::from_required_json(&row.input_json)?,
        state: JsonFieldSummary::from_required_json(&row.state_json)?,
        result: JsonFieldSummary::from_optional_json(row.result_json.as_deref())?,
        error_category: row.error_category,
        error_message: row.error_message,
        created_at: row.created_at,
        updated_at: row.updated_at,
        completed_at: row.completed_at,
    })
}

fn activity_summary(row: ActivityProjection) -> Result<ActivitySummary, DurableError> {
    Ok(ActivitySummary {
        id: ActivityId::new(row.id)?,
        workflow_id: WorkflowId::new(row.workflow_id)?,
        kind: row.kind,
        version: row.version,
        topic: row.topic,
        status: row.status.to_string(),
        replacement_number: row.replacement_number,
        attempt_count: row.attempt_count,
        max_attempts: row.max_attempts,
        operation_key: row.operation_key,
        root_activity_id: row.root_activity_id.map(ActivityId::new).transpose()?,
        replaces_activity_id: row.replaces_activity_id.map(ActivityId::new).transpose()?,
        payload: required_json_summary(row.payload_bytes)?,
        provider_result: optional_json_summary(row.provider_result_bytes)?,
        error_category: row.last_error_category,
        error_message: row.last_error_message,
        available_at: if row.available_at == crate::transition::CONTINUATION_READY_AT_MILLIS {
            row.created_at
        } else {
            row.available_at
        },
        created_at: row.created_at,
        updated_at: row.updated_at,
        completed_at: row.completed_at,
    })
}

fn activity_detail_summary(row: ActivityDetailProjection) -> Result<ActivitySummary, DurableError> {
    Ok(ActivitySummary {
        id: ActivityId::new(row.id)?,
        workflow_id: WorkflowId::new(row.workflow_id)?,
        kind: row.kind,
        version: row.version,
        topic: row.topic,
        status: row.status.to_string(),
        replacement_number: row.replacement_number,
        attempt_count: row.attempt_count,
        max_attempts: row.max_attempts,
        operation_key: row.operation_key,
        root_activity_id: row.root_activity_id.map(ActivityId::new).transpose()?,
        replaces_activity_id: row.replaces_activity_id.map(ActivityId::new).transpose()?,
        payload: JsonFieldSummary::from_required_json(&row.payload_json)?,
        provider_result: JsonFieldSummary::from_optional_json(row.provider_result_json.as_deref())?,
        error_category: row.last_error_category,
        error_message: row.last_error_message,
        available_at: if row.available_at == crate::transition::CONTINUATION_READY_AT_MILLIS {
            row.created_at
        } else {
            row.available_at
        },
        created_at: row.created_at,
        updated_at: row.updated_at,
        completed_at: row.completed_at,
    })
}

fn attempt_summary(row: AttemptProjection) -> Result<ActivityAttemptSummary, DurableError> {
    Ok(ActivityAttemptSummary {
        activity_id: ActivityId::new(row.activity_id)?,
        attempt_number: row.attempt_number,
        worker_id: row.worker_id,
        started_at: row.started_at,
        heartbeat_at: row.heartbeat_at,
        finished_at: row.finished_at,
        outcome: row.outcome,
        error_category: row.error_category,
        error_message: row.error_message,
        provider_result: optional_json_summary(row.provider_result_bytes)?,
    })
}

fn attempt_detail_summary(
    row: AttemptDetailProjection,
) -> Result<ActivityAttemptSummary, DurableError> {
    Ok(ActivityAttemptSummary {
        activity_id: ActivityId::new(row.activity_id)?,
        attempt_number: row.attempt_number,
        worker_id: row.worker_id,
        started_at: row.started_at,
        heartbeat_at: row.heartbeat_at,
        finished_at: row.finished_at,
        outcome: row.outcome,
        error_category: row.error_category,
        error_message: row.error_message,
        provider_result: JsonFieldSummary::from_optional_json(row.provider_result_json.as_deref())?,
    })
}

fn approval_summary(row: ApprovalProjection) -> Result<ApprovalSummary, DurableError> {
    Ok(ApprovalSummary {
        id: ApprovalId::new(row.id)?,
        workflow_id: WorkflowId::new(row.workflow_id)?,
        kind: row.kind,
        version: row.version,
        status: row.status,
        decision: optional_json_summary(row.decision_bytes)?,
        decided_by: row.decided_by,
        operator_reason: row.operator_reason,
        requested_at: row.requested_at,
        expires_at: row.expires_at,
        resolved_at: row.resolved_at,
    })
}

fn progress_summary(
    row: (
        i64,
        i32,
        i32,
        String,
        String,
        Option<i64>,
        Option<i64>,
        String,
        i64,
    ),
) -> Result<ProgressSummary, DurableError> {
    Ok(ProgressSummary {
        activity_id: ActivityId::new(row.0)?,
        attempt_number: row.1,
        sequence: row.2,
        code: row.3,
        description: row.4,
        completed_units: row.5,
        total_units: row.6,
        severity: row.7,
        created_at: row.8,
    })
}

fn required_json_summary(bytes: i64) -> Result<JsonFieldSummary, DurableError> {
    Ok(JsonFieldSummary {
        present: true,
        bytes: usize::try_from(bytes).map_err(|_| {
            DurableError::InvalidState("negative persisted JSON byte length".to_string())
        })?,
        value: None,
    })
}

fn optional_json_summary(bytes: Option<i64>) -> Result<JsonFieldSummary, DurableError> {
    match bytes {
        Some(bytes) => required_json_summary(bytes),
        None => Ok(JsonFieldSummary {
            present: false,
            bytes: 0,
            value: None,
        }),
    }
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

fn decode_timeline_cursor(scope: &str, cursor: &str) -> Result<TimelineCursor, DurableError> {
    let position = decode_cursor(scope, cursor)?;
    let mut parts = position.tie_breaker.split(':');
    let rank = parts
        .next()
        .ok_or_else(|| DurableError::InvalidCursor("timeline cursor has no rank".to_string()))?
        .parse::<u8>()
        .map_err(|_| DurableError::InvalidCursor("timeline cursor rank is invalid".to_string()))?;
    let components = parts
        .map(|part| {
            part.parse::<i64>().map_err(|_| {
                DurableError::InvalidCursor("timeline cursor component is not numeric".to_string())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected_components = match rank {
        0 | 1 | 4 => 1,
        2 => 2,
        3 => 3,
        _ => {
            return Err(DurableError::InvalidCursor(
                "timeline cursor rank is outside the supported range".to_string(),
            ));
        }
    };
    if components.len() != expected_components || components.iter().any(|value| *value <= 0) {
        return Err(DurableError::InvalidCursor(
            "timeline cursor has invalid components".to_string(),
        ));
    }
    let attempt_number = match components.get(1) {
        Some(value) => i32::try_from(*value).map_err(|_| {
            DurableError::InvalidCursor(
                "timeline cursor attempt number is outside the supported range".to_string(),
            )
        })?,
        None => 0,
    };
    let sequence = match components.get(2) {
        Some(value) => i32::try_from(*value).map_err(|_| {
            DurableError::InvalidCursor(
                "timeline cursor sequence is outside the supported range".to_string(),
            )
        })?,
        None => 0,
    };
    Ok(TimelineCursor {
        timestamp: position.timestamp,
        rank,
        id: components[0],
        attempt_number,
        sequence,
    })
}

fn timeline_key(rank: u8, components: &[i64]) -> String {
    let components = components
        .iter()
        .map(|component| format!("{component:020}"))
        .collect::<Vec<_>>()
        .join(":");
    format!("{rank}:{components}")
}

fn validate_workflow_filter(filter: &WorkflowListFilter) -> Result<(), DurableError> {
    if filter.version.is_some_and(|version| version <= 0) {
        return Err(DurableError::InvalidDefinition(
            "workflow version filter must be positive".to_string(),
        ));
    }
    validate_time_range(filter.created_after, filter.created_before)
}

fn validate_activity_filter(filter: &ActivityListFilter) -> Result<(), DurableError> {
    if filter.version.is_some_and(|version| version <= 0) {
        return Err(DurableError::InvalidDefinition(
            "activity version filter must be positive".to_string(),
        ));
    }
    validate_time_range(filter.created_after, filter.created_before)
}

fn validate_time_range(after: Option<i64>, before: Option<i64>) -> Result<(), DurableError> {
    if matches!((after, before), (Some(after), Some(before)) if after >= before) {
        return Err(DurableError::InvalidDefinition(
            "createdAfter must precede createdBefore".to_string(),
        ));
    }
    Ok(())
}

fn not_found(resource: &'static str, identifier: impl ToString) -> DurableError {
    DurableError::NotFound {
        resource,
        identifier: identifier.to_string(),
    }
}
