use std::sync::Arc;

use chrono::NaiveDateTime;
use diesel::{
    dsl::not, ExpressionMethods, JoinOnDsl, NullableExpressionMethods, QueryDsl, SelectableHelper,
};
use diesel_async::RunQueryDsl;

use crate::{
    persistence::{self, NewScheduleRunRow, ScheduleRunRow, ScheduleStateRow, WorkflowStatus},
    schema::{durable_schedule_run, durable_schedule_state, durable_workflow},
    DurableError, DurablePool, LocalTimeDisposition, MisfirePolicy, OverlapPolicy,
    ScheduleCalendar, ScheduleOccurrence, ScheduleRegistry, ScheduleRunId,
    ScheduleStateReconcileOutcome,
};

const MAX_DUE_OCCURRENCES_PER_TICK: usize = 10_000;
const LOCAL_OCCURRENCE_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleMaterializationReport {
    pub schedule_key: String,
    pub inspected: u32,
    pub started: u32,
    pub queued: u32,
    pub skipped: u32,
    pub coalesced: u32,
    pub paused: bool,
}

impl ScheduleMaterializationReport {
    fn empty(schedule_key: &str) -> Self {
        Self {
            schedule_key: schedule_key.to_string(),
            inspected: 0,
            started: 0,
            queued: 0,
            skipped: 0,
            coalesced: 0,
            paused: false,
        }
    }
}

pub struct ScheduleMaterializer<C> {
    pool: DurablePool,
    context: Arc<C>,
    registry: Arc<ScheduleRegistry<C>>,
}

impl<C> ScheduleMaterializer<C>
where
    C: Send + Sync + 'static,
{
    pub fn new(pool: DurablePool, context: Arc<C>, registry: Arc<ScheduleRegistry<C>>) -> Self {
        Self {
            pool,
            context,
            registry,
        }
    }

    pub async fn materialize_schedule_now(
        &self,
        schedule_key: &str,
    ) -> Result<ScheduleMaterializationReport, DurableError> {
        let mut connection = self.pool.get().await?;
        let now = persistence::database_now_millis(&mut connection).await?;
        drop(connection);
        self.materialize_schedule(schedule_key, now).await
    }

    pub async fn materialize_schedule(
        &self,
        schedule_key: &str,
        now: i64,
    ) -> Result<ScheduleMaterializationReport, DurableError> {
        match self
            .registry
            .reconcile_state(schedule_key, &self.pool, now)
            .await?
        {
            ScheduleStateReconcileOutcome::Inserted | ScheduleStateReconcileOutcome::Upgraded => {
                return Ok(ScheduleMaterializationReport::empty(schedule_key));
            }
            ScheduleStateReconcileOutcome::Preserved => {}
            ScheduleStateReconcileOutcome::NewerPersisted => {
                return Err(DurableError::Conflict(format!(
                    "schedule {schedule_key} has a newer persisted definition"
                )));
            }
        }

        let metadata =
            self.registry
                .get(schedule_key)
                .cloned()
                .ok_or_else(|| DurableError::NotFound {
                    resource: "schedule definition",
                    identifier: schedule_key.to_string(),
                })?;
        let calendar = self
            .registry
            .calendar(schedule_key)
            .cloned()
            .ok_or_else(|| DurableError::NotFound {
                resource: "schedule calendar",
                identifier: schedule_key.to_string(),
            })?;
        let context = self.context.clone();
        let registry = self.registry.clone();
        let schedule_key = schedule_key.to_string();
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            crate::trace::declare_unmodeled("schedule_materialize", true);
            let state = durable_schedule_state::table
                .find(&schedule_key)
                .for_update()
                .select(ScheduleStateRow::as_select())
                .first::<ScheduleStateRow>(connection)
                .await?;
            if state.definition_version != metadata.version
                || state.definition_fingerprint != metadata.fingerprint
            {
                return Err(DurableError::Conflict(format!(
                    "schedule {schedule_key} definition changed while materializing"
                )));
            }
            let mut report = ScheduleMaterializationReport::empty(&schedule_key);
            if state.paused_at.is_some() {
                report.paused = true;
                return Ok(report);
            }
            let mut active = active_workflow_count(connection, &schedule_key).await?;
            let mut queued = queued_run_exists(connection, &schedule_key).await?;
            if metadata.overlap == OverlapPolicy::QueueOne && active == 0 && queued {
                promote_queued(
                    connection,
                    registry.as_ref(),
                    context.as_ref(),
                    &schedule_key,
                )
                .await?;
                active = 1;
                queued = false;
                report.started = report.started.saturating_add(1);
            }

            let local = NaiveDateTime::parse_from_str(
                &state.next_local_occurrence,
                LOCAL_OCCURRENCE_FORMAT,
            )
            .map_err(|error| {
                DurableError::InvalidState(format!(
                    "schedule {schedule_key} has invalid persisted local occurrence: {error}"
                ))
            })?;
            let mut occurrence = calendar.occurrence_at_local(local)?;
            if occurrence.local_occurrence != state.next_local_occurrence
                || occurrence.due_at != state.next_occurrence_at
            {
                return Err(DurableError::InvalidState(format!(
                    "schedule {schedule_key} persisted local and UTC occurrences disagree"
                )));
            }
            let (due, next, outcomes) = plan_due_chunk(
                &calendar,
                occurrence,
                metadata.misfire,
                metadata.misfire_grace_millis,
                now,
            )?;
            occurrence = next;
            report.inspected = u32::try_from(due.len()).map_err(|_| {
                DurableError::InvalidState("schedule occurrence count exceeds u32".to_string())
            })?;
            for (occurrence, outcome) in due.into_iter().zip(outcomes) {
                let target = MaterializationTarget {
                    registry: registry.as_ref(),
                    context: context.as_ref(),
                    schedule_key: &schedule_key,
                    overlap: metadata.overlap,
                    active: &mut active,
                    queued: &mut queued,
                    now,
                };
                materialize_occurrence(connection, target, occurrence, outcome, &mut report)
                    .await?;
            }
            if report.inspected == 0 {
                return Ok(report);
            }
            let changed = diesel::update(
                durable_schedule_state::table
                    .find(&schedule_key)
                    .filter(durable_schedule_state::definition_version.eq(metadata.version))
                    .filter(
                        durable_schedule_state::definition_fingerprint.eq(&metadata.fingerprint),
                    )
                    .filter(
                        durable_schedule_state::next_local_occurrence
                            .eq(&state.next_local_occurrence),
                    ),
            )
            .set((
                durable_schedule_state::next_local_occurrence.eq(&occurrence.local_occurrence),
                durable_schedule_state::next_occurrence_at.eq(occurrence.due_at),
                durable_schedule_state::last_materialized_at.eq(Some(now)),
                durable_schedule_state::updated_at.eq(now),
            ))
            .execute(connection)
            .await?;
            ensure_single_change(changed)?;
            Ok(report)
        })
        .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OccurrenceOutcome {
    Start,
    Skip(&'static str),
    Coalesce,
}

fn plan_due_chunk(
    calendar: &ScheduleCalendar,
    mut occurrence: ScheduleOccurrence,
    policy: MisfirePolicy,
    grace_millis: i64,
    now: i64,
) -> Result<
    (
        Vec<ScheduleOccurrence>,
        ScheduleOccurrence,
        Vec<OccurrenceOutcome>,
    ),
    DurableError,
> {
    let mut due = Vec::new();
    while occurrence.due_at <= now && due.len() < MAX_DUE_OCCURRENCES_PER_TICK {
        due.push(occurrence.clone());
        occurrence = calendar.next_after_local(occurrence.local_datetime)?;
    }
    let budget = match policy {
        MisfirePolicy::Skip => 0,
        MisfirePolicy::RunLatest => 1,
        MisfirePolicy::CatchUp { max_occurrences } => max_occurrences,
    };
    let mut following_runnable = 0;
    if occurrence.due_at <= now {
        if let Some(last) = due.last() {
            let mut after = last.local_datetime;
            // Later runnable slots determine which entries in this chunk are
            // outside the global latest-N budget. They remain pending for the next tick.
            while following_runnable < budget {
                let Some(next) = calendar.next_runnable_through(after, now)? else {
                    break;
                };
                following_runnable += 1;
                after = next.local_datetime;
            }
        }
    }
    let outcomes = classify(&due, policy, grace_millis, now, following_runnable);
    Ok((due, occurrence, outcomes))
}

fn classify(
    due: &[ScheduleOccurrence],
    policy: MisfirePolicy,
    grace_millis: i64,
    now: i64,
    following_runnable: u32,
) -> Vec<OccurrenceOutcome> {
    let mut outcomes = vec![OccurrenceOutcome::Skip("dst_gap"); due.len()];
    let runnable = due
        .iter()
        .enumerate()
        .filter_map(|(index, occurrence)| {
            (occurrence.disposition != LocalTimeDisposition::Gap).then_some(index)
        })
        .collect::<Vec<_>>();
    match policy {
        MisfirePolicy::Skip => {
            for index in runnable {
                outcomes[index] = if due[index].due_at.saturating_add(grace_millis) < now {
                    OccurrenceOutcome::Skip("misfire_skip")
                } else {
                    OccurrenceOutcome::Start
                };
            }
        }
        MisfirePolicy::RunLatest => {
            if let Some((&latest, earlier)) = runnable.split_last() {
                outcomes[latest] = if following_runnable == 0 {
                    OccurrenceOutcome::Start
                } else {
                    OccurrenceOutcome::Coalesce
                };
                for index in earlier {
                    outcomes[*index] = OccurrenceOutcome::Coalesce;
                }
            }
        }
        MisfirePolicy::CatchUp { max_occurrences } => {
            let start_count = usize::try_from(max_occurrences.saturating_sub(following_runnable))
                .unwrap_or(usize::MAX)
                .min(runnable.len());
            let split = runnable.len().saturating_sub(start_count);
            for index in &runnable[..split] {
                outcomes[*index] = OccurrenceOutcome::Skip("catch_up_limit");
            }
            for index in &runnable[split..] {
                outcomes[*index] = OccurrenceOutcome::Start;
            }
        }
    }
    outcomes
}

struct MaterializationTarget<'a, C> {
    registry: &'a ScheduleRegistry<C>,
    context: &'a C,
    schedule_key: &'a str,
    overlap: OverlapPolicy,
    active: &'a mut i64,
    queued: &'a mut bool,
    now: i64,
}

async fn materialize_occurrence<C>(
    connection: &mut crate::DurableConnection,
    target: MaterializationTarget<'_, C>,
    occurrence: ScheduleOccurrence,
    outcome: OccurrenceOutcome,
    report: &mut ScheduleMaterializationReport,
) -> Result<(), DurableError>
where
    C: Send + Sync + 'static,
{
    let outcome = match (outcome, target.overlap) {
        (OccurrenceOutcome::Start, OverlapPolicy::SkipIfActive) if *target.active > 0 => {
            OccurrenceOutcome::Skip("overlap_active")
        }
        (OccurrenceOutcome::Start, OverlapPolicy::QueueOne)
            if *target.active > 0 && *target.queued =>
        {
            OccurrenceOutcome::Skip("overlap_queue_full")
        }
        (OccurrenceOutcome::Start, OverlapPolicy::QueueOne) if *target.active > 0 => {
            diesel::insert_into(durable_schedule_run::table)
                .values(NewScheduleRunRow {
                    schedule_key: target.schedule_key.to_string(),
                    local_occurrence: occurrence.local_occurrence,
                    scheduled_for: occurrence.scheduled_for,
                    materialized_at: target.now,
                    status: "queued".to_string(),
                    reason: Some("overlap_queue_one".to_string()),
                    actor_id: None,
                    workflow_id: None,
                    created_at: target.now,
                })
                .execute(connection)
                .await?;
            *target.queued = true;
            report.queued = report.queued.saturating_add(1);
            return Ok(());
        }
        (outcome, _) => outcome,
    };
    let (status, reason) = match outcome {
        OccurrenceOutcome::Start => ("materializing", None),
        OccurrenceOutcome::Skip(reason) => ("skipped", Some(reason.to_string())),
        OccurrenceOutcome::Coalesce => ("coalesced", Some("run_latest".to_string())),
    };
    let run_id = crate::dialect::insert_schedule_run(
        connection,
        NewScheduleRunRow {
            schedule_key: target.schedule_key.to_string(),
            local_occurrence: occurrence.local_occurrence,
            scheduled_for: occurrence.scheduled_for,
            materialized_at: target.now,
            status: status.to_string(),
            reason,
            actor_id: None,
            workflow_id: None,
            created_at: target.now,
        },
    )
    .await?;
    match outcome {
        OccurrenceOutcome::Start => {
            let schedule_run_id = ScheduleRunId::new(run_id)?;
            let workflow_id = target
                .registry
                .start_occurrence(
                    target.schedule_key,
                    target.context,
                    connection,
                    schedule_run_id,
                    occurrence.scheduled_for,
                )
                .await?;
            let changed = diesel::update(
                durable_schedule_run::table
                    .find(schedule_run_id.get())
                    .filter(durable_schedule_run::status.eq("materializing")),
            )
            .set((
                durable_schedule_run::status.eq("started"),
                durable_schedule_run::workflow_id.eq(Some(workflow_id.get())),
            ))
            .execute(connection)
            .await?;
            ensure_single_change(changed)?;
            *target.active = target.active.saturating_add(1);
            report.started = report.started.saturating_add(1);
        }
        OccurrenceOutcome::Skip(_) => report.skipped = report.skipped.saturating_add(1),
        OccurrenceOutcome::Coalesce => report.coalesced = report.coalesced.saturating_add(1),
    }
    Ok(())
}

async fn active_workflow_count(
    connection: &mut crate::DurableConnection,
    schedule_key: &str,
) -> Result<i64, DurableError> {
    Ok(durable_workflow::table
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
        .count()
        .get_result::<i64>(connection)
        .await?)
}

async fn queued_run_exists(
    connection: &mut crate::DurableConnection,
    schedule_key: &str,
) -> Result<bool, DurableError> {
    Ok(durable_schedule_run::table
        .filter(durable_schedule_run::schedule_key.eq(schedule_key))
        .filter(durable_schedule_run::status.eq("queued"))
        .count()
        .get_result::<i64>(connection)
        .await?
        > 0)
}

async fn promote_queued<C>(
    connection: &mut crate::DurableConnection,
    registry: &ScheduleRegistry<C>,
    context: &C,
    schedule_key: &str,
) -> Result<(), DurableError>
where
    C: Send + Sync + 'static,
{
    let run = durable_schedule_run::table
        .filter(durable_schedule_run::schedule_key.eq(schedule_key))
        .filter(durable_schedule_run::status.eq("queued"))
        .order((
            durable_schedule_run::scheduled_for.asc(),
            durable_schedule_run::id.asc(),
        ))
        .for_update()
        .select(ScheduleRunRow::as_select())
        .first::<ScheduleRunRow>(connection)
        .await?;
    let schedule_run_id = ScheduleRunId::new(run.id)?;
    let workflow_id = registry
        .start_occurrence(
            schedule_key,
            context,
            connection,
            schedule_run_id,
            run.scheduled_for,
        )
        .await?;
    let changed = diesel::update(
        durable_schedule_run::table
            .find(run.id)
            .filter(durable_schedule_run::status.eq("queued")),
    )
    .set((
        durable_schedule_run::status.eq("started"),
        durable_schedule_run::workflow_id.eq(Some(workflow_id.get())),
        durable_schedule_run::reason.eq(Some("queue_one_promoted".to_string())),
    ))
    .execute(connection)
    .await?;
    ensure_single_change(changed)
}

fn ensure_single_change(changed: usize) -> Result<(), DurableError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(DurableError::FencedWrite)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn assert_chunked_matches_full(timezone: &str, start: chrono::DateTime<Utc>, now: i64) {
        let calendar = ScheduleCalendar::new("0 * * * * *", timezone).expect("calendar");
        let first = calendar.next_after(start).expect("first occurrence");
        let mut next = first.clone();
        let mut all = Vec::new();
        while next.due_at <= now {
            all.push(next.clone());
            next = calendar
                .next_after_local(next.local_datetime)
                .expect("next");
        }
        assert!(all.len() > MAX_DUE_OCCURRENCES_PER_TICK);
        for policy in [
            MisfirePolicy::Skip,
            MisfirePolicy::RunLatest,
            MisfirePolicy::CatchUp { max_occurrences: 2 },
            MisfirePolicy::CatchUp {
                max_occurrences: 100,
            },
        ] {
            let expected = classify(&all, policy, 60_000, now, 0);
            let mut actual = Vec::new();
            let mut occurrence = first.clone();
            let mut ticks = 0;
            while occurrence.due_at <= now {
                let (due, next, outcomes) =
                    plan_due_chunk(&calendar, occurrence.clone(), policy, 60_000, now)
                        .expect("recoverable chunk");
                assert!(!due.is_empty());
                assert!(due.len() <= MAX_DUE_OCCURRENCES_PER_TICK);
                assert!(next.local_datetime > occurrence.local_datetime);
                actual.extend(due.into_iter().zip(outcomes));
                occurrence = next;
                ticks += 1;
                assert!(ticks <= 4, "recovery must make bounded progress");
            }
            assert_eq!(
                actual,
                all.iter().cloned().zip(expected).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn over_bound_backlog_recovers_without_repeating_the_latest_n_budget() {
        let start = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("start");
        assert_chunked_matches_full("UTC", start, start.timestamp_millis() + 20_001 * 60_000);
    }

    #[test]
    fn chunk_boundary_preserves_dst_gap_and_fold_policies() {
        for (month, day, hour) in [(3, 8, 9), (11, 1, 7)] {
            let boundary = Utc
                .with_ymd_and_hms(2026, month, day, hour, 0, 0)
                .single()
                .expect("boundary");
            let start = boundary - chrono::Duration::minutes(10_000);
            assert_chunked_matches_full(
                "America/Denver",
                start,
                (boundary + chrono::Duration::hours(3)).timestamp_millis(),
            );
        }
    }
}
