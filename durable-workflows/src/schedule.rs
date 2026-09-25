use std::{collections::HashMap, marker::PhantomData, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, LocalResult, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    persistence::{NewScheduleStateRow, ScheduleStateRow},
    schema::durable_schedule_state,
    DurableConnection, DurableError, DurablePool, ScheduleRunId, WorkflowId,
};

const MAX_SCHEDULE_KEY_BYTES: usize = 191;
const MAX_GAP_SCAN_SECONDS: i64 = 86_400;
const MAX_CATCH_UP_OCCURRENCES: u32 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum MisfirePolicy {
    Skip,
    RunLatest,
    CatchUp { max_occurrences: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum OverlapPolicy {
    Allow,
    SkipIfActive,
    QueueOne,
}

pub trait DurableSchedule: Send + Sync + 'static {
    const KEY: &'static str;
    const VERSION: i32;
    const CRON: &'static str;
    const TIMEZONE: &'static str;
    const MISFIRE: MisfirePolicy;
    const OVERLAP: OverlapPolicy;
    const MISFIRE_GRACE: Duration;
}

/// Starts the workflow for one schedule occurrence.
///
/// `start_occurrence` runs inside the materializer's transaction on the
/// library's connection. It must start exactly one workflow on `connection`
/// with `schedule_run_id` set, and must not commit it separately.
///
/// Return every error, and do not run more statements after one fails:
/// Postgres aborts the whole transaction on any failed statement. To recover
/// from a statement that may fail, run that step in `connection.transaction(..)`
/// (a savepoint). Any error rolls back the whole materializer tick on both
/// backends.
#[async_trait]
pub trait ScheduleHandler: DurableSchedule {
    type Context: Send + Sync + 'static;

    async fn start_occurrence(
        context: &Self::Context,
        connection: &mut DurableConnection,
        schedule_run_id: ScheduleRunId,
        scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalTimeDisposition {
    Exact,
    Gap,
    AmbiguousEarlier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleOccurrence {
    pub local_datetime: NaiveDateTime,
    pub local_occurrence: String,
    pub scheduled_for: i64,
    pub due_at: i64,
    pub disposition: LocalTimeDisposition,
}

#[derive(Clone)]
pub struct ScheduleCalendar {
    schedule: Schedule,
    timezone: Tz,
}

impl ScheduleCalendar {
    pub fn new(cron: &str, timezone: &str) -> Result<Self, DurableError> {
        let schedule = Schedule::from_str(cron).map_err(|error| {
            DurableError::InvalidDefinition(format!("invalid schedule cron `{cron}`: {error}"))
        })?;
        let timezone = Tz::from_str(timezone).map_err(|_| {
            DurableError::InvalidDefinition(format!("invalid schedule IANA timezone `{timezone}`"))
        })?;
        Ok(Self { schedule, timezone })
    }

    pub fn next_after(&self, after: DateTime<Utc>) -> Result<ScheduleOccurrence, DurableError> {
        self.next_after_local(after.with_timezone(&self.timezone).naive_local())
    }

    pub fn next_after_local(
        &self,
        after: NaiveDateTime,
    ) -> Result<ScheduleOccurrence, DurableError> {
        let wall_clock = DateTime::<Utc>::from_naive_utc_and_offset(after, Utc);
        let candidate = self
            .schedule
            .after(&wall_clock)
            .next()
            .ok_or_else(|| {
                DurableError::InvalidDefinition("schedule has no next occurrence".into())
            })?
            .naive_utc();
        self.resolve(candidate)
    }

    pub(crate) fn next_runnable_through(
        &self,
        after: NaiveDateTime,
        through: i64,
    ) -> Result<Option<ScheduleOccurrence>, DurableError> {
        let mut next = self.next_after_local(after)?;
        while next.due_at <= through {
            if next.disposition != LocalTimeDisposition::Gap {
                return Ok(Some(next));
            }
            let gap_end = DateTime::from_timestamp_millis(next.due_at)
                .ok_or_else(|| DurableError::InvalidState("invalid gap timestamp".into()))?
                .with_timezone(&self.timezone)
                .naive_local();
            let before_gap_end = gap_end
                .checked_sub_signed(chrono::Duration::seconds(1))
                .ok_or_else(|| DurableError::InvalidState("gap timestamp underflow".into()))?;
            next = self.next_after_local(before_gap_end)?;
        }
        Ok(None)
    }

    pub fn occurrence_at_local(
        &self,
        local_datetime: NaiveDateTime,
    ) -> Result<ScheduleOccurrence, DurableError> {
        self.resolve(local_datetime)
    }

    fn resolve(&self, local_datetime: NaiveDateTime) -> Result<ScheduleOccurrence, DurableError> {
        let local_occurrence = local_datetime.format("%Y-%m-%dT%H:%M:%S").to_string();
        match self.timezone.from_local_datetime(&local_datetime) {
            LocalResult::Single(instant) => Ok(occurrence(
                local_datetime,
                local_occurrence,
                instant.with_timezone(&Utc).timestamp_millis(),
                LocalTimeDisposition::Exact,
            )),
            LocalResult::Ambiguous(first, second) => {
                let scheduled_for = first
                    .with_timezone(&Utc)
                    .min(second.with_timezone(&Utc))
                    .timestamp_millis();
                Ok(occurrence(
                    local_datetime,
                    local_occurrence,
                    scheduled_for,
                    LocalTimeDisposition::AmbiguousEarlier,
                ))
            }
            LocalResult::None => {
                for offset in 1..=MAX_GAP_SCAN_SECONDS {
                    let Some(probe) =
                        local_datetime.checked_add_signed(chrono::Duration::seconds(offset))
                    else {
                        break;
                    };
                    if let LocalResult::Single(instant) = self.timezone.from_local_datetime(&probe)
                    {
                        return Ok(occurrence(
                            local_datetime,
                            local_occurrence,
                            instant.with_timezone(&Utc).timestamp_millis(),
                            LocalTimeDisposition::Gap,
                        ));
                    }
                }
                Err(DurableError::InvalidDefinition(format!(
                    "timezone {} has a local-time gap longer than the supported bound",
                    self.timezone
                )))
            }
        }
    }
}

fn occurrence(
    local_datetime: NaiveDateTime,
    local_occurrence: String,
    scheduled_for: i64,
    disposition: LocalTimeDisposition,
) -> ScheduleOccurrence {
    ScheduleOccurrence {
        local_datetime,
        local_occurrence,
        scheduled_for,
        due_at: scheduled_for,
        disposition,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleDefinitionMetadata {
    pub key: String,
    pub version: i32,
    pub fingerprint: String,
    pub cron: String,
    pub timezone: String,
    pub misfire: MisfirePolicy,
    pub overlap: OverlapPolicy,
    pub misfire_grace_millis: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleStateReconcileOutcome {
    Inserted,
    Preserved,
    Upgraded,
    NewerPersisted,
}

#[async_trait]
trait ErasedScheduleHandler<C>: Send + Sync {
    async fn start_occurrence(
        &self,
        context: &C,
        connection: &mut DurableConnection,
        schedule_run_id: ScheduleRunId,
        scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError>;
}

struct ScheduleAdapter<S>(PhantomData<S>);

#[async_trait]
impl<C, S> ErasedScheduleHandler<C> for ScheduleAdapter<S>
where
    C: Send + Sync + 'static,
    S: ScheduleHandler<Context = C>,
{
    async fn start_occurrence(
        &self,
        context: &C,
        connection: &mut DurableConnection,
        schedule_run_id: ScheduleRunId,
        scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError> {
        S::start_occurrence(context, connection, schedule_run_id, scheduled_for).await
    }
}

struct RegisteredSchedule<C> {
    metadata: ScheduleDefinitionMetadata,
    calendar: ScheduleCalendar,
    handler: Arc<dyn ErasedScheduleHandler<C>>,
}

pub struct ScheduleRegistry<C> {
    definitions: HashMap<String, RegisteredSchedule<C>>,
}

impl<C> Default for ScheduleRegistry<C> {
    fn default() -> Self {
        Self {
            definitions: HashMap::new(),
        }
    }
}

impl<C> ScheduleRegistry<C>
where
    C: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<S>(&mut self) -> Result<(), DurableError>
    where
        S: ScheduleHandler<Context = C>,
    {
        validate_definition::<S>()?;
        if self.definitions.contains_key(S::KEY) {
            return Err(DurableError::DuplicateDefinition {
                kind: S::KEY.to_string(),
                version: S::VERSION,
            });
        }
        let calendar = ScheduleCalendar::new(S::CRON, S::TIMEZONE)?;
        let metadata = metadata::<S>()?;
        self.definitions.insert(
            S::KEY.to_string(),
            RegisteredSchedule {
                metadata,
                calendar,
                handler: Arc::new(ScheduleAdapter::<S>(PhantomData)),
            },
        );
        Ok(())
    }

    pub fn definitions(&self) -> Vec<ScheduleDefinitionMetadata> {
        let mut definitions: Vec<_> = self
            .definitions
            .values()
            .map(|entry| entry.metadata.clone())
            .collect();
        definitions.sort_by(|left, right| left.key.cmp(&right.key));
        definitions
    }

    pub fn get(&self, key: &str) -> Option<&ScheduleDefinitionMetadata> {
        self.definitions.get(key).map(|entry| &entry.metadata)
    }

    pub fn calendar(&self, key: &str) -> Option<&ScheduleCalendar> {
        self.definitions.get(key).map(|entry| &entry.calendar)
    }

    pub async fn start_occurrence(
        &self,
        key: &str,
        context: &C,
        connection: &mut DurableConnection,
        schedule_run_id: ScheduleRunId,
        scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError> {
        let definition = self
            .definitions
            .get(key)
            .ok_or_else(|| DurableError::NotFound {
                resource: "schedule definition",
                identifier: key.to_string(),
            })?;
        definition
            .handler
            .start_occurrence(context, connection, schedule_run_id, scheduled_for)
            .await
    }

    pub async fn reconcile_state(
        &self,
        key: &str,
        pool: &DurablePool,
        deployed_at: i64,
    ) -> Result<ScheduleStateReconcileOutcome, DurableError> {
        let definition = self
            .definitions
            .get(key)
            .ok_or_else(|| DurableError::NotFound {
                resource: "schedule definition",
                identifier: key.to_string(),
            })?;
        let deployed_at = DateTime::<Utc>::from_timestamp_millis(deployed_at).ok_or_else(|| {
            DurableError::InvalidDefinition("schedule deployment timestamp is invalid".to_string())
        })?;
        let next = definition.calendar.next_after(deployed_at)?;
        let metadata = definition.metadata.clone();
        let now = deployed_at.timestamp_millis();
        let mut connection = pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            crate::trace::declare_unmodeled("schedule_state", false);
            let inserted = crate::dialect::insert_schedule_state_if_absent(
                connection,
                NewScheduleStateRow {
                    schedule_key: metadata.key.clone(),
                    definition_fingerprint: metadata.fingerprint.clone(),
                    definition_version: metadata.version,
                    next_local_occurrence: next.local_occurrence.clone(),
                    next_occurrence_at: next.due_at,
                    last_materialized_at: None,
                    paused_at: None,
                    paused_by: None,
                    pause_reason: None,
                    created_at: now,
                    updated_at: now,
                },
            )
            .await?;
            let row = durable_schedule_state::table
                .find(&metadata.key)
                .for_update()
                .select(ScheduleStateRow::as_select())
                .first::<ScheduleStateRow>(connection)
                .await?;
            if inserted {
                return Ok(ScheduleStateReconcileOutcome::Inserted);
            }
            if row.definition_version > metadata.version {
                return Ok(ScheduleStateReconcileOutcome::NewerPersisted);
            }
            if row.definition_version == metadata.version {
                if row.definition_fingerprint == metadata.fingerprint {
                    return Ok(ScheduleStateReconcileOutcome::Preserved);
                }
                return Err(DurableError::Conflict(format!(
                    "schedule {} v{} metadata changed without a version bump",
                    metadata.key, metadata.version
                )));
            }
            diesel::update(durable_schedule_state::table.find(&metadata.key))
                .set((
                    durable_schedule_state::definition_fingerprint.eq(&metadata.fingerprint),
                    durable_schedule_state::definition_version.eq(metadata.version),
                    durable_schedule_state::next_local_occurrence.eq(&next.local_occurrence),
                    durable_schedule_state::next_occurrence_at.eq(next.due_at),
                    durable_schedule_state::updated_at.eq(now),
                ))
                .execute(connection)
                .await?;
            Ok(ScheduleStateReconcileOutcome::Upgraded)
        })
        .await
    }
}

fn validate_definition<S: DurableSchedule>() -> Result<(), DurableError> {
    let key = S::KEY;
    let valid_key = !key.is_empty()
        && key.len() <= MAX_SCHEDULE_KEY_BYTES
        && !key.starts_with('_')
        && !key.ends_with('_')
        && !key.contains("__")
        && key
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    let valid_catch_up = match S::MISFIRE {
        MisfirePolicy::CatchUp { max_occurrences } => {
            (1..=MAX_CATCH_UP_OCCURRENCES).contains(&max_occurrences)
        }
        MisfirePolicy::Skip | MisfirePolicy::RunLatest => true,
    };
    if !valid_key || S::VERSION <= 0 || S::MISFIRE_GRACE.is_zero() || !valid_catch_up {
        return Err(DurableError::InvalidDefinition(
            "schedule key, version, misfire policy, or grace is invalid".to_string(),
        ));
    }
    Ok(())
}

fn metadata<S: DurableSchedule>() -> Result<ScheduleDefinitionMetadata, DurableError> {
    let misfire_grace_millis = i64::try_from(S::MISFIRE_GRACE.as_millis()).map_err(|_| {
        DurableError::InvalidDefinition("schedule misfire grace exceeds i64 milliseconds".into())
    })?;
    let canonical = serde_json::to_vec(&(
        S::KEY,
        S::VERSION,
        S::CRON,
        S::TIMEZONE,
        S::MISFIRE,
        S::OVERLAP,
        misfire_grace_millis,
    ))?;
    let fingerprint = format!("{:x}", Sha256::digest(canonical));
    Ok(ScheduleDefinitionMetadata {
        key: S::KEY.to_string(),
        version: S::VERSION,
        fingerprint,
        cron: S::CRON.to_string(),
        timezone: S::TIMEZONE.to_string(),
        misfire: S::MISFIRE,
        overlap: S::OVERLAP,
        misfire_grace_millis,
    })
}

#[macro_export]
macro_rules! register_durable_schedules {
    ($context:ty; $($schedule:ty),* $(,)?) => {{
        let mut registry = $crate::ScheduleRegistry::<$context>::new();
        (|| -> Result<_, $crate::DurableError> {
            $(registry.register::<$schedule>()?;)*
            Ok(registry)
        })()
    }};
}
