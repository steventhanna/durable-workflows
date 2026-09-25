use diesel::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;

use crate::{
    error::ensure_size,
    persistence::{self, ActivityStatus, NewProgressEventRow},
    schema::{durable_activity, durable_progress_event},
    ActivityId, DurableError, DurablePool, MAX_EVENT_METADATA_BYTES,
};

pub const MAX_PROGRESS_EVENTS_PER_ATTEMPT: i32 = 100;
pub const MAX_PROGRESS_DESCRIPTION_BYTES: usize = 2_048;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressSeverity {
    Info,
    Warning,
    Error,
}

impl ProgressSeverity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressEvent {
    pub code: String,
    pub description: String,
    pub completed_units: Option<i64>,
    pub total_units: Option<i64>,
    pub severity: ProgressSeverity,
    pub metadata_json: Option<String>,
}

impl ProgressEvent {
    pub fn new(code: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            description: description.into(),
            completed_units: None,
            total_units: None,
            severity: ProgressSeverity::Info,
            metadata_json: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressReportOutcome {
    Persisted { sequence: u32 },
    LimitReached,
}

#[derive(Debug, Clone)]
pub struct ProgressReporter {
    pool: DurablePool,
    activity_id: ActivityId,
    attempt_number: i32,
    lease_token: String,
}

impl ProgressReporter {
    pub(crate) fn new(
        pool: DurablePool,
        activity_id: ActivityId,
        attempt_number: i32,
        lease_token: String,
    ) -> Self {
        Self {
            pool,
            activity_id,
            attempt_number,
            lease_token,
        }
    }

    pub async fn report(
        &self,
        event: ProgressEvent,
    ) -> Result<ProgressReportOutcome, DurableError> {
        validate_event(&event)?;
        let activity_id = self.activity_id.get();
        let attempt_number = self.attempt_number;
        let lease_token = self.lease_token.clone();
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            let fenced = durable_activity::table
                .find(activity_id)
                .filter(durable_activity::status.eq(ActivityStatus::Running))
                .filter(durable_activity::attempt_count.eq(attempt_number))
                .filter(durable_activity::lease_token.eq(&lease_token))
                .for_update()
                .select(durable_activity::id)
                .first::<i64>(connection)
                .await
                .optional()?;
            if fenced.is_none() {
                return Err(DurableError::FencedWrite);
            }
            // T-W4 writes only durable_progress_event, which the model does not cover.
            crate::trace::declare_unmodeled("progress", false);

            let last = durable_progress_event::table
                .filter(durable_progress_event::activity_id.eq(activity_id))
                .filter(durable_progress_event::attempt_number.eq(attempt_number))
                .select(diesel::dsl::max(durable_progress_event::sequence))
                .get_result::<Option<i32>>(connection)
                .await?
                .unwrap_or(0);
            if last >= MAX_PROGRESS_EVENTS_PER_ATTEMPT {
                return Ok(ProgressReportOutcome::LimitReached);
            }
            let sequence = last.checked_add(1).ok_or_else(|| {
                DurableError::InvalidState("progress sequence overflow".to_string())
            })?;
            diesel::insert_into(durable_progress_event::table)
                .values(NewProgressEventRow {
                    activity_id,
                    attempt_number,
                    sequence,
                    code: event.code,
                    description_bytes: i32::try_from(event.description.len()).map_err(|_| {
                        DurableError::InvalidState(
                            "progress description length overflow".to_string(),
                        )
                    })?,
                    description: event.description,
                    completed_units: event.completed_units,
                    total_units: event.total_units,
                    severity: event.severity.as_str().to_string(),
                    metadata_json: event.metadata_json,
                    created_at: persistence::database_now_millis(connection).await?,
                })
                .execute(connection)
                .await?;
            Ok(ProgressReportOutcome::Persisted {
                sequence: u32::try_from(sequence).map_err(|_| {
                    DurableError::InvalidState("negative progress sequence".to_string())
                })?,
            })
        })
        .await
    }
}

fn validate_event(event: &ProgressEvent) -> Result<(), DurableError> {
    if event.code.is_empty() || event.code.len() > 64 || !event.code.is_ascii() {
        return Err(DurableError::InvalidDefinition(
            "progress code must be 1-64 ASCII bytes".to_string(),
        ));
    }
    ensure_size(
        "progress description",
        &event.description,
        MAX_PROGRESS_DESCRIPTION_BYTES,
    )?;
    if let Some(metadata) = event.metadata_json.as_deref() {
        ensure_size("progress metadata", metadata, MAX_EVENT_METADATA_BYTES)?;
        let _: serde_json::Value = serde_json::from_str(metadata)?;
    }
    let has_negative_units = event.completed_units.is_some_and(|units| units < 0)
        || event.total_units.is_some_and(|units| units < 0);
    let completed_exceeds_total = matches!(
        (event.completed_units, event.total_units),
        (Some(completed), Some(total)) if completed > total
    );
    if has_negative_units || completed_exceeds_total {
        return Err(DurableError::InvalidDefinition(
            "progress units must satisfy 0 <= completed <= total".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_progress_units_must_be_non_negative() {
        for (completed_units, total_units) in [(Some(-1), None), (None, Some(-1))] {
            let mut event = ProgressEvent::new("working", "Still working");
            event.completed_units = completed_units;
            event.total_units = total_units;

            let error = validate_event(&event).expect_err("negative units must be rejected");
            assert!(error.to_string().contains("0 <= completed <= total"));
        }
    }
}
