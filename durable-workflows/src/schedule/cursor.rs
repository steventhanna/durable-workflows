//! The schedule cursor (S27) and the local-occurrence key it is made of.

use std::{fmt, str::FromStr};

use chrono::{DateTime, NaiveDateTime, SubsecRound, Utc};
use diesel::{ExpressionMethods, QueryDsl, TextExpressionMethods};
use diesel_async::RunQueryDsl;

use super::{ScheduleCalendar, ScheduleOccurrence};
use crate::{schema::durable_schedule_run, DurableConnection, DurableError};

/// The `local_occurrence` prefix of an admin run-now row (T-A9). Those rows
/// are not calendar occurrences and never bound the cursor.
pub(crate) const MANUAL_OCCURRENCE_PREFIX: &str = "manual:";

/// The local wall-clock key of a schedule occurrence, in whole seconds.
///
/// This is the only parser and formatter for the text stored in
/// `durable_schedule_state.next_local_occurrence` and
/// `durable_schedule_run.local_occurrence`, so the order of the values agrees
/// with the order of the persisted keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct LocalOccurrence(NaiveDateTime);

impl LocalOccurrence {
    const FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S";

    /// Drops sub-second precision: the persisted key has none.
    pub(crate) fn new(local: NaiveDateTime) -> Self {
        Self(local.trunc_subsecs(0))
    }

    pub(crate) fn datetime(self) -> NaiveDateTime {
        self.0
    }
}

impl fmt::Display for LocalOccurrence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.format(Self::FORMAT))
    }
}

impl FromStr for LocalOccurrence {
    type Err = chrono::ParseError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        NaiveDateTime::parse_from_str(text, Self::FORMAT).map(Self)
    }
}

/// The largest local occurrence of a schedule that has a run row, if any:
/// the last materialized occurrence. Run-now rows (`manual:{t}`) do not count.
///
/// Only [`MaterializedFloor::load`] makes one, so a floor is always read from
/// the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MaterializedFloor(Option<LocalOccurrence>);

impl MaterializedFloor {
    /// One query. Call it with the schedule state row locked: a tick inserts
    /// run rows only under that lock, so the floor cannot rise before commit.
    pub(crate) async fn load(
        connection: &mut DurableConnection,
        schedule_key: &str,
    ) -> Result<Self, DurableError> {
        // Local keys are fixed width, so the text maximum is the latest key.
        let largest = durable_schedule_run::table
            .filter(durable_schedule_run::schedule_key.eq(schedule_key))
            .filter(
                durable_schedule_run::local_occurrence
                    .not_like(format!("{MANUAL_OCCURRENCE_PREFIX}%")),
            )
            .select(diesel::dsl::max(durable_schedule_run::local_occurrence))
            .get_result::<Option<String>>(connection)
            .await?;
        largest
            .map(|text| {
                text.parse::<LocalOccurrence>().map_err(|error| {
                    DurableError::InvalidState(format!(
                        "schedule {schedule_key} has an invalid run occurrence {text}: {error}"
                    ))
                })
            })
            .transpose()
            .map(Self)
    }
}

/// A schedule's persisted cursor: the next local occurrence to materialize.
///
/// Invariant (S27): the cursor is strictly after the last materialized
/// occurrence, so it never names an occurrence that already has a run row
/// (G5). The field is private to this module and each way to set it keeps the
/// invariant:
/// - [`ScheduleCursor::initial`] starts a new state row, which has no runs;
/// - [`ScheduleCursor::advance_to`] is the materializer tick; it rejects a
///   value that is not strictly later than the cursor, which is itself after
///   every occurrence the tick materialized;
/// - [`ScheduleCursor::upgrade`] is a version upgrade (T-S1 `Upgraded`); it
///   takes the first new-calendar occurrence after `now` that is strictly
///   after the [`MaterializedFloor`]. It may move the cursor back in local
///   time (a new calendar with an earlier slot), but never to or before an
///   occurrence with a run row, also across a timezone change.
#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub(crate) struct ScheduleCursor(LocalOccurrence);

impl ScheduleCursor {
    /// The cursor of a state row that does not exist yet.
    pub(crate) fn initial(first: LocalOccurrence) -> Self {
        Self(first)
    }

    /// The cursor after a version upgrade, and the occurrence it names: the
    /// first occurrence of `calendar` strictly after `now` as an instant and
    /// strictly after `floor` in local time.
    pub(crate) fn upgrade(
        calendar: &ScheduleCalendar,
        now: DateTime<Utc>,
        floor: MaterializedFloor,
    ) -> Result<(Self, ScheduleOccurrence), DurableError> {
        let mut next = calendar.next_after(now)?;
        if let Some(floor) = floor.0 {
            if next.local() <= floor {
                // Later in local time on the same calendar, so not earlier as
                // an instant: still after `now`.
                next = calendar.next_after_local(floor.datetime())?;
            }
        }
        Ok((Self(next.local()), next))
    }

    /// The cursor persisted for `schedule_key`.
    pub(crate) fn load(schedule_key: &str, persisted: &str) -> Result<Self, DurableError> {
        persisted.parse().map(Self).map_err(|error| {
            DurableError::InvalidState(format!(
                "schedule {schedule_key} has invalid persisted local occurrence: {error}"
            ))
        })
    }

    pub(crate) fn local(&self) -> LocalOccurrence {
        self.0
    }

    /// Moves the cursor to `next`, which must be strictly later in local time.
    pub(crate) fn advance_to(self, next: LocalOccurrence) -> Result<Self, DurableError> {
        if next > self.0 {
            Ok(Self(next))
        } else {
            Err(DurableError::InvalidState(format!(
                "schedule cursor must advance: {next} is not after {}",
                self.0
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local(text: &str) -> LocalOccurrence {
        text.parse().expect("local occurrence")
    }

    #[test]
    fn local_occurrence_round_trips_and_orders_like_its_text() {
        let earlier = local("2026-11-01T01:30:00");
        let later = local("2026-11-02T01:30:00");
        assert_eq!(earlier.to_string(), "2026-11-01T01:30:00");
        assert!(earlier < later);
        assert!(earlier.to_string() < later.to_string());
        let with_nanos = earlier.datetime() + chrono::Duration::milliseconds(750);
        assert_eq!(LocalOccurrence::new(with_nanos), earlier);
        assert!("2026-11-01 01:30:00".parse::<LocalOccurrence>().is_err());
    }

    #[test]
    fn cursor_advances_only_to_a_strictly_later_occurrence() {
        let cursor = ScheduleCursor::initial(local("2026-11-02T01:30:00"));
        assert!(matches!(
            ScheduleCursor::initial(cursor.local()).advance_to(cursor.local()),
            Err(DurableError::InvalidState(_))
        ));
        assert!(matches!(
            ScheduleCursor::initial(cursor.local()).advance_to(local("2026-11-01T01:30:00")),
            Err(DurableError::InvalidState(_))
        ));
        let advanced = cursor
            .advance_to(local("2026-11-03T01:30:00"))
            .expect("later occurrence");
        assert_eq!(advanced.local(), local("2026-11-03T01:30:00"));
    }

    fn upgrade(cron: &str, zone: &str, now: DateTime<Utc>, floor: Option<&str>) -> (String, i64) {
        let calendar = ScheduleCalendar::new(cron, zone).expect("calendar");
        let (cursor, occurrence) =
            ScheduleCursor::upgrade(&calendar, now, MaterializedFloor(floor.map(local)))
                .expect("upgrade");
        assert_eq!(cursor.local(), occurrence.local());
        assert!(occurrence.scheduled_for > now.timestamp_millis());
        (cursor.local().to_string(), occurrence.scheduled_for)
    }

    fn utc(month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        use chrono::TimeZone;
        Utc.with_ymd_and_hms(2026, month, day, hour, minute, 0)
            .single()
            .expect("instant")
    }

    #[test]
    fn upgrade_takes_the_first_occurrence_after_now_and_after_the_floor() {
        // Daily 08:00 -> 07:00 at 05:00 MST, yesterday 08:00 materialized:
        // today's 07:00 runs, although the persisted cursor was today 08:00.
        assert_eq!(
            upgrade(
                "0 0 7 * * *",
                "America/Denver",
                utc(1, 11, 12, 0),
                Some("2026-01-10T08:00:00")
            ),
            (
                "2026-01-11T07:00:00".to_string(),
                utc(1, 11, 14, 0).timestamp_millis()
            )
        );
        assert_eq!(
            upgrade("0 0 7 * * *", "America/Denver", utc(1, 11, 12, 0), None).0,
            "2026-01-11T07:00:00"
        );
        // A floor at the next occurrence rejects it: it has a run row.
        assert_eq!(
            upgrade(
                "0 0 7 * * *",
                "America/Denver",
                utc(1, 11, 12, 0),
                Some("2026-01-11T07:00:00")
            )
            .0,
            "2026-01-12T07:00:00"
        );
        // Timezone change west (Asia/Tokyo -> UTC) at 18:30Z: UTC's next key
        // 2026-01-10T19:00 is after now but before Tokyo's materialized
        // 2026-01-11T03:00, so the cursor skips past the floor.
        assert_eq!(
            upgrade(
                "0 0 * * * *",
                "UTC",
                utc(1, 10, 18, 30),
                Some("2026-01-11T03:00:00")
            ),
            (
                "2026-01-11T04:00:00".to_string(),
                utc(1, 11, 4, 0).timestamp_millis()
            )
        );
    }

    #[test]
    fn load_rejects_a_malformed_persisted_cursor() {
        assert!(matches!(
            ScheduleCursor::load("key", "manual:1"),
            Err(DurableError::InvalidState(_))
        ));
        assert_eq!(
            ScheduleCursor::load("key", "2026-01-11T08:00:00")
                .expect("valid cursor")
                .local(),
            local("2026-01-11T08:00:00")
        );
    }
}
