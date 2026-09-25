//! Property tests for `ScheduleCalendar` occurrence computation across DST
//! transitions, plus misfire-policy validation (pure, no database).

use std::{collections::HashSet, sync::OnceLock, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, LocalResult, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use durable_workflows::DurableConnection;
use durable_workflows::{
    DurableError, DurableSchedule, LocalTimeDisposition, MisfirePolicy, OverlapPolicy,
    ScheduleCalendar, ScheduleHandler, ScheduleOccurrence, ScheduleRegistry, ScheduleRunId,
    WorkflowId,
};
use proptest::prelude::*;

const ZONES: &[&str] = &[
    "America/Denver",
    "Europe/London",
    "Australia/Lord_Howe", // 30-minute DST shift
    "Pacific/Apia",        // skipped 2011-12-30 entirely
    "America/Sao_Paulo",   // historical midnight gaps
    "Antarctica/Troll",    // 2-hour DST shift
    "Pacific/Chatham",     // +12:45 / +13:45
    "America/St_Johns",    // -3:30
    "Africa/Casablanca",   // Ramadan DST suspension
    "Asia/Kolkata",
    "UTC",
];

const FIXED_CRONS: &[&str] = &[
    "0 30 2 * * *",
    "0 30 1 * * *",
    "0 0 * * * *",
    "0 */15 * * * *",
    "0 0 0 * * *",
    "0 45 1 * * *",
    "0 0 2 * * Sun",
    "0 */7 * * * *",
    "30 15 1,2,3 * * *",
    "0 0 12 1 * *",
];

fn tz(name: &str) -> Tz {
    name.parse().expect("known zone")
}

fn millis_to_utc(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).expect("valid millis")
}

fn local_of(zone: Tz, millis: i64) -> NaiveDateTime {
    millis_to_utc(millis).with_timezone(&zone).naive_local()
}

/// UTC instants (millis) at which the zone's offset changes, 2005..2035,
/// found by an hourly scan. Cached per zone.
fn transitions(zone_index: usize) -> &'static [i64] {
    static CACHE: OnceLock<Vec<Vec<i64>>> = OnceLock::new();
    &CACHE.get_or_init(|| {
        let start = Utc
            .with_ymd_and_hms(2005, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        let end = Utc
            .with_ymd_and_hms(2035, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        ZONES
            .iter()
            .map(|name| {
                let zone = tz(name);
                let offset_at = |secs: i64| {
                    zone.offset_from_utc_datetime(
                        &DateTime::from_timestamp(secs, 0).unwrap().naive_utc(),
                    )
                    .fix()
                };
                let mut found = Vec::new();
                let mut previous = offset_at(start);
                let mut secs = start + 3_600;
                while secs < end {
                    let current = offset_at(secs);
                    if current != previous {
                        found.push(secs * 1_000);
                        previous = current;
                    }
                    secs += 3_600;
                }
                found
            })
            .collect()
    })[zone_index]
}

fn cron_expression() -> impl Strategy<Value = String> {
    let fixed = (0..FIXED_CRONS.len()).prop_map(|index| FIXED_CRONS[index].to_string());
    let second = prop_oneof![
        Just("0".to_string()),
        Just("30".to_string()),
        Just("*/20".to_string())
    ];
    let minute = prop_oneof![
        Just("*".to_string()),
        (1_u32..=30).prop_map(|step| format!("*/{step}")),
        (0_u32..60).prop_map(|minute| minute.to_string()),
        (0_u32..30, 30_u32..60).prop_map(|(a, b)| format!("{a},{b}")),
    ];
    let hour = prop_oneof![
        Just("*".to_string()),
        (0_u32..24).prop_map(|hour| hour.to_string()),
        (1_u32..=12).prop_map(|step| format!("*/{step}")),
        Just("1-3".to_string()),
    ];
    let weekday = prop_oneof![
        Just("*".to_string()),
        Just("Sun".to_string()),
        Just("Mon-Fri".to_string())
    ];
    let generated = (second, minute, hour, weekday).prop_map(|(second, minute, hour, weekday)| {
        format!("{second} {minute} {hour} * * {weekday}")
    });
    prop_oneof![fixed, generated]
}

/// A UTC instant (millis) near one of the zone's DST transitions, or uniform
/// in 2005..2035.
fn instant_for(zone_index: usize, window_secs: i64) -> impl Strategy<Value = i64> {
    let transitions = transitions(zone_index);
    let uniform = (1_104_537_600_000_i64..2_051_222_400_000).boxed();
    if transitions.is_empty() {
        return uniform;
    }
    let near = (0..transitions.len(), -window_secs..=window_secs)
        .prop_map(move |(index, offset)| transitions[index] + offset * 1_000)
        .boxed();
    prop_oneof![1 => uniform, 3 => near].boxed()
}

fn case(window_secs: i64) -> impl Strategy<Value = (usize, String, i64)> {
    (0..ZONES.len(), cron_expression()).prop_flat_map(move |(zone_index, cron)| {
        (
            Just(zone_index),
            Just(cron),
            instant_for(zone_index, window_secs),
        )
    })
}

/// Independent check of an occurrence against chrono-tz.
fn check_disposition(zone: Tz, occurrence: &ScheduleOccurrence) -> Result<(), TestCaseError> {
    let local = occurrence.local_datetime;
    prop_assert_eq!(
        &occurrence.local_occurrence,
        &local.format("%Y-%m-%dT%H:%M:%S").to_string()
    );
    prop_assert_eq!(occurrence.due_at, occurrence.scheduled_for);
    match zone.from_local_datetime(&local) {
        LocalResult::Single(instant) => {
            prop_assert_eq!(occurrence.disposition, LocalTimeDisposition::Exact);
            prop_assert_eq!(occurrence.scheduled_for, instant.timestamp_millis());
        }
        LocalResult::Ambiguous(first, second) => {
            prop_assert_eq!(
                occurrence.disposition,
                LocalTimeDisposition::AmbiguousEarlier
            );
            prop_assert_eq!(
                occurrence.scheduled_for,
                first.timestamp_millis().min(second.timestamp_millis())
            );
            prop_assert_eq!(local_of(zone, occurrence.scheduled_for), local);
        }
        LocalResult::None => {
            prop_assert_eq!(occurrence.disposition, LocalTimeDisposition::Gap);
            // Gap occurrences resolve to the first valid instant after the gap.
            prop_assert!(local_of(zone, occurrence.scheduled_for) > local);
            prop_assert!(local_of(zone, occurrence.scheduled_for - 1_000) < local);
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// The next occurrence's local wall clock is strictly after `t`'s.
    #[test]
    fn next_after_is_later_in_local_time((zone_index, cron, after) in case(3 * 3_600)) {
        let zone = tz(ZONES[zone_index]);
        let calendar = ScheduleCalendar::new(&cron, ZONES[zone_index]).unwrap();
        let next = calendar.next_after(millis_to_utc(after)).unwrap();
        prop_assert!(next.local_datetime > local_of(zone, after));
        check_disposition(zone, &next)?;
    }

    /// `next_after(t)` is strictly after `t` as an instant.
    #[test]
    #[ignore = "finding (documents G5): in a fall-back repeated hour, next_after resolves to the earlier pass, before t (src/schedule.rs:93-94,151-161)"]
    fn next_after_is_later_as_an_instant((zone_index, cron, after) in case(3 * 3_600)) {
        let calendar = ScheduleCalendar::new(&cron, ZONES[zone_index]).unwrap();
        let next = calendar.next_after(millis_to_utc(after)).unwrap();
        prop_assert!(
            next.scheduled_for > after,
            "after {} ({}), next {:?}",
            millis_to_utc(after),
            ZONES[zone_index],
            next
        );
    }

    /// Walking the calendar by local occurrence: local times strictly
    /// increase, keys are unique, instants never go backwards, and non-gap
    /// instants strictly increase.
    #[test]
    fn walked_occurrences_are_ordered_and_unique((zone_index, cron, after) in case(12 * 3_600)) {
        let zone = tz(ZONES[zone_index]);
        let calendar = ScheduleCalendar::new(&cron, ZONES[zone_index]).unwrap();
        let mut current = calendar.next_after(millis_to_utc(after)).unwrap();
        let mut keys = HashSet::new();
        let mut last_runnable: Option<i64> = None;
        for _ in 0..120 {
            check_disposition(zone, &current)?;
            prop_assert!(keys.insert(current.local_occurrence.clone()), "duplicate {}", current.local_occurrence);
            if current.disposition != LocalTimeDisposition::Gap {
                if let Some(previous) = last_runnable {
                    prop_assert!(current.scheduled_for > previous, "{:?} not after {}", current, previous);
                }
                last_runnable = Some(current.scheduled_for);
            }
            let next = calendar.next_after_local(current.local_datetime).unwrap();
            prop_assert!(next.local_datetime > current.local_datetime);
            prop_assert!(next.scheduled_for >= current.scheduled_for, "{:?} then {:?}", current, next);
            current = next;
        }
    }

    /// `occurrence_at_local` classifies any local time consistently with the
    /// time zone database.
    #[test]
    fn occurrence_at_local_matches_tz_database(
        zone_index in 0..ZONES.len(),
        window in -7_200_i64..=7_200,
        pick in any::<prop::sample::Index>(),
    ) {
        let zone = tz(ZONES[zone_index]);
        let transitions = transitions(zone_index);
        let base = if transitions.is_empty() { 1_700_000_000_000 } else { *pick.get(transitions) };
        // Probe both sides of the transition in local wall-clock terms.
        let local = local_of(zone, base) + chrono::Duration::seconds(window);
        let calendar = ScheduleCalendar::new("0 0 0 * * *", ZONES[zone_index]).unwrap();
        let occurrence = calendar.occurrence_at_local(local).unwrap();
        prop_assert_eq!(occurrence.local_datetime, local);
        check_disposition(zone, &occurrence)?;
    }

    /// No panic for any representable instant; far-future instants return an
    /// error instead of an occurrence.
    #[test]
    fn next_after_never_panics(
        zone_index in 0..ZONES.len(),
        cron in cron_expression(),
        after in prop_oneof![
            DateTime::<Utc>::MIN_UTC.timestamp_millis()..=DateTime::<Utc>::MAX_UTC.timestamp_millis(),
            -2_208_988_800_000_i64..=7_258_118_400_000, // 1900..2200
        ],
    ) {
        let calendar = ScheduleCalendar::new(&cron, ZONES[zone_index]).unwrap();
        let _ = calendar.next_after(millis_to_utc(after));
    }
}

#[test]
fn next_after_instant_minimal_counterexample() {
    // Denver, 2026-11-01 01:20 MST (second pass of the repeated hour).
    let calendar = ScheduleCalendar::new("0 30 1 * * *", "America/Denver").unwrap();
    let after = Utc.with_ymd_and_hms(2026, 11, 1, 8, 20, 0).unwrap();
    let next = calendar.next_after(after).unwrap();
    assert_eq!(next.disposition, LocalTimeDisposition::AmbiguousEarlier);
    // Documents current behavior (G5): the earlier pass, 07:30Z, is before t.
    assert_eq!(
        next.scheduled_for,
        Utc.with_ymd_and_hms(2026, 11, 1, 7, 30, 0)
            .unwrap()
            .timestamp_millis()
    );
    assert!(next.scheduled_for < after.timestamp_millis());
}

#[test]
fn apia_skipped_day_resolves_within_scan_bound() {
    let calendar = ScheduleCalendar::new("0 0 0 * * *", "Pacific/Apia").unwrap();
    let local = NaiveDateTime::parse_from_str("2011-12-30T00:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();
    let occurrence = calendar.occurrence_at_local(local).unwrap();
    assert_eq!(occurrence.disposition, LocalTimeDisposition::Gap);
    let next = calendar.next_after_local(local).unwrap();
    assert_eq!(next.local_occurrence, "2011-12-31T00:00:00");
    assert_eq!(next.disposition, LocalTimeDisposition::Exact);
    assert_eq!(next.scheduled_for, occurrence.scheduled_for);
}

macro_rules! catch_up_schedule {
    ($name:ident, $key:literal, $max:expr) => {
        struct $name;
        impl DurableSchedule for $name {
            const KEY: &'static str = $key;
            const VERSION: i32 = 1;
            const CRON: &'static str = "0 0 * * * *";
            const TIMEZONE: &'static str = "UTC";
            const MISFIRE: MisfirePolicy = MisfirePolicy::CatchUp {
                max_occurrences: $max,
            };
            const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
            const MISFIRE_GRACE: Duration = Duration::from_secs(60);
        }
        #[async_trait]
        impl ScheduleHandler for $name {
            type Context = ();
            async fn start_occurrence(
                _context: &(),
                _connection: &mut DurableConnection,
                _schedule_run_id: ScheduleRunId,
                _scheduled_for: i64,
            ) -> Result<WorkflowId, DurableError> {
                WorkflowId::new(1)
            }
        }
    };
}

catch_up_schedule!(CatchUp0, "catch_up_zero", 0);
catch_up_schedule!(CatchUp1, "catch_up_one", 1);
catch_up_schedule!(CatchUp100, "catch_up_hundred", 100);
catch_up_schedule!(CatchUp101, "catch_up_hundred_one", 101);
catch_up_schedule!(CatchUpMax, "catch_up_max", u32::MAX);

/// `CatchUp { max_occurrences }` registers iff `1..=100`. Consts cannot be
/// generated at runtime, so this samples the boundaries.
#[test]
fn catch_up_bound_is_validated_at_registration() {
    let mut registry = ScheduleRegistry::<()>::new();
    assert!(registry.register::<CatchUp0>().is_err());
    assert!(registry.register::<CatchUp1>().is_ok());
    assert!(registry.register::<CatchUp100>().is_ok());
    assert!(registry.register::<CatchUp101>().is_err());
    assert!(registry.register::<CatchUpMax>().is_err());
}
