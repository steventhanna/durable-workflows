use std::time::Duration;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use durable_workflows::DurableConnection;
use durable_workflows::{
    DurableError, DurableSchedule, LocalTimeDisposition, MisfirePolicy, OverlapPolicy,
    ScheduleCalendar, ScheduleHandler, ScheduleRegistry, ScheduleRunId, WorkflowId,
};

#[derive(DurableSchedule)]
#[schedule(
    key = "daily_denver",
    version = 3,
    cron = "0 30 2 * * *",
    timezone = "America/Denver",
    misfire = RunLatest,
    overlap = QueueOne,
    misfire_grace_secs = 90,
)]
struct DailyDenver;

#[async_trait]
impl ScheduleHandler for DailyDenver {
    type Context = ();

    async fn start_occurrence(
        _context: &Self::Context,
        _connection: &mut DurableConnection,
        _schedule_run_id: ScheduleRunId,
        _scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError> {
        WorkflowId::new(1)
    }
}

#[derive(DurableSchedule)]
#[schedule(
    key = "daily_denver_skip",
    version = 1,
    cron = "0 30 2 * * *",
    timezone = "America/Denver",
    misfire = Skip,
    overlap = SkipIfActive,
    misfire_grace_secs = 60,
)]
struct DailyDenverSkip;

#[async_trait]
impl ScheduleHandler for DailyDenverSkip {
    type Context = ();

    async fn start_occurrence(
        _context: &Self::Context,
        _connection: &mut DurableConnection,
        _schedule_run_id: ScheduleRunId,
        _scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError> {
        WorkflowId::new(1)
    }
}

#[derive(DurableSchedule)]
#[schedule(
    key = "daily_denver_catch_up",
    version = 1,
    cron = "0 30 2 * * *",
    timezone = "America/Denver",
    misfire = CatchUp,
    catch_up_limit = 7,
    overlap = Allow,
    misfire_grace_secs = 120,
)]
struct DailyDenverCatchUp;

#[async_trait]
impl ScheduleHandler for DailyDenverCatchUp {
    type Context = ();

    async fn start_occurrence(
        _context: &Self::Context,
        _connection: &mut DurableConnection,
        _schedule_run_id: ScheduleRunId,
        _scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError> {
        WorkflowId::new(1)
    }
}

#[test]
fn derive_emits_explicit_schedule_metadata() {
    assert_eq!(DailyDenver::KEY, "daily_denver");
    assert_eq!(DailyDenver::VERSION, 3);
    assert_eq!(DailyDenver::CRON, "0 30 2 * * *");
    assert_eq!(DailyDenver::TIMEZONE, "America/Denver");
    assert_eq!(DailyDenver::MISFIRE, MisfirePolicy::RunLatest);
    assert_eq!(DailyDenver::OVERLAP, OverlapPolicy::QueueOne);
    assert_eq!(DailyDenver::MISFIRE_GRACE, Duration::from_secs(90));
    assert_eq!(DailyDenverSkip::MISFIRE, MisfirePolicy::Skip);
    assert_eq!(
        DailyDenverCatchUp::MISFIRE,
        MisfirePolicy::CatchUp { max_occurrences: 7 }
    );
}

#[test]
fn registry_validates_calendar_and_computes_canonical_fingerprint() {
    let mut first = ScheduleRegistry::<()>::new();
    first.register::<DailyDenver>().expect("valid schedule");
    let metadata = first.get("daily_denver").expect("metadata");
    assert_eq!(metadata.fingerprint.len(), 64);
    assert_eq!(metadata.cron, "0 30 2 * * *");
    assert_eq!(metadata.timezone, "America/Denver");
    assert_eq!(metadata.misfire, MisfirePolicy::RunLatest);
    assert_eq!(metadata.overlap, OverlapPolicy::QueueOne);
    assert_eq!(metadata.misfire_grace_millis, 90_000);

    let mut second = ScheduleRegistry::<()>::new();
    second.register::<DailyDenver>().expect("same schedule");
    assert_eq!(
        metadata.fingerprint,
        second
            .get("daily_denver")
            .expect("second metadata")
            .fingerprint
    );
    assert!(matches!(
        first.register::<DailyDenver>(),
        Err(DurableError::DuplicateDefinition { .. })
    ));
}

#[test]
fn invalid_cron_and_timezone_fail_registration() {
    struct InvalidCron;
    impl DurableSchedule for InvalidCron {
        const KEY: &'static str = "invalid_cron";
        const VERSION: i32 = 1;
        const CRON: &'static str = "not a cron";
        const TIMEZONE: &'static str = "America/Denver";
        const MISFIRE: MisfirePolicy = MisfirePolicy::Skip;
        const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
        const MISFIRE_GRACE: Duration = Duration::from_secs(60);
    }
    #[async_trait]
    impl ScheduleHandler for InvalidCron {
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

    struct InvalidTimezone;
    impl DurableSchedule for InvalidTimezone {
        const KEY: &'static str = "invalid_timezone";
        const VERSION: i32 = 1;
        const CRON: &'static str = "0 0 8 * * *";
        const TIMEZONE: &'static str = "Mountain/Imaginary";
        const MISFIRE: MisfirePolicy = MisfirePolicy::Skip;
        const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
        const MISFIRE_GRACE: Duration = Duration::from_secs(60);
    }
    #[async_trait]
    impl ScheduleHandler for InvalidTimezone {
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

    assert!(ScheduleRegistry::<()>::new()
        .register::<InvalidCron>()
        .is_err());
    assert!(ScheduleRegistry::<()>::new()
        .register::<InvalidTimezone>()
        .is_err());
}

#[test]
fn denver_spring_gap_is_a_persistable_skipped_occurrence() {
    let calendar = ScheduleCalendar::new("0 30 2 * * *", "America/Denver").expect("valid calendar");
    let after = Utc
        .with_ymd_and_hms(2026, 3, 7, 10, 0, 0)
        .single()
        .expect("UTC instant");
    let occurrence = calendar.next_after(after).expect("next occurrence");

    assert_eq!(occurrence.local_occurrence, "2026-03-08T02:30:00");
    assert_eq!(occurrence.disposition, LocalTimeDisposition::Gap);
    assert_eq!(
        occurrence.due_at,
        Utc.with_ymd_and_hms(2026, 3, 8, 9, 0, 0)
            .single()
            .expect("gap end")
            .timestamp_millis()
    );
    assert_eq!(occurrence.scheduled_for, occurrence.due_at);
}

#[test]
fn denver_fall_repeat_selects_the_earlier_instant_once() {
    let calendar = ScheduleCalendar::new("0 30 1 * * *", "America/Denver").expect("valid calendar");
    let after = Utc
        .with_ymd_and_hms(2026, 10, 31, 10, 0, 0)
        .single()
        .expect("UTC instant");
    let occurrence = calendar.next_after(after).expect("ambiguous occurrence");
    assert_eq!(occurrence.local_occurrence, "2026-11-01T01:30:00");
    assert_eq!(
        occurrence.disposition,
        LocalTimeDisposition::AmbiguousEarlier
    );
    assert_eq!(
        occurrence.scheduled_for,
        Utc.with_ymd_and_hms(2026, 11, 1, 7, 30, 0)
            .single()
            .expect("earlier repeated instant")
            .timestamp_millis()
    );

    let next = calendar
        .next_after_local(occurrence.local_datetime)
        .expect("next local candidate");
    assert_eq!(next.local_occurrence, "2026-11-02T01:30:00");
}
