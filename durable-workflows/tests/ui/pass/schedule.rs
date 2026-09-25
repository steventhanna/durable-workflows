use durable_workflows::{DurableSchedule as _, MisfirePolicy, OverlapPolicy};

#[derive(durable_workflows::DurableSchedule)]
#[schedule(
    key = "daily_summary",
    version = 2,
    cron = "0 0 8 * * *",
    timezone = "America/Denver",
    misfire = CatchUp,
    catch_up_limit = 5,
    overlap = QueueOne,
    misfire_grace_secs = 300,
)]
struct DailySummary;

fn main() {
    assert_eq!(DailySummary::KEY, "daily_summary");
    assert_eq!(DailySummary::VERSION, 2);
    assert_eq!(
        DailySummary::MISFIRE,
        MisfirePolicy::CatchUp { max_occurrences: 5 }
    );
    assert_eq!(DailySummary::OVERLAP, OverlapPolicy::QueueOne);
}
