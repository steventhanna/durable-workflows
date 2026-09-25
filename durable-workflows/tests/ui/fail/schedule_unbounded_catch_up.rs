#[derive(durable_workflows::DurableSchedule)]
#[schedule(
    key = "unbounded_schedule",
    version = 1,
    cron = "0 0 8 * * *",
    timezone = "UTC",
    misfire = CatchUp,
    overlap = Allow,
    misfire_grace_secs = 60,
)]
struct UnboundedSchedule;

fn main() {}
