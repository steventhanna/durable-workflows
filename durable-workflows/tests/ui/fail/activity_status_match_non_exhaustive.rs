// `ActivityStatus` is `#[non_exhaustive]`: the engine may add statuses in a
// minor release, so a match outside the crate needs a wildcard arm. (The
// status is matched inside a tuple so the expected output does not quote the
// crate's source, whose path differs between builds.)
use durable_workflows::persistence::ActivityStatus;

fn label(status: ActivityStatus) -> &'static str {
    match (status,) {
        (ActivityStatus::Pending,) => "pending",
        (ActivityStatus::Running,) => "running",
        (ActivityStatus::Succeeded,) => "succeeded",
        (ActivityStatus::DeadLettered,) => "dead_lettered",
        (ActivityStatus::Cancelled,) => "cancelled",
    }
}

fn main() {
    let _ = label(ActivityStatus::Pending);
}
