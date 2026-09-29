// A function that compares with or writes a timestamp column takes a
// `DbMillis` (epoch millis on the database clock), so a raw `i64`, such as a
// host wall-clock stamp, does not compile (E0308).
use durable_workflows::TimerMaterializer;

async fn wake(timers: &TimerMaterializer) {
    let host_now: i64 = 1_800_000_000_000;
    let _ = timers.wake_one(host_now).await;
}

fn main() {
    let _ = wake;
}
