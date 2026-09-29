#[derive(Clone, Copy)]
enum Topics {
    Fax,
}

#[derive(serde::Serialize, serde::Deserialize, durable_workflows::DurableActivity)]
#[activity(
    kind = "unbounded_retry_activity",
    version = 1,
    topic = Topics::Fax,
    max_attempts = 3,
    timeout_secs = 30,
    lease_secs = 60,
    backoff = fixed(delay_secs = 18446744073709551615),
)]
struct UnboundedRetryActivity;

fn main() {}
