#[derive(Clone, Copy)]
enum Topics {
    Fax,
}

#[derive(serde::Serialize, serde::Deserialize, durable_workflows::DurableActivity)]
#[activity(
    kind = "invalid_activity",
    version = 1,
    topic = Topics::Fax,
    max_attempts = 3,
    timeout_secs = 30,
    lease_secs = 30,
    backoff = fixed(delay_secs = 5),
)]
struct InvalidActivityPolicy;

fn main() {}
