#[derive(serde::Serialize, serde::Deserialize, durable_workflows::DurableActivity)]
#[activity(
    kind = "unqualified_topic",
    version = 1,
    topic = Fax,
    max_attempts = 3,
    timeout_secs = 30,
    lease_secs = 60,
    backoff = fixed(delay_secs = 5),
)]
struct UnqualifiedActivityTopic;

fn main() {}
