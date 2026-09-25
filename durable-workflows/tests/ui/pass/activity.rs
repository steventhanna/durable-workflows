use durable_workflows::{ActivityTopic as _, BackoffPolicy, DurableActivity as _};

#[derive(Clone, Copy)]
enum Topics {
    Fax,
}

impl durable_workflows::ActivityTopic for Topics {
    fn key(self) -> &'static str {
        match self {
            Self::Fax => "fax",
        }
    }

    fn max_concurrency(self) -> u32 {
        match self {
            Self::Fax => 4,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, durable_workflows::DurableActivity)]
#[activity(
    kind = "submit_fax",
    version = 1,
    topic = Topics::Fax,
    max_attempts = 8,
    timeout_secs = 120,
    lease_secs = 180,
    backoff = exponential(initial_secs = 5, max_secs = 300, jitter_percent = 20),
)]
struct SubmitFax {
    file_id: i32,
}

fn main() {
    assert_eq!(SubmitFax::KIND, "submit_fax");
    assert_eq!(SubmitFax::VERSION, 1);
    assert_eq!(SubmitFax::topic().key(), "fax");
    assert_eq!(SubmitFax::topic().max_concurrency(), 4);
    assert_eq!(SubmitFax::MAX_ATTEMPTS, 8);
    assert_eq!(SubmitFax::TIMEOUT, std::time::Duration::from_secs(120));
    assert_eq!(
        SubmitFax::LEASE_DURATION,
        std::time::Duration::from_secs(180)
    );
    assert_eq!(
        SubmitFax::retry_policy().backoff(),
        BackoffPolicy::Exponential {
            initial_secs: 5,
            max_secs: 300,
            jitter_percent: 20,
        }
    );
}
