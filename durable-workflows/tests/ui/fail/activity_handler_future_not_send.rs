// The trait declares `execute`'s future `Send`, so the runtime can run it on
// any worker thread. An `async fn` that holds a non-`Send` value across an
// await does not implement the trait.
use std::rc::Rc;
use std::time::Duration;

use durable_workflows::{
    ActivityContext, ActivityError, ActivityHandler, ActivityTopic, DurableActivity,
    RetryPolicy,
};

#[derive(Clone, Copy)]
struct Topic;

impl ActivityTopic for Topic {
    fn key(self) -> &'static str {
        "topic"
    }

    fn max_concurrency(self) -> u32 {
        1
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct NotSendActivity;

impl DurableActivity for NotSendActivity {
    type Topic = Topic;
    const KIND: &'static str = "not_send";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 1;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const LEASE_DURATION: Duration = Duration::from_secs(10);

    fn topic() -> Self::Topic {
        Topic
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("policy")
    }
}

impl ActivityHandler for NotSendActivity {
    type Context = ();
    type Output = ();

    async fn execute(&self, _context: ActivityContext<'_, ()>) -> Result<(), ActivityError> {
        let held = Rc::new(());
        std::future::ready(()).await;
        drop(held);
        Ok(())
    }
}

fn main() {}
