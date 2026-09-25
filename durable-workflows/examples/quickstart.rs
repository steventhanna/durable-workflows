//! Minimal end-to-end example: one workflow that runs one activity.
//!
//! Postgres (the default backend):
//!
//! ```sh
//! createdb durable_example
//! for f in durable-workflows/migrations/postgres/*/up.sql; do psql durable_example -f "$f"; done
//! DATABASE_URL=postgres://localhost/durable_example \
//!     cargo run -p durable-workflows --example quickstart
//! ```
//!
//! MySQL:
//!
//! ```sh
//! mysql -e 'CREATE DATABASE durable_example'
//! for f in durable-workflows/migrations/mysql/*/up.sql; do mysql durable_example < "$f"; done
//! DATABASE_URL=mysql://root@127.0.0.1/durable_example \
//!     cargo run -p durable-workflows --example quickstart --no-default-features --features mysql
//! ```

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
use durable_workflows::{
    durable_flow, ActivityContext, ActivityError, ActivityHandler, ActivityRegistry,
    DurableConnection, DurableRuntime, DurableStore, RuntimeConfig, StartOptions, TopicRegistry,
    WfCtx, WfError, WorkflowRegistry,
};
use serde::{Deserialize, Serialize};

/// Shared application state handed to every workflow and activity.
struct App {
    greeting: String,
}

#[derive(Clone, Copy)]
enum Topics {
    Email,
}

impl durable_workflows::ActivityTopic for Topics {
    fn key(self) -> &'static str {
        match self {
            Self::Email => "email",
        }
    }

    fn max_concurrency(self) -> u32 {
        match self {
            Self::Email => 4,
        }
    }
}

/// An activity is a retryable side effect. Its output is journaled, so a
/// replayed workflow never runs it twice after it succeeds.
#[derive(Serialize, Deserialize, durable_workflows::DurableActivity)]
#[activity(
    kind = "send_greeting",
    version = 1,
    topic = Topics::Email,
    max_attempts = 5,
    timeout_secs = 30,
    lease_secs = 60,
    backoff = exponential(initial_secs = 1, max_secs = 60, jitter_percent = 20),
)]
struct SendGreeting {
    name: String,
}

#[async_trait]
impl ActivityHandler for SendGreeting {
    type Context = App;
    type Output = String;

    async fn execute(&self, ctx: ActivityContext<'_, App>) -> Result<String, ActivityError> {
        let message = format!("{}, {}!", ctx.application().greeting, self.name);
        println!("{message}");
        Ok(message)
    }
}

/// A workflow is deterministic code over durable steps. `#[durable_flow]`
/// generates a `GreetFlow { name }` struct that you start and register.
#[durable_flow(kind = "greet", version = 1)]
async fn greet_flow(ctx: &mut WfCtx<'_, App>, name: String) -> Result<String, WfError> {
    let message = ctx.run(&SendGreeting { name }).await?;
    Ok(message)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let pool = Pool::builder()
        .build(AsyncDieselConnectionManager::<DurableConnection>::new(url))
        .await?;

    let mut workflows = WorkflowRegistry::new();
    workflows.register::<GreetFlow>()?;
    let mut activities = ActivityRegistry::new();
    activities.register::<SendGreeting>()?;
    let mut topics = TopicRegistry::new();
    topics.register(Topics::Email)?;

    let runtime = DurableRuntime::new(
        pool.clone(),
        Arc::new(App {
            greeting: "Hello".to_string(),
        }),
        Arc::new(workflows),
        Arc::new(activities),
        Arc::new(topics),
        "quickstart-1",
        RuntimeConfig::default(),
    )?
    .spawn()
    .await?;

    let outcome = DurableStore::new(pool)
        .start(
            &GreetFlow {
                name: "world".to_string(),
            },
            StartOptions::default().with_deduplication_key("greet:world"),
        )
        .await?;
    println!("started workflow {}", outcome.workflow_id);

    tokio::time::sleep(Duration::from_secs(5)).await;
    runtime.shutdown(Duration::from_secs(10)).await?;
    Ok(())
}
