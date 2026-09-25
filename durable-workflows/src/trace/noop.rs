//! Inert stand-ins for the `trace-model` recorder (`trace/mod.rs`), used
//! when the feature is off. Declarations and notes take closures that are
//! never evaluated here.

use serde_json::Value;

use crate::{dialect::TransactionCallback, DurableConnection, DurablePool};

pub(crate) const ENABLED: bool = false;

#[derive(Debug, Clone)]
pub struct Action;

impl Action {
    #[inline(always)]
    pub fn new(_name: impl Into<String>, _params: Value) -> Self {
        Self
    }
}

#[inline(always)]
pub(crate) async fn scoped<R, E, F>(connection: &mut DurableConnection, callback: F) -> Result<R, E>
where
    for<'r> F: AsyncFnOnce(&'r mut DurableConnection) -> Result<R, E>
        + TransactionCallback<&'r mut DurableConnection, Result<R, E>, Fut: Send>
        + Send,
    E: Send,
    R: Send,
{
    callback(connection).await
}

#[inline(always)]
pub(crate) fn declare(_action: impl FnOnce() -> Action) {}

#[inline(always)]
pub(crate) fn actor(_name: &str) {}

#[inline(always)]
pub(crate) fn touch_wf(_id: i64) {}

#[inline(always)]
pub(crate) fn touch_act(_id: i64) {}

#[inline(always)]
pub(crate) fn touch_att(_activity_id: i64, _attempt_number: i32) {}

#[inline(always)]
pub(crate) fn touch_event(_workflow_id: i64, _delivery_sequence: i32, _event_type: &str) {}

#[inline(always)]
pub(crate) fn note(_key: &str, _value: impl FnOnce() -> Value) {}

#[inline(always)]
pub(crate) fn sample_now(_millis: i64) {}

#[inline(always)]
pub async fn record_local(_pool: &DurablePool, _actor: &str, _action: Action) {}

#[inline(always)]
pub(crate) fn declare_rollback(_action: impl FnOnce() -> Action) {}

#[inline(always)]
pub(crate) async fn capture_rollback<T>(
    future: impl std::future::Future<Output = T>,
) -> (T, Option<Action>) {
    (future.await, None)
}

#[inline(always)]
pub(crate) fn declare_unmodeled(_name: &'static str, _writes_modeled: bool) {}

#[inline(always)]
pub(crate) fn next_heartbeat_id() -> u64 {
    0
}

#[inline(always)]
pub(crate) async fn record_local_on(
    _connection: &mut DurableConnection,
    _actor: &str,
    _action: Action,
) {
}
