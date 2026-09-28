// `CoordinatorConfig` (like `RuntimeConfig`, `WorkerConfig` and
// `HealthScannerConfig`) is `#[non_exhaustive]`: the engine may add fields in
// a minor release, so code outside the crate cannot build it with a struct
// expression, not even with `..Default::default()`. Start from `default()`
// and use the `with_*` setters.
use std::time::Duration;

use durable_workflows::CoordinatorConfig;

fn main() {
    let _ = CoordinatorConfig {
        lease_duration: Duration::from_secs(10),
        ..CoordinatorConfig::default()
    };
}
