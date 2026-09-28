// The deduplication key and the engine's restart lineage are one private
// field (an enum), so a start cannot carry both; the key is set only through
// `with_deduplication_key` and read through `deduplication_key()`.
use durable_workflows::StartOptions;

fn main() {
    let mut options = StartOptions::default();
    options.deduplication_key = Some("key".to_string());
}
