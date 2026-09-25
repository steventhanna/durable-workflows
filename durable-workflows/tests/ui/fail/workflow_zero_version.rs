#[derive(serde::Serialize, serde::Deserialize, durable_workflows::DurableWorkflow)]
#[workflow(kind = "zero_version", version = 0)]
struct ZeroVersion;

fn main() {}
