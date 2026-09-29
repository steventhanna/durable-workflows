use diesel::prelude::*;
use durable_workflows::schema::durable_workflow;

fn main() {
    let _ = durable_workflow::available_at.le(0_i64);
}
