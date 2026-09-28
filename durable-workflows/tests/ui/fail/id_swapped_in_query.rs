// Each id column has its own SQL type (`durable_workflows::ids::sql_types`),
// so comparing a workflow id column with an `ActivityId` does not compile
// (E0277): the ids of two tables cannot be swapped in a query.
use diesel::prelude::*;
use durable_workflows::schema::durable_workflow;
use durable_workflows::ActivityId;

fn main() {
    let activity_id = ActivityId::new(1).expect("positive id");
    let _swapped = durable_workflow::id.eq(activity_id);
}
