// `DbMillis` has no arithmetic operator: a duration is added through a named
// method (`plus`, `checked_plus_millis`, ...), so `now + 1` does not compile
// (E0369).
use durable_workflows::DbMillis;

fn main() {
    let now = DbMillis::from_database_millis(0);
    let _later = now + 1;
}
