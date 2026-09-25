use diesel::{QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;

use crate::{
    persistence::ActivityRow, schema::durable_activity, ActivityId, DurableConnection, DurableError,
};

pub async fn find_activity_by_id(
    connection: &mut DurableConnection,
    activity_id: ActivityId,
) -> Result<ActivityRow, DurableError> {
    Ok(durable_activity::table
        .find(activity_id.get())
        .select(ActivityRow::as_select())
        .first(connection)
        .await?)
}
