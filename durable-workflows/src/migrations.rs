//! The durable tables' schema, for the backend this build targets.
//!
//! Apply it in one of three ways:
//!
//! 1. Copy the SQL from `migrations/<backend>/` in this crate into your own
//!    Diesel migration tree.
//! 2. Run [`MIGRATIONS`] with `diesel_migrations::MigrationHarness` on a
//!    connection you manage.
//! 3. Call [`apply`] with a database URL.
//!
//! The migrations are recorded in Diesel's `__diesel_schema_migrations`
//! table, so they combine with an application's own Diesel migrations.

use diesel::Connection;
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};

use crate::{DurableConnection, DurableError};

/// The durable migrations for this build's backend.
#[cfg(feature = "mysql")]
pub const MIGRATIONS: EmbeddedMigrations = diesel_migrations::embed_migrations!("migrations/mysql");
/// The durable migrations for this build's backend.
#[cfg(feature = "postgres")]
pub const MIGRATIONS: EmbeddedMigrations =
    diesel_migrations::embed_migrations!("migrations/postgres");

/// The baseline `up.sql` for this build's backend.
#[cfg(feature = "mysql")]
pub const BASELINE_UP_SQL: &str =
    include_str!("../migrations/mysql/2026-09-24-000000_durable_baseline/up.sql");
/// The baseline `up.sql` for this build's backend.
#[cfg(feature = "postgres")]
pub const BASELINE_UP_SQL: &str =
    include_str!("../migrations/postgres/2026-09-24-000000_durable_baseline/up.sql");

/// Connects to `database_url` and applies every pending durable migration.
///
/// Returns the versions applied by this call, oldest first; an up-to-date
/// database returns an empty list. The migrations run on a blocking thread,
/// so this must be called from inside a Tokio runtime.
pub async fn apply(database_url: &str) -> Result<Vec<String>, DurableError> {
    let database_url = database_url.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut connection = AsyncConnectionWrapper::<DurableConnection>::establish(&database_url)
            .map_err(|error| DurableError::Migration(Box::new(error)))?;
        let versions = connection
            .run_pending_migrations(MIGRATIONS)
            .map_err(DurableError::Migration)?;
        Ok(versions
            .into_iter()
            .map(|version| version.to_string())
            .collect())
    })
    .await
    .map_err(|error| DurableError::Migration(Box::new(error)))?
}
