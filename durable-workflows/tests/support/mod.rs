use std::{
    cell::RefCell,
    sync::atomic::{AtomicBool, AtomicU32, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use durable_workflows::{BackendKind, DurableConnection, BACKEND};

const DATABASE_URL_VAR: &str = "DURABLE_WORKFLOWS_TEST_DATABASE_URL";
const SKIP_VAR: &str = "DURABLE_WORKFLOWS_SKIP_DB_TESTS";

const MIGRATIONS: &[(&str, &str)] = &[(
    "durable baseline",
    durable_workflows::migrations::BASELINE_UP_SQL,
)];

static DATABASE_COUNTER: AtomicU32 = AtomicU32::new(0);
static STALE_SWEEP_DONE: AtomicBool = AtomicBool::new(false);

// Concurrent `CREATE DATABASE` statements on Postgres fail when they copy the
// same template, so the process creates databases one at a time.
#[cfg(feature = "postgres")]
static CREATE_DATABASE_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

thread_local! {
    // `#[tokio::test]` drives the test body on the test's own thread, so this
    // holds the database created for the running test.
    static CURRENT_DATABASE_URL: RefCell<Option<String>> = const { RefCell::new(None) };
}

fn server_database_url() -> Option<String> {
    match std::env::var(DATABASE_URL_VAR) {
        Ok(value) if value.trim().is_empty() => panic!("{DATABASE_URL_VAR} must not be empty"),
        Ok(value) => {
            assert_backend_matches(&value);
            Some(value)
        }
        Err(std::env::VarError::NotPresent) if std::env::var_os(SKIP_VAR).is_some() => {
            eprintln!("skipping durable database test: {SKIP_VAR} is set");
            None
        }
        Err(std::env::VarError::NotPresent) => panic!(
            "{DATABASE_URL_VAR} is not set. Point it at a {BACKEND:?} server the tests may \
             create databases on (see CONTRIBUTING.md), or set {SKIP_VAR}=1 to skip database \
             tests."
        ),
        Err(error) => panic!("invalid {DATABASE_URL_VAR}: {error}"),
    }
}

/// The backend is the cargo feature, never the URL; a URL for the other
/// backend is a setup mistake.
fn assert_backend_matches(url: &str) {
    let scheme_matches = match BACKEND {
        BackendKind::Mysql => url.starts_with("mysql://"),
        BackendKind::Postgres => url.starts_with("postgres://") || url.starts_with("postgresql://"),
    };
    assert!(
        scheme_matches,
        "{DATABASE_URL_VAR} must point at a {BACKEND:?} server because the tests were built with \
         the {BACKEND:?} feature; its scheme does not match. Use mysql:// with \
         `--features durable-workflows/mysql` or postgres:// with \
         `--features durable-workflows/postgres`."
    );
}

fn quote_identifier(name: &str) -> String {
    match BACKEND {
        BackendKind::Mysql => format!("`{name}`"),
        BackendKind::Postgres => format!("\"{name}\""),
    }
}

fn drop_database_sql(name: &str) -> String {
    let name = quote_identifier(name);
    match BACKEND {
        BackendKind::Mysql => format!("DROP DATABASE IF EXISTS {name}"),
        // A Postgres database cannot be dropped while sessions (pooled test
        // connections included) are still connected to it.
        BackendKind::Postgres => format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
    }
}

/// Opens a connection to the server URL, not to a per-test database.
#[allow(dead_code)]
pub async fn server_connection() -> Option<DurableConnection> {
    use diesel_async::AsyncConnection;

    let server_url = server_database_url()?;
    Some(
        DurableConnection::establish(&server_url)
            .await
            .unwrap_or_else(|error| panic!("failed to connect to durable test server: {error}")),
    )
}

/// Creates an empty database named `name` on the server `server` is
/// connected to.
#[allow(dead_code)]
pub async fn create_database(server: &mut DurableConnection, name: &str) {
    use diesel_async::SimpleAsyncConnection;

    #[cfg(feature = "mysql")]
    let sql = format!("CREATE DATABASE {}", quote_identifier(name));
    #[cfg(feature = "postgres")]
    let sql = format!(
        "CREATE DATABASE {} TEMPLATE template0",
        quote_identifier(name)
    );
    #[cfg(feature = "postgres")]
    let _guard = CREATE_DATABASE_LOCK.lock().await;
    server
        .batch_execute(&sql)
        .await
        .unwrap_or_else(|error| panic!("failed to create test database {name}: {error}"));
}

/// Drops the database named `name`. `server` must not be connected to it.
#[allow(dead_code)]
pub async fn drop_database(server: &mut DurableConnection, name: &str) {
    use diesel_async::SimpleAsyncConnection;

    server
        .batch_execute(&drop_database_sql(name))
        .await
        .unwrap_or_else(|error| panic!("failed to drop test database {name}: {error}"));
}

/// Freezes the database clock that `durable_workflows` reads at `millis`
/// since the epoch, for this session only.
///
/// MySQL uses `SET TIMESTAMP`. Postgres sets the `durable.fake_now_millis`
/// setting, which the library reads only when built with the `fake-clock`
/// feature; tests that call this need
/// `--features durable-workflows/postgres,durable-workflows/fake-clock`.
#[allow(dead_code)]
pub async fn freeze_database_clock(connection: &mut DurableConnection, millis: i64) {
    use diesel_async::SimpleAsyncConnection;

    assert!(
        BACKEND == BackendKind::Mysql || cfg!(feature = "fake-clock"),
        "freeze_database_clock on Postgres requires the durable-workflows/fake-clock feature"
    );
    #[cfg(feature = "mysql")]
    let sql = format!(
        "SET TIMESTAMP = {}.{:03}",
        millis.div_euclid(1_000),
        millis.rem_euclid(1_000)
    );
    #[cfg(feature = "postgres")]
    let sql = format!("SELECT set_config('durable.fake_now_millis', '{millis}', false)");
    connection
        .batch_execute(&sql)
        .await
        .unwrap_or_else(|error| panic!("failed to freeze the database clock: {error}"));
}

/// URL of the database created for the running test, or the server URL when
/// the test has not created one yet.
#[allow(dead_code)]
pub fn durable_database_url() -> Option<String> {
    CURRENT_DATABASE_URL
        .with(|current| current.borrow().clone())
        .or_else(server_database_url)
}

pub fn with_database_name(url: &str, name: &str) -> String {
    let path_start = url
        .find("://")
        .and_then(|scheme_end| {
            url[scheme_end + 3..]
                .find('/')
                .map(|slash| scheme_end + 3 + slash)
        })
        .unwrap_or(url.len());
    let query_start = url[path_start..]
        .find('?')
        .map_or(url.len(), |offset| path_start + offset);
    format!("{}/{name}{}", &url[..path_start], &url[query_start..])
}

pub fn unique_database_name() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_millis();
    format!(
        "dwt_{}_{}_{}",
        std::process::id(),
        DATABASE_COUNTER.fetch_add(1, Ordering::Relaxed),
        millis
    )
}

/// Most tests never drop their database, so each process removes test
/// databases that earlier runs left behind more than an hour ago.
async fn drop_stale_databases(server: &mut DurableConnection) {
    use diesel::{sql_types::Text, QueryableByName};
    use diesel_async::RunQueryDsl;

    #[derive(QueryableByName)]
    struct Schema {
        #[diesel(sql_type = Text)]
        name: String,
    }

    let cutoff = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_millis()
        .saturating_sub(60 * 60 * 1000);
    let list_sql = match BACKEND {
        BackendKind::Mysql => {
            "SELECT schema_name AS name FROM information_schema.schemata \
             WHERE schema_name LIKE 'dwt\\_%'"
        }
        BackendKind::Postgres => {
            "SELECT datname AS name FROM pg_database WHERE datname LIKE 'dwt\\_%'"
        }
    };
    let schemas: Vec<Schema> = diesel::sql_query(list_sql)
        .load(server)
        .await
        .unwrap_or_else(|error| panic!("failed to list test databases: {error}"));
    for schema in schemas {
        let created_millis = schema
            .name
            .rsplit('_')
            .next()
            .and_then(|millis| millis.parse::<u128>().ok());
        if created_millis.is_some_and(|millis| millis < cutoff) {
            drop_database(server, &schema.name).await;
        }
    }
}

async fn fresh_database() -> Option<String> {
    use diesel_async::{AsyncConnection, SimpleAsyncConnection};

    let server_url = server_database_url()?;
    let name = unique_database_name();
    let mut server = server_connection().await?;
    if !STALE_SWEEP_DONE.swap(true, Ordering::Relaxed) {
        drop_stale_databases(&mut server).await;
    }
    create_database(&mut server, &name).await;
    drop(server);

    let url = with_database_name(&server_url, &name);
    let mut connection = DurableConnection::establish(&url)
        .await
        .unwrap_or_else(|error| panic!("failed to connect to test database {name}: {error}"));
    for (label, sql) in MIGRATIONS {
        connection
            .batch_execute(sql)
            .await
            .unwrap_or_else(|error| panic!("failed to apply {label} migration: {error}"));
    }
    #[cfg(feature = "trace-model")]
    {
        connection
            .batch_execute(&durable_workflows::trace::trace_up_sql())
            .await
            .unwrap_or_else(|error| panic!("failed to create the trace tables: {error}"));
        // `<test binary>::<test path>`; the binary's file stem is `<target>-<hash>`.
        let binary = std::env::current_exe()
            .ok()
            .and_then(|path| {
                path.file_stem()
                    .map(|stem| stem.to_string_lossy().into_owned())
            })
            .map(|stem| {
                stem.rsplit_once('-')
                    .map_or(stem.clone(), |(target, _)| target.to_string())
            });
        let test = match (binary, std::thread::current().name()) {
            (Some(binary), Some(thread)) => format!("{binary}::{thread}"),
            (None, Some(thread)) => thread.to_string(),
            (_, None) => name.clone(),
        };
        durable_workflows::trace::begin_trace(&mut connection, &test).await;
    }
    CURRENT_DATABASE_URL.with(|current| *current.borrow_mut() = Some(url.clone()));
    Some(url)
}

#[allow(dead_code)]
pub async fn fresh_connection() -> Option<DurableConnection> {
    use diesel_async::AsyncConnection;

    let url = fresh_database().await?;
    Some(
        DurableConnection::establish(&url)
            .await
            .unwrap_or_else(|error| panic!("failed to connect to test database: {error}")),
    )
}

#[allow(dead_code)]
pub async fn fresh_pool() -> Option<durable_workflows::DurablePool> {
    fresh_pool_with_max_size(4).await
}

#[allow(dead_code)]
pub async fn fresh_pool_with_max_size(max_size: u32) -> Option<durable_workflows::DurablePool> {
    let url = fresh_database().await?;
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<DurableConnection>::new(
            url,
        );
    Some(
        diesel_async::pooled_connection::bb8::Pool::builder()
            .max_size(max_size)
            .build(manager)
            .await
            .unwrap_or_else(|error| panic!("failed to build durable test pool: {error}")),
    )
}

/// Drops the running test's database.
///
/// MySQL drops it through `connection`. Postgres cannot drop the database a
/// session is connected to, so it drops it from a new server connection and
/// terminates the test's remaining sessions.
#[allow(dead_code)]
pub async fn drop_durable_tables(connection: &mut DurableConnection) {
    // Trace runs keep every database for `durable-trace dump`; the stale sweep removes them.
    if cfg!(feature = "trace-model") {
        return;
    }
    let Some(url) = CURRENT_DATABASE_URL.with(|current| current.borrow_mut().take()) else {
        return;
    };
    let name = url
        .rsplit('/')
        .next()
        .and_then(|tail| tail.split('?').next())
        .expect("test database URL has a database name");
    match BACKEND {
        BackendKind::Mysql => drop_database(connection, name).await,
        BackendKind::Postgres => {
            let mut server = server_connection()
                .await
                .expect("the server URL is set when a test database exists");
            drop_database(&mut server, name).await;
        }
    }
}
