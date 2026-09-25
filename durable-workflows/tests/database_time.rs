mod support;

#[tokio::test]
async fn database_clock_uses_database_session_time() {
    let Some(pool) = support::fresh_pool_with_max_size(1).await else {
        return;
    };
    let fixed_seconds = 1_800_000_000_i64;
    let fixed_millis = fixed_seconds * 1_000 + 125;
    let mut connection = pool.get().await.expect("connection");
    support::freeze_database_clock(&mut connection, fixed_millis).await;

    let actual = durable_workflows::persistence::database_now_millis(&mut connection)
        .await
        .expect("database time");

    assert_eq!(actual, fixed_millis);
}
