mod support;

#[tokio::test]
async fn apply_runs_the_baseline_once_on_an_empty_database() {
    let Some(server_url) = support::durable_database_url() else {
        return;
    };
    let name = support::unique_database_name();
    let mut server = support::server_connection()
        .await
        .expect("connect to the test server");
    support::create_database(&mut server, &name).await;
    let url = support::with_database_name(&server_url, &name);

    let first = durable_workflows::migrations::apply(&url)
        .await
        .expect("first apply");
    let second = durable_workflows::migrations::apply(&url)
        .await
        .expect("second apply");

    support::drop_database(&mut server, &name).await;
    assert_eq!(first, vec!["20260924000000".to_string()]);
    assert!(second.is_empty(), "second apply ran {second:?}");
}
