mod common;

use common::SessionTestClient;

#[tokio::test]
async fn list_databases_default_connection() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get("/api/lane/databases").await;
    assert_eq!(res.status(), 200);
    let body: Vec<serde_json::Value> = res.json().await.unwrap();
    assert!(!body.is_empty(), "should list at least one database");
    assert!(
        body[0].get("name").is_some(),
        "each database should have 'name'"
    );
}

#[tokio::test]
async fn list_databases_named_connection() {
    let c = SessionTestClient::login_admin().await;
    // First get the connection name
    let conns: Vec<serde_json::Value> = c
        .get("/api/lane/connections")
        .await
        .json()
        .await
        .unwrap();
    let name = conns[0]["name"].as_str().unwrap();

    let res = c
        .get_query("/api/lane/databases", &[("connection", name)])
        .await;
    assert_eq!(res.status(), 200);
    let body: Vec<serde_json::Value> = res.json().await.unwrap();
    assert!(!body.is_empty());
}

#[tokio::test]
async fn list_databases_invalid_connection_returns_400() {
    let c = SessionTestClient::login_admin().await;
    let res = c
        .get_query(
            "/api/lane/databases",
            &[("connection", "nonexistent-conn-xyz")],
        )
        .await;
    assert_eq!(res.status(), 400);
}
