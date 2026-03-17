mod common;

use common::SessionTestClient;

#[tokio::test]
async fn list_connections_returns_array() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get("/api/lane/connections").await;
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    let arr = body.as_array().expect("response should be an array");
    assert!(!arr.is_empty(), "should have at least one connection");

    let first = &arr[0];
    assert!(first.get("name").is_some(), "connection should have 'name'");
    assert!(
        first.get("is_default").is_some(),
        "connection should have 'is_default'"
    );
    assert!(first.get("type").is_some(), "connection should have 'type'");
}

#[tokio::test]
async fn connections_have_a_default() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get("/api/lane/connections").await;
    let body: Vec<serde_json::Value> = res.json().await.unwrap();
    let has_default = body.iter().any(|c| c["is_default"].as_bool() == Some(true));
    assert!(has_default, "at least one connection should be marked default");
}
