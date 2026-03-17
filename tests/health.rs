mod common;

use common::SessionTestClient;

#[tokio::test]
async fn health_returns_200() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get_no_auth("/health").await;
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["status"], "healthy");
}

#[tokio::test]
async fn health_does_not_require_auth() {
    let c = SessionTestClient::login_admin().await;
    // No auth header
    let res = c.get_no_auth("/health").await;
    assert_eq!(res.status(), 200);
}
