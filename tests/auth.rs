mod common;

use common::SessionTestClient;

#[tokio::test]
async fn missing_auth_returns_401() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get_no_auth("/api/lane/connections").await;
    assert_eq!(res.status(), 401);
}

#[tokio::test]
async fn bad_api_key_returns_401() {
    let base_url =
        std::env::var("TEST_BASE_URL").unwrap_or_else(|_| "http://localhost:3401".to_string());
    let res = reqwest::Client::new()
        .get(format!("{}/api/lane/connections", base_url))
        .header("x-api-key", "totally-wrong-key")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
}

#[tokio::test]
async fn valid_session_returns_200() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get("/api/lane/connections").await;
    assert_eq!(res.status(), 200);
}
