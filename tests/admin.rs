mod common;

use common::SessionTestClient;
use serde_json::json;

/// Admin endpoints may return 503 if access control is not enabled.
/// Tests accept either 200 (success) or 503 (feature off) as passing.

#[tokio::test]
async fn list_users_responds() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get("/api/lane/admin/users").await;
    let status = res.status().as_u16();
    assert!(
        status == 200 || status == 503,
        "expected 200 or 503, got {}",
        status
    );
}

#[tokio::test]
async fn list_tokens_responds() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get("/api/lane/admin/tokens").await;
    let status = res.status().as_u16();
    assert!(
        status == 200 || status == 503,
        "expected 200 or 503, got {}",
        status
    );
}

#[tokio::test]
async fn audit_log_responds() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get("/api/lane/admin/audit").await;
    let status = res.status().as_u16();
    assert!(
        status == 200 || status == 503,
        "expected 200 or 503, got {}",
        status
    );
}

#[tokio::test]
async fn user_crud_roundtrip() {
    let c = SessionTestClient::login_admin().await;
    let email = format!("test-{}@integration.test", uuid_v4_short());

    // Create
    let res = c
        .post_json(
            "/api/lane/admin/users",
            &json!({ "email": email, "display_name": "Integration Test", "is_admin": false }),
        )
        .await;
    let status = res.status().as_u16();
    if status == 503 {
        // Access control not enabled — skip rest
        return;
    }
    assert!(status == 200 || status == 201, "create user: got {}", status);

    // Update
    let res = c
        .put_json(
            &format!("/api/lane/admin/users/{}", email),
            &json!({ "display_name": "Updated Name" }),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Delete
    let res = c
        .delete(&format!("/api/lane/admin/users/{}", email))
        .await;
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn token_roundtrip() {
    let c = SessionTestClient::login_admin().await;
    let email = format!("test-{}@integration.test", uuid_v4_short());

    // Create user first
    let res = c
        .post_json(
            "/api/lane/admin/users",
            &json!({ "email": email }),
        )
        .await;
    if res.status() == 503 {
        return;
    }

    // Generate token
    let res = c
        .post_json(
            "/api/lane/admin/tokens/generate",
            &json!({ "email": email, "label": "test-token" }),
        )
        .await;
    let status = res.status().as_u16();
    assert!(
        status == 200 || status == 201,
        "generate token: got {}",
        status
    );
    let body: serde_json::Value = res.json().await.unwrap();
    let token = body["token"].as_str().unwrap();
    assert!(!token.is_empty());

    // Revoke token
    let res = c
        .post_json(
            "/api/lane/admin/tokens/revoke",
            &json!({ "token": token }),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Cleanup: delete user
    c.delete(&format!("/api/lane/admin/users/{}", email))
        .await;
}

/// Quick pseudo-random short ID for test isolation.
fn uuid_v4_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{:08x}", nanos)
}
