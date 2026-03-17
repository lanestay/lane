mod common;

use common::SessionTestClient;
use serde_json::{json, Value};

/// Helper: create a test user, return the email.
async fn create_test_user(c: &SessionTestClient, suffix: &str) -> Option<String> {
    let email = format!("perm-test-{}@integration.test", suffix);
    let res = c
        .post_json(
            "/api/lane/admin/users",
            &json!({ "email": email, "is_admin": false }),
        )
        .await;
    if res.status() == 503 {
        return None; // access control not enabled
    }
    assert!(
        res.status().is_success(),
        "create user: got {}",
        res.status()
    );
    Some(email)
}

/// Helper: generate a token for a user, return the token string.
async fn generate_token(c: &SessionTestClient, email: &str) -> String {
    let res = c
        .post_json(
            "/api/lane/admin/tokens/generate",
            &json!({ "email": email, "label": "test" }),
        )
        .await;
    assert!(res.status().is_success(), "generate token: {}", res.status());
    let body: Value = res.json().await.unwrap();
    body["token"].as_str().unwrap().to_string()
}

/// Helper: set permissions for a user.
async fn set_permissions(c: &SessionTestClient, email: &str, perms: &Value) {
    let res = c
        .post_json(
            "/api/lane/admin/permissions",
            &json!({ "email": email, "permissions": perms }),
        )
        .await;
    assert!(res.status().is_success(), "set permissions: {}", res.status());
}

/// Helper: send a query using a per-user API token (x-api-key header).
async fn query_with_token(token: &str, database: &str, query: &str) -> reqwest::Response {
    let base_url =
        std::env::var("TEST_BASE_URL").unwrap_or_else(|_| "http://localhost:3401".to_string());
    reqwest::Client::new()
        .post(format!("{}/api/lane", base_url))
        .header("x-api-key", token)
        .json(&json!({ "database": database, "query": query }))
        .send()
        .await
        .expect("request failed")
}

/// Helper: cleanup user at end of test.
async fn cleanup_user(c: &SessionTestClient, email: &str) {
    let _ = c
        .delete(&format!("/api/lane/admin/users/{}", email))
        .await;
}

/// Helper: get a user from the users list by email.
async fn get_user(c: &SessionTestClient, email: &str) -> Option<Value> {
    let res = c.get("/api/lane/admin/users").await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let users = body["users"].as_array().unwrap();
    users.iter().find(|u| u["email"] == email).cloned()
}

/// Quick pseudo-random short ID for test isolation.
fn test_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{:08x}", nanos)
}

// ============================================================================
// SQL mode gate tests
// ============================================================================

/// Non-admin user with sql_mode=none (default) should be blocked from query.
#[tokio::test]
async fn sql_mode_none_blocked_by_default() {
    let c = SessionTestClient::login_admin().await;
    let email = match create_test_user(&c, &format!("sqlmode-blocked-{}", test_id())).await {
        Some(e) => e,
        None => return,
    };

    // Grant read permission so we isolate the SQL mode check
    set_permissions(
        &c,
        &email,
        &json!([
            { "database_name": "*", "can_read": true }
        ]),
    )
    .await;

    let token = generate_token(&c, &email).await;

    // Attempt query — should be 403
    let res = query_with_token(&token, "master", "SELECT 1").await;
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap_or(json!({}));
    assert_eq!(
        status, 403,
        "expected 403 for sql_mode=none user, got {} body: {}",
        status, body
    );
    let err_msg = body["error"]["message"].as_str().unwrap_or("");
    assert!(
        err_msg.contains("Raw SQL"),
        "expected raw SQL error message, got: {}",
        err_msg
    );

    cleanup_user(&c, &email).await;
}

/// Non-admin user with sql_mode=full should be allowed to run query.
#[tokio::test]
async fn sql_mode_full_allows_query() {
    let c = SessionTestClient::login_admin().await;
    let email = match create_test_user(&c, &format!("sqlmode-full-{}", test_id())).await {
        Some(e) => e,
        None => return,
    };

    // Enable full SQL mode
    let res = c
        .put_json(
            &format!("/api/lane/admin/users/{}", email),
            &json!({ "sql_mode": "full" }),
        )
        .await;
    assert!(res.status().is_success());

    // Grant read permission
    set_permissions(
        &c,
        &email,
        &json!([
            { "database_name": "*", "can_read": true }
        ]),
    )
    .await;

    let token = generate_token(&c, &email).await;

    // Attempt query — should succeed (200) or at least not 403
    let res = query_with_token(&token, "master", "SELECT 1 AS test").await;
    assert_ne!(
        res.status(),
        403,
        "should not get 403 with sql_mode=full"
    );

    cleanup_user(&c, &email).await;
}

/// Non-admin user with sql_mode=read_only should be blocked from write queries.
#[tokio::test]
async fn sql_mode_read_only_blocks_writes() {
    let c = SessionTestClient::login_admin().await;
    let email = match create_test_user(&c, &format!("sqlmode-ro-{}", test_id())).await {
        Some(e) => e,
        None => return,
    };

    // Set read_only mode
    let res = c
        .put_json(
            &format!("/api/lane/admin/users/{}", email),
            &json!({ "sql_mode": "read_only" }),
        )
        .await;
    assert!(res.status().is_success());

    // Grant full permission so we isolate the SQL mode check
    set_permissions(
        &c,
        &email,
        &json!([
            { "database_name": "*", "can_read": true, "can_write": true }
        ]),
    )
    .await;

    let token = generate_token(&c, &email).await;

    // SELECT should work
    let res = query_with_token(&token, "master", "SELECT 1 AS test").await;
    assert_ne!(res.status(), 403, "SELECT should be allowed in read_only mode");

    // INSERT should be blocked
    let res = query_with_token(&token, "master", "INSERT INTO test VALUES (1)").await;
    assert_eq!(res.status(), 403, "INSERT should be blocked in read_only mode");

    cleanup_user(&c, &email).await;
}

/// Non-admin user with sql_mode=supervised should be blocked from DDL but allowed DML.
#[tokio::test]
async fn sql_mode_supervised_blocks_ddl() {
    let c = SessionTestClient::login_admin().await;
    let email = match create_test_user(&c, &format!("sqlmode-sup-{}", test_id())).await {
        Some(e) => e,
        None => return,
    };

    // Set supervised mode
    let res = c
        .put_json(
            &format!("/api/lane/admin/users/{}", email),
            &json!({ "sql_mode": "supervised" }),
        )
        .await;
    assert!(res.status().is_success());

    // Grant full permission
    set_permissions(
        &c,
        &email,
        &json!([
            { "database_name": "*", "can_read": true, "can_write": true }
        ]),
    )
    .await;

    let token = generate_token(&c, &email).await;

    // SELECT should work
    let res = query_with_token(&token, "master", "SELECT 1 AS test").await;
    assert_ne!(res.status(), 403, "SELECT should be allowed in supervised mode");

    // DDL should be blocked
    let res = query_with_token(&token, "master", "CREATE TABLE test_table (id INT)").await;
    assert_eq!(res.status(), 403, "DDL should be blocked in supervised mode");

    cleanup_user(&c, &email).await;
}

/// Admin session should always bypass SQL mode gate.
#[tokio::test]
async fn admin_session_bypasses_sql_mode_gate() {
    let c = SessionTestClient::login_admin().await;

    let res = c
        .post_json(
            "/api/lane",
            &json!({ "database": "master", "query": "SELECT 1 AS test" }),
        )
        .await;
    // Admin session should never get 403 for raw SQL
    assert_ne!(
        res.status(),
        403,
        "Admin session should bypass SQL mode gate"
    );
}

// ============================================================================
// Admin API: sql_mode field
// ============================================================================

/// PUT /api/admin/users/{email} should accept sql_mode and reflect it in GET.
#[tokio::test]
async fn update_user_sql_mode_field() {
    let c = SessionTestClient::login_admin().await;
    let email = match create_test_user(&c, &format!("sqlmode-field-{}", test_id())).await {
        Some(e) => e,
        None => return,
    };

    // Set sql_mode = full
    let res = c
        .put_json(
            &format!("/api/lane/admin/users/{}", email),
            &json!({ "sql_mode": "full" }),
        )
        .await;
    assert!(res.status().is_success());

    // Verify it shows in user list
    let user = get_user(&c, &email).await;
    assert!(user.is_some(), "user should exist in list");
    assert_eq!(user.unwrap()["sql_mode"], "full");

    cleanup_user(&c, &email).await;
}

// ============================================================================
// Granular permission fields (can_update, can_delete)
// ============================================================================

/// Permissions API should accept and return can_update/can_delete fields.
#[tokio::test]
async fn permissions_granular_fields() {
    let c = SessionTestClient::login_admin().await;
    let email = match create_test_user(&c, &format!("granular-{}", test_id())).await {
        Some(e) => e,
        None => return,
    };

    // Set permissions with granular fields
    set_permissions(
        &c,
        &email,
        &json!([
            {
                "database_name": "testdb",
                "table_pattern": "*",
                "can_read": true,
                "can_write": true,
                "can_update": false,
                "can_delete": false
            }
        ]),
    )
    .await;

    // Fetch permissions from user object
    let user = get_user(&c, &email).await.expect("user should exist");
    let perms = user["permissions"].as_array().expect("permissions array");
    assert_eq!(perms.len(), 1);
    assert_eq!(perms[0]["can_read"], true);
    assert_eq!(perms[0]["can_write"], true);
    assert_eq!(perms[0]["can_update"], false);
    assert_eq!(perms[0]["can_delete"], false);

    cleanup_user(&c, &email).await;
}

/// When can_update/can_delete are omitted, they should default to can_write value.
#[tokio::test]
async fn permissions_defaults_from_can_write() {
    let c = SessionTestClient::login_admin().await;
    let email = match create_test_user(&c, &format!("defaults-{}", test_id())).await {
        Some(e) => e,
        None => return,
    };

    // Set permissions without can_update/can_delete — should inherit from can_write
    set_permissions(
        &c,
        &email,
        &json!([
            {
                "database_name": "testdb",
                "table_pattern": "*",
                "can_read": true,
                "can_write": true
            }
        ]),
    )
    .await;

    let user = get_user(&c, &email).await.expect("user should exist");
    let perms = user["permissions"].as_array().expect("permissions array");
    assert_eq!(perms.len(), 1);
    assert_eq!(perms[0]["can_write"], true);
    assert_eq!(
        perms[0]["can_update"], true,
        "can_update should default to can_write"
    );
    assert_eq!(
        perms[0]["can_delete"], true,
        "can_delete should default to can_write"
    );

    cleanup_user(&c, &email).await;
}
