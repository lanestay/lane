mod common;

use common::SessionTestClient;
use serde_json::{json, Value};

// ============================================================================
// Admin endpoints
// ============================================================================

#[tokio::test]
async fn admin_list_tables() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get("/api/data/tables").await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body.is_array());
}

#[tokio::test]
async fn admin_enable_table() {
    let c = SessionTestClient::login_admin().await;
    let res = c
        .post_json(
            "/api/data/tables",
            &json!({
                "connection": "mssql",
                "database": "master",
                "table": "spt_monitor"
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["success"], true);
}

#[tokio::test]
async fn admin_enable_and_disable_table() {
    let c = SessionTestClient::login_admin().await;

    // Enable
    let res = c
        .post_json(
            "/api/data/tables",
            &json!({
                "connection": "mssql",
                "database": "master",
                "table": "spt_values"
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Verify it's in the list
    let res = c.get("/api/data/tables").await;
    let body: Value = res.json().await.unwrap();
    let tables = body.as_array().unwrap();
    assert!(tables.iter().any(|t| t["table_name"] == "spt_values"));

    // Disable
    let res = c
        .delete_json(
            "/api/data/tables",
            &json!({
                "connection": "mssql",
                "database": "master",
                "table": "spt_values"
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
}

// ============================================================================
// CRUD — List
// ============================================================================

#[tokio::test]
async fn list_rows_returns_data() {
    let c = SessionTestClient::login_admin().await;

    // Enable table first
    c.post_json(
        "/api/data/tables",
        &json!({"connection": "mssql", "database": "master", "table": "spt_monitor"}),
    )
    .await;

    let res = c.get("/api/data/mssql/master/spt_monitor").await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body["data"].is_array());
    assert!(body["total"].is_number());
    assert!(body["limit"].is_number());
}

#[tokio::test]
async fn list_with_limit_and_offset() {
    let c = SessionTestClient::login_admin().await;

    c.post_json(
        "/api/data/tables",
        &json!({"connection": "mssql", "database": "master", "table": "spt_values"}),
    )
    .await;

    let res = c
        .get_query(
            "/api/data/mssql/master/spt_values",
            &[("limit", "5"), ("offset", "0")],
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let data = body["data"].as_array().unwrap();
    assert!(data.len() <= 5);
    assert_eq!(body["limit"], 5);
    assert_eq!(body["offset"], 0);
}

#[tokio::test]
async fn list_with_select() {
    let c = SessionTestClient::login_admin().await;

    c.post_json(
        "/api/data/tables",
        &json!({"connection": "mssql", "database": "master", "table": "spt_values"}),
    )
    .await;

    let res = c
        .get_query(
            "/api/data/mssql/master/spt_values",
            &[("select", "name,type"), ("limit", "3")],
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let data = body["data"].as_array().unwrap();
    assert!(!data.is_empty());
    let first = &data[0];
    assert!(first.get("name").is_some());
    assert!(first.get("type").is_some());
}

#[tokio::test]
async fn list_with_eq_filter() {
    let c = SessionTestClient::login_admin().await;

    c.post_json(
        "/api/data/tables",
        &json!({"connection": "mssql", "database": "master", "table": "spt_values"}),
    )
    .await;

    let res = c
        .get_query(
            "/api/data/mssql/master/spt_values",
            &[("type", "eq.P"), ("limit", "5")],
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let data = body["data"].as_array().unwrap();
    for row in data {
        assert_eq!(row["type"].as_str().unwrap().trim(), "P");
    }
}

// ============================================================================
// Disabled table returns 404
// ============================================================================

#[tokio::test]
async fn disabled_table_returns_404() {
    let c = SessionTestClient::login_admin().await;
    let res = c
        .get("/api/data/mssql/master/nonexistent_table_xyz")
        .await;
    assert_eq!(res.status(), 404);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["code"], "NOT_FOUND");
}

// ============================================================================
// Auth required
// ============================================================================

#[tokio::test]
async fn no_auth_returns_401() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get_no_auth("/api/data/tables").await;
    assert_eq!(res.status(), 401);
}

// ============================================================================
// OpenAPI
// ============================================================================

#[tokio::test]
async fn openapi_returns_valid_spec() {
    let c = SessionTestClient::login_admin().await;
    let res = c.get("/api/data/openapi.json").await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["openapi"], "3.0.3");
    assert!(body["paths"].is_object());
    assert!(body["components"]["schemas"].is_object());
}
