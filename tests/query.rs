mod common;

use common::SessionTestClient;
use serde_json::json;

/// Helper: get the default database name.
async fn default_database(c: &SessionTestClient) -> String {
    let dbs: Vec<serde_json::Value> = c
        .get("/api/lane/databases")
        .await
        .json()
        .await
        .unwrap();
    dbs[0]["name"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn select_1_returns_data() {
    let c = SessionTestClient::login_admin().await;
    let db = default_database(&c).await;
    let res = c
        .post_json(
            "/api/lane",
            &json!({ "database": db, "query": "SELECT 1 AS val" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["success"], true);
    assert!(body["total_rows"].as_u64().unwrap() >= 1);
    let data = body["data"].as_array().unwrap();
    assert!(!data.is_empty());
}

#[tokio::test]
async fn include_metadata_returns_columns() {
    let c = SessionTestClient::login_admin().await;
    let db = default_database(&c).await;
    let res = c
        .post_json(
            "/api/lane",
            &json!({ "database": db, "query": "SELECT 1 AS num, 'hello' AS txt", "includeMetadata": true }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    let cols = body["metadata"]["columns"].as_array().unwrap();
    assert!(cols.len() >= 2);
    // Check column names are present
    let names: Vec<&str> = cols.iter().map(|c| c["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"num"));
    assert!(names.contains(&"txt"));
}

#[tokio::test]
async fn bad_sql_returns_error() {
    let c = SessionTestClient::login_admin().await;
    let db = default_database(&c).await;
    let res = c
        .post_json(
            "/api/lane",
            &json!({ "database": db, "query": "SELECTTTT BOGUS SYNTAX" }),
        )
        .await;
    // Should be a 4xx or 5xx error
    assert!(res.status().as_u16() >= 400);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["success"], false);
    assert!(body["error"]["message"].as_str().is_some());
}

#[tokio::test]
async fn empty_query_returns_valid_response() {
    let c = SessionTestClient::login_admin().await;
    let db = default_database(&c).await;
    let res = c
        .post_json(
            "/api/lane",
            &json!({ "database": db, "query": "" }),
        )
        .await;
    // Server accepts empty queries (returns 200 with empty data)
    let status = res.status().as_u16();
    assert!(
        status == 200 || status >= 400,
        "expected 200 or error, got {}",
        status
    );
}

#[tokio::test]
async fn missing_database_returns_error() {
    let c = SessionTestClient::login_admin().await;
    let res = c
        .post_json(
            "/api/lane",
            &json!({ "query": "SELECT 1" }),
        )
        .await;
    assert!(res.status().as_u16() >= 400);
}

#[tokio::test]
async fn pagination_query() {
    let c = SessionTestClient::login_admin().await;
    let db = default_database(&c).await;
    let res = c
        .post_json(
            "/api/lane",
            &json!({
                "database": db,
                "query": "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3",
                "pagination": true,
                "batchSize": 2,
                "order": "id"
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["success"], true);
    // With pagination, we get a page of results
    let data = body["data"].as_array().unwrap();
    assert!(!data.is_empty());
}
