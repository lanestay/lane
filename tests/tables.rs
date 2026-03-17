mod common;

use common::SessionTestClient;

/// Helper: get the default database name from the first connection.
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
async fn list_tables_returns_array() {
    let c = SessionTestClient::login_admin().await;
    let db = default_database(&c).await;
    let res = c
        .get_query("/api/lane/tables", &[("database", &db)])
        .await;
    assert_eq!(res.status(), 200);
    let body: Vec<serde_json::Value> = res.json().await.unwrap();
    // The database should have at least one table (system or user)
    if !body.is_empty() {
        assert!(
            body[0].get("TABLE_NAME").is_some(),
            "tables should have TABLE_NAME"
        );
        assert!(
            body[0].get("TABLE_SCHEMA").is_some(),
            "tables should have TABLE_SCHEMA"
        );
    }
}

#[tokio::test]
async fn describe_table_returns_columns() {
    let c = SessionTestClient::login_admin().await;
    let db = default_database(&c).await;

    // Get the first table
    let tables: Vec<serde_json::Value> = c
        .get_query("/api/lane/tables", &[("database", &db)])
        .await
        .json()
        .await
        .unwrap();

    if tables.is_empty() {
        // No tables to describe — skip gracefully
        return;
    }

    let table_name = tables[0]["TABLE_NAME"].as_str().unwrap();
    let schema = tables[0]["TABLE_SCHEMA"].as_str().unwrap_or("dbo");

    let res = c
        .get_query(
            "/api/lane/describe",
            &[
                ("database", &db),
                ("table", table_name),
                ("schema", schema),
            ],
        )
        .await;
    assert_eq!(res.status(), 200);
    let cols: Vec<serde_json::Value> = res.json().await.unwrap();
    assert!(!cols.is_empty(), "table should have at least one column");
    assert!(
        cols[0].get("COLUMN_NAME").is_some(),
        "column should have COLUMN_NAME"
    );
    assert!(
        cols[0].get("DATA_TYPE").is_some(),
        "column should have DATA_TYPE"
    );
}

#[tokio::test]
async fn list_tables_missing_database_returns_400() {
    let c = SessionTestClient::login_admin().await;
    // No database param
    let res = c.get("/api/lane/tables").await;
    assert_eq!(res.status(), 400);
}
