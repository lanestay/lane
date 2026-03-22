use axum::{
    extract::{Multipart, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::{self, AuthResult};
use crate::auth::access_control::SqlMode;
use crate::import::{parser, sql_gen, type_infer, InferredColumn, ParsedFile};
use crate::query::QueryParams;

use super::errors::*;
use super::{AppState, CachedFile};

// ============================================================================
// Request / Response Types
// ============================================================================

#[derive(Debug, Serialize)]
struct PreviewResponse {
    preview_id: String,
    file_name: String,
    total_rows: usize,
    columns: Vec<InferredColumn>,
    preview_rows: Vec<Vec<Option<String>>>,
}

#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    preview_id: String,
    connection: Option<String>,
    database: String,
    schema: Option<String>,
    table_name: String,
    if_exists: Option<String>, // "create" or "append"
    columns: Option<Vec<ColumnOverride>>,
}

#[derive(Debug, Deserialize)]
struct ColumnOverride {
    name: String,
    sql_type: Option<String>,
    include: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ExecuteResponse {
    success: bool,
    rows_imported: usize,
    batches: usize,
    table_created: bool,
    execution_time_ms: u128,
}

// ============================================================================
// Helpers
// ============================================================================

fn extract_auth_email(auth: &AuthResult) -> Option<&str> {
    match auth {
        AuthResult::SessionAccess { email, .. } => Some(email.as_str()),
        AuthResult::TokenAccess { email, .. } => Some(email.as_str()),
        _ => None,
    }
}

fn check_write_permission(auth: &AuthResult, state: &AppState, database: &str) -> Result<(), Response> {
    if let Some(email) = extract_auth_email(auth) {
        if let Some(ref access_db) = state.access_db {
            if !access_db.check_permission(email, database, true) {
                return Err(request_error(
                    "FORBIDDEN",
                    &format!("No write permission for database '{}'", database),
                    None,
                )
                .to_response(StatusCode::FORBIDDEN));
            }
        }
    }
    Ok(())
}

fn check_connection_access(auth: &AuthResult, state: &AppState, connection_name: &str) -> Result<(), Response> {
    if let Some(email) = extract_auth_email(auth) {
        if let Some(ref access_db) = state.access_db {
            if !access_db.check_connection_access(email, connection_name) {
                return Err(request_error(
                    "FORBIDDEN",
                    &format!("Access denied to connection '{}'", connection_name),
                    None,
                )
                .to_response(StatusCode::FORBIDDEN));
            }
        }
    }
    Ok(())
}

// ============================================================================
// Preview Handler
// ============================================================================

pub async fn preview_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Response {
    // Authenticate
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    // Parse multipart fields
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut file_name: Option<String> = None;
    let mut connection: Option<String> = None;
    let mut database: Option<String> = None;
    let mut schema: Option<String> = None;
    let mut table_name: Option<String> = None;
    let mut has_header = true;

    while let Ok(Some(field)) = multipart.next_field().await {
        let name = match field.name() {
            Some(n) => n.to_string(),
            None => continue,
        };

        match name.as_str() {
            "file" => {
                file_name = field.file_name().map(|s| s.to_string());
                match field.bytes().await {
                    Ok(b) => file_bytes = Some(b.to_vec()),
                    Err(e) => {
                        return bad_request(format!("Failed to read file: {}", e), None);
                    }
                }
            }
            "connection" => {
                if let Ok(v) = field.text().await {
                    if !v.is_empty() {
                        connection = Some(v);
                    }
                }
            }
            "database" => {
                if let Ok(v) = field.text().await {
                    database = Some(v);
                }
            }
            "schema" => {
                if let Ok(v) = field.text().await {
                    if !v.is_empty() {
                        schema = Some(v);
                    }
                }
            }
            "table_name" => {
                if let Ok(v) = field.text().await {
                    table_name = Some(v);
                }
            }
            "has_header" => {
                if let Ok(v) = field.text().await {
                    has_header = !matches!(v.to_ascii_lowercase().as_str(), "false" | "0" | "no");
                }
            }
            _ => {} // ignore unknown fields
        }
    }

    // Validate required fields
    let file_bytes = match file_bytes {
        Some(b) => b,
        None => return bad_request("Missing required field: file", None),
    };
    let file_name = file_name.unwrap_or_else(|| "upload.csv".to_string());
    let database = match database {
        Some(d) if !d.is_empty() => d,
        _ => return bad_request("Missing required field: database", None),
    };
    let _table_name = match table_name {
        Some(t) if !t.is_empty() => t,
        _ => return bad_request("Missing required field: table_name", None),
    };

    // Check connection access
    if let Some(ref conn) = connection {
        if let Err(resp) = check_connection_access(&auth, &state, conn) {
            return resp;
        }
    }

    // Check write permission
    if let Err(resp) = check_write_permission(&auth, &state, &database) {
        return resp;
    }

    // Resolve connection to get dialect
    let db = match state.registry.resolve(connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("CONNECTION_NOT_FOUND", &format!("{:#}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };
    let dialect = db.dialect();

    // Determine default schema based on dialect
    let _schema = schema.unwrap_or_else(|| match dialect {
        crate::db::Dialect::Mssql => "dbo".to_string(),
        crate::db::Dialect::DuckDb => "main".to_string(),
        crate::db::Dialect::ClickHouse => "default".to_string(),
        crate::db::Dialect::Postgres => "public".to_string(),
    });

    // Parse the file
    let parsed = if has_header {
        parser::parse_file(&file_bytes, &file_name)
    } else {
        parser::parse_csv(&file_bytes, false)
    };

    let parsed = match parsed {
        Ok(p) => p,
        Err(e) => {
            return bad_request(format!("Failed to parse file: {:#}", e), None);
        }
    };

    if parsed.headers.is_empty() {
        return bad_request("File has no columns", None);
    }

    // Infer column types
    let columns = type_infer::infer_columns(&parsed, dialect);

    // Get preview rows (first 10)
    let preview_rows: Vec<Vec<Option<String>>> = parsed.rows.iter().take(10).cloned().collect();
    let total_rows = parsed.total_rows;

    // Cache the parsed file
    let preview_id = uuid::Uuid::new_v4().to_string();
    {
        let serialized = serde_json::to_vec(&parsed).unwrap_or_default();
        let mut cache = state.downloads.write().await;
        cache.insert(
            preview_id.clone(),
            CachedFile {
                bytes: serialized,
                content_type: "application/json".to_string(),
                filename: "preview.json".to_string(),
                created_at: std::time::Instant::now(),
            },
        );
    }

    let response = PreviewResponse {
        preview_id,
        file_name,
        total_rows,
        columns,
        preview_rows,
    };

    (StatusCode::OK, Json(response)).into_response()
}

// ============================================================================
// Execute Handler
// ============================================================================

const BATCH_SIZE: usize = 500;

pub async fn execute_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<ExecuteRequest>,
) -> Response {
    let start = std::time::Instant::now();

    // Authenticate
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    // Check connection access (including default connection)
    let default_name = state.registry.default_name();
    let conn_name = req.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    // Require at least Supervised sql_mode (writes data)
    if let AuthResult::ServiceAccountAccess { account_name } = &auth {
        if let Some(ref access_db) = state.access_db {
            let mode = access_db.get_sa_sql_mode(account_name);
            match mode {
                SqlMode::None | SqlMode::ReadOnly => {
                    return request_error(
                        "FORBIDDEN",
                        "Data import requires at least supervised SQL access.",
                        None,
                    )
                    .to_response(StatusCode::FORBIDDEN);
                }
                _ => {}
            }
        }
    }
    if let Some(email) = extract_auth_email(&auth) {
        if let Some(ref access_db) = state.access_db {
            let mode = access_db.get_sql_mode(email);
            match mode {
                SqlMode::None | SqlMode::ReadOnly => {
                    return request_error(
                        "FORBIDDEN",
                        "Data import requires at least supervised SQL access.",
                        Some("Contact an admin to upgrade your SQL access mode."),
                    )
                    .to_response(StatusCode::FORBIDDEN);
                }
                _ => {}
            }
        }
    }

    // Check write permission
    if let Err(resp) = check_write_permission(&auth, &state, &req.database) {
        return resp;
    }

    // Resolve connection
    let db = match state.registry.resolve(req.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("CONNECTION_NOT_FOUND", &format!("{:#}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };
    let dialect = db.dialect();

    let schema = req.schema.unwrap_or_else(|| match dialect {
        crate::db::Dialect::Mssql => "dbo".to_string(),
        crate::db::Dialect::DuckDb => "main".to_string(),
        crate::db::Dialect::ClickHouse => "default".to_string(),
        crate::db::Dialect::Postgres => "public".to_string(),
    });

    // Retrieve cached parsed file
    let parsed: ParsedFile = {
        let mut cache = state.downloads.write().await;
        let cached = match cache.remove(&req.preview_id) {
            Some(c) => c,
            None => {
                return bad_request(
                    "Preview not found or expired. Please upload and preview again.",
                    None,
                );
            }
        };
        match serde_json::from_slice(&cached.bytes) {
            Ok(p) => p,
            Err(e) => {
                return bad_request(
                    format!("Failed to deserialize cached data: {}", e),
                    None,
                );
            }
        }
    };

    // Build final column list, applying user overrides
    let mut base_columns = type_infer::infer_columns(&parsed, dialect);

    if let Some(ref overrides) = req.columns {
        for over in overrides {
            if let Some(col) = base_columns.iter_mut().find(|c| c.name == over.name) {
                if let Some(ref st) = over.sql_type {
                    col.sql_type = st.clone();
                }
            }
        }
    }

    // Filter to only included columns
    let included_indices: Vec<usize> = if let Some(ref overrides) = req.columns {
        base_columns
            .iter()
            .enumerate()
            .filter(|(_, col)| {
                overrides
                    .iter()
                    .find(|o| o.name == col.name)
                    .and_then(|o| o.include)
                    .unwrap_or(true)
            })
            .map(|(i, _)| i)
            .collect()
    } else {
        (0..base_columns.len()).collect()
    };

    let final_columns: Vec<InferredColumn> = included_indices
        .iter()
        .map(|&i| base_columns[i].clone())
        .collect();

    // Filter row data to only included columns
    let filtered_rows: Vec<Vec<Option<String>>> = parsed
        .rows
        .iter()
        .map(|row| included_indices.iter().map(|&i| row.get(i).cloned().flatten()).collect())
        .collect();

    let if_exists = req.if_exists.as_deref().unwrap_or("create");
    let mut table_created = false;

    // CREATE TABLE if needed
    if if_exists == "create" {
        let create_sql =
            sql_gen::generate_create_table(&final_columns, &schema, &req.table_name, dialect);

        let params = QueryParams {
            database: req.database.clone(),
            query: create_sql,
            ..Default::default()
        };

        if let Err(e) = db.execute_query(&params).await {
            return request_error(
                "CREATE_TABLE_FAILED",
                &format!("Failed to create table: {:#}", e),
                None,
            )
            .to_response(StatusCode::INTERNAL_SERVER_ERROR);
        }
        table_created = true;
    }

    // Insert in batches
    let total_rows = filtered_rows.len();
    let mut rows_imported = 0;
    let mut batches = 0;

    for chunk in filtered_rows.chunks(BATCH_SIZE) {
        let insert_sql = sql_gen::generate_insert_batch(
            &final_columns,
            chunk,
            &schema,
            &req.table_name,
            dialect,
        );

        let params = QueryParams {
            database: req.database.clone(),
            query: insert_sql,
            ..Default::default()
        };

        if let Err(e) = db.execute_query(&params).await {
            return request_error(
                "INSERT_FAILED",
                &format!(
                    "Failed at batch {} (rows {}-{}): {:#}",
                    batches + 1,
                    rows_imported + 1,
                    (rows_imported + chunk.len()).min(total_rows),
                    e
                ),
                Some(&format!(
                    "{} rows were imported before this error",
                    rows_imported
                )),
            )
            .to_response(StatusCode::INTERNAL_SERVER_ERROR);
        }

        rows_imported += chunk.len();
        batches += 1;
    }

    let response = ExecuteResponse {
        success: true,
        rows_imported,
        batches,
        table_created,
        execution_time_ms: start.elapsed().as_millis(),
    };

    (StatusCode::OK, Json(response)).into_response()
}
