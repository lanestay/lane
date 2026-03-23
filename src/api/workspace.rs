use axum::{
    extract::{Multipart, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::auth::{self, AuthResult};
use crate::auth::access_control::SqlMode;
use crate::db::{ConnectionRegistry, DatabaseBackend};
use crate::query::QueryParams;
use crate::query::validation::is_read_only_safe;

use super::errors::*;
use super::AppState;

fn extract_email(auth: &AuthResult) -> Option<&str> {
    match auth {
        AuthResult::SessionAccess { email, .. } => Some(email.as_str()),
        AuthResult::TokenAccess { email, .. } => Some(email.as_str()),
        _ => None,
    }
}

fn require_write_mode(auth: &AuthResult, state: &AppState) -> Result<(), Response> {
    if let AuthResult::ServiceAccountAccess { account_name } = auth {
        if let Some(ref access_db) = state.access_db {
            let mode = access_db.get_sa_sql_mode(account_name);
            match mode {
                SqlMode::None | SqlMode::ReadOnly => {
                    return Err(request_error(
                        "FORBIDDEN",
                        "Workspace write operations require at least supervised SQL access.",
                        None,
                    )
                    .to_response(StatusCode::FORBIDDEN));
                }
                _ => {}
            }
        }
    }
    if let Some(email) = extract_email(auth) {
        if let Some(ref access_db) = state.access_db {
            let mode = access_db.get_sql_mode(email);
            match mode {
                SqlMode::None | SqlMode::ReadOnly => {
                    return Err(request_error(
                        "FORBIDDEN",
                        "Workspace write operations require at least supervised SQL access.",
                        Some("Contact an admin to upgrade your SQL access mode."),
                    )
                    .to_response(StatusCode::FORBIDDEN));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Get the workspace DB or return an error response.
fn get_workspace(
    state: &AppState,
) -> Result<&Arc<crate::db::duckdb_backend::DuckDbBackend>, Response> {
    state
        .workspace_db
        .as_ref()
        .ok_or_else(|| {
            request_error(
                "WORKSPACE_NOT_AVAILABLE",
                "DuckDB workspace is not initialized",
                None,
            )
            .to_response(StatusCode::SERVICE_UNAVAILABLE)
        })
}

/// Sanitize a filename into a valid table name.
fn sanitize_table_name(filename: &str) -> String {
    // Strip extension
    let stem = filename.rsplit('.').skip(1).collect::<Vec<_>>();
    let stem = if stem.is_empty() {
        filename
    } else {
        &stem.into_iter().rev().collect::<Vec<_>>().join(".")
    };

    // Lowercase, replace non-alnum with underscore, collapse multiple underscores
    let sanitized: String = stem
        .to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect();

    // Collapse consecutive underscores and trim leading/trailing
    let mut result = String::new();
    let mut prev_underscore = false;
    for c in sanitized.chars() {
        if c == '_' {
            if !prev_underscore && !result.is_empty() {
                result.push(c);
            }
            prev_underscore = true;
        } else {
            prev_underscore = false;
            result.push(c);
        }
    }
    let result = result.trim_end_matches('_').to_string();

    // Prefix with t_ if starts with digit or is empty
    if result.is_empty() || result.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
        format!("t_{}", result)
    } else {
        result
    }
}

// ============================================================================
// Upload Handler
// ============================================================================

pub async fn upload_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }
    if let Err(resp) = require_write_mode(&auth, &state) {
        return resp;
    }

    let ws = match get_workspace(&state) {
        Ok(ws) => ws,
        Err(resp) => return resp,
    };

    let mut file_bytes: Option<Vec<u8>> = None;
    let mut file_name: Option<String> = None;
    let mut table_name_override: Option<String> = None;

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
                    Err(e) => return bad_request(format!("Failed to read file: {}", e), None),
                }
            }
            "table_name" => {
                if let Ok(v) = field.text().await {
                    if !v.is_empty() {
                        table_name_override = Some(v);
                    }
                }
            }
            _ => {}
        }
    }

    let file_bytes = match file_bytes {
        Some(b) => b,
        None => return bad_request("Missing required field: file", None),
    };
    let original_filename = file_name.unwrap_or_else(|| "upload.csv".to_string());
    let table_name = table_name_override
        .unwrap_or_else(|| sanitize_table_name(&original_filename));

    // Determine file type from extension
    let ext = original_filename
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();

    // Write to temp file for DuckDB to read
    let ws_dir = match &state.workspace_dir {
        Some(d) => d.clone(),
        None => return bad_request("Workspace directory not configured", None),
    };

    let temp_path = ws_dir.join(format!("_upload_tmp_{}.{}", uuid::Uuid::new_v4(), ext));

    match ext.as_str() {
        "csv" | "tsv" | "json" => {
            // Write bytes to temp file, use DuckDB's native reader
            if let Err(e) = std::fs::write(&temp_path, &file_bytes) {
                return request_error(
                    "IO_ERROR",
                    &format!("Failed to write temp file: {}", e),
                    None,
                )
                .to_response(StatusCode::INTERNAL_SERVER_ERROR);
            }

            let safe_table = table_name.replace('"', "\"\"");
            let temp_path_str = temp_path.to_string_lossy().replace('\'', "''");

            let create_sql = if ext == "json" {
                format!(
                    "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_json_auto('{}')",
                    safe_table, temp_path_str
                )
            } else {
                format!(
                    "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_csv_auto('{}')",
                    safe_table, temp_path_str
                )
            };

            if let Err(e) = ws.execute_sql(&create_sql).await {
                let _ = std::fs::remove_file(&temp_path);
                return request_error(
                    "IMPORT_FAILED",
                    &format!("Failed to import file: {:#}", e),
                    None,
                )
                .to_response(StatusCode::INTERNAL_SERVER_ERROR);
            }

            let _ = std::fs::remove_file(&temp_path);
        }
        "xlsx" | "xls" | "xlsb" | "ods" => {
            // Parse with calamine, then insert into DuckDB
            let parsed = match crate::import::parser::parse_excel(&file_bytes) {
                Ok(p) => p,
                Err(e) => return bad_request(format!("Failed to parse Excel file: {:#}", e), None),
            };

            if parsed.headers.is_empty() {
                return bad_request("Excel file has no columns", None);
            }

            // Create table with VARCHAR columns
            let safe_table = table_name.replace('"', "\"\"");
            let col_defs: Vec<String> = parsed
                .headers
                .iter()
                .map(|h| {
                    let safe_col = h.replace('"', "\"\"");
                    format!("\"{}\" VARCHAR", safe_col)
                })
                .collect();
            let create_sql = format!(
                "CREATE OR REPLACE TABLE \"{}\" ({})",
                safe_table,
                col_defs.join(", ")
            );
            if let Err(e) = ws.execute_sql(&create_sql).await {
                return request_error(
                    "CREATE_TABLE_FAILED",
                    &format!("Failed to create table: {:#}", e),
                    None,
                )
                .to_response(StatusCode::INTERNAL_SERVER_ERROR);
            }

            // Insert rows in batches
            for chunk in parsed.rows.chunks(500) {
                let value_rows: Vec<String> = chunk
                    .iter()
                    .map(|row| {
                        let vals: Vec<String> = row
                            .iter()
                            .map(|cell| match cell {
                                Some(v) => format!("'{}'", v.replace('\'', "''")),
                                None => "NULL".to_string(),
                            })
                            .collect();
                        format!("({})", vals.join(", "))
                    })
                    .collect();

                let insert_sql = format!(
                    "INSERT INTO \"{}\" VALUES {}",
                    safe_table,
                    value_rows.join(", ")
                );
                if let Err(e) = ws.execute_sql(&insert_sql).await {
                    return request_error(
                        "INSERT_FAILED",
                        &format!("Failed to insert rows: {:#}", e),
                        None,
                    )
                    .to_response(StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
        }
        _ => {
            return bad_request(
                format!("Unsupported file type '.{}'. Supported: csv, tsv, json, xlsx, xls, xlsb, ods", ext),
                None,
            );
        }
    }

    // Get row count and column info
    let safe_table = table_name.replace('\'', "''");
    let row_count = ws
        .query_count(&format!("SELECT COUNT(*) FROM \"{}\"", table_name.replace('"', "\"\"")))
        .await
        .unwrap_or(0);

    let columns = ws
        .query_rows(&format!(
            "SELECT column_name FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position",
            safe_table
        ))
        .await
        .unwrap_or_default();

    let column_names: Vec<String> = columns
        .iter()
        .filter_map(|c| c.get("column_name").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();
    let column_count = column_names.len() as i64;

    // Insert/update metadata
    let meta_sql = format!(
        "INSERT OR REPLACE INTO __workspace_meta (table_name, original_filename, uploaded_at, row_count, column_count) \
         VALUES ('{}', '{}', CURRENT_TIMESTAMP, {}, {})",
        safe_table,
        original_filename.replace('\'', "''"),
        row_count,
        column_count
    );
    let _ = ws.execute_sql(&meta_sql).await;

    (
        StatusCode::OK,
        Json(json!({
            "table_name": table_name,
            "row_count": row_count,
            "column_count": column_count,
            "columns": column_names,
        })),
    )
        .into_response()
}

// ============================================================================
// List Tables Handler
// ============================================================================

pub async fn list_tables_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let ws = match get_workspace(&state) {
        Ok(ws) => ws,
        Err(resp) => return resp,
    };

    let tables = match ws
        .query_rows(
            "SELECT table_name, original_filename, uploaded_at, row_count, column_count \
             FROM __workspace_meta ORDER BY uploaded_at DESC",
        )
        .await
    {
        Ok(t) => t,
        Err(e) => {
            return request_error(
                "QUERY_FAILED",
                &format!("Failed to list tables: {:#}", e),
                None,
            )
            .to_response(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    (StatusCode::OK, Json(json!({ "tables": tables }))).into_response()
}

// ============================================================================
// Delete Table Handler
// ============================================================================

pub async fn delete_table_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }
    if let Err(resp) = require_write_mode(&auth, &state) {
        return resp;
    }

    let ws = match get_workspace(&state) {
        Ok(ws) => ws,
        Err(resp) => return resp,
    };

    let safe_name = name.replace('"', "\"\"");
    if let Err(e) = ws
        .execute_sql(&format!("DROP TABLE IF EXISTS \"{}\"", safe_name))
        .await
    {
        return request_error(
            "DROP_FAILED",
            &format!("Failed to drop table: {:#}", e),
            None,
        )
        .to_response(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let safe_meta = name.replace('\'', "''");
    let _ = ws
        .execute_sql(&format!(
            "DELETE FROM __workspace_meta WHERE table_name = '{}'",
            safe_meta
        ))
        .await;

    (StatusCode::OK, Json(json!({ "success": true }))).into_response()
}

// ============================================================================
// Clear Handler
// ============================================================================

pub async fn clear_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }
    if let Err(resp) = require_write_mode(&auth, &state) {
        return resp;
    }

    let ws = match get_workspace(&state) {
        Ok(ws) => ws,
        Err(resp) => return resp,
    };

    // Get all table names from metadata
    let tables = match ws
        .query_rows("SELECT table_name FROM __workspace_meta")
        .await
    {
        Ok(t) => t,
        Err(e) => {
            return request_error(
                "QUERY_FAILED",
                &format!("Failed to list tables: {:#}", e),
                None,
            )
            .to_response(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let mut dropped = 0;
    for row in &tables {
        if let Some(name) = row.get("table_name").and_then(|v| v.as_str()) {
            let safe_name = name.replace('"', "\"\"");
            if ws
                .execute_sql(&format!("DROP TABLE IF EXISTS \"{}\"", safe_name))
                .await
                .is_ok()
            {
                dropped += 1;
            }
        }
    }

    let _ = ws
        .execute_sql("DELETE FROM __workspace_meta")
        .await;

    (
        StatusCode::OK,
        Json(json!({ "success": true, "tables_dropped": dropped })),
    )
        .into_response()
}

// ============================================================================
// Query Handler
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct WorkspaceQueryRequest {
    pub query: String,
    #[serde(default)]
    pub pagination: Option<bool>,
    #[serde(default)]
    pub batch_size: Option<usize>,
    #[serde(default)]
    pub include_metadata: Option<bool>,
}

pub async fn query_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<WorkspaceQueryRequest>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let ws = match get_workspace(&state) {
        Ok(ws) => ws,
        Err(resp) => return resp,
    };

    let params = QueryParams {
        database: "workspace".to_string(),
        query: req.query,
        pagination: req.pagination.unwrap_or(false),
        batch_size: req.batch_size.unwrap_or(1000),
        include_metadata: req.include_metadata.unwrap_or(true),
        ..Default::default()
    };

    match ws.execute_query(&params).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => request_error(
            "QUERY_FAILED",
            &format!("{:#}", e),
            None,
        )
        .to_response(StatusCode::BAD_REQUEST),
    }
}

// ============================================================================
// List Files Handler
// ============================================================================

pub async fn list_files_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let ws_dir = match &state.workspace_dir {
        Some(d) => d,
        None => {
            return request_error(
                "WORKSPACE_NOT_AVAILABLE",
                "Workspace directory not configured",
                None,
            )
            .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let entries = match std::fs::read_dir(ws_dir) {
        Ok(e) => e,
        Err(e) => {
            return request_error(
                "IO_ERROR",
                &format!("Failed to read workspace directory: {}", e),
                None,
            )
            .to_response(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let mut files = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        // Skip internal files
        if name.starts_with('_') || name.starts_with('.') || name == "workspace.duckdb" || name.ends_with(".wal") {
            continue;
        }
        let meta = entry.metadata().ok();
        files.push(json!({
            "name": name,
            "size_bytes": meta.as_ref().map(|m| m.len()).unwrap_or(0),
            "modified": meta.and_then(|m| m.modified().ok()).map(|t| {
                let dt: chrono::DateTime<chrono::Utc> = t.into();
                dt.to_rfc3339()
            }),
        }));
    }

    (StatusCode::OK, Json(json!({ "files": files }))).into_response()
}

// ============================================================================
// Type Mapping
// ============================================================================

/// Map a source database column type to a DuckDB type.
pub fn map_to_duckdb_type(source_type: &str) -> &'static str {
    let upper = source_type.to_uppercase();
    match upper.as_str() {
        // Integer types
        "INT" | "INTEGER" | "INT4" => "INTEGER",
        "BIGINT" | "INT8" => "BIGINT",
        "SMALLINT" | "INT2" | "TINYINT" => "SMALLINT",
        // ClickHouse integer types
        "UINT8" | "UINT16" | "INT16" => "SMALLINT",
        "UINT32" | "INT32" => "INTEGER",
        "UINT64" | "INT64" => "BIGINT",
        "UINT128" | "INT128" | "UINT256" | "INT256" => "BIGINT",

        // Floating point
        "FLOAT" | "REAL" | "FLOAT4" => "FLOAT",
        "DOUBLE" | "FLOAT8" | "DOUBLE PRECISION" => "DOUBLE",
        // ClickHouse float types
        "FLOAT32" => "FLOAT",
        "FLOAT64" => "DOUBLE",

        // Decimal/numeric
        _ if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") || upper.starts_with("MONEY") => "DOUBLE",

        // String types
        _ if upper.starts_with("VARCHAR") || upper.starts_with("NVARCHAR")
            || upper.starts_with("CHAR") || upper.starts_with("NCHAR")
            || upper.starts_with("TEXT") || upper.starts_with("NTEXT") => "VARCHAR",
        "STRING" | "XML" | "UNIQUEIDENTIFIER" => "VARCHAR",
        // ClickHouse string types
        _ if upper.starts_with("FIXEDSTRING") || upper.starts_with("ENUM") => "VARCHAR",
        "UUID" => "VARCHAR",

        // Boolean
        "BIT" | "BOOLEAN" | "BOOL" => "BOOLEAN",

        // Date/time
        "DATE" | "DATE32" => "DATE",
        "TIME" | "TIME WITHOUT TIME ZONE" => "TIME",
        "TIMESTAMP" | "DATETIME" | "DATETIME2" | "SMALLDATETIME"
            | "TIMESTAMP WITHOUT TIME ZONE" => "TIMESTAMP",
        _ if upper.starts_with("TIMESTAMP") || upper.starts_with("DATETIME") => "TIMESTAMP",
        "DATETIMEOFFSET" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "TIMESTAMP",

        // Binary
        _ if upper.starts_with("BINARY") || upper.starts_with("VARBINARY") || upper == "IMAGE" => "BLOB",

        // Default fallback
        _ => "VARCHAR",
    }
}

// ============================================================================
// Workspace Import (shared logic)
// ============================================================================

/// Result of a workspace import operation.
#[derive(Debug, serde::Serialize)]
pub struct ImportResult {
    pub table_name: String,
    pub row_count: usize,
    pub column_count: usize,
    pub columns: Vec<serde_json::Value>,
}

/// Import the result of a query from a source connection into the DuckDB workspace.
pub async fn do_workspace_import(
    registry: &ConnectionRegistry,
    ws: &crate::db::duckdb_backend::DuckDbBackend,
    connection: Option<&str>,
    database: Option<&str>,
    query: &str,
    table_name: &str,
    if_exists: Option<&str>,
) -> Result<ImportResult, String> {
    // Validate table name
    if table_name.is_empty()
        || table_name.starts_with("__")
        || !table_name.chars().all(|c| c.is_alphanumeric() || c == '_')
    {
        return Err("Invalid table name. Use only letters, numbers, and underscores. Must not start with '__'.".to_string());
    }

    // Only allow read queries
    if !is_read_only_safe(query) {
        return Err("Only SELECT/read queries can be imported into the workspace".to_string());
    }

    // Resolve source connection
    let db = registry.resolve(connection).map_err(|e| format!("{}", e))?;

    let database = database
        .map(|d| d.to_string())
        .unwrap_or_else(|| db.default_database().to_string());

    let qp = QueryParams {
        database,
        query: query.to_string(),
        include_metadata: true,
        ..Default::default()
    };

    let result = db.execute_query(&qp).await
        .map_err(|e| format!("Source query failed: {:#}", e))?;

    // Handle if_exists
    let if_exists = if_exists.unwrap_or("fail");
    let table_exists = ws
        .query_count(&format!(
            "SELECT COUNT(*) FROM __workspace_meta WHERE table_name = '{}'",
            table_name
        ))
        .await
        .unwrap_or(0)
        > 0;

    if table_exists {
        match if_exists {
            "replace" => {
                let _ = ws.execute_sql(&format!("DROP TABLE IF EXISTS \"{}\"", table_name)).await;
                let _ = ws.execute_sql(&format!(
                    "DELETE FROM __workspace_meta WHERE table_name = '{}'",
                    table_name
                )).await;
            }
            _ => {
                return Err(format!(
                    "Table '{}' already exists. Use if_exists='replace' to overwrite.",
                    table_name
                ));
            }
        }
    }

    // Build CREATE TABLE from metadata
    let columns = match &result.metadata {
        Some(meta) => &meta.columns,
        None => return Err("Source query returned no column metadata".to_string()),
    };

    if columns.is_empty() {
        return Err("Source query returned no columns".to_string());
    }

    let col_defs: Vec<String> = columns
        .iter()
        .map(|col| {
            let duck_type = map_to_duckdb_type(&col.data_type);
            format!("\"{}\" {}", col.name, duck_type)
        })
        .collect();

    let create_sql = format!("CREATE TABLE \"{}\" ({})", table_name, col_defs.join(", "));
    ws.execute_sql(&create_sql)
        .await
        .map_err(|e| format!("Failed to create workspace table: {:#}", e))?;

    // Insert rows in batches
    let col_names: Vec<String> = columns.iter().map(|c| format!("\"{}\"", c.name)).collect();
    let col_names_str = col_names.join(", ");
    let mut inserted = 0usize;

    for chunk in result.data.chunks(500) {
        if chunk.is_empty() {
            continue;
        }

        let mut values_parts: Vec<String> = Vec::with_capacity(chunk.len());
        for row in chunk {
            let vals: Vec<String> = columns
                .iter()
                .map(|col| match row.get(&col.name) {
                    None | Some(serde_json::Value::Null) => "NULL".to_string(),
                    Some(serde_json::Value::Bool(b)) => b.to_string(),
                    Some(serde_json::Value::Number(n)) => n.to_string(),
                    Some(serde_json::Value::String(s)) => {
                        format!("'{}'", s.replace('\'', "''"))
                    }
                    Some(other) => format!("'{}'", other.to_string().replace('\'', "''")),
                })
                .collect();
            values_parts.push(format!("({})", vals.join(", ")));
        }

        let insert_sql = format!(
            "INSERT INTO \"{}\" ({}) VALUES {}",
            table_name, col_names_str, values_parts.join(", ")
        );

        if let Err(e) = ws.execute_sql(&insert_sql).await {
            let _ = ws.execute_sql(&format!("DROP TABLE IF EXISTS \"{}\"", table_name)).await;
            return Err(format!("Failed to insert data: {:#}", e));
        }
        inserted += chunk.len();
    }

    // Update workspace metadata
    let meta_sql = format!(
        "INSERT INTO __workspace_meta (table_name, original_filename, row_count, column_count) VALUES ('{}', '{}', {}, {})",
        table_name, "query_import", inserted, columns.len()
    );
    let _ = ws.execute_sql(&meta_sql).await;

    let col_info: Vec<serde_json::Value> = columns
        .iter()
        .map(|c| json!({"name": c.name, "type": map_to_duckdb_type(&c.data_type)}))
        .collect();

    Ok(ImportResult {
        table_name: table_name.to_string(),
        row_count: inserted,
        column_count: columns.len(),
        columns: col_info,
    })
}

// ============================================================================
// Import Query REST Handler
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ImportQueryRequest {
    pub connection: Option<String>,
    pub database: Option<String>,
    pub query: String,
    pub table_name: String,
    pub if_exists: Option<String>,
}

pub async fn import_query_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<ImportQueryRequest>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }
    if let Err(resp) = require_write_mode(&auth, &state) {
        return resp;
    }

    // Check connection-level access
    if let Some(ref access_db) = state.access_db {
        let conn_name = req.connection.as_deref().unwrap_or("");
        if !conn_name.is_empty() {
            match &auth {
                AuthResult::ServiceAccountAccess { account_name } => {
                    if !access_db.check_sa_connection_access(account_name, conn_name) {
                        return request_error(
                            "FORBIDDEN",
                            &format!("Access denied to connection '{}'", conn_name),
                            None,
                        )
                        .to_response(StatusCode::FORBIDDEN);
                    }
                }
                _ => {
                    if let Some(email) = extract_email(&auth) {
                        if !access_db.check_connection_access(email, conn_name) {
                            return request_error(
                                "FORBIDDEN",
                                &format!("Access denied to connection '{}'", conn_name),
                                None,
                            )
                            .to_response(StatusCode::FORBIDDEN);
                        }
                    }
                }
            }
        }
    }

    let ws = match get_workspace(&state) {
        Ok(ws) => ws,
        Err(resp) => return resp,
    };

    match do_workspace_import(
        &state.registry,
        ws,
        req.connection.as_deref(),
        req.database.as_deref(),
        &req.query,
        &req.table_name,
        req.if_exists.as_deref(),
    )
    .await
    {
        Ok(result) => (StatusCode::OK, Json(json!({
            "table_name": result.table_name,
            "row_count": result.row_count,
            "column_count": result.column_count,
            "columns": result.columns,
        }))).into_response(),
        Err(e) => request_error("IMPORT_FAILED", &e, None)
            .to_response(StatusCode::BAD_REQUEST),
    }
}
