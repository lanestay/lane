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
use crate::db::DatabaseBackend;
use crate::query::QueryParams;

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
