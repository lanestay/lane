use axum::{
    extract::{Multipart, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::auth::{self, AuthResult};
use crate::auth::access_control::{SqlMode, StoragePermAction};
use crate::db::DatabaseBackend;
use crate::storage::StorageClient;
use crate::export::{ExportFormat, infer_export_format};

use super::AppState;

// ============================================================================
// Helpers
// ============================================================================

async fn check_auth(headers: &HeaderMap, state: &AppState) -> Result<AuthResult, Response> {
    let auth = auth::authenticate(headers, state).await;
    match &auth {
        AuthResult::Denied(reason) => Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": reason})),
        )
            .into_response()),
        _ => Ok(auth),
    }
}

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
                    return Err((
                        StatusCode::FORBIDDEN,
                        Json(json!({"error": "Storage write operations require at least supervised SQL access.", "code": "FORBIDDEN"})),
                    ).into_response());
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
                    return Err((
                        StatusCode::FORBIDDEN,
                        Json(json!({"error": "Storage write operations require at least supervised SQL access.", "code": "FORBIDDEN"})),
                    ).into_response());
                }
                _ => {}
            }
        }
    }
    Ok(())
}

async fn resolve_client(
    state: &AppState,
    connection: &str,
) -> Result<Arc<StorageClient>, Response> {
    state
        .storage_registry
        .get(connection)
        .await
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("Storage connection '{}' not found", connection)})),
            )
                .into_response()
        })
}

/// Check connection-level access for a storage connection.
fn check_storage_connection_access(auth: &AuthResult, state: &AppState, connection: &str) -> Result<(), Response> {
    if let Some(ref access_db) = state.access_db {
        let allowed = match auth {
            AuthResult::FullAccess => true,
            AuthResult::SessionAccess { email, .. } | AuthResult::TokenAccess { email, .. } => {
                access_db.check_connection_access(email, connection)
            }
            AuthResult::ServiceAccountAccess { account_name } => {
                access_db.check_sa_connection_access(account_name, connection)
            }
            _ => false,
        };
        if !allowed {
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({"error": format!("Access denied to storage connection '{}'", connection), "code": "FORBIDDEN"})),
            ).into_response());
        }
    }
    Ok(())
}

/// Check bucket-level storage permission for a specific action.
fn check_bucket_access(auth: &AuthResult, state: &AppState, connection: &str, bucket: &str, action: StoragePermAction) -> Result<(), Response> {
    if let Some(ref access_db) = state.access_db {
        let allowed = match auth {
            AuthResult::FullAccess => true,
            AuthResult::SessionAccess { email, .. } | AuthResult::TokenAccess { email, .. } => {
                access_db.check_storage_access(email, connection, bucket, action)
            }
            AuthResult::ServiceAccountAccess { account_name } => {
                access_db.check_sa_storage_access(account_name, connection, bucket, action)
            }
            _ => false,
        };
        if !allowed {
            let action_str = match action {
                StoragePermAction::Read => "read",
                StoragePermAction::Write => "write",
                StoragePermAction::Delete => "delete",
            };
            return Err((
                StatusCode::FORBIDDEN,
                Json(json!({"error": format!("Storage {} access denied for bucket '{}'", action_str, bucket), "code": "FORBIDDEN"})),
            ).into_response());
        }
    }
    Ok(())
}

/// Log a storage operation to the audit log.
fn log_storage_access(auth: &AuthResult, state: &AppState, connection: &str, action: &str, details: &str) {
    if let Some(ref access_db) = state.access_db {
        let (email, token_prefix) = match auth {
            AuthResult::SessionAccess { email, .. } => (Some(email.as_str()), None),
            AuthResult::TokenAccess { email, .. } => (Some(email.as_str()), None),
            AuthResult::ServiceAccountAccess { account_name } => (Some(account_name.as_str()), None),
            _ => (None, None),
        };
        access_db.log_access(
            token_prefix,
            email,
            None,
            Some(connection),
            Some("storage"),
            action,
            Some(details),
        );
    }
}

// ============================================================================
// Query params
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ConnectionQuery {
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ObjectsQuery {
    pub connection: String,
    pub bucket: String,
    pub prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ObjectKeyQuery {
    pub connection: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateBucketRequest {
    pub connection: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct PreviewRequest {
    pub connection: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Deserialize)]
pub struct ExportQueryRequest {
    pub connection: Option<String>,
    pub database: Option<String>,
    pub query: String,
    pub storage_connection: String,
    pub bucket: String,
    pub key: String,
    pub format: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ImportToWorkspaceRequest {
    pub connection: String,
    pub bucket: String,
    pub key: String,
    pub table_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WorkspaceExportRequest {
    pub query: String,
    pub storage_connection: String,
    pub bucket: String,
    pub key: String,
    pub format: Option<String>,
}

// ============================================================================
// Handlers
// ============================================================================

/// GET /api/lane/storage/connections — list storage connection names
pub async fn list_storage_connections_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let names = state.storage_registry.list_names().await;
    // Filter by connection-level access
    let names: Vec<String> = names.into_iter().filter(|name| {
        check_storage_connection_access(&auth, &state, name).is_ok()
    }).collect();
    (StatusCode::OK, Json(json!({ "connections": names }))).into_response()
}

/// GET /api/lane/storage/buckets?connection=X
pub async fn list_buckets_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ConnectionQuery>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let conn_name = match q.connection {
        Some(c) => c,
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "connection parameter required"}))).into_response(),
    };
    if let Err(resp) = check_storage_connection_access(&auth, &state, &conn_name) {
        return resp;
    }
    let client = match resolve_client(&state, &conn_name).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    match client.list_buckets().await {
        Ok(buckets) => (StatusCode::OK, Json(json!({ "buckets": buckets }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("{:#}", e)}))).into_response(),
    }
}

/// POST /api/lane/storage/buckets — create a bucket
pub async fn create_bucket_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateBucketRequest>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_write_mode(&auth, &state) {
        return resp;
    }
    if let Err(resp) = check_storage_connection_access(&auth, &state, &body.connection) {
        return resp;
    }
    if let Err(resp) = check_bucket_access(&auth, &state, &body.connection, &body.name, StoragePermAction::Write) {
        return resp;
    }
    let client = match resolve_client(&state, &body.connection).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    match client.create_bucket(&body.name).await {
        Ok(()) => {
            log_storage_access(&auth, &state, &body.connection, "create_bucket", &body.name);
            (StatusCode::CREATED, Json(json!({"success": true, "name": body.name}))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("{:#}", e)}))).into_response(),
    }
}

/// DELETE /api/lane/storage/buckets/{name}?connection=X
pub async fn delete_bucket_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(q): Query<ConnectionQuery>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_write_mode(&auth, &state) {
        return resp;
    }
    let conn_name = match q.connection {
        Some(c) => c,
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "connection parameter required"}))).into_response(),
    };
    if let Err(resp) = check_storage_connection_access(&auth, &state, &conn_name) {
        return resp;
    }
    if let Err(resp) = check_bucket_access(&auth, &state, &conn_name, &name, StoragePermAction::Delete) {
        return resp;
    }
    let client = match resolve_client(&state, &conn_name).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    match client.delete_bucket(&name).await {
        Ok(()) => {
            log_storage_access(&auth, &state, &conn_name, "delete_bucket", &name);
            (StatusCode::OK, Json(json!({"success": true}))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("{:#}", e)}))).into_response(),
    }
}

/// GET /api/lane/storage/objects?connection=X&bucket=Y&prefix=Z
pub async fn list_objects_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ObjectsQuery>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = check_storage_connection_access(&auth, &state, &q.connection) {
        return resp;
    }
    if let Err(resp) = check_bucket_access(&auth, &state, &q.connection, &q.bucket, StoragePermAction::Read) {
        return resp;
    }
    let client = match resolve_client(&state, &q.connection).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    match client
        .list_objects(&q.bucket, q.prefix.as_deref(), Some("/"))
        .await
    {
        Ok(objects) => (StatusCode::OK, Json(json!({ "objects": objects }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("{:#}", e)}))).into_response(),
    }
}

/// POST /api/lane/storage/upload — multipart (connection, bucket, key, file)
pub async fn upload_object_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_write_mode(&auth, &state) {
        return resp;
    }

    let mut connection = None;
    let mut bucket = None;
    let mut key = None;
    let mut file_data: Option<Vec<u8>> = None;
    let mut content_type = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        let field_name = field.name().unwrap_or("").to_string();
        match field_name.as_str() {
            "connection" => {
                connection = field.text().await.ok();
            }
            "bucket" => {
                bucket = field.text().await.ok();
            }
            "key" => {
                key = field.text().await.ok();
            }
            "file" => {
                // Use filename as fallback key if not provided
                let fname = field.file_name().map(|s| s.to_string());
                let ct = field.content_type().map(|s| s.to_string());
                content_type = ct;
                if key.is_none() {
                    key = fname;
                }
                file_data = field.bytes().await.ok().map(|b| b.to_vec());
            }
            _ => {}
        }
    }

    let connection = match connection {
        Some(c) => c,
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "connection field required"}))).into_response(),
    };
    let bucket = match bucket {
        Some(b) => b,
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "bucket field required"}))).into_response(),
    };
    let key = match key {
        Some(k) => k,
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "key or file with filename required"}))).into_response(),
    };
    let data = match file_data {
        Some(d) => d,
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "file field required"}))).into_response(),
    };

    if let Err(resp) = check_storage_connection_access(&auth, &state, &connection) {
        return resp;
    }
    if let Err(resp) = check_bucket_access(&auth, &state, &connection, &bucket, StoragePermAction::Write) {
        return resp;
    }

    let client = match resolve_client(&state, &connection).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    match client
        .upload_object(&bucket, &key, &data, content_type.as_deref())
        .await
    {
        Ok(()) => {
            log_storage_access(&auth, &state, &connection, "upload_object", &format!("{}/{}", bucket, key));
            (StatusCode::OK, Json(json!({"success": true, "key": key, "size": data.len()}))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("{:#}", e)}))).into_response(),
    }
}

/// GET /api/lane/storage/download?connection=X&bucket=Y&key=Z
pub async fn download_object_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ObjectKeyQuery>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = check_storage_connection_access(&auth, &state, &q.connection) {
        return resp;
    }
    if let Err(resp) = check_bucket_access(&auth, &state, &q.connection, &q.bucket, StoragePermAction::Read) {
        return resp;
    }
    let client = match resolve_client(&state, &q.connection).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    match client.download_object(&q.bucket, &q.key).await {
        Ok(data) => {
            let filename = q.key.rsplit('/').next().unwrap_or(&q.key);
            let content_type = mime_from_extension(filename);
            (
                StatusCode::OK,
                [
                    (axum::http::header::CONTENT_TYPE, content_type),
                    (
                        axum::http::header::CONTENT_DISPOSITION,
                        format!("attachment; filename=\"{}\"", filename),
                    ),
                ],
                data,
            )
                .into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("{:#}", e)}))).into_response()
        }
    }
}

/// DELETE /api/lane/storage/objects?connection=X&bucket=Y&key=Z
pub async fn delete_object_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ObjectKeyQuery>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_write_mode(&auth, &state) {
        return resp;
    }
    if let Err(resp) = check_storage_connection_access(&auth, &state, &q.connection) {
        return resp;
    }
    if let Err(resp) = check_bucket_access(&auth, &state, &q.connection, &q.bucket, StoragePermAction::Delete) {
        return resp;
    }
    let client = match resolve_client(&state, &q.connection).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    match client.delete_object(&q.bucket, &q.key).await {
        Ok(()) => {
            log_storage_access(&auth, &state, &q.connection, "delete_object", &format!("{}/{}", q.bucket, q.key));
            (StatusCode::OK, Json(json!({"success": true}))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("{:#}", e)}))).into_response(),
    }
}

/// GET /api/lane/storage/metadata?connection=X&bucket=Y&key=Z
pub async fn object_metadata_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ObjectKeyQuery>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = check_storage_connection_access(&auth, &state, &q.connection) {
        return resp;
    }
    if let Err(resp) = check_bucket_access(&auth, &state, &q.connection, &q.bucket, StoragePermAction::Read) {
        return resp;
    }
    let client = match resolve_client(&state, &q.connection).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    match client.object_metadata(&q.bucket, &q.key).await {
        Ok(meta) => (StatusCode::OK, Json(json!(meta))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("{:#}", e)}))).into_response(),
    }
}

/// POST /api/lane/storage/preview — download to workspace dir, load into DuckDB
pub async fn preview_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<PreviewRequest>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = check_storage_connection_access(&auth, &state, &body.connection) {
        return resp;
    }
    if let Err(resp) = check_bucket_access(&auth, &state, &body.connection, &body.bucket, StoragePermAction::Read) {
        return resp;
    }

    let _ = &auth; // suppress unused warning
    let client = match resolve_client(&state, &body.connection).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    // Download the file
    let data = match client.download_object(&body.bucket, &body.key).await {
        Ok(d) => d,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Download failed: {:#}", e)}))).into_response(),
    };

    // Check workspace is available
    #[cfg(feature = "duckdb_backend")]
    {
        let ws_dir = match state.workspace_dir.as_ref() {
            Some(d) => d,
            None => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "Workspace directory not available"}))).into_response(),
        };
        let ws_db = match state.workspace_db.as_ref() {
            Some(d) => d,
            None => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "DuckDB workspace not available"}))).into_response(),
        };

        // Write file to workspace dir
        let filename = body.key.rsplit('/').next().unwrap_or(&body.key);
        let file_path = ws_dir.join(filename);
        if let Err(e) = tokio::fs::write(&file_path, &data).await {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to write file: {}", e)}))).into_response();
        }

        // Load into DuckDB based on extension
        let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
        let table_name = sanitize_table_name(filename);
        let path_str = file_path.to_string_lossy();

        let create_sql = match ext.as_str() {
            "csv" | "tsv" => format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_csv_auto('{}')",
                table_name, path_str
            ),
            "parquet" => format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_parquet('{}')",
                table_name, path_str
            ),
            "json" | "jsonl" | "ndjson" => format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_json_auto('{}')",
                table_name, path_str
            ),
            "xlsx" | "xls" => format!(
                "INSTALL spatial; LOAD spatial; CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM st_read('{}')",
                table_name, path_str
            ),
            _ => {
                return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("Unsupported file type for preview: .{}", ext)}))).into_response();
            }
        };

        match ws_db.execute_sql(&create_sql).await {
            Ok(_) => {
                // Get row count
                let count_sql = format!("SELECT COUNT(*) FROM \"{}\"", table_name);
                let row_count = ws_db.query_count(&count_sql).await.unwrap_or(0);

                (StatusCode::OK, Json(json!({
                    "success": true,
                    "table_name": table_name,
                    "filename": filename,
                    "row_count": row_count,
                }))).into_response()
            }
            Err(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to load into DuckDB: {:#}", e)}))).into_response()
            }
        }
    }

    #[cfg(not(feature = "duckdb_backend"))]
    {
        let _ = data;
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "DuckDB workspace feature not enabled"}))).into_response()
    }
}

// ============================================================================
// Export Query Results to Storage
// ============================================================================

/// POST /api/lane/storage/export-query — run a SQL query and upload results to storage
pub async fn export_query_to_storage_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<ExportQueryRequest>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_write_mode(&auth, &state) {
        return resp;
    }

    // Check storage connection access + bucket write
    if let Err(resp) = check_storage_connection_access(&auth, &state, &body.storage_connection) {
        return resp;
    }
    if let Err(resp) = check_bucket_access(&auth, &state, &body.storage_connection, &body.bucket, StoragePermAction::Write) {
        return resp;
    }

    // Infer format
    let fmt = match infer_export_format(&body.key, body.format.as_deref()) {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    };

    // Resolve database connection and execute query
    let db = match state.registry.resolve(body.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("{}", e)}))).into_response(),
    };

    let params = crate::query::QueryParams {
        database: body.database.unwrap_or_else(|| db.default_database().to_string()),
        query: body.query.clone(),
        pagination: false,
        include_metadata: true,
        ..Default::default()
    };

    let result = match db.execute_query(&params).await {
        Ok(r) => r,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Query failed: {:#}", e)}))).into_response(),
    };

    let row_count = result.total_rows;

    // Convert to bytes based on format
    let data = match fmt {
        ExportFormat::Csv => {
            match crate::export::csv::query_result_to_csv(&result) {
                Ok(d) => d,
                Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("CSV conversion failed: {:#}", e)}))).into_response(),
            }
        }
        ExportFormat::Json => {
            match serde_json::to_vec_pretty(&result.data) {
                Ok(d) => d,
                Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("JSON conversion failed: {}", e)}))).into_response(),
            }
        }
        #[cfg(feature = "xlsx")]
        ExportFormat::Xlsx => {
            match crate::export::xlsx::query_result_to_xlsx(&result) {
                Ok(d) => d,
                Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("XLSX conversion failed: {:#}", e)}))).into_response(),
            }
        }
        ExportFormat::Parquet => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": "Parquet export is only supported from workspace queries. Use csv, json, or xlsx."}))).into_response();
        }
    };

    let size = data.len();

    // Upload to storage
    let client = match resolve_client(&state, &body.storage_connection).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    match client.upload_object(&body.bucket, &body.key, &data, Some(fmt.content_type())).await {
        Ok(()) => {
            log_storage_access(&auth, &state, &body.storage_connection, "export_query", &format!("{}/{} ({})", body.bucket, body.key, fmt.as_str()));
            (StatusCode::OK, Json(json!({
                "success": true,
                "key": body.key,
                "size": size,
                "row_count": row_count,
                "format": fmt.as_str(),
            }))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Upload failed: {:#}", e)}))).into_response(),
    }
}

// ============================================================================
// Import from Storage to Workspace
// ============================================================================

/// POST /api/lane/storage/import-to-workspace — download a file from storage and load into DuckDB workspace
pub async fn import_to_workspace_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<ImportToWorkspaceRequest>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = check_storage_connection_access(&auth, &state, &body.connection) {
        return resp;
    }
    if let Err(resp) = check_bucket_access(&auth, &state, &body.connection, &body.bucket, StoragePermAction::Read) {
        return resp;
    }

    let client = match resolve_client(&state, &body.connection).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    // Download the file
    let data = match client.download_object(&body.bucket, &body.key).await {
        Ok(d) => d,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Download failed: {:#}", e)}))).into_response(),
    };

    #[cfg(feature = "duckdb_backend")]
    {
        let ws_dir = match state.workspace_dir.as_ref() {
            Some(d) => d,
            None => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "Workspace directory not available"}))).into_response(),
        };
        let ws_db = match state.workspace_db.as_ref() {
            Some(d) => d,
            None => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "DuckDB workspace not available"}))).into_response(),
        };

        // Write file to workspace dir
        let filename = body.key.rsplit('/').next().unwrap_or(&body.key);
        let file_path = ws_dir.join(filename);
        if let Err(e) = tokio::fs::write(&file_path, &data).await {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to write file: {}", e)}))).into_response();
        }

        // Determine table name
        let table_name = body.table_name.unwrap_or_else(|| sanitize_table_name(filename));

        // Load into DuckDB based on extension
        let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
        let path_str = file_path.to_string_lossy();

        let create_sql = match ext.as_str() {
            "csv" | "tsv" => format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_csv_auto('{}')",
                table_name, path_str
            ),
            "parquet" => format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_parquet('{}')",
                table_name, path_str
            ),
            "json" | "jsonl" | "ndjson" => format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_json_auto('{}')",
                table_name, path_str
            ),
            "xlsx" | "xls" => format!(
                "INSTALL spatial; LOAD spatial; CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM st_read('{}')",
                table_name, path_str
            ),
            _ => {
                return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("Unsupported file type for import: .{}", ext)}))).into_response();
            }
        };

        match ws_db.execute_sql(&create_sql).await {
            Ok(_) => {
                let count_sql = format!("SELECT COUNT(*) FROM \"{}\"", table_name);
                let row_count = ws_db.query_count(&count_sql).await.unwrap_or(0);

                // Update workspace metadata
                let meta_sql = format!(
                    "INSERT OR REPLACE INTO __workspace_meta (table_name, original_filename, uploaded_at, row_count, column_count) \
                     VALUES ('{}', '{}', CURRENT_TIMESTAMP, {}, (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{}'))",
                    table_name.replace('\'', "''"),
                    filename.replace('\'', "''"),
                    row_count,
                    table_name
                );
                let _ = ws_db.execute_sql(&meta_sql).await;

                log_storage_access(&auth, &state, &body.connection, "import_to_workspace", &format!("{}/{} -> {}", body.bucket, body.key, table_name));

                (StatusCode::OK, Json(json!({
                    "success": true,
                    "table_name": table_name,
                    "row_count": row_count,
                    "source": {
                        "connection": body.connection,
                        "bucket": body.bucket,
                        "key": body.key,
                    }
                }))).into_response()
            }
            Err(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to load into DuckDB: {:#}", e)}))).into_response()
            }
        }
    }

    #[cfg(not(feature = "duckdb_backend"))]
    {
        let _ = data;
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "DuckDB workspace feature not enabled"}))).into_response()
    }
}

// ============================================================================
// Workspace Export to Storage
// ============================================================================

/// POST /api/lane/storage/workspace-export — query workspace DuckDB and upload results to storage
pub async fn workspace_export_to_storage_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<WorkspaceExportRequest>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_write_mode(&auth, &state) {
        return resp;
    }
    if let Err(resp) = check_storage_connection_access(&auth, &state, &body.storage_connection) {
        return resp;
    }
    if let Err(resp) = check_bucket_access(&auth, &state, &body.storage_connection, &body.bucket, StoragePermAction::Write) {
        return resp;
    }

    let fmt = match infer_export_format(&body.key, body.format.as_deref()) {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    };

    #[cfg(feature = "duckdb_backend")]
    {
        let ws_db = match state.workspace_db.as_ref() {
            Some(d) => d,
            None => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "DuckDB workspace not available"}))).into_response(),
        };

        let (data, row_count) = match fmt {
            ExportFormat::Parquet => {
                // Use DuckDB's native COPY for Parquet
                let ws_dir = match state.workspace_dir.as_ref() {
                    Some(d) => d,
                    None => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "Workspace directory not available"}))).into_response(),
                };

                let temp_path = ws_dir.join(format!("_export_tmp_{}.parquet", uuid::Uuid::new_v4()));
                let temp_path_str = temp_path.to_string_lossy().replace('\'', "''");

                let copy_sql = format!(
                    "COPY ({}) TO '{}' (FORMAT PARQUET)",
                    body.query, temp_path_str
                );

                if let Err(e) = ws_db.execute_sql(&copy_sql).await {
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Parquet export failed: {:#}", e)}))).into_response();
                }

                let bytes = match tokio::fs::read(&temp_path).await {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tokio::fs::remove_file(&temp_path).await;
                        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to read parquet file: {}", e)}))).into_response();
                    }
                };
                let _ = tokio::fs::remove_file(&temp_path).await;

                // Get row count by running the query
                let count_sql = format!("SELECT COUNT(*) FROM ({})", body.query);
                let row_count = ws_db.query_count(&count_sql).await.unwrap_or(0);

                (bytes, row_count)
            }
            _ => {
                // Execute workspace query to get results
                let params = crate::query::QueryParams {
                    database: "workspace".to_string(),
                    query: body.query.clone(),
                    pagination: false,
                    include_metadata: true,
                    ..Default::default()
                };

                let result = match ws_db.execute_query(&params).await {
                    Ok(r) => r,
                    Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Workspace query failed: {:#}", e)}))).into_response(),
                };

                let row_count = result.total_rows;

                let bytes = match fmt {
                    ExportFormat::Csv => {
                        match crate::export::csv::query_result_to_csv(&result) {
                            Ok(d) => d,
                            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("CSV conversion failed: {:#}", e)}))).into_response(),
                        }
                    }
                    ExportFormat::Json => {
                        match serde_json::to_vec_pretty(&result.data) {
                            Ok(d) => d,
                            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("JSON conversion failed: {}", e)}))).into_response(),
                        }
                    }
                    #[cfg(feature = "xlsx")]
                    ExportFormat::Xlsx => {
                        match crate::export::xlsx::query_result_to_xlsx(&result) {
                            Ok(d) => d,
                            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("XLSX conversion failed: {:#}", e)}))).into_response(),
                        }
                    }
                    ExportFormat::Parquet => unreachable!(),
                };

                (bytes, row_count)
            }
        };

        let size = data.len();

        let client = match resolve_client(&state, &body.storage_connection).await {
            Ok(c) => c,
            Err(resp) => return resp,
        };

        match client.upload_object(&body.bucket, &body.key, &data, Some(fmt.content_type())).await {
            Ok(()) => {
                log_storage_access(&auth, &state, &body.storage_connection, "workspace_export", &format!("{}/{} ({})", body.bucket, body.key, fmt.as_str()));
                (StatusCode::OK, Json(json!({
                    "success": true,
                    "key": body.key,
                    "size": size,
                    "row_count": row_count,
                    "format": fmt.as_str(),
                }))).into_response()
            }
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Upload failed: {:#}", e)}))).into_response(),
        }
    }

    #[cfg(not(feature = "duckdb_backend"))]
    {
        let _ = fmt;
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "DuckDB workspace feature not enabled"}))).into_response()
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn mime_from_extension(filename: &str) -> String {
    let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "csv" => "text/csv".to_string(),
        "json" => "application/json".to_string(),
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".to_string(),
        "xls" => "application/vnd.ms-excel".to_string(),
        "parquet" => "application/octet-stream".to_string(),
        "txt" => "text/plain".to_string(),
        "pdf" => "application/pdf".to_string(),
        "png" => "image/png".to_string(),
        "jpg" | "jpeg" => "image/jpeg".to_string(),
        _ => "application/octet-stream".to_string(),
    }
}

#[cfg(feature = "duckdb_backend")]
fn sanitize_table_name(filename: &str) -> String {
    let stem = filename.rsplit('.').skip(1).collect::<Vec<_>>();
    let stem = if stem.is_empty() {
        filename
    } else {
        &stem.into_iter().rev().collect::<Vec<_>>().join(".")
    };

    let sanitized: String = stem
        .to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect();

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

    if result.is_empty() || result.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
        format!("t_{}", result)
    } else {
        result
    }
}
