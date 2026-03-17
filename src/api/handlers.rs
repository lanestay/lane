use axum::{
    body::Body,
    extract::{FromRequest, Multipart, State},
    http::header::CONTENT_TYPE,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use tokio_stream::wrappers::ReceiverStream;
use serde::Deserialize;
use serde_json::json;
use std::env;
use std::sync::Arc;

use crate::auth::{self, AuthResult};
use crate::auth::access_control::SqlMode;
use crate::db::metadata;
use crate::query::{CountMode, BlobFormat, QueryParams};
use super::errors::*;
use super::AppState;

/// Extract user email from auth result. Returns None for FullAccess (system API key).
fn extract_auth_email(auth: &AuthResult) -> Option<&str> {
    match auth {
        AuthResult::SessionAccess { email, .. } => Some(email.as_str()),
        AuthResult::TokenAccess { email, .. } => Some(email.as_str()),
        _ => None,
    }
}

/// Build a PiiContext from the auth result.
fn build_pii_context(auth: &AuthResult) -> crate::query::PiiContext {
    match auth {
        AuthResult::FullAccess => crate::query::PiiContext {
            token_pii_mode: None,
            email: None,
            is_full_access: true,
        },
        AuthResult::TokenAccess { email, pii_mode } => crate::query::PiiContext {
            token_pii_mode: pii_mode.clone(),
            email: Some(email.clone()),
            is_full_access: false,
        },
        AuthResult::SessionAccess { email, .. } => crate::query::PiiContext {
            token_pii_mode: None,
            email: Some(email.clone()),
            is_full_access: false,
        },
        AuthResult::ServiceAccountAccess { .. } => crate::query::PiiContext {
            token_pii_mode: None,
            email: None,
            is_full_access: true,
        },
        AuthResult::Denied(_) => crate::query::PiiContext {
            token_pii_mode: None,
            email: None,
            is_full_access: false,
        },
    }
}

/// Check if the authenticated user has access to a given connection.
/// Returns Ok(()) if allowed, Err(Response) if denied.
fn check_connection_access(auth: &AuthResult, state: &AppState, connection_name: &str) -> Result<(), Response> {
    if let AuthResult::ServiceAccountAccess { account_name } = auth {
        if let Some(ref access_db) = state.access_db {
            if !access_db.check_sa_connection_access(account_name, connection_name) {
                return Err(request_error(
                    "FORBIDDEN",
                    &format!("Access denied to connection '{}'", connection_name),
                    None,
                )
                .to_response(StatusCode::FORBIDDEN));
            }
        }
    } else if let Some(email) = extract_auth_email(auth) {
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
// Request Types
// ============================================================================

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct BatchQueryRequest {
    pub database: String,
    pub query: String,

    /// Named connection to route this request to (omit for default).
    pub connection: Option<String>,

    // Basic options
    #[serde(rename = "batchSize")]
    pub batch_size: Option<usize>,
    pub debug: Option<bool>,

    // JSON Output Options
    pub json: Option<bool>,
    pub quiet: Option<bool>,
    #[serde(rename = "jsonStream", alias = "ndjson")]
    pub json_stream: Option<bool>,
    #[serde(rename = "maxMemoryMb")]
    pub max_memory_mb: Option<u64>,
    #[serde(rename = "validationTimeoutSec")]
    pub validation_timeout_sec: Option<u64>,
    #[serde(rename = "bodyLimitMb")]
    pub body_limit_mb: Option<usize>,
    #[serde(rename = "includeMetadata")]
    pub include_metadata: Option<bool>,

    // Pagination Options
    pub pagination: Option<bool>,
    #[serde(rename = "countMode")]
    pub count_mode: Option<String>,
    pub order: Option<String>,
    #[serde(rename = "allowUnstablePagination")]
    pub allow_unstable_pagination: Option<bool>,

    // Data Format Options
    #[serde(rename = "preserveDecimalPrecision")]
    pub preserve_decimal_precision: Option<bool>,
    pub blobs: Option<String>,
    #[serde(rename = "piiMode", alias = "pii_mode")]
    pub pii_mode: Option<String>,
    #[serde(rename = "piiColumnHints", alias = "pii_column_hints")]
    pub pii_column_hints: Option<Vec<String>>,
    #[serde(rename = "piiColumnExcludes", alias = "pii_column_excludes")]
    pub pii_column_excludes: Option<Vec<String>>,
    #[serde(rename = "rowLimit")]
    pub row_limit: Option<usize>,

    // Multiple Result Set Options
    #[serde(rename = "resultSetSeparator")]
    pub result_set_separator: Option<String>,
    #[serde(rename = "outputFormat")]
    pub output_format: Option<String>,

    // Validation Options
    #[serde(rename = "skipValidation")]
    pub skip_validation: Option<bool>,
    #[serde(rename = "dryRun")]
    pub dry_run: Option<bool>,

    // DDL Execution Options
    /// Wrap query in EXEC sp_executesql for DDL statements (CREATE PROCEDURE, etc.)
    #[serde(rename = "execSql")]
    pub exec_sql: Option<bool>,

    /// Internal: set by handler when user has ReadOnly sql_mode (not from JSON input)
    #[serde(skip)]
    pub read_only: bool,
}

// ============================================================================
// Parsing Helpers
// ============================================================================

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

fn parse_u64(value: &str) -> Option<u64> {
    value.parse::<u64>().ok()
}

fn parse_usize(value: &str) -> Option<usize> {
    value.parse::<usize>().ok()
}

fn env_body_limit_mb() -> usize {
    env::var("LANE_BODY_LIMIT_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10)
}

fn parse_env_pii_mode() -> Option<String> {
    env::var("LANE_DEFAULT_PII_MODE")
        .ok()
        .map(|v| v.to_ascii_lowercase())
        .and_then(|v| match v.as_str() {
            "scrub" => Some(v),
            _ => None,
        })
}

fn parse_env_force_pii() -> bool {
    env::var("LANE_FORCE_PII")
        .ok()
        .map(|v| v.to_ascii_lowercase())
        .map(|v| v == "true" || v == "1" || v == "yes")
        .unwrap_or(false)
}

// ============================================================================
// Request Parsing (JSON + Multipart)
// ============================================================================

pub async fn parse_request(
    headers: &HeaderMap,
    req: axum::http::Request<Body>,
) -> Result<BatchQueryRequest, Response> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    let content_length = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let env_limit_mb = env_body_limit_mb();

    if content_type.starts_with("application/json") || content_type.is_empty() {
        parse_json_body(req, content_length, env_limit_mb).await
    } else if content_type.starts_with("multipart/form-data") {
        parse_multipart_body(req, content_length, env_limit_mb).await
    } else {
        Err(unsupported_media_type())
    }
}

pub async fn parse_json_body(
    req: axum::http::Request<Body>,
    content_length: Option<u64>,
    env_limit_mb: usize,
) -> Result<BatchQueryRequest, Response> {
    let mut read_limit_mb = env_limit_mb;
    if let Some(cl) = content_length {
        let clen_mb = (cl as usize).div_ceil(1024 * 1024);
        read_limit_mb = read_limit_mb.max(clen_mb);
    }
    let read_limit_bytes = read_limit_mb * 1024 * 1024;
    if let Some(clen) = content_length {
        if clen > read_limit_bytes as u64 {
            return Err(payload_too_large(read_limit_mb));
        }
    }

    let bytes = match axum::body::to_bytes(req.into_body(), read_limit_bytes).await {
        Ok(b) => b,
        Err(e) => return Err(bad_request(format!("Failed to read body: {}", e), None)),
    };

    let payload = serde_json::from_slice::<BatchQueryRequest>(&bytes).map_err(|e| {
        bad_request(
            format!("Invalid JSON payload: {}", e),
            Some("Ensure the body is valid JSON matching BatchQueryRequest".to_string()),
        )
    })?;

    if let Some(limit_mb) = payload.body_limit_mb {
        let limit_bytes = limit_mb * 1024 * 1024;
        if bytes.len() > limit_bytes {
            return Err(payload_too_large(limit_mb));
        }
    }

    Ok(payload)
}

pub async fn parse_multipart_body(
    req: axum::http::Request<Body>,
    content_length: Option<u64>,
    env_limit_mb: usize,
) -> Result<BatchQueryRequest, Response> {
    let mut read_limit_mb = env_limit_mb;
    if let Some(cl) = content_length {
        let clen_mb = (cl as usize).div_ceil(1024 * 1024);
        read_limit_mb = read_limit_mb.max(clen_mb);
    }
    let read_limit_bytes = read_limit_mb * 1024 * 1024;
    if let Some(clen) = content_length {
        if clen > read_limit_bytes as u64 {
            return Err(payload_too_large(read_limit_mb));
        }
    }

    let mut multipart = match Multipart::from_request(req, &()).await {
        Ok(m) => m,
        Err(e) => {
            return Err(bad_request(
                format!("Invalid multipart payload: {}", e),
                None,
            ))
        }
    };

    let mut database: Option<String> = None;
    let mut query: Option<String> = None;
    let mut batch_size: Option<usize> = None;
    let mut debug: Option<bool> = None;
    let mut json_opt: Option<bool> = None;
    let mut quiet: Option<bool> = None;
    let mut json_stream: Option<bool> = None;
    let mut max_memory_mb: Option<u64> = None;
    let mut include_metadata: Option<bool> = None;
    let mut pagination: Option<bool> = None;
    let mut count_mode: Option<String> = None;
    let mut order: Option<String> = None;
    let mut allow_unstable_pagination: Option<bool> = None;
    let mut preserve_decimal_precision: Option<bool> = None;
    let mut blobs: Option<String> = None;
    let mut pii_mode: Option<String> = None;
    let mut pii_column_hints: Vec<String> = Vec::new();
    let mut pii_column_excludes: Vec<String> = Vec::new();
    let mut result_set_separator: Option<String> = None;
    let mut output_format: Option<String> = None;
    let mut row_limit: Option<usize> = None;
    let mut skip_validation: Option<bool> = None;
    let mut dry_run: Option<bool> = None;
    let mut exec_sql: Option<bool> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| bad_request(format!("Failed to read multipart field: {}", e), None))?
    {
        let name = match field.name() {
            Some(n) => n.to_string(),
            None => continue,
        };

        match name.as_str() {
            "database" => {
                database = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| bad_request(format!("Invalid database value: {}", e), None))?,
                )
            }
            "query" => {
                if let Some(_file_name) = field.file_name() {
                    let data = field.bytes().await.map_err(|e| {
                        bad_request(format!("Failed to read query file: {}", e), None)
                    })?;
                    query = Some(String::from_utf8(data.to_vec()).map_err(|e| {
                        bad_request(
                            format!("Query file must be UTF-8: {}", e),
                            Some("Ensure uploaded SQL files are UTF-8 encoded".to_string()),
                        )
                    })?);
                } else {
                    query =
                        Some(field.text().await.map_err(|e| {
                            bad_request(format!("Invalid query field: {}", e), None)
                        })?);
                }
            }
            "batchSize" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid batchSize: {}", e), None))?;
                batch_size = parse_usize(&v);
                if batch_size.is_none() {
                    return Err(bad_request("batchSize must be an integer", None));
                }
            }
            "debug" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid debug: {}", e), None))?;
                debug = parse_bool(&v);
                if debug.is_none() {
                    return Err(bad_request("debug must be a boolean", None));
                }
            }
            "json" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid json: {}", e), None))?;
                json_opt = parse_bool(&v);
                if json_opt.is_none() {
                    return Err(bad_request("json must be a boolean", None));
                }
            }
            "quiet" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid quiet: {}", e), None))?;
                quiet = parse_bool(&v);
                if quiet.is_none() {
                    return Err(bad_request("quiet must be a boolean", None));
                }
            }
            "jsonStream" | "ndjson" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid jsonStream: {}", e), None))?;
                json_stream = parse_bool(&v);
                if json_stream.is_none() {
                    return Err(bad_request("jsonStream must be a boolean", None));
                }
            }
            "maxMemoryMb" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid maxMemoryMb: {}", e), None))?;
                max_memory_mb = parse_u64(&v);
                if max_memory_mb.is_none() {
                    return Err(bad_request("maxMemoryMb must be an integer", None));
                }
            }
            "includeMetadata" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid includeMetadata: {}", e), None))?;
                include_metadata = parse_bool(&v);
                if include_metadata.is_none() {
                    return Err(bad_request("includeMetadata must be a boolean", None));
                }
            }
            "pagination" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid pagination: {}", e), None))?;
                pagination = parse_bool(&v);
                if pagination.is_none() {
                    return Err(bad_request("pagination must be a boolean", None));
                }
            }
            "countMode" => {
                count_mode = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| bad_request(format!("Invalid countMode: {}", e), None))?,
                )
            }
            "order" => {
                order = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| bad_request(format!("Invalid order: {}", e), None))?,
                )
            }
            "allowUnstablePagination" => {
                let v = field.text().await.map_err(|e| {
                    bad_request(format!("Invalid allowUnstablePagination: {}", e), None)
                })?;
                allow_unstable_pagination = parse_bool(&v);
                if allow_unstable_pagination.is_none() {
                    return Err(bad_request(
                        "allowUnstablePagination must be a boolean",
                        None,
                    ));
                }
            }
            "preserveDecimalPrecision" => {
                let v = field.text().await.map_err(|e| {
                    bad_request(format!("Invalid preserveDecimalPrecision: {}", e), None)
                })?;
                preserve_decimal_precision = parse_bool(&v);
                if preserve_decimal_precision.is_none() {
                    return Err(bad_request(
                        "preserveDecimalPrecision must be a boolean",
                        None,
                    ));
                }
            }
            "blobs" => {
                blobs = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| bad_request(format!("Invalid blobs: {}", e), None))?,
                )
            }
            "piiMode" => {
                pii_mode = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| bad_request(format!("Invalid piiMode: {}", e), None))?,
                )
            }
            "piiColumnHints" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid piiColumnHints: {}", e), None))?;
                if !v.is_empty() {
                    pii_column_hints.push(v);
                }
            }
            "piiColumnExcludes" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid piiColumnExcludes: {}", e), None))?;
                if !v.is_empty() {
                    pii_column_excludes.push(v);
                }
            }
            "rowLimit" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid rowLimit: {}", e), None))?;
                row_limit = parse_usize(&v);
                if row_limit.is_none() {
                    return Err(bad_request("rowLimit must be an integer", None));
                }
            }
            "resultSetSeparator" => {
                result_set_separator =
                    Some(field.text().await.map_err(|e| {
                        bad_request(format!("Invalid resultSetSeparator: {}", e), None)
                    })?)
            }
            "outputFormat" => {
                output_format = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| bad_request(format!("Invalid outputFormat: {}", e), None))?,
                )
            }
            "skipValidation" | "skip_validation" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid skipValidation: {}", e), None))?;
                skip_validation = parse_bool(&v);
            }
            "dryRun" | "dry_run" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid dryRun: {}", e), None))?;
                dry_run = parse_bool(&v);
            }
            "execSql" | "exec_sql" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| bad_request(format!("Invalid execSql: {}", e), None))?;
                exec_sql = parse_bool(&v);
            }
            _ => {}
        }
    }

    let database = match database {
        Some(v) if !v.is_empty() => v,
        _ => return Err(bad_request("Missing required field 'database'", None)),
    };
    let query = match query {
        Some(v) if !v.is_empty() => v,
        _ => return Err(bad_request("Missing required field 'query'", None)),
    };

    let payload = BatchQueryRequest {
        database,
        query,
        connection: None,
        batch_size,
        debug,
        json: json_opt,
        quiet,
        json_stream,
        max_memory_mb,
        validation_timeout_sec: None,
        body_limit_mb: None,
        include_metadata,
        pagination,
        count_mode,
        order,
        allow_unstable_pagination,
        preserve_decimal_precision,
        blobs,
        pii_mode,
        pii_column_hints: if pii_column_hints.is_empty() {
            None
        } else {
            Some(pii_column_hints)
        },
        pii_column_excludes: if pii_column_excludes.is_empty() {
            None
        } else {
            Some(pii_column_excludes)
        },
        row_limit,
        result_set_separator,
        output_format,
        skip_validation,
        dry_run,
        exec_sql,
        read_only: false,
    };

    if let Some(limit_mb) = payload.body_limit_mb {
        let limit_bytes = limit_mb * 1024 * 1024;
        if let Some(clen) = content_length {
            if clen > limit_bytes as u64 {
                return Err(payload_too_large(limit_mb));
            }
        }
    }

    Ok(payload)
}

// ============================================================================
// Build QueryParams from BatchQueryRequest
// ============================================================================

fn build_query_params(payload: &BatchQueryRequest) -> Result<QueryParams, Response> {
    let pagination = payload.pagination.unwrap_or(false);
    let json_stream = payload.json_stream.unwrap_or(false);

    let max_memory_bytes = payload.max_memory_mb.map(|mb| mb as usize * 1024 * 1024).or({
        if !pagination && !json_stream {
            Some(256 * 1024 * 1024) // 256 MB default guard
        } else {
            None
        }
    });

    // Resolve PII mode with env defaults
    let mut pii_mode = payload.pii_mode.clone().map(|v| v.to_ascii_lowercase());
    let env_pii_default = parse_env_pii_mode();
    let force_pii = parse_env_force_pii();

    if force_pii {
        if pii_mode.is_none() {
            pii_mode = env_pii_default;
        }
        if pii_mode.is_none() {
            return Err(bad_request(
                "PII is required. Set piiMode to scrub or configure LANE_DEFAULT_PII_MODE",
                None,
            ));
        }
    }

    let count_mode = match payload.count_mode.as_deref() {
        Some(s) => CountMode::from_str(s).map_err(|e| {
            bad_request(format!("Invalid countMode: {}", e), None)
        })?,
        None => CountMode::Window,
    };

    let blob_format = match payload.blobs.as_deref() {
        Some(s) => BlobFormat::from_str(s).map_err(|e| {
            bad_request(format!("Invalid blobs format: {}", e), None)
        })?,
        None => BlobFormat::Length,
    };

    let query = if payload.exec_sql.unwrap_or(false) {
        crate::query::validation::wrap_exec_sql(&payload.query)
    } else {
        payload.query.clone()
    };

    Ok(QueryParams {
        database: payload.database.clone(),
        query,
        batch_size: payload.batch_size.unwrap_or(50_000),
        pagination,
        count_mode,
        order: payload.order.clone(),
        allow_unstable_pagination: payload.allow_unstable_pagination.unwrap_or(false),
        preserve_decimal_precision: payload.preserve_decimal_precision.unwrap_or(true),
        blob_format,
        include_metadata: payload.include_metadata.unwrap_or(false),
        max_memory_bytes,
        pii_mode,
        pii_column_hints: payload.pii_column_hints.clone(),
        pii_column_excludes: payload.pii_column_excludes.clone(),
        pii_processor_override: None,
        json_stream,
        read_only: payload.read_only,
    })
}

// ============================================================================
// Handlers
// ============================================================================

pub async fn health_check() -> impl IntoResponse {
    Json(json!({ "status": "healthy" }))
}

pub async fn help_handler() -> impl IntoResponse {
    let sql_type = env::var("LANE_SQL_TYPE").unwrap_or_else(|_| "mssql".to_string());

    Json(json!({
        "name": "lane API",
        "description": "HTTP API for executing SQL queries against SQL Server databases",
        "sqlType": sql_type,
        "sqlDialect": match sql_type.to_lowercase().as_str() {
            "mssql" | "sqlserver" => "T-SQL (Microsoft SQL Server)",
            "postgres" | "postgresql" => "PostgreSQL",
            "mysql" => "MySQL",
            _ => &sql_type
        },
        "usage": {
            "endpoint": "POST /api/lane",
            "headers": {
                "Content-Type": "application/json",
                "x-api-key": "<your-api-key>"
            },
            "examples": {
                "simple_query": {
                    "description": "Simple query (default, no pagination needed)",
                    "curl": "curl -X POST http://HOST:PORT/api/lane -H \"Content-Type: application/json\" -H \"x-api-key: YOUR_KEY\" -d '{\"database\": \"mydb\", \"query\": \"SELECT * FROM users\"}'"
                },
                "paginated_query": {
                    "description": "Large query with pagination (requires ORDER BY)",
                    "curl": "curl -X POST http://HOST:PORT/api/lane -H \"Content-Type: application/json\" -H \"x-api-key: YOUR_KEY\" -d '{\"database\": \"mydb\", \"query\": \"SELECT * FROM big_table ORDER BY id\", \"pagination\": true}'"
                },
                "streaming_query": {
                    "description": "Stream large results as NDJSON (one JSON object per line)",
                    "curl": "curl -X POST http://HOST:PORT/api/lane -H \"Content-Type: application/json\" -H \"x-api-key: YOUR_KEY\" -d '{\"database\": \"mydb\", \"query\": \"SELECT * FROM big_table ORDER BY id\", \"pagination\": true, \"jsonStream\": true}'"
                },
                "multipart_query": {
                    "description": "Upload SQL via multipart to avoid escaping newlines/comments",
                    "curl": "curl -X POST http://HOST:PORT/api/lane -H \"x-api-key: YOUR_KEY\" -F \"database=mydb\" -F \"query=@path/to/query.sql\""
                },
                "excel_export": {
                    "description": "Export query results as an Excel (.xlsx) file",
                    "curl": "curl -X POST http://HOST:PORT/api/lane -H \"Content-Type: application/json\" -H \"x-api-key: YOUR_KEY\" -d '{\"database\": \"mydb\", \"query\": \"SELECT * FROM users\", \"outputFormat\": \"xlsx\"}'\n# Returns: {\"success\": true, \"total_rows\": 5, \"execution_time_ms\": 23, \"download_url\": \"/api/lane/download/UUID\"}\n# Then download: curl http://HOST:PORT/api/lane/download/UUID -o results.xlsx"
                },
                "ai_endpoint_with_env": {
                    "description": "AI endpoint using .env.ai credentials (PII scrubbing enforced). Create query.sql first or use heredoc: cat > $LANE_AUTOMATION_TMP/query.sql << 'EOF'\nSELECT TOP 10 * FROM MyTable\nEOF",
                    "bash": "source .env.ai\n\ncurl -X POST \"$LANE_AUTOMATION_URL\" \\\n  -H \"x-api-key: $LANE_AUTOMATION_KEY\" \\\n  -F \"database=mydb\" \\\n  -F \"query=@$LANE_AUTOMATION_TMP/query.sql\""
                }
            }
        },
        "parameters": {
            "required": {
                "database": {
                    "type": "string",
                    "description": "Database name to connect to"
                },
                "query": {
                    "type": "string",
                    "description": "SQL query to execute"
                }
            },
            "optional": {
                "pagination": {
                    "type": "boolean",
                    "default": false,
                    "description": "Enable pagination for large result sets (default: false). When true, REQUIRES an ORDER BY clause. When false (default) and not streaming, the API applies an auto-streaming guard around 256 MB to avoid excessive buffering."
                },
                "batchSize": {
                    "type": "integer",
                    "default": 50000,
                    "description": "Internal batch size for pagination. Controls how many rows are fetched from SQL Server at a time. Does NOT affect the response - all rows are still returned together unless jsonStream is true."
                },
                "jsonStream": {
                    "type": "boolean",
                    "default": false,
                    "description": "Stream results as NDJSON (one JSON object per line) instead of a buffered JSON array. Use for large datasets to avoid memory issues. Response Content-Type will be application/x-ndjson.",
                    "aliases": ["ndjson"]
                },
                "includeMetadata": {
                    "type": "boolean",
                    "default": false,
                    "description": "Include column metadata (name, type) in response"
                },
                "order": {
                    "type": "string",
                    "default": null,
                    "description": "ORDER BY clause for pagination. Overrides any ORDER BY in the query. Required when pagination is true unless the query already has ORDER BY."
                },
                "allowUnstablePagination": {
                    "type": "boolean",
                    "default": false,
                    "description": "Allow pagination without ORDER BY. WARNING: Results may be non-deterministic and rows could be duplicated or skipped."
                },
                "countMode": {
                    "type": "string",
                    "default": "window",
                    "options": ["window", "subquery", "exact"],
                    "description": "Method for counting total rows during pagination. 'window' uses ROW_NUMBER() (most compatible), 'subquery' wraps query, 'exact' runs separate COUNT query."
                },
                "preserveDecimalPrecision": {
                    "type": "boolean",
                    "default": true,
                    "description": "Serialize DECIMAL/MONEY as strings to preserve precision. Set false to get numbers (may lose precision)."
                },
                "blobs": {
                    "type": "string",
                    "default": "length",
                    "options": ["length", "base64", "hex"],
                    "description": "Binary data format: 'length' shows byte count like [BINARY 1024 bytes], 'base64' encodes as Base64, 'hex' encodes as hexadecimal."
                },
                "piiMode": {
                    "type": "string",
                    "default": null,
                    "options": ["none", "scrub"],
                    "description": "PII handling: 'none' disables PII handling (overrides env defaults), 'scrub' replaces detected PII with <type> placeholders."
                },
                "piiColumnHints": {
                    "type": "array[string]",
                    "default": null,
                    "description": "Column name patterns (case-insensitive substring) to force PII handling even if regex detection misses values."
                },
                "piiColumnExcludes": {
                    "type": "array[string]",
                    "default": null,
                    "description": "Column name patterns to skip PII handling."
                },
                "rowLimit": {
                    "type": "integer",
                    "default": null,
                    "description": "AI endpoint only: wrap SELECT/CTE queries with SELECT TOP <rowLimit> to keep responses small. Ignored for INSERT/UPDATE/DELETE/DDL/EXEC statements."
                },
                "maxMemoryMb": {
                    "type": "integer",
                    "default": "auto (256MB when pagination=false and jsonStream=false)",
                    "description": "Max MB to buffer before auto-switching to streaming mode. 0 or null disables this guard. The API applies ~256 MB when pagination=false and jsonStream=false."
                },
                "validationTimeoutSec": {
                    "type": "integer",
                    "default": 30,
                    "description": "Seconds to allow for validation (PARSEONLY + NOEXEC) before timing out."
                },
                "skipValidation": {
                    "type": "boolean",
                    "default": false,
                    "description": "Skip SQL validation before execution. Required for DDL statements like CREATE PROCEDURE, ALTER, DROP, etc. Use with caution - invalid SQL will fail at execution time instead of validation."
                },
                "dryRun": {
                    "type": "boolean",
                    "default": false,
                    "description": "Validate the query without executing it. Returns validation result (valid/invalid with message) and a preview of the query. Useful for testing queries before running them."
                },
                "execSql": {
                    "type": "boolean",
                    "default": false,
                    "description": "Wrap query in EXEC sp_executesql for DDL execution. Use this for CREATE PROCEDURE, ALTER, DROP, and other DDL statements. Automatically escapes single quotes. Best combined with skipValidation=true."
                },
                "bodyLimitMb": {
                    "type": "integer",
                    "default": "env LANE_BODY_LIMIT_MB (default 10MB)",
                    "description": "Optional per-request body size limit in MB. Effective limit is max(env, this value). 413 returned on exceed."
                },
                "outputFormat": {
                    "type": "string",
                    "default": null,
                    "options": ["xlsx"],
                    "description": "Output format override. 'xlsx' exports results as an Excel spreadsheet. Returns JSON with a single-use download_url instead of inline data. Each result set becomes a separate sheet. Column types are preserved (numbers, booleans, strings). Download link expires after 5 minutes."
                },
                "debug": {
                    "type": "boolean",
                    "default": false,
                    "description": "Enable debug mode to log column types (appears in server logs, not response)."
                }
            },
            "internal_do_not_change": {
                "json": {
                    "type": "boolean",
                    "locked": true,
                    "description": "Always true for API. Do not set to false - will cause parse errors."
                },
                "quiet": {
                    "type": "boolean",
                    "locked": true,
                    "description": "Always true for API. Do not set to false - status messages will corrupt JSON output."
                }
            }
        },
        "commonPatterns": {
            "small_query": {
                "description": "Best for queries returning < 1000 rows",
                "params": {"pagination": false},
                "notes": "Fastest option. No ORDER BY required."
            },
            "large_query_buffered": {
                "description": "For large results that fit in memory",
                "params": {"pagination": true, "query": "... ORDER BY id"},
                "notes": "Results fetched in batches internally but returned as single JSON array. ORDER BY required."
            },
            "large_query_streaming": {
                "description": "For very large results or memory-constrained clients",
                "params": {"pagination": true, "jsonStream": true, "query": "... ORDER BY id"},
                "notes": "Returns NDJSON - one JSON object per line. Pipe to jq or process line-by-line."
            },
            "stored_procedure": {
                "description": "For EXEC statements",
                "params": {"pagination": false, "query": "EXEC sp_name @param=value"},
                "notes": "Stored procedures are auto-detected and run without pagination regardless of setting."
            },
            "ddl_create_procedure": {
                "description": "For CREATE PROCEDURE, ALTER, DROP, and other DDL statements",
                "params": {"execSql": true, "skipValidation": true, "query": "CREATE PROCEDURE MyProc AS SELECT 1"},
                "notes": "Use execSql=true to wrap in sp_executesql (handles DDL properly). skipValidation=true bypasses NOEXEC validation. Use multipart -F \"query=@file.sql\" for complex DDL with proper quote escaping."
            },
            "dry_run_test": {
                "description": "Test query validation without executing",
                "params": {"dryRun": true, "query": "SELECT * FROM users WHERE ..."},
                "notes": "Returns {validation: {valid: true/false, message: ...}} without executing the query. Useful for testing complex queries or checking syntax before running."
            },
            "excel_export": {
                "description": "Export results as a downloadable Excel file",
                "params": {"outputFormat": "xlsx", "query": "SELECT * FROM users"},
                "notes": "Returns JSON with a single-use download_url. GET that URL to download the .xlsx file. Link expires after 5 minutes. Multiple result sets become separate sheets."
            }
        },
        "endpoints": {
            "GET /health": "Health check - returns {\"status\": \"healthy\"}",
            "GET /api/lane/help": "This help documentation",
            "POST /api/lane": "Execute a SQL query",
            "POST /api/lane/ai": "Execute a SQL query with enforced PII handling (default scrub) and optional rowLimit guard",
            "GET /api/lane/download/{id}": "Download a generated file (xlsx). Single-use, expires after 5 minutes."
        },
        "notes": {
            "validation": "All queries are validated with SET PARSEONLY + SET NOEXEC before execution. Invalid SQL returns 400 with the SQL Server parse error. Use skipValidation=true for DDL statements (CREATE PROCEDURE, ALTER, DROP) as PARSEONLY doesn't handle them well.",
            "multipart": "Content-Type: multipart/form-data is supported with the same fields as JSON (e.g., -F \"database=mydb\" -F \"query=@file.sql\")."
        },
        "troubleshooting": {
            "ddl_validation_failed": {
                "error": "VALIDATION_FAILED when running CREATE PROCEDURE, ALTER, or DROP",
                "solution": "Add skipValidation=true (or -F \"skip_validation=true\" for multipart). DDL statements don't work with PARSEONLY validation."
            },
            "pagination_error_no_order_by": {
                "error": "Pagination requires an ORDER BY clause",
                "solution": "Either add ORDER BY to your query, use the 'order' parameter, set 'pagination': false, or set 'allowUnstablePagination': true (not recommended)."
            },
            "parse_error": {
                "error": "expected value at line 1 column 1",
                "solution": "Do not set 'json': false or 'quiet': false - these break JSON parsing."
            },
            "timeout_large_results": {
                "error": "Request timeout on large datasets",
                "solution": "Use 'jsonStream': true to stream results, or reduce result size with WHERE/TOP."
            }
        }
    }))
}

/// Main query handler — authenticates, parses, builds QueryParams, executes via DatabaseBackend
pub async fn query_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    req: axum::http::Request<Body>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;

    match &auth {
        AuthResult::Denied(reason) => {
            return request_error(
                "UNAUTHORIZED",
                reason,
                Some("Include header 'x-api-key: YOUR_KEY'. See GET /api/lane/help for usage."),
            )
            .to_response(StatusCode::UNAUTHORIZED);
        }
        AuthResult::FullAccess => {} // proceed without permission checks
        AuthResult::TokenAccess { .. } | AuthResult::SessionAccess { .. } | AuthResult::ServiceAccountAccess { .. } => {}
    }

    let mut payload = match parse_request(&headers, req).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    // Track read-only mode for database-level enforcement (Postgres READ ONLY transactions)
    let mut is_read_only = false;

    // SQL mode gate for service accounts
    if let AuthResult::ServiceAccountAccess { account_name } = &auth {
        if let Some(ref access_db) = state.access_db {
            let mode = access_db.get_sa_sql_mode(account_name);
            match mode {
                SqlMode::None => {
                    return request_error(
                        "FORBIDDEN",
                        "Raw SQL access is disabled for this service account.",
                        None,
                    )
                    .to_response(StatusCode::FORBIDDEN);
                }
                SqlMode::ReadOnly => {
                    if !crate::query::validation::is_read_only_safe(&payload.query) {
                        return request_error(
                            "FORBIDDEN",
                            "Read-only SQL mode. Only SELECT queries are allowed.",
                            None,
                        )
                        .to_response(StatusCode::FORBIDDEN);
                    }
                    is_read_only = true;
                }
                SqlMode::Supervised | SqlMode::Confirmed => {
                    if crate::query::validation::is_ddl_query(&payload.query) {
                        return request_error(
                            "FORBIDDEN",
                            "DDL is not allowed for this service account.",
                            None,
                        )
                        .to_response(StatusCode::FORBIDDEN);
                    }
                }
                SqlMode::Full => {}
            }
        }
    }

    // SQL mode gate (must be after parse_request so we can inspect the query)
    if let AuthResult::TokenAccess { .. } | AuthResult::SessionAccess { .. } = &auth {
        if let Some(email) = extract_auth_email(&auth) {
            if let Some(ref access_db) = state.access_db {
                let mode = access_db.get_sql_mode(email);
                match mode {
                    SqlMode::None => {
                        return request_error(
                            "FORBIDDEN",
                            "Raw SQL access is disabled for this user. Use the REST API endpoints instead.",
                            Some("Contact an admin to enable raw SQL access."),
                        )
                        .to_response(StatusCode::FORBIDDEN);
                    }
                    SqlMode::ReadOnly => {
                        if !crate::query::validation::is_read_only_safe(&payload.query) {
                            return request_error(
                                "FORBIDDEN",
                                "Read-only SQL mode. Only SELECT queries are allowed.",
                                Some("Contact an admin to change your SQL access mode."),
                            )
                            .to_response(StatusCode::FORBIDDEN);
                        }
                        is_read_only = true;
                    }
                    SqlMode::Supervised | SqlMode::Confirmed => {
                        if crate::query::validation::is_ddl_query(&payload.query) {
                            return request_error(
                                "FORBIDDEN",
                                "DDL requires human review. Open the SQL Editor to execute this statement.",
                                Some("Use the web UI SQL Editor to run DDL statements."),
                            )
                            .to_response(StatusCode::FORBIDDEN);
                        }
                    }
                    SqlMode::Full => {}
                }
            }
        }
    }

    // Check connection access (always check, even for default connection)
    {
        let default_name = state.registry.default_name();
        let conn_name = payload.connection.as_deref().unwrap_or(&default_name);
        if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
            return resp;
        }
    }

    // For service accounts, check permissions
    if let AuthResult::ServiceAccountAccess { account_name } = &auth {
        if let Some(ref access_db) = state.access_db {
            let is_write = !crate::query::validation::is_select_like(&payload.query);
            if !access_db.check_sa_permission(account_name, &payload.database, is_write) {
                let action = if is_write { "write" } else { "read" };
                return request_error(
                    "FORBIDDEN",
                    &format!("No {} permission for database '{}'", action, payload.database),
                    None,
                )
                .to_response(StatusCode::FORBIDDEN);
            }
        }
    }

    // For token/session-authenticated users, check permissions
    let auth_email = extract_auth_email(&auth);
    if let Some(email) = auth_email {
        if let Some(ref access_db) = state.access_db {
            let is_write = !crate::query::validation::is_select_like(&payload.query);

            if !access_db.check_permission(email, &payload.database, is_write) {
                let action = if is_write { "write" } else { "read" };
                access_db.log_access(
                    None,
                    Some(email),
                    None,
                    Some(&payload.database),
                    Some(action),
                    "denied",
                    Some(&format!(
                        "No {} permission for database '{}'",
                        action, payload.database
                    )),
                );
                return request_error(
                    "FORBIDDEN",
                    &format!(
                        "No {} permission for database '{}'",
                        action, payload.database
                    ),
                    None,
                )
                .to_response(StatusCode::FORBIDDEN);
            }

            let action = if is_write { "write" } else { "read" };
            access_db.log_access(
                None,
                Some(email),
                None,
                Some(&payload.database),
                Some(action),
                "allowed",
                None,
            );
        }
    }

    // Build enriched PII processor using resolution chain
    let pii_ctx = build_pii_context(&auth);
    let pii_override = crate::query::build_enriched_pii_processor(
        &crate::query::QueryParams {
            database: payload.database.clone(),
            pii_mode: payload.pii_mode.clone(),
            pii_column_hints: payload.pii_column_hints.clone(),
            pii_column_excludes: payload.pii_column_excludes.clone(),
            ..Default::default()
        },
        state.access_db.as_deref(),
        payload.connection.as_deref(),
        &pii_ctx,
    );

    // Capture info for history logging before moving payload
    let history_email = auth_email.map(|e| e.to_string()).or_else(|| {
        if let AuthResult::ServiceAccountAccess { account_name } = &auth {
            Some(format!("sa:{}", account_name))
        } else {
            None
        }
    });
    let history_connection = payload.connection.clone();
    let history_database = payload.database.clone();
    let history_sql = payload.query.clone();

    payload.read_only = is_read_only;
    let start = std::time::Instant::now();
    let response = execute_payload(&state, payload, pii_override).await;

    // Log query history for authenticated users
    if let Some(email) = &history_email {
        if let Some(ref access_db) = state.access_db {
            let elapsed = start.elapsed().as_millis() as i64;

            // Try to parse the response to determine success/failure
            // We peek at the status code — 2xx means success
            let is_success = response.status().is_success();

            access_db.log_query_history(
                email,
                history_connection.as_deref(),
                Some(&history_database),
                &history_sql,
                Some(elapsed),
                None, // row_count not easily extractable from Response
                is_success,
                if !is_success { Some("Query failed") } else { None },
            );

            // Index query for FTS search
            if is_success {
                if let Some(ref search_db) = state.search_db {
                    search_db.index_query(
                        email,
                        history_connection.as_deref().unwrap_or(""),
                        &history_database,
                        &history_sql,
                    );
                }
            }
        }
    }

    // Emit realtime event for successful write queries
    if response.status().is_success() && !crate::query::validation::is_select_like(&history_sql) {
        let resolved_conn = history_connection.clone()
            .unwrap_or_else(|| state.registry.default_name());
        crate::api::realtime::try_emit_realtime_event(
            &state,
            &resolved_conn,
            &history_database,
            &history_sql,
            None,
            history_email.as_deref(),
        );
    }

    response
}

/// AI-enhanced query handler — enforces PII, applies rowLimit
pub async fn query_ai_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    req: axum::http::Request<Body>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;

    match &auth {
        AuthResult::Denied(reason) => {
            return request_error(
                "UNAUTHORIZED",
                reason,
                Some("Include header 'x-api-key: YOUR_KEY'. See GET /api/lane/help for usage."),
            )
            .to_response(StatusCode::UNAUTHORIZED);
        }
        AuthResult::FullAccess => {}
        AuthResult::TokenAccess { .. } | AuthResult::SessionAccess { .. } | AuthResult::ServiceAccountAccess { .. } => {}
    }

    let mut payload = match parse_request(&headers, req).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let mut is_read_only = false;

    // SQL mode gate for service accounts
    if let AuthResult::ServiceAccountAccess { account_name } = &auth {
        if let Some(ref access_db) = state.access_db {
            let mode = access_db.get_sa_sql_mode(account_name);
            match mode {
                SqlMode::None => {
                    return request_error(
                        "FORBIDDEN",
                        "Raw SQL access is disabled for this service account.",
                        None,
                    )
                    .to_response(StatusCode::FORBIDDEN);
                }
                SqlMode::ReadOnly => {
                    if !crate::query::validation::is_read_only_safe(&payload.query) {
                        return request_error(
                            "FORBIDDEN",
                            "Read-only SQL mode. Only SELECT queries are allowed.",
                            None,
                        )
                        .to_response(StatusCode::FORBIDDEN);
                    }
                    is_read_only = true;
                }
                SqlMode::Supervised | SqlMode::Confirmed => {
                    if crate::query::validation::is_ddl_query(&payload.query) {
                        return request_error(
                            "FORBIDDEN",
                            "DDL is not allowed for this service account.",
                            None,
                        )
                        .to_response(StatusCode::FORBIDDEN);
                    }
                }
                SqlMode::Full => {}
            }
        }
    }

    // SQL mode gate (must be after parse_request so we can inspect the query)
    if let AuthResult::TokenAccess { .. } | AuthResult::SessionAccess { .. } = &auth {
        if let Some(email) = extract_auth_email(&auth) {
            if let Some(ref access_db) = state.access_db {
                let mode = access_db.get_sql_mode(email);
                match mode {
                    SqlMode::None => {
                        return request_error(
                            "FORBIDDEN",
                            "Raw SQL access is disabled for this user. Use the REST API endpoints instead.",
                            Some("Contact an admin to enable raw SQL access."),
                        )
                        .to_response(StatusCode::FORBIDDEN);
                    }
                    SqlMode::ReadOnly => {
                        if !crate::query::validation::is_read_only_safe(&payload.query) {
                            return request_error(
                                "FORBIDDEN",
                                "Read-only SQL mode. Only SELECT queries are allowed.",
                                Some("Contact an admin to change your SQL access mode."),
                            )
                            .to_response(StatusCode::FORBIDDEN);
                        }
                        is_read_only = true;
                    }
                    SqlMode::Supervised | SqlMode::Confirmed => {
                        if crate::query::validation::is_ddl_query(&payload.query) {
                            return request_error(
                                "FORBIDDEN",
                                "DDL requires human review. Open the SQL Editor to execute this statement.",
                                Some("Use the web UI SQL Editor to run DDL statements."),
                            )
                            .to_response(StatusCode::FORBIDDEN);
                        }
                    }
                    SqlMode::Full => {}
                }
            }
        }
    }

    // Check connection access (always check, even for default connection)
    {
        let default_name = state.registry.default_name();
        let conn_name = payload.connection.as_deref().unwrap_or(&default_name);
        if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
            return resp;
        }
    }

    // For service accounts, check permissions
    if let AuthResult::ServiceAccountAccess { account_name } = &auth {
        if let Some(ref access_db) = state.access_db {
            let is_write = !crate::query::validation::is_select_like(&payload.query);
            if !access_db.check_sa_permission(account_name, &payload.database, is_write) {
                let action = if is_write { "write" } else { "read" };
                return request_error(
                    "FORBIDDEN",
                    &format!("No {} permission for database '{}'", action, payload.database),
                    None,
                )
                .to_response(StatusCode::FORBIDDEN);
            }
        }
    }

    // For token/session-authenticated users, check permissions
    let auth_email = extract_auth_email(&auth);
    if let Some(email) = auth_email {
        if let Some(ref access_db) = state.access_db {
            let is_write = !crate::query::validation::is_select_like(&payload.query);

            // AI endpoint: reject writes for users without write permission
            if is_write && !access_db.check_permission(email, &payload.database, true) {
                access_db.log_access(
                    None,
                    Some(email),
                    None,
                    Some(&payload.database),
                    Some("write"),
                    "denied",
                    Some("Write operation rejected on AI endpoint"),
                );
                return request_error(
                    "FORBIDDEN",
                    "Write operations not allowed with your permissions",
                    None,
                )
                .to_response(StatusCode::FORBIDDEN);
            }

            if !access_db.check_permission(email, &payload.database, false) {
                access_db.log_access(
                    None,
                    Some(email),
                    None,
                    Some(&payload.database),
                    Some("read"),
                    "denied",
                    Some(&format!(
                        "No read permission for database '{}'",
                        payload.database
                    )),
                );
                return request_error(
                    "FORBIDDEN",
                    &format!(
                        "No read permission for database '{}'",
                        payload.database
                    ),
                    None,
                )
                .to_response(StatusCode::FORBIDDEN);
            }

            let action = if is_write { "write" } else { "read" };
            access_db.log_access(
                None,
                Some(email),
                None,
                Some(&payload.database),
                Some(action),
                "allowed",
                None,
            );
        }
    }

    // Force pagination off; optional rowLimit wrapping for SELECT queries
    payload.pagination = Some(false);
    let ai_connection = payload.connection.clone();
    let ai_database = payload.database.clone();
    let ai_sql = payload.query.clone();
    if let Some(limit) = payload.row_limit {
        let dialect = state
            .registry
            .resolve(payload.connection.as_deref())
            .map(|db| db.dialect())
            .unwrap_or(crate::db::Dialect::Mssql);
        payload.query =
            crate::query::validation::apply_row_limit_dialect(&payload.query, limit, dialect);
    }

    // Build enriched PII processor using resolution chain
    let pii_ctx = build_pii_context(&auth);
    let pii_override = crate::query::build_enriched_pii_processor(
        &crate::query::QueryParams {
            database: payload.database.clone(),
            pii_mode: payload.pii_mode.clone(),
            pii_column_hints: payload.pii_column_hints.clone(),
            pii_column_excludes: payload.pii_column_excludes.clone(),
            ..Default::default()
        },
        state.access_db.as_deref(),
        payload.connection.as_deref(),
        &pii_ctx,
    );

    payload.read_only = is_read_only;
    let response = execute_payload(&state, payload, pii_override).await;

    // Emit realtime event for successful write queries
    if response.status().is_success() && !crate::query::validation::is_select_like(&ai_sql) {
        let resolved_conn = ai_connection.clone()
            .unwrap_or_else(|| state.registry.default_name());
        crate::api::realtime::try_emit_realtime_event(
            &state,
            &resolved_conn,
            &ai_database,
            &ai_sql,
            None,
            auth_email,
        );
    }

    response
}

// ============================================================================
// Core Execution (calls DatabaseBackend directly)
// ============================================================================

async fn execute_payload(
    state: &AppState,
    payload: BatchQueryRequest,
    pii_override: Option<crate::pii::PiiProcessor>,
) -> Response {
    // Resolve the database backend from the connection registry
    let db = match state.registry.resolve(payload.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error(
                "INVALID_CONNECTION",
                &format!("{}", e),
                Some("Specify a valid connection name or omit for the default."),
            )
            .to_response(StatusCode::BAD_REQUEST);
        }
    };
    let dialect = db.dialect();

    // Build query params from the request
    let mut params = match build_query_params(&payload) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    // Apply enriched PII processor override if available
    if let Some(processor) = pii_override {
        params.pii_processor_override = Some(processor);
    }

    // Handle dry run mode — validate only and return result
    if payload.dry_run.unwrap_or(false) {
        let validation_result = db.validate_query(&params.database, &params.query).await;
        return match validation_result {
            Ok(()) => (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "dry_run": true,
                    "validation": {
                        "valid": true,
                        "message": "Query syntax is valid"
                    },
                    "query_preview": if params.query.len() > 500 {
                        format!("{}...", &params.query[..500])
                    } else {
                        params.query.clone()
                    },
                    "would_execute": !payload.skip_validation.unwrap_or(false)
                })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "dry_run": true,
                    "validation": {
                        "valid": false,
                        "message": e
                    },
                    "query_preview": if params.query.len() > 500 {
                        format!("{}...", &params.query[..500])
                    } else {
                        params.query.clone()
                    }
                })),
            )
                .into_response(),
        };
    }

    // Skip validation if explicitly requested (useful for DDL like CREATE PROCEDURE)
    if !payload.skip_validation.unwrap_or(false) {
        if let Err(e) = db.validate_query(&params.database, &params.query).await {
            // For 207/208/1038 errors, fall through to execution for enriched error hints
            // 1038 is a false positive from NOEXEC for valid SELECT queries
            if let Some((sql_code, _, _)) = parse_sql_error(&e) {
                if sql_code != 207 && sql_code != 208 && sql_code != 1038 {
                    return validation_error(&e, dialect).to_response(StatusCode::BAD_REQUEST);
                }
            } else {
                return validation_error(&e, dialect).to_response(StatusCode::BAD_REQUEST);
            }
        }
    }

    // NDJSON streaming path
    if params.json_stream {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        match db.execute_query_streaming(&params, tx).await {
            Ok(()) => {
                let stream = ReceiverStream::new(rx);
                return Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/x-ndjson")
                    .body(Body::from_stream(stream))
                    .unwrap();
            }
            Err(e) => {
                return execution_error(&format!("{:#}", e), dialect)
                    .to_response(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    // Execute query via DatabaseBackend (buffered path)
    match db.execute_query(&params).await {
        Ok(result) => {
            match payload.output_format.as_deref() {
                Some("csv") => {
                    match crate::export::csv::query_result_to_csv(&result) {
                        Ok(bytes) => {
                            let id = uuid::Uuid::new_v4().to_string();
                            let total_rows = result.total_rows;
                            let execution_time_ms = result.execution_time_ms;
                            state.downloads.write().await.insert(
                                id.clone(),
                                super::CachedFile {
                                    bytes,
                                    content_type: "text/csv".to_string(),
                                    filename: "results.csv".to_string(),
                                    created_at: std::time::Instant::now(),
                                },
                            );
                            (StatusCode::OK, Json(json!({
                                "success": true,
                                "total_rows": total_rows,
                                "execution_time_ms": execution_time_ms,
                                "download_url": format!("/api/lane/download/{}", id),
                            }))).into_response()
                        }
                        Err(e) => execution_error(&format!("csv export failed: {:#}", e), dialect)
                            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
                    }
                }
                #[cfg(feature = "xlsx")]
                Some("xlsx") => {
                    match crate::export::xlsx::query_result_to_xlsx(&result) {
                        Ok(bytes) => {
                            let id = uuid::Uuid::new_v4().to_string();
                            let total_rows = result.total_rows;
                            let execution_time_ms = result.execution_time_ms;
                            state.downloads.write().await.insert(
                                id.clone(),
                                super::CachedFile {
                                    bytes,
                                    content_type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".to_string(),
                                    filename: "results.xlsx".to_string(),
                                    created_at: std::time::Instant::now(),
                                },
                            );
                            (StatusCode::OK, Json(json!({
                                "success": true,
                                "total_rows": total_rows,
                                "execution_time_ms": execution_time_ms,
                                "download_url": format!("/api/lane/download/{}", id),
                            }))).into_response()
                        }
                        Err(e) => execution_error(&format!("xlsx export failed: {:#}", e), dialect)
                            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
                    }
                }
                #[cfg(not(feature = "xlsx"))]
                Some("xlsx") => {
                    bad_request("xlsx export is not enabled on this server", None)
                }
                _ => {
                    (StatusCode::OK, Json(result)).into_response()
                }
            }
        }
        Err(e) => execution_error(&format!("{:#}", e), dialect).to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// ============================================================================
// Download Handler
// ============================================================================

pub async fn download_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    let file = state.downloads.write().await.remove(&id);
    match file {
        Some(cached) => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", &cached.content_type)
            .header(
                "Content-Disposition",
                format!("attachment; filename=\"{}\"", cached.filename),
            )
            .body(Body::from(cached.bytes))
            .unwrap(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "success": false,
                "error": "Download not found or already consumed"
            })),
        )
            .into_response(),
    }
}

// ============================================================================
// Metadata Endpoints (mirrors MCP tools for web UI)
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct DatabasesQuery {
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SchemasQuery {
    pub database: String,
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TablesQuery {
    pub database: String,
    pub schema: Option<String>,
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DescribeQuery {
    pub database: String,
    pub table: String,
    pub schema: Option<String>,
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ObjectDefinitionQuery {
    pub database: String,
    pub name: String,
    pub object_type: String,
    pub schema: Option<String>,
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TableObjectQuery {
    pub database: String,
    pub table: String,
    pub schema: Option<String>,
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TriggerDefinitionQuery {
    pub database: String,
    pub name: String,
    pub schema: Option<String>,
    pub connection: Option<String>,
}

pub async fn list_connections_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let mut connections = metadata::list_connections(&state.registry);

    // Filter by connection access permissions
    if let Some(email) = extract_auth_email(&auth) {
        if let Some(ref access_db) = state.access_db {
            if let Ok(Some(allowed)) = access_db.get_allowed_connections(email) {
                connections.retain(|c| allowed.iter().any(|a| a == &c.name));
            }
        }
    }

    (StatusCode::OK, Json(connections)).into_response()
}

pub async fn list_databases_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<DatabasesQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    // Check connection access
    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    match metadata::list_databases(db.as_ref()).await {
        Ok(databases) => (StatusCode::OK, Json(databases)).into_response(),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn list_schemas_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<SchemasQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    match metadata::list_schemas(db.as_ref(), &params.database).await {
        Ok(schemas) => (StatusCode::OK, Json(schemas)).into_response(),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn list_tables_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<TablesQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let default_schema = match db.dialect() {
        crate::db::Dialect::Postgres => "public",
        crate::db::Dialect::DuckDb => "main",
        _ => "dbo",
    };
    let schema = params.schema.as_deref().unwrap_or(default_schema);

    match metadata::list_tables(db.as_ref(), &params.database, schema).await {
        Ok(tables) => (StatusCode::OK, Json(tables)).into_response(),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn describe_table_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<DescribeQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let default_schema = match db.dialect() {
        crate::db::Dialect::Postgres => "public",
        crate::db::Dialect::DuckDb => "main",
        _ => "dbo",
    };
    let schema = params.schema.as_deref().unwrap_or(default_schema);

    match metadata::describe_table(db.as_ref(), &params.database, &params.table, schema).await {
        Ok(columns) => (StatusCode::OK, Json(columns)).into_response(),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn list_views_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<TablesQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let default_schema = match db.dialect() {
        crate::db::Dialect::Postgres => "public",
        crate::db::Dialect::DuckDb => "main",
        _ => "dbo",
    };
    let schema = params.schema.as_deref().unwrap_or(default_schema);

    match metadata::list_views(db.as_ref(), &params.database, schema).await {
        Ok(views) => (StatusCode::OK, Json(views)).into_response(),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn list_routines_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<TablesQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let default_schema = match db.dialect() {
        crate::db::Dialect::Postgres => "public",
        crate::db::Dialect::DuckDb => "main",
        _ => "dbo",
    };
    let schema = params.schema.as_deref().unwrap_or(default_schema);

    match metadata::list_routines(db.as_ref(), &params.database, schema).await {
        Ok(routines) => (StatusCode::OK, Json(routines)).into_response(),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn get_object_definition_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<ObjectDefinitionQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let default_schema = match db.dialect() {
        crate::db::Dialect::Postgres => "public",
        crate::db::Dialect::DuckDb => "main",
        _ => "dbo",
    };
    let schema = params.schema.as_deref().unwrap_or(default_schema);

    match metadata::get_object_definition(db.as_ref(), &params.database, schema, &params.name, &params.object_type).await {
        Ok(Some(def)) => (StatusCode::OK, Json(def)).into_response(),
        Ok(None) => request_error("NOT_FOUND", "Object definition not found (may be encrypted)", None)
            .to_response(StatusCode::NOT_FOUND),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn list_triggers_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<TableObjectQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let default_schema = match db.dialect() {
        crate::db::Dialect::Postgres => "public",
        crate::db::Dialect::DuckDb => "main",
        _ => "dbo",
    };
    let schema = params.schema.as_deref().unwrap_or(default_schema);

    match metadata::list_triggers(db.as_ref(), &params.database, schema, &params.table).await {
        Ok(triggers) => (StatusCode::OK, Json(triggers)).into_response(),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn get_trigger_definition_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<TriggerDefinitionQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let default_schema = match db.dialect() {
        crate::db::Dialect::Postgres => "public",
        crate::db::Dialect::DuckDb => "main",
        _ => "dbo",
    };
    let schema = params.schema.as_deref().unwrap_or(default_schema);

    match metadata::get_trigger_definition(db.as_ref(), &params.database, schema, &params.name).await {
        Ok(Some(def)) => (StatusCode::OK, Json(def)).into_response(),
        Ok(None) => request_error("NOT_FOUND", "Trigger definition not found", None)
            .to_response(StatusCode::NOT_FOUND),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn get_related_objects_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<TableObjectQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let default_schema = match db.dialect() {
        crate::db::Dialect::Postgres => "public",
        crate::db::Dialect::DuckDb => "main",
        _ => "dbo",
    };
    let schema = params.schema.as_deref().unwrap_or(default_schema);

    match metadata::get_related_objects(db.as_ref(), &params.database, schema, &params.table).await {
        Ok(objects) => (StatusCode::OK, Json(objects)).into_response(),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// ============================================================================
// RLS (Row-Level Security) Handlers
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct GenerateRlsSqlQuery {
    pub database: String,
    pub table: String,
    pub action: String,
    pub schema: Option<String>,
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GenerateRlsSqlBody {
    pub policy_name: Option<String>,
    pub command: Option<String>,
    pub permissive: Option<String>,
    pub roles: Option<String>,
    pub using_expr: Option<String>,
    pub with_check_expr: Option<String>,
    pub predicate_type: Option<String>,
    pub predicate_function: Option<String>,
    pub predicate_args: Option<String>,
}

pub async fn list_rls_policies_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<TableObjectQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    // Check connection access
    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let default_schema = match db.dialect() {
        crate::db::Dialect::Postgres => "public",
        crate::db::Dialect::DuckDb => "main",
        _ => "dbo",
    };
    let schema = params.schema.as_deref().unwrap_or(default_schema);

    match metadata::list_rls_policies(db.as_ref(), &params.database, schema, &params.table).await {
        Ok(policies) => (StatusCode::OK, Json(policies)).into_response(),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn get_rls_status_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<TableObjectQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    // Check connection access
    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let default_schema = match db.dialect() {
        crate::db::Dialect::Postgres => "public",
        crate::db::Dialect::DuckDb => "main",
        _ => "dbo",
    };
    let schema = params.schema.as_deref().unwrap_or(default_schema);

    match metadata::get_rls_status(db.as_ref(), &params.database, schema, &params.table).await {
        Ok(status) => (StatusCode::OK, Json(status)).into_response(),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn generate_rls_sql_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<GenerateRlsSqlQuery>,
    Json(body): Json<GenerateRlsSqlBody>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None)
            .to_response(StatusCode::UNAUTHORIZED);
    }

    // Check connection access
    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    // Require at least Supervised sql_mode (generates DDL)
    if let AuthResult::ServiceAccountAccess { account_name } = &auth {
        if let Some(ref access_db) = state.access_db {
            let mode = access_db.get_sa_sql_mode(account_name);
            match mode {
                SqlMode::None | SqlMode::ReadOnly => {
                    return request_error(
                        "FORBIDDEN",
                        "RLS management requires at least supervised SQL access.",
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
                        "RLS management requires at least supervised SQL access.",
                        Some("Contact an admin to upgrade your SQL access mode."),
                    )
                    .to_response(StatusCode::FORBIDDEN);
                }
                _ => {}
            }
        }
    }

    let db = match state.registry.resolve(params.connection.as_deref()) {
        Ok(db) => db,
        Err(e) => {
            return request_error("INVALID_CONNECTION", &format!("{}", e), None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    if db.dialect() == crate::db::Dialect::DuckDb {
        return request_error("UNSUPPORTED", "RLS not supported for DuckDB connections", None)
            .to_response(StatusCode::BAD_REQUEST);
    }

    let default_schema = match db.dialect() {
        crate::db::Dialect::Postgres => "public",
        _ => "dbo",
    };
    let schema = params.schema.as_deref().unwrap_or(default_schema);

    // Build params map from body
    let mut rls_params = std::collections::HashMap::new();
    if let Some(v) = &body.policy_name { rls_params.insert("policy_name".to_string(), v.clone()); }
    if let Some(v) = &body.command { rls_params.insert("command".to_string(), v.clone()); }
    if let Some(v) = &body.permissive { rls_params.insert("permissive".to_string(), v.clone()); }
    if let Some(v) = &body.roles { rls_params.insert("roles".to_string(), v.clone()); }
    if let Some(v) = &body.using_expr { rls_params.insert("using_expr".to_string(), v.clone()); }
    if let Some(v) = &body.with_check_expr { rls_params.insert("with_check_expr".to_string(), v.clone()); }
    if let Some(v) = &body.predicate_type { rls_params.insert("predicate_type".to_string(), v.clone()); }
    if let Some(v) = &body.predicate_function { rls_params.insert("predicate_function".to_string(), v.clone()); }
    if let Some(v) = &body.predicate_args { rls_params.insert("predicate_args".to_string(), v.clone()); }

    match metadata::generate_rls_sql(db.as_ref(), &params.database, schema, &params.table, &params.action, &rls_params).await {
        Ok(sql) => (StatusCode::OK, Json(json!({ "sql": sql }))).into_response(),
        Err(e) => execution_error(&format!("{:#}", e), db.dialect())
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
