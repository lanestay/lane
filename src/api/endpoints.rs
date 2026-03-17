use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

use crate::auth::{self, AuthResult};
use crate::query::QueryParams;
use crate::query::validation::{apply_row_limit_dialect, is_select_like};

use super::errors::*;
use super::AppState;

const DEFAULT_ENDPOINT_LIMIT: usize = 10_000;
const MAX_ENDPOINT_LIMIT: usize = 100_000;

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateEndpointRequest {
    pub name: String,
    pub connection_name: String,
    pub database_name: String,
    pub query: String,
    pub description: Option<String>,
    pub parameters: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateEndpointRequest {
    pub connection_name: String,
    pub database_name: String,
    pub query: String,
    pub description: Option<String>,
    pub parameters: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SetEndpointPermissionsRequest {
    pub emails: Vec<String>,
}

// ============================================================================
// Parameter substitution
// ============================================================================

/// Extract `{{param_name}}` placeholders from a query string.
pub fn extract_parameters(query: &str) -> Vec<String> {
    let re = Regex::new(r"\{\{(\w+)\}\}").unwrap();
    let mut params: Vec<String> = Vec::new();
    for cap in re.captures_iter(query) {
        let name = cap[1].to_string();
        if !params.contains(&name) {
            params.push(name);
        }
    }
    params
}

/// Validate a parameter value is safe for substitution.
/// Rejects values containing SQL injection patterns: semicolons, comment markers,
/// and multi-statement attempts.
fn validate_param_value(name: &str, value: &str) -> Result<(), String> {
    if value.contains(';') {
        return Err(format!("Parameter '{}': semicolons not allowed", name));
    }
    if value.contains("--") {
        return Err(format!("Parameter '{}': SQL comments not allowed", name));
    }
    if value.contains("/*") || value.contains("*/") {
        return Err(format!("Parameter '{}': block comments not allowed", name));
    }
    // Reject common SQL injection keywords when combined with whitespace
    let upper = value.to_uppercase();
    for keyword in &["DROP ", "DELETE ", "INSERT ", "UPDATE ", "ALTER ", "EXEC ", "EXECUTE ", "TRUNCATE ", "CREATE ", "GRANT ", "REVOKE "] {
        if upper.contains(keyword) {
            return Err(format!("Parameter '{}': SQL keywords not allowed in values", name));
        }
    }
    Ok(())
}

/// Substitute `{{param_name}}` placeholders with values from the provided map.
/// Values are validated against injection patterns, then single quotes are escaped.
pub fn substitute_parameters(
    query: &str,
    values: &HashMap<String, String>,
    param_defs: &[ParamDef],
) -> Result<String, String> {
    let placeholders = extract_parameters(query);
    let defaults: HashMap<&str, &str> = param_defs
        .iter()
        .filter_map(|p| p.default.as_deref().map(|d| (p.name.as_str(), d)))
        .collect();

    // Check all required params are present and validate values
    for p in &placeholders {
        let val = values
            .get(p)
            .map(|s| s.as_str())
            .or_else(|| defaults.get(p.as_str()).copied());
        match val {
            None => return Err(format!("Missing required parameter: {}", p)),
            Some(v) => validate_param_value(p, v)?,
        }
    }

    let re = Regex::new(r"\{\{(\w+)\}\}").unwrap();
    let result = re.replace_all(query, |caps: &regex::Captures| {
        let name = &caps[1];
        let val = values
            .get(name)
            .map(|s| s.as_str())
            .or_else(|| defaults.get(name).copied())
            .unwrap_or("");
        // Escape single quotes to prevent SQL injection
        val.replace('\'', "''")
    });

    Ok(result.into_owned())
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ParamDef {
    pub name: String,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    pub param_type: Option<String>,
    pub default: Option<String>,
}

pub fn parse_param_defs(json_str: Option<&str>) -> Vec<ParamDef> {
    match json_str {
        Some(s) if !s.is_empty() => serde_json::from_str(s).unwrap_or_default(),
        _ => Vec::new(),
    }
}

// ============================================================================
// Auth helpers
// ============================================================================

fn extract_auth_email(auth: &AuthResult) -> Option<&str> {
    match auth {
        AuthResult::SessionAccess { email, .. } => Some(email.as_str()),
        AuthResult::TokenAccess { email, .. } => Some(email.as_str()),
        _ => None,
    }
}

fn require_admin(auth: &AuthResult) -> Result<(), Response> {
    match auth {
        AuthResult::FullAccess => Ok(()),
        AuthResult::SessionAccess { is_admin: true, .. } => Ok(()),
        AuthResult::SessionAccess { is_admin: false, .. } => Err(
            request_error("FORBIDDEN", "Admin access required", None)
                .to_response(StatusCode::FORBIDDEN),
        ),
        AuthResult::Denied(reason) => Err(
            request_error("UNAUTHORIZED", reason, None).to_response(StatusCode::UNAUTHORIZED),
        ),
        _ => Err(
            request_error("FORBIDDEN", "Admin access required", None)
                .to_response(StatusCode::FORBIDDEN),
        ),
    }
}

// ============================================================================
// Public data endpoints (for consumers)
// ============================================================================

/// GET /api/data/endpoints — list endpoints the user has access to
pub async fn list_data_endpoints_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None).to_response(StatusCode::UNAUTHORIZED);
    }

    let access_db = match &state.access_db {
        Some(db) => db,
        None => {
            return (StatusCode::OK, Json(json!([]))).into_response();
        }
    };

    let all_endpoints = match access_db.list_endpoints() {
        Ok(eps) => eps,
        Err(e) => {
            return request_error("INTERNAL_ERROR", &e, None)
                .to_response(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Filter to endpoints the user has access to
    let visible: Vec<_> = match &auth {
        AuthResult::FullAccess => all_endpoints,
        AuthResult::SessionAccess { email, is_admin, .. } => {
            if *is_admin {
                all_endpoints
            } else {
                all_endpoints
                    .into_iter()
                    .filter(|ep| access_db.check_endpoint_access(email, &ep.name))
                    .collect()
            }
        }
        AuthResult::TokenAccess { email, .. } => all_endpoints
            .into_iter()
            .filter(|ep| access_db.check_endpoint_access(email, &ep.name))
            .collect(),
        AuthResult::ServiceAccountAccess { account_name } => all_endpoints
            .into_iter()
            .filter(|ep| access_db.check_sa_endpoint_access(account_name, &ep.name))
            .collect(),
        _ => vec![],
    };

    // Return without the query field for listing
    let result: Vec<_> = visible
        .iter()
        .map(|ep| {
            json!({
                "name": ep.name,
                "connection_name": ep.connection_name,
                "database_name": ep.database_name,
                "description": ep.description,
                "parameters": ep.parameters.as_deref()
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()),
                "created_by": ep.created_by,
                "created_at": ep.created_at,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(result))).into_response()
}

/// GET /api/data/endpoints/{name} — execute an endpoint
pub async fn execute_data_endpoint_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(query_params): Query<HashMap<String, String>>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None).to_response(StatusCode::UNAUTHORIZED);
    }

    execute_endpoint_inner(&state, &auth, &name, &query_params).await
}

/// Shared endpoint execution logic (used by REST and MCP).
pub async fn execute_endpoint_inner(
    state: &AppState,
    auth: &AuthResult,
    endpoint_name: &str,
    params: &HashMap<String, String>,
) -> Response {
    let access_db = match &state.access_db {
        Some(db) => db,
        None => {
            return request_error(
                "NOT_CONFIGURED",
                "Access control is not enabled",
                None,
            )
            .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Load endpoint
    let endpoint = match access_db.get_endpoint(endpoint_name) {
        Ok(Some(ep)) => ep,
        Ok(None) => {
            return request_error(
                "NOT_FOUND",
                &format!("Endpoint '{}' not found", endpoint_name),
                None,
            )
            .to_response(StatusCode::NOT_FOUND);
        }
        Err(e) => {
            return request_error("INTERNAL_ERROR", &e, None)
                .to_response(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Check endpoint access
    match auth {
        AuthResult::FullAccess => {}
        AuthResult::SessionAccess { email, is_admin, .. } => {
            if !is_admin && !access_db.check_endpoint_access(email, endpoint_name) {
                return request_error(
                    "FORBIDDEN",
                    &format!("Access denied to endpoint '{}'", endpoint_name),
                    None,
                )
                .to_response(StatusCode::FORBIDDEN);
            }
        }
        AuthResult::TokenAccess { email, .. } => {
            if !access_db.check_endpoint_access(email, endpoint_name) {
                return request_error(
                    "FORBIDDEN",
                    &format!("Access denied to endpoint '{}'", endpoint_name),
                    None,
                )
                .to_response(StatusCode::FORBIDDEN);
            }
        }
        AuthResult::ServiceAccountAccess { account_name } => {
            if !access_db.check_sa_endpoint_access(account_name, endpoint_name) {
                return request_error(
                    "FORBIDDEN",
                    &format!("Access denied to endpoint '{}'", endpoint_name),
                    None,
                )
                .to_response(StatusCode::FORBIDDEN);
            }
        }
        _ => {
            return request_error("UNAUTHORIZED", "Authentication required", None)
                .to_response(StatusCode::UNAUTHORIZED);
        }
    }

    // Check connection access
    match auth {
        AuthResult::ServiceAccountAccess { account_name } => {
            if !access_db.check_sa_connection_access(account_name, &endpoint.connection_name) {
                return request_error(
                    "FORBIDDEN",
                    &format!(
                        "Access denied to connection '{}'",
                        endpoint.connection_name
                    ),
                    None,
                )
                .to_response(StatusCode::FORBIDDEN);
            }
        }
        _ => {
            if let Some(email) = extract_auth_email(auth) {
                if !access_db.check_connection_access(email, &endpoint.connection_name) {
                    return request_error(
                        "FORBIDDEN",
                        &format!(
                            "Access denied to connection '{}'",
                            endpoint.connection_name
                        ),
                        None,
                    )
                    .to_response(StatusCode::FORBIDDEN);
                }
            }
        }
    }

    // Resolve connection
    let db = match state.registry.resolve(Some(&endpoint.connection_name)) {
        Ok(db) => db,
        Err(e) => {
            return request_error(
                "INVALID_CONNECTION",
                &format!("{}", e),
                Some("The endpoint's connection may no longer exist."),
            )
            .to_response(StatusCode::BAD_REQUEST);
        }
    };

    // Parse parameter definitions and substitute
    let param_defs = parse_param_defs(endpoint.parameters.as_deref());
    let query = match substitute_parameters(&endpoint.query, params, &param_defs) {
        Ok(q) => q,
        Err(e) => {
            return request_error("INVALID_REQUEST", &e, None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    // Enforce read-only: endpoints can only execute SELECT/WITH queries
    if !is_select_like(&query) {
        return request_error(
            "FORBIDDEN",
            "Endpoints can only execute read-only queries (SELECT/WITH)",
            None,
        )
        .to_response(StatusCode::FORBIDDEN);
    }

    // Parse consumer query params for limit/offset/format
    let ndjson = params.get("format").map(|v| v == "ndjson").unwrap_or(false);
    let raw_limit = params.get("limit").map(|v| v.as_str());
    let mut warning: Option<String> = None;

    let req_limit: Option<usize> = match raw_limit {
        Some("unlimited") if ndjson => None,
        Some("unlimited") => {
            warning = Some("limit=unlimited requires format=ndjson. Falling back to 10,000 row limit.".to_string());
            Some(DEFAULT_ENDPOINT_LIMIT)
        }
        Some(v) => {
            if let Ok(n) = v.parse::<usize>() {
                if ndjson {
                    // NDJSON: any limit is fine
                    Some(n)
                } else if n > DEFAULT_ENDPOINT_LIMIT {
                    warning = Some(format!(
                        "JSON format is capped at {} rows. Use format=ndjson for up to {} rows, or format=ndjson&limit=unlimited for no cap.",
                        DEFAULT_ENDPOINT_LIMIT, MAX_ENDPOINT_LIMIT
                    ));
                    Some(DEFAULT_ENDPOINT_LIMIT)
                } else {
                    Some(n)
                }
            } else {
                Some(DEFAULT_ENDPOINT_LIMIT)
            }
        }
        None => {
            if ndjson {
                warning = Some(format!(
                    "Defaulting to {} row limit. Set limit=N for a specific amount, or limit=unlimited for no cap.",
                    MAX_ENDPOINT_LIMIT
                ));
                Some(MAX_ENDPOINT_LIMIT)
            } else {
                Some(DEFAULT_ENDPOINT_LIMIT)
            }
        }
    };

    // Apply row limit via dialect-aware wrapping (skip if unlimited)
    let limited_query = match req_limit {
        Some(limit) => apply_row_limit_dialect(&query, limit, db.dialect()),
        None => query.clone(),
    };

    // Execute
    let qp = QueryParams {
        database: endpoint.database_name.clone(),
        query: limited_query,
        pagination: false,
        json_stream: ndjson,
        include_metadata: true,
        ..Default::default()
    };

    // NDJSON streaming path
    if ndjson {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        match db.execute_query_streaming(&qp, tx).await {
            Ok(()) => {
                let stream = ReceiverStream::new(rx);
                return Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/x-ndjson")
                    .body(Body::from_stream(stream))
                    .unwrap();
            }
            Err(e) => {
                return execution_error(&format!("{:#}", e), db.dialect())
                    .to_response(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    match db.execute_query(&qp).await {
        Ok(result) => {
            let mut resp = json!(result);
            // Add pagination metadata + warnings
            if let Some(obj) = resp.as_object_mut() {
                obj.insert("limit".to_string(), req_limit.map(|l| json!(l)).unwrap_or(json!("unlimited")));
                if let Some(ref w) = warning {
                    obj.insert("warning".to_string(), json!(w));
                }
            }
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => {
            execution_error(&format!("{:#}", e), db.dialect())
                .to_response(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// ============================================================================
// Admin endpoints (CRUD)
// ============================================================================

/// POST /api/lane/admin/endpoints — create endpoint
pub async fn create_endpoint_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateEndpointRequest>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let access_db = match &state.access_db {
        Some(db) => db,
        None => {
            return request_error("NOT_CONFIGURED", "Access control is not enabled", None)
                .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Enforce read-only at creation time
    if !is_select_like(&body.query) {
        return request_error(
            "INVALID_REQUEST",
            "Endpoint queries must be read-only (SELECT/WITH)",
            None,
        )
        .to_response(StatusCode::BAD_REQUEST);
    }

    let created_by = extract_auth_email(&auth).map(|s| s.to_string());

    match access_db.create_endpoint(
        &body.name,
        &body.connection_name,
        &body.database_name,
        &body.query,
        body.description.as_deref(),
        body.parameters.as_deref(),
        created_by.as_deref(),
    ) {
        Ok(()) => {
            if let Some(ref search_db) = state.search_db {
                search_db.index_endpoint(
                    &body.name,
                    &body.connection_name,
                    &body.database_name,
                    body.description.as_deref().unwrap_or(""),
                    &body.query,
                );
            }
            (StatusCode::CREATED, Json(json!({"success": true}))).into_response()
        }
        Err(e) => request_error("CREATE_FAILED", &e, None).to_response(StatusCode::BAD_REQUEST),
    }
}

/// GET /api/lane/admin/endpoints — list all endpoints (admin)
pub async fn list_endpoints_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let access_db = match &state.access_db {
        Some(db) => db,
        None => return (StatusCode::OK, Json(json!([]))).into_response(),
    };

    match access_db.list_endpoints() {
        Ok(endpoints) => (StatusCode::OK, Json(json!(endpoints))).into_response(),
        Err(e) => request_error("QUERY_FAILED", &e, None)
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// PUT /api/lane/admin/endpoints/{name} — update endpoint
pub async fn update_endpoint_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<UpdateEndpointRequest>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let access_db = match &state.access_db {
        Some(db) => db,
        None => {
            return request_error("NOT_CONFIGURED", "Access control is not enabled", None)
                .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Enforce read-only at update time
    if !is_select_like(&body.query) {
        return request_error(
            "INVALID_REQUEST",
            "Endpoint queries must be read-only (SELECT/WITH)",
            None,
        )
        .to_response(StatusCode::BAD_REQUEST);
    }

    match access_db.update_endpoint(
        &name,
        &body.connection_name,
        &body.database_name,
        &body.query,
        body.description.as_deref(),
        body.parameters.as_deref(),
    ) {
        Ok(()) => {
            if let Some(ref search_db) = state.search_db {
                search_db.remove_endpoint(&name);
                search_db.index_endpoint(
                    &name,
                    &body.connection_name,
                    &body.database_name,
                    body.description.as_deref().unwrap_or(""),
                    &body.query,
                );
            }
            (StatusCode::OK, Json(json!({"success": true}))).into_response()
        }
        Err(e) => request_error("UPDATE_FAILED", &e, None).to_response(StatusCode::BAD_REQUEST),
    }
}

/// DELETE /api/lane/admin/endpoints/{name} — delete endpoint
pub async fn delete_endpoint_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let access_db = match &state.access_db {
        Some(db) => db,
        None => {
            return request_error("NOT_CONFIGURED", "Access control is not enabled", None)
                .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    match access_db.delete_endpoint(&name) {
        Ok(()) => {
            if let Some(ref search_db) = state.search_db {
                search_db.remove_endpoint(&name);
            }
            (StatusCode::OK, Json(json!({"success": true}))).into_response()
        }
        Err(e) => request_error("DELETE_FAILED", &e, None)
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// GET /api/lane/admin/endpoints/{name}/permissions — get permissions
pub async fn get_endpoint_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let access_db = match &state.access_db {
        Some(db) => db,
        None => return (StatusCode::OK, Json(json!([]))).into_response(),
    };

    match access_db.get_endpoint_permissions(&name) {
        Ok(emails) => (StatusCode::OK, Json(json!(emails))).into_response(),
        Err(e) => request_error("QUERY_FAILED", &e, None)
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// PUT /api/lane/admin/endpoints/{name}/permissions — set permissions
pub async fn set_endpoint_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<SetEndpointPermissionsRequest>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let access_db = match &state.access_db {
        Some(db) => db,
        None => {
            return request_error("NOT_CONFIGURED", "Access control is not enabled", None)
                .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    match access_db.set_endpoint_permissions(&name, &body.emails) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true}))).into_response(),
        Err(e) => request_error("UPDATE_FAILED", &e, None).to_response(StatusCode::BAD_REQUEST),
    }
}
