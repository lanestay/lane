use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::env;
use std::sync::Arc;

use crate::auth::access_control::{
    CreateServiceAccountRequest, CreateUserRequest, GenerateTokenRequest,
    SetPermissionsRequest, SetSaStoragePermissionsRequest, SetStoragePermissionsRequest,
    UpdateServiceAccountRequest, UpdateUserRequest,
};
use crate::auth::{authenticate, AuthResult};
use crate::api::AppState;

// ============================================================================
// Admin auth helper
// ============================================================================

/// Check if the caller is authorized for admin operations.
/// Accepts: system API key, admin token, or admin session.
async fn check_admin_auth(headers: &HeaderMap, state: &AppState) -> Result<(), Response> {
    let auth = authenticate(headers, state).await;

    match auth {
        AuthResult::FullAccess => Ok(()),
        AuthResult::SessionAccess { is_admin: true, .. } => Ok(()),
        AuthResult::SessionAccess { is_admin: false, .. } => Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Admin access required"})),
        )
            .into_response()),
        AuthResult::TokenAccess { ref email, .. } => {
            if let Some(ref access_db) = state.access_db {
                if access_db.is_admin(email) {
                    return Ok(());
                }
            }
            Err((
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Admin access required"})),
            )
                .into_response())
        }
        AuthResult::ServiceAccountAccess { .. } => Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Service accounts cannot perform admin operations"})),
        )
            .into_response()),
        AuthResult::Denied(reason) => Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": reason})),
        )
            .into_response()),
    }
}

/// Get the access_db or return 503.
fn require_access_db(state: &AppState) -> Result<&crate::auth::access_control::AccessControlDb, Response> {
    state.access_db.as_deref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "Access control is not enabled"})),
        )
            .into_response()
    })
}

// ============================================================================
// User lookup (system-key only, not admin-only — used by NextJS to gate page access)
// ============================================================================

#[derive(Deserialize)]
pub struct UserCheckQuery {
    pub email: String,
}

pub async fn check_user_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<UserCheckQuery>,
) -> Response {
    // Require system API key (not token auth — this is for NextJS internal use)
    let header_key = headers
        .get("x-api-key")
        .or_else(|| headers.get("x-lane-key"))
        .and_then(|v| v.to_str().ok());

    let current_api_key = state.api_key.read().await;
    let authorized = match header_key {
        Some(k) if k == *current_api_key => true,
        Some(k) => {
            if let Ok(automation_key) = env::var("LANE_AUTOMATION_KEY") {
                k == automation_key
            } else {
                false
            }
        }
        None => false,
    };

    if !authorized {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "Invalid API key"}))).into_response();
    }

    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let exists = db.user_exists(&query.email);
    (StatusCode::OK, Json(json!({"exists": exists, "email": query.email}))).into_response()
}

// ============================================================================
// Token endpoints
// ============================================================================

pub async fn generate_token_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<GenerateTokenRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    // Read token policy
    let max_lifespan = db.get_config("token_max_lifespan_hours")
        .ok().flatten()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let default_lifespan = db.get_config("token_default_lifespan_hours")
        .ok().flatten()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    // Admin can pass expires_hours: 0 to mean "permanent" (no expiry).
    // None (omitted) = apply default policy. Some(0) = explicitly permanent.
    let explicit = body.expires_hours.is_some();
    let mut expires_hours = match body.expires_hours {
        Some(0) => None,   // explicit permanent
        other => other,
    };

    // Apply default only if not explicitly specified
    if !explicit && default_lifespan > 0 {
        expires_hours = Some(default_lifespan);
    }

    // Cap to max if max is set (but explicit 0 / permanent is allowed when max=0)
    if max_lifespan > 0 {
        expires_hours = Some(match expires_hours {
            Some(h) if h > max_lifespan => max_lifespan,
            Some(h) => h,
            None => max_lifespan, // permanent requested but max is set → apply max
        });
    }

    // Validate pii_mode if provided
    if let Some(ref mode) = body.pii_mode {
        if !["scrub", "none"].contains(&mode.as_str()) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "pii_mode must be one of: scrub, none"})),
            )
                .into_response();
        }
    }

    match db.generate_token(&body.email, body.label.as_deref(), expires_hours, body.pii_mode.as_deref()) {
        Ok(token) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "token": token,
                "email": body.email,
                "label": body.label,
                "expires_hours": expires_hours,
                "pii_mode": body.pii_mode,
                "note": "Store this token securely — it cannot be retrieved again."
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct RevokeTokenRequest {
    pub token: String,
}

pub async fn revoke_token_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<RevokeTokenRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.revoke_token(&body.token) {
        Ok(affected) => (
            StatusCode::OK,
            Json(json!({"success": true, "revoked": affected})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct ListTokensQuery {
    pub email: Option<String>,
}

pub async fn list_tokens_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ListTokensQuery>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.list_tokens(query.email.as_deref()) {
        Ok(tokens) => (StatusCode::OK, Json(json!({"tokens": tokens}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

// ============================================================================
// User endpoints
// ============================================================================

pub async fn create_user_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateUserRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.create_user(
        &body.email,
        body.display_name.as_deref(),
        body.is_admin.unwrap_or(false),
    ) {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({"success": true, "email": body.email})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

pub async fn update_user_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(email): Path<String>,
    Json(body): Json<UpdateUserRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    // Validate pii_mode if provided
    if let Some(ref mode) = body.pii_mode {
        if !mode.is_empty() && mode != "inherit" && !["scrub", "none"].contains(&mode.as_str()) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "pii_mode must be one of: scrub, none, or inherit"})),
            )
                .into_response();
        }
    }

    match db.update_user(
        &email,
        body.display_name.as_deref(),
        body.is_admin,
        body.is_enabled,
        body.mcp_enabled,
        body.pii_mode.as_deref(),
        body.sql_mode.as_deref(),
        body.max_pending_approvals.map(|v| if v == 0 { None } else { Some(v) }),
    ) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"success": true, "email": email})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

pub async fn list_users_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.list_users() {
        Ok(users) => {
            // Also fetch permissions and connection permissions for each user
            let mut enriched = Vec::new();
            for user in users {
                let perms = db.get_permissions(&user.email).unwrap_or_default();
                let conn_perms = db.get_allowed_connections(&user.email).unwrap_or(None);
                enriched.push(json!({
                    "email": user.email,
                    "display_name": user.display_name,
                    "is_admin": user.is_admin,
                    "is_enabled": user.is_enabled,
                    "mcp_enabled": user.mcp_enabled,
                    "pii_mode": user.pii_mode,
                    "sql_mode": user.sql_mode,
                    "max_pending_approvals": user.max_pending_approvals,
                    "created_at": user.created_at,
                    "updated_at": user.updated_at,
                    "permissions": perms,
                    "connection_permissions": conn_perms,
                }));
            }
            (StatusCode::OK, Json(json!({"users": enriched}))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

pub async fn delete_user_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(email): Path<String>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    if db.is_admin(&email) {
        let admin_count = match db.count_admins() {
            Ok(c) => c,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": e})),
                )
                    .into_response()
            }
        };
        if admin_count <= 1 {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "cannot delete the last admin; promote another admin first",
                    "code": "LAST_ADMIN"
                })),
            )
                .into_response();
        }
    }

    match db.delete_user(&email) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"success": true, "deleted": email})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

pub async fn purge_user_sessions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(email): Path<String>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.delete_user_sessions(&email) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"success": true, "email": email})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

// ============================================================================
// Permission endpoints
// ============================================================================

pub async fn set_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetPermissionsRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.set_permissions(&body.email, &body.permissions) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"success": true, "email": body.email})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

// ============================================================================
// Audit log endpoint
// ============================================================================

#[derive(Deserialize)]
pub struct AuditLogQuery {
    pub email: Option<String>,
    pub action: Option<String>,
    pub limit: Option<usize>,
}

pub async fn audit_log_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AuditLogQuery>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let limit = query.limit.unwrap_or(100);

    match db.query_audit_log(query.email.as_deref(), query.action.as_deref(), limit) {
        Ok(entries) => (StatusCode::OK, Json(json!({"entries": entries}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

// ============================================================================
// Self-service token endpoints (for authenticated non-admin users)
// ============================================================================

/// Extract the caller's email from token auth or session auth.
/// Returns the email or an error response.
async fn check_self_auth(headers: &HeaderMap, state: &AppState) -> Result<String, Response> {
    // Try x-api-key token auth first
    let header_key = headers
        .get("x-api-key")
        .or_else(|| headers.get("x-lane-key"))
        .and_then(|v| v.to_str().ok());

    if let Some(key) = header_key {
        if key == *state.api_key.read().await {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "System keys should use admin endpoint"})),
            )
                .into_response());
        }
        if let Some(ref access_db) = state.access_db {
            if let Ok(info) = access_db.validate_token(key) {
                return Ok(info.email);
            }
        }
    }

    // Fall back to session auth
    let auth = authenticate(headers, state).await;
    match auth {
        AuthResult::SessionAccess { email, .. } => Ok(email),
        AuthResult::TokenAccess { email, .. } => Ok(email),
        AuthResult::ServiceAccountAccess { .. } => Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Service accounts cannot perform self-service operations"})),
        )
            .into_response()),
        _ => Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Authentication required"})),
        )
            .into_response()),
    }
}

/// Generate a token for the calling user.
/// Accepts both token auth (x-api-key) and session auth (Authorization: Bearer).
pub async fn self_generate_token_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SelfGenerateTokenRequest>,
) -> Response {
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let email = match check_self_auth(&headers, &state).await {
        Ok(e) => e,
        Err(resp) => return resp,
    };

    // Read token policy
    let max_lifespan = db.get_config("token_max_lifespan_hours")
        .ok().flatten()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let default_lifespan = db.get_config("token_default_lifespan_hours")
        .ok().flatten()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    // Self-service tokens always expire
    let mut expires_hours = match body.expires_hours {
        Some(h) => h,
        None => {
            if default_lifespan > 0 { default_lifespan }
            else if max_lifespan > 0 { max_lifespan }
            else { 36 } // fallback default
        }
    };

    // Cap to max if set
    if max_lifespan > 0 && expires_hours > max_lifespan {
        expires_hours = max_lifespan;
    }

    match db.generate_token(&email, body.label.as_deref(), Some(expires_hours), None) {
        Ok(token) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "token": token,
                "email": email,
                "label": body.label,
                "expires_hours": expires_hours,
                "note": "Store this token securely — it cannot be retrieved again."
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct SelfGenerateTokenRequest {
    pub label: Option<String>,
    pub expires_hours: Option<u64>,
}

/// List tokens for the calling user.
/// Accepts both token auth (x-api-key) and session auth (Authorization: Bearer).
pub async fn self_list_tokens_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let email = match check_self_auth(&headers, &state).await {
        Ok(e) => e,
        Err(resp) => return resp,
    };

    match db.list_tokens(Some(&email)) {
        Ok(tokens) => (
            StatusCode::OK,
            Json(json!({"email": email, "tokens": tokens})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

/// Revoke a token owned by the calling user (by prefix).
pub async fn self_revoke_token_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(prefix): Path<String>,
) -> Response {
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let email = match check_self_auth(&headers, &state).await {
        Ok(e) => e,
        Err(resp) => return resp,
    };

    // Verify the token belongs to this user by listing their tokens
    let user_tokens = match db.list_tokens(Some(&email)) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e})),
            )
                .into_response()
        }
    };

    let owns_token = user_tokens.iter().any(|t| t.token_prefix == prefix);
    if !owns_token {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Token not found or not owned by you"})),
        )
            .into_response();
    }

    match db.revoke_token(&prefix) {
        Ok(affected) => (
            StatusCode::OK,
            Json(json!({"success": true, "revoked": affected})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

// ============================================================================
// Token policy endpoints
// ============================================================================

#[derive(Deserialize)]
pub struct TokenPolicyRequest {
    pub max_lifespan_hours: u64,
    pub default_lifespan_hours: u64,
}

pub async fn get_token_policy_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let max = db.get_config("token_max_lifespan_hours")
        .ok().flatten()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let default = db.get_config("token_default_lifespan_hours")
        .ok().flatten()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    (StatusCode::OK, Json(json!({
        "max_lifespan_hours": max,
        "default_lifespan_hours": default,
    }))).into_response()
}

pub async fn set_token_policy_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<TokenPolicyRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    if let Err(e) = db.set_config("token_max_lifespan_hours", &body.max_lifespan_hours.to_string()) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response();
    }
    if let Err(e) = db.set_config("token_default_lifespan_hours", &body.default_lifespan_hours.to_string()) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response();
    }

    (StatusCode::OK, Json(json!({
        "success": true,
        "max_lifespan_hours": body.max_lifespan_hours,
        "default_lifespan_hours": body.default_lifespan_hours,
    }))).into_response()
}

// ============================================================================
// Admin password reset
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct AdminSetPasswordRequest {
    pub password: String,
}

// ============================================================================
// Inventory endpoint — returns all connections, databases, and tables
// ============================================================================

pub async fn inventory_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }

    let connections = crate::db::metadata::list_connections(&state.registry);
    let mut result = Vec::new();

    for conn_meta in &connections {
        let db = match state.registry.resolve(Some(&conn_meta.name)) {
            Ok(db) => db,
            Err(_) => continue,
        };

        let conn_type = conn_meta.connection_type;

        // List databases for this connection
        let databases_raw = match crate::db::metadata::list_databases(db.as_ref()).await {
            Ok(dbs) => dbs,
            Err(_) => {
                result.push(json!({
                    "name": conn_meta.name,
                    "type": conn_type,
                    "databases": [],
                }));
                continue;
            }
        };

        let mut db_entries = Vec::new();
        for db_row in &databases_raw {
            let db_name = match db_row.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => continue,
            };

            // List tables for this database (use default schema)
            let schema = match conn_type {
                "postgres" => "public",
                "clickhouse" => "default",
                "duckdb" => "main",
                _ => "dbo",
            };
            let tables = match crate::db::metadata::list_tables(db.as_ref(), db_name, schema).await {
                Ok(t) => t
                    .into_iter()
                    .map(|row| {
                        let table_schema = row
                            .get("TABLE_SCHEMA")
                            .and_then(|v| v.as_str())
                            .unwrap_or(schema)
                            .to_string();
                        let table_name = row
                            .get("TABLE_NAME")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        json!({"schema": table_schema, "name": table_name})
                    })
                    .collect::<Vec<_>>(),
                Err(_) => Vec::new(),
            };

            db_entries.push(json!({
                "name": db_name,
                "tables": tables,
            }));
        }

        result.push(json!({
            "name": conn_meta.name,
            "type": conn_type,
            "databases": db_entries,
        }));
    }

    (StatusCode::OK, Json(json!({"connections": result}))).into_response()
}

// ============================================================================
// Admin password reset
// ============================================================================

pub async fn admin_set_password_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(email): Path<String>,
    Json(body): Json<AdminSetPasswordRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    if body.password.len() < 8 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Password must be at least 8 characters"})),
        )
            .into_response();
    }

    match db.set_password(&email, &body.password) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"success": true, "email": email})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

// ============================================================================
// API Key Rotation
// ============================================================================

pub async fn rotate_api_key_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let new_key = crate::auth::access_control::generate_random_hex(32);

    if let Err(e) = db.set_config("system_api_key", &new_key) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to persist new API key: {}", e)})),
        )
            .into_response();
    }

    // Update runtime state
    *state.api_key.write().await = new_key.clone();

    // Audit log
    db.log_access(None, None, None, None, Some("api_key_rotated"), "success", Some("System API key rotated by admin"));

    (
        StatusCode::OK,
        Json(json!({
            "success": true,
            "api_key": new_key,
            "note": "Save this key — it cannot be retrieved again. All existing integrations using the old key will stop working."
        })),
    )
        .into_response()
}

// ============================================================================
// Connection Permission endpoints
// ============================================================================

#[derive(Deserialize)]
pub struct ConnectionPermQuery {
    pub email: String,
}

pub async fn get_connection_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ConnectionPermQuery>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.get_allowed_connections(&query.email) {
        Ok(perms) => (StatusCode::OK, Json(json!({ "connections": perms }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct SetConnectionPermRequest {
    pub email: String,
    pub connections: Vec<String>,
}

pub async fn set_connection_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetConnectionPermRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.set_connection_permissions(&body.email, &body.connections) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"success": true, "email": body.email})),
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

// ============================================================================
// Storage permission endpoints
// ============================================================================

#[derive(Deserialize)]
pub struct StoragePermQuery {
    pub email: String,
}

pub async fn get_storage_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<StoragePermQuery>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.get_storage_permissions(&query.email) {
        Ok(perms) => (StatusCode::OK, Json(json!({ "permissions": perms }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn set_storage_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetStoragePermissionsRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.set_storage_permissions(&body.email, &body.permissions) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true, "email": body.email}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct SaStoragePermQuery {
    pub name: String,
}

pub async fn get_sa_storage_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<SaStoragePermQuery>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.get_sa_storage_permissions(&query.name) {
        Ok(perms) => (StatusCode::OK, Json(json!({ "permissions": perms }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn set_sa_storage_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetSaStoragePermissionsRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.set_sa_storage_permissions(&body.name, &body.permissions) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true, "name": body.name}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

// ============================================================================
// PII Rule endpoints
// ============================================================================

pub async fn list_pii_rules_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.list_pii_rules() {
        Ok(rules) => (StatusCode::OK, Json(json!({"rules": rules}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct CreatePiiRuleRequest {
    pub name: String,
    pub description: Option<String>,
    pub regex_pattern: String,
    pub replacement_text: String,
    pub entity_kind: String,
}

pub async fn create_pii_rule_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreatePiiRuleRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.create_pii_rule(
        &body.name,
        body.description.as_deref(),
        &body.regex_pattern,
        &body.replacement_text,
        &body.entity_kind,
    ) {
        Ok(id) => (
            StatusCode::CREATED,
            Json(json!({"success": true, "id": id})),
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct UpdatePiiRuleRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub regex_pattern: Option<String>,
    pub replacement_text: Option<String>,
    pub entity_kind: Option<String>,
    pub is_enabled: Option<bool>,
}

pub async fn update_pii_rule_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(body): Json<UpdatePiiRuleRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.update_pii_rule(
        id,
        body.name.as_deref(),
        body.description.as_deref(),
        body.regex_pattern.as_deref(),
        body.replacement_text.as_deref(),
        body.entity_kind.as_deref(),
        body.is_enabled,
    ) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn delete_pii_rule_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.delete_pii_rule(id) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct TestPiiRuleRequest {
    pub regex_pattern: String,
    pub replacement_text: String,
    pub sample_text: String,
}

pub async fn test_pii_rule_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<TestPiiRuleRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }

    let regex = match regex::Regex::new(&body.regex_pattern) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("Invalid regex: {}", e)})),
            )
                .into_response()
        }
    };

    let mut matches = Vec::new();
    let mut scrubbed = body.sample_text.clone();

    for mat in regex.find_iter(&body.sample_text) {
        matches.push(json!({
            "start": mat.start(),
            "end": mat.end(),
            "text": mat.as_str(),
        }));
    }

    // Build scrubbed text by replacing from end to start
    let all_matches: Vec<_> = regex.find_iter(&body.sample_text).collect();
    for mat in all_matches.iter().rev() {
        scrubbed.replace_range(mat.start()..mat.end(), &body.replacement_text);
    }

    (
        StatusCode::OK,
        Json(json!({
            "matches": matches,
            "scrubbed_text": scrubbed,
        })),
    )
        .into_response()
}

// ============================================================================
// PII Column endpoints
// ============================================================================

#[derive(Deserialize)]
pub struct ListPiiColumnsQuery {
    pub connection: Option<String>,
    pub database: Option<String>,
}

pub async fn list_pii_columns_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ListPiiColumnsQuery>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.list_pii_columns(query.connection.as_deref(), query.database.as_deref()) {
        Ok(columns) => (StatusCode::OK, Json(json!({"columns": columns}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct SetPiiColumnRequest {
    pub connection_name: String,
    pub database_name: String,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub column_name: String,
    pub pii_type: Option<String>,
    pub custom_replacement: Option<String>,
}

pub async fn set_pii_column_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetPiiColumnRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    // Resolve default schema based on connection dialect
    let default_schema = match state.registry.resolve(Some(&body.connection_name)) {
        Ok(conn) => match conn.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        },
        Err(_) => "dbo",
    };
    let schema = body.schema_name.as_deref().unwrap_or(default_schema);
    let pii_type = body.pii_type.as_deref().unwrap_or("auto");

    match db.set_pii_column(
        &body.connection_name,
        &body.database_name,
        schema,
        &body.table_name,
        &body.column_name,
        pii_type,
        body.custom_replacement.as_deref(),
    ) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn remove_pii_column_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.remove_pii_column(id) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

// ============================================================================
// Storage Column Links
// ============================================================================

#[cfg(feature = "storage")]
#[derive(Deserialize)]
pub struct ListStorageColumnLinksQuery {
    pub connection: Option<String>,
    pub database: Option<String>,
}

#[cfg(feature = "storage")]
pub async fn list_storage_column_links_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ListStorageColumnLinksQuery>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.list_storage_column_links(query.connection.as_deref(), query.database.as_deref()) {
        Ok(links) => (StatusCode::OK, Json(json!({"links": links}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

#[cfg(feature = "storage")]
#[derive(Deserialize)]
pub struct SetStorageColumnLinkRequest {
    pub connection_name: String,
    pub database_name: String,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub column_name: String,
    pub storage_connection: String,
    pub bucket_name: String,
    pub key_prefix: Option<String>,
}

#[cfg(feature = "storage")]
pub async fn set_storage_column_link_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetStorageColumnLinkRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.set_storage_column_link(
        &body.connection_name,
        &body.database_name,
        body.schema_name.as_deref(),
        &body.table_name,
        &body.column_name,
        &body.storage_connection,
        &body.bucket_name,
        body.key_prefix.as_deref(),
    ) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

#[cfg(feature = "storage")]
pub async fn remove_storage_column_link_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.remove_storage_column_link(id) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

/// Public (non-admin) endpoint for query UI to fetch active storage column links
#[cfg(feature = "storage")]
pub async fn list_storage_column_links_public_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ListStorageColumnLinksQuery>,
) -> Response {
    // Accept any authenticated user (API key, token, or session)
    match authenticate(&headers, &state).await {
        AuthResult::Denied(reason) => {
            return (StatusCode::UNAUTHORIZED, Json(json!({"error": reason}))).into_response();
        }
        _ => {}
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.list_storage_column_links(query.connection.as_deref(), query.database.as_deref()) {
        Ok(links) => (StatusCode::OK, Json(json!({"links": links}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct DiscoverPiiColumnsRequest {
    pub connection: String,
    pub database: String,
    pub schema: Option<String>,
    pub table: String,
    pub sample_rows: Option<usize>,
}

pub async fn discover_pii_columns_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<DiscoverPiiColumnsRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }

    let backend = match state.registry.resolve(Some(&body.connection)) {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{}", e)})),
            )
                .into_response()
        }
    };

    let sample_rows = body.sample_rows.unwrap_or(100);
    let default_schema = match backend.dialect() {
        crate::db::Dialect::Postgres => "public",
        crate::db::Dialect::DuckDb => "main",
        crate::db::Dialect::ClickHouse => "default",
        _ => "dbo",
    };
    let schema = body.schema.as_deref().unwrap_or(default_schema);

    // Build a SELECT TOP N / LIMIT N query
    let sample_query = match backend.dialect() {
        crate::db::Dialect::Postgres | crate::db::Dialect::DuckDb | crate::db::Dialect::ClickHouse => format!(
            "SELECT * FROM \"{}\".\"{}\" LIMIT {}",
            schema, body.table, sample_rows
        ),
        _ => format!(
            "SELECT TOP {} * FROM [{}].[{}]",
            sample_rows, schema, body.table
        ),
    };

    let qp = crate::query::QueryParams {
        database: body.database.clone(),
        query: sample_query,
        include_metadata: true,
        ..Default::default()
    };

    let result = match backend.execute_query(&qp).await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Query failed: {:#}", e)})),
            )
                .into_response()
        }
    };

    // Run PII detection on each column
    let processor = crate::pii::PiiProcessor::new(
        crate::pii::PiiMode::Scrub,
        Vec::new(),
        Vec::new(),
    );

    let mut column_stats: std::collections::HashMap<String, (Vec<String>, usize, Vec<String>)> =
        std::collections::HashMap::new();

    for row in &result.data {
        for (col_name, value) in row {
            if let serde_json::Value::String(s) = value {
                let original = s.clone();
                let processed = processor.process_text(s);
                if processed != original {
                    let entry = column_stats
                        .entry(col_name.clone())
                        .or_insert_with(|| (Vec::new(), 0, Vec::new()));
                    entry.1 += 1;
                    // Detect which types were found
                    if processed.contains("<ssn>") && !entry.0.contains(&"ssn".to_string()) {
                        entry.0.push("ssn".to_string());
                    }
                    if processed.contains("<credit_card>")
                        && !entry.0.contains(&"credit_card".to_string())
                    {
                        entry.0.push("credit_card".to_string());
                    }
                    if processed.contains("<email_address>")
                        && !entry.0.contains(&"email".to_string())
                    {
                        entry.0.push("email".to_string());
                    }
                    if processed.contains("<phone_number>")
                        && !entry.0.contains(&"phone".to_string())
                    {
                        entry.0.push("phone".to_string());
                    }
                    if entry.2.len() < 3 {
                        entry.2.push(original);
                    }
                }
            }
        }
    }

    let suggestions: Vec<_> = column_stats
        .into_iter()
        .map(|(col, (types, count, samples))| {
            json!({
                "column_name": col,
                "detected_types": types,
                "match_count": count,
                "sample_matches": samples,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!({"suggestions": suggestions}))).into_response()
}

// ============================================================================
// PII Settings endpoints
// ============================================================================

pub async fn get_pii_settings_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let global_enabled = db
        .get_config("pii_global_enabled")
        .ok()
        .flatten()
        .map(|v| v == "true")
        .unwrap_or(false);

    let default_mode = db
        .get_config("pii_default_mode")
        .ok()
        .flatten()
        .unwrap_or_else(|| "scrub".to_string());

    // Gather per-connection overrides
    let connections = crate::db::metadata::list_connections(&state.registry);
    let mut overrides = serde_json::Map::new();
    for conn in &connections {
        if let Ok(Some(mode)) = db.get_config(&format!("pii_override_{}", conn.name)) {
            overrides.insert(conn.name.clone(), json!(mode));
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "global_enabled": global_enabled,
            "default_mode": default_mode,
            "connection_overrides": overrides,
        })),
    )
        .into_response()
}

#[derive(Deserialize)]
pub struct SetPiiSettingsRequest {
    pub global_enabled: Option<bool>,
    pub default_mode: Option<String>,
    pub connection_overrides: Option<std::collections::HashMap<String, String>>,
}

pub async fn set_pii_settings_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetPiiSettingsRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    if let Some(enabled) = body.global_enabled {
        if let Err(e) = db.set_config("pii_global_enabled", if enabled { "true" } else { "false" }) {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response();
        }
    }

    if let Some(ref mode) = body.default_mode {
        if mode != "scrub" {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "default_mode must be scrub"})),
            )
                .into_response();
        }
        if let Err(e) = db.set_config("pii_default_mode", mode) {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response();
        }
    }

    if let Some(ref overrides) = body.connection_overrides {
        for (conn_name, mode) in overrides {
            let key = format!("pii_override_{}", conn_name);
            if mode.is_empty() {
                // Remove override — set to empty string (effectively unset)
                let _ = db.set_config(&key, "");
            } else {
                if mode != "scrub" {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": format!("Invalid mode '{}' for connection '{}'", mode, conn_name)})),
                    )
                        .into_response();
                }
                if let Err(e) = db.set_config(&key, mode) {
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e})))
                        .into_response();
                }
            }
        }
    }

    (StatusCode::OK, Json(json!({"success": true}))).into_response()
}

// ============================================================================
// Service Account endpoints
// ============================================================================

pub async fn create_service_account_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateServiceAccountRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.create_service_account(&body.name, body.description.as_deref(), body.sql_mode.as_deref()) {
        Ok(api_key) => (
            StatusCode::CREATED,
            Json(json!({"name": body.name, "api_key": api_key})),
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn list_service_accounts_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.list_service_accounts() {
        Ok(accounts) => {
            let mut enriched = Vec::new();
            for sa in accounts {
                let perms = db.get_sa_permissions(&sa.name).unwrap_or_default();
                let conn_perms = db.get_sa_allowed_connections(&sa.name).unwrap_or(None);
                enriched.push(json!({
                    "name": sa.name,
                    "description": sa.description,
                    "api_key_prefix": sa.api_key_prefix,
                    "sql_mode": sa.sql_mode,
                    "is_enabled": sa.is_enabled,
                    "created_at": sa.created_at,
                    "updated_at": sa.updated_at,
                    "permissions": perms,
                    "connection_permissions": conn_perms,
                }));
            }
            (StatusCode::OK, Json(json!({"service_accounts": enriched}))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn update_service_account_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<UpdateServiceAccountRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.update_service_account(
        &name,
        body.description.as_deref(),
        body.sql_mode.as_deref(),
        body.is_enabled,
    ) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true, "name": name}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn delete_service_account_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.delete_service_account(&name) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true, "deleted": name}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn rotate_sa_key_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.rotate_service_account_key(&name) {
        Ok(api_key) => (StatusCode::OK, Json(json!({"api_key": api_key}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct SaPermQuery {
    pub name: String,
}

pub async fn get_sa_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<SaPermQuery>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.get_sa_permissions(&query.name) {
        Ok(perms) => (StatusCode::OK, Json(json!({"permissions": perms}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct SetSaPermissionsRequest {
    pub name: String,
    pub permissions: Vec<crate::auth::access_control::PermissionEntry>,
}

pub async fn set_sa_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetSaPermissionsRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.set_sa_permissions(&body.name, &body.permissions) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true, "name": body.name}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn get_sa_connections_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<SaPermQuery>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.get_sa_allowed_connections(&query.name) {
        Ok(conns) => (StatusCode::OK, Json(json!({"connections": conns}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct SetSaConnectionsRequest {
    pub name: String,
    pub connections: Vec<String>,
}

pub async fn set_sa_connections_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetSaConnectionsRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.set_sa_connection_permissions(&body.name, &body.connections) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true, "name": body.name}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn get_sa_endpoint_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<SaPermQuery>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.get_sa_endpoint_permissions(&query.name) {
        Ok(endpoints) => (StatusCode::OK, Json(json!({"endpoints": endpoints}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct SetSaEndpointPermissionsRequest {
    pub name: String,
    pub endpoints: Vec<String>,
}

pub async fn set_sa_endpoint_permissions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetSaEndpointPermissionsRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.set_sa_endpoint_permissions(&body.name, &body.endpoints) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true, "name": body.name}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

// ============================================================================
// Teams CRUD
// ============================================================================

#[derive(Deserialize)]
pub struct CreateTeamRequest {
    pub name: String,
    pub webhook_url: Option<String>,
}

pub async fn create_team_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateTeamRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.create_team(&body.name, body.webhook_url.as_deref()) {
        Ok(id) => (StatusCode::OK, Json(json!({"success": true, "id": id}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn list_teams_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.list_teams() {
        Ok(teams) => Json(json!(teams)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct UpdateTeamRequest {
    pub name: Option<String>,
    pub webhook_url: Option<Option<String>>,
}

pub async fn update_team_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<UpdateTeamRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.update_team(&id, body.name.as_deref(), body.webhook_url.as_ref().map(|o| o.as_deref())) {
        Ok(()) => Json(json!({"success": true})).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn delete_team_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.delete_team(&id) {
        Ok(()) => Json(json!({"success": true})).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

// ============================================================================
// Team Members
// ============================================================================

#[derive(Deserialize)]
pub struct AddTeamMemberRequest {
    pub email: String,
    pub role: Option<String>,
}

pub async fn list_team_members_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(team_id): Path<String>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.list_team_members(&team_id) {
        Ok(members) => Json(json!(members)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn add_team_member_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(team_id): Path<String>,
    Json(body): Json<AddTeamMemberRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    let role = body.role.as_deref().unwrap_or("member");
    match db.add_team_member(&team_id, &body.email, role) {
        Ok(()) => Json(json!({"success": true})).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct SetMemberRoleRequest {
    pub role: String,
}

pub async fn set_team_member_role_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((team_id, email)): Path<(String, String)>,
    Json(body): Json<SetMemberRoleRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.set_team_member_role(&team_id, &email, &body.role) {
        Ok(()) => Json(json!({"success": true})).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn remove_team_member_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((team_id, email)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.remove_team_member(&team_id, &email) {
        Ok(()) => Json(json!({"success": true})).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

// ============================================================================
// Projects CRUD
// ============================================================================

#[derive(Deserialize)]
pub struct CreateProjectRequest {
    pub name: String,
}

pub async fn create_project_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(team_id): Path<String>,
    Json(body): Json<CreateProjectRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.create_project(&team_id, &body.name) {
        Ok(id) => (StatusCode::OK, Json(json!({"success": true, "id": id}))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn list_projects_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(team_id): Path<String>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.list_projects(&team_id) {
        Ok(projects) => Json(json!(projects)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct UpdateProjectRequest {
    pub name: String,
}

pub async fn update_project_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<UpdateProjectRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.update_project(&id, &body.name) {
        Ok(()) => Json(json!({"success": true})).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn delete_project_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.delete_project(&id) {
        Ok(()) => Json(json!({"success": true})).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

// ============================================================================
// Project Members
// ============================================================================

pub async fn list_project_members_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.list_project_members(&id) {
        Ok(members) => Json(json!(members)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn add_project_member_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<AddTeamMemberRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    let role = body.role.as_deref().unwrap_or("member");
    match db.add_project_member(&id, &body.email, role) {
        Ok(()) => Json(json!({"success": true})).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn set_project_member_role_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((project_id, email)): Path<(String, String)>,
    Json(body): Json<SetMemberRoleRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.set_project_member_role(&project_id, &email, &body.role) {
        Ok(()) => Json(json!({"success": true})).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn remove_project_member_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((project_id, email)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await { return resp; }
    let db = match require_access_db(&state) { Ok(db) => db, Err(resp) => return resp };
    match db.remove_project_member(&project_id, &email) {
        Ok(()) => Json(json!({"success": true})).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}
