use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::auth::{self, AuthResult};
use super::AppState;

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct MonitorQuery {
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct KillRequest {
    pub connection: Option<String>,
    pub process_id: i64,
}

// ============================================================================
// Auth helpers
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

fn check_connection_access(auth: &AuthResult, state: &AppState, connection_name: &str) -> Result<(), Response> {
    let email = match auth {
        AuthResult::SessionAccess { email, .. } => Some(email.as_str()),
        AuthResult::TokenAccess { email, .. } => Some(email.as_str()),
        _ => None,
    };
    if let Some(email) = email {
        if let Some(ref access_db) = state.access_db {
            if !access_db.check_connection_access(email, connection_name) {
                return Err((
                    StatusCode::FORBIDDEN,
                    Json(json!({"error": format!("Access denied to connection '{}'", connection_name), "code": "FORBIDDEN"})),
                ).into_response());
            }
        }
    }
    Ok(())
}

async fn check_admin_auth(headers: &HeaderMap, state: &AppState) -> Result<AuthResult, Response> {
    let auth = auth::authenticate(headers, state).await;
    match &auth {
        AuthResult::FullAccess => Ok(auth),
        AuthResult::SessionAccess { is_admin: true, .. } => Ok(auth),
        AuthResult::SessionAccess { is_admin: false, .. } => Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Admin access required"})),
        )
            .into_response()),
        AuthResult::TokenAccess { email, .. } => {
            if let Some(ref access_db) = state.access_db {
                if access_db.is_admin(email) {
                    return Ok(auth);
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

// ============================================================================
// Handlers
// ============================================================================

pub async fn list_queries_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<MonitorQuery>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let default_name = state.registry.default_name();
    let conn_name = params.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let backend = match state.registry.resolve(params.connection.as_deref()) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{}", e)})),
            )
                .into_response();
        }
    };

    match backend.list_active_queries().await {
        Ok(queries) => (StatusCode::OK, Json(json!({ "queries": queries }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{:#}", e)})),
        )
            .into_response(),
    }
}

pub async fn kill_query_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<KillRequest>,
) -> Response {
    let auth = match check_admin_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let default_name = state.registry.default_name();
    let conn_name = body.connection.as_deref().unwrap_or(&default_name);
    if let Err(resp) = check_connection_access(&auth, &state, conn_name) {
        return resp;
    }

    let backend = match state.registry.resolve(body.connection.as_deref()) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{}", e)})),
            )
                .into_response();
        }
    };

    match backend.kill_query(body.process_id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "message": format!("Process {} terminated", body.process_id),
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{:#}", e)})),
        )
            .into_response(),
    }
}
