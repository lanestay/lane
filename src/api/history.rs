use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::auth::{authenticate, AuthResult};
use super::AppState;

// ============================================================================
// Helpers
// ============================================================================

/// Extract user email from auth result. Returns None for FullAccess (system API key).
fn extract_email(auth: &AuthResult) -> Option<&str> {
    match auth {
        AuthResult::SessionAccess { email, .. } => Some(email.as_str()),
        AuthResult::TokenAccess { email, .. } => Some(email.as_str()),
        _ => None,
    }
}

fn require_email(auth: &AuthResult) -> Result<&str, Response> {
    extract_email(auth).ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Query history requires user authentication (session or token)"})),
        )
            .into_response()
    })
}

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
// Request types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub search: Option<String>,
    pub favorites_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct FavoriteRequest {
    pub is_favorite: bool,
}

// ============================================================================
// Handlers
// ============================================================================

/// GET /api/lane/history — list own query history
pub async fn list_history_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<HistoryQuery>,
) -> Response {
    let auth = authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": reason}))).into_response();
    }
    let email = match require_email(&auth) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let limit = query.limit.unwrap_or(50).min(200);
    let offset = query.offset.unwrap_or(0);
    let favorites_only = query.favorites_only.unwrap_or(false);

    match db.list_query_history(email, limit, offset, query.search.as_deref(), favorites_only) {
        Ok(entries) => (StatusCode::OK, Json(json!({ "entries": entries }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

/// POST /api/lane/history/{id}/favorite — toggle favorite
pub async fn toggle_favorite_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(body): Json<FavoriteRequest>,
) -> Response {
    let auth = authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": reason}))).into_response();
    }
    let email = match require_email(&auth) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.toggle_favorite(id, email, body.is_favorite) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true}))).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(json!({"error": e}))).into_response(),
    }
}

/// DELETE /api/lane/history/{id} — delete own history entry
pub async fn delete_history_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let auth = authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": reason}))).into_response();
    }
    let email = match require_email(&auth) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.delete_history_entry(id, email) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true}))).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(json!({"error": e}))).into_response(),
    }
}
