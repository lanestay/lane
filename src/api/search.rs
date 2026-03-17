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

use super::errors::*;
use super::AppState;

// ============================================================================
// Query params
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub q: Option<String>,
    pub limit: Option<usize>,
    pub email: Option<String>,
}

// ============================================================================
// Auth helpers
// ============================================================================

fn require_authenticated(auth: &AuthResult) -> Result<(), Response> {
    match auth {
        AuthResult::Denied(reason) => Err(
            request_error("UNAUTHORIZED", reason, None).to_response(StatusCode::UNAUTHORIZED),
        ),
        _ => Ok(()),
    }
}

fn require_admin(auth: &AuthResult) -> Result<(), Response> {
    match auth {
        AuthResult::FullAccess => Ok(()),
        AuthResult::SessionAccess { is_admin: true, .. } => Ok(()),
        AuthResult::SessionAccess {
            is_admin: false, ..
        } => Err(
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
// Search handlers
// ============================================================================

/// GET /api/lane/search — unified search across all types
pub async fn unified_search(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_authenticated(&auth) {
        return resp;
    }

    let search_db = match &state.search_db {
        Some(db) => db,
        None => {
            return request_error("NOT_CONFIGURED", "Search is not available", None)
                .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let q = match &params.q {
        Some(q) if !q.trim().is_empty() => q.trim(),
        _ => {
            return request_error("INVALID_REQUEST", "Query parameter 'q' is required", None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let limit = params.limit.unwrap_or(20).min(100);
    let results = search_db.search_all(q, limit);

    (StatusCode::OK, Json(json!(results))).into_response()
}

/// GET /api/lane/search/schema — schema objects only
pub async fn search_schema(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_authenticated(&auth) {
        return resp;
    }

    let search_db = match &state.search_db {
        Some(db) => db,
        None => {
            return request_error("NOT_CONFIGURED", "Search is not available", None)
                .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let q = match &params.q {
        Some(q) if !q.trim().is_empty() => q.trim(),
        _ => {
            return request_error("INVALID_REQUEST", "Query parameter 'q' is required", None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let limit = params.limit.unwrap_or(20).min(100);
    let results = search_db.search_schema(q, limit);

    (StatusCode::OK, Json(json!({ "results": results, "total": results.len() }))).into_response()
}

/// GET /api/lane/search/queries — query history only
pub async fn search_queries(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_authenticated(&auth) {
        return resp;
    }

    let search_db = match &state.search_db {
        Some(db) => db,
        None => {
            return request_error("NOT_CONFIGURED", "Search is not available", None)
                .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let q = match &params.q {
        Some(q) if !q.trim().is_empty() => q.trim(),
        _ => {
            return request_error("INVALID_REQUEST", "Query parameter 'q' is required", None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let limit = params.limit.unwrap_or(20).min(100);
    let results = search_db.search_queries(q, params.email.as_deref(), limit);

    (StatusCode::OK, Json(json!({ "results": results, "total": results.len() }))).into_response()
}

/// GET /api/lane/search/endpoints — endpoints only
pub async fn search_endpoints(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_authenticated(&auth) {
        return resp;
    }

    let search_db = match &state.search_db {
        Some(db) => db,
        None => {
            return request_error("NOT_CONFIGURED", "Search is not available", None)
                .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let q = match &params.q {
        Some(q) if !q.trim().is_empty() => q.trim(),
        _ => {
            return request_error("INVALID_REQUEST", "Query parameter 'q' is required", None)
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    let limit = params.limit.unwrap_or(20).min(100);
    let results = search_db.search_endpoints(q, limit);

    (StatusCode::OK, Json(json!({ "results": results, "total": results.len() }))).into_response()
}

/// POST /api/lane/admin/search/reindex — trigger full re-index
pub async fn admin_reindex(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let search_db = match &state.search_db {
        Some(db) => db.clone(),
        None => {
            return request_error("NOT_CONFIGURED", "Search is not available", None)
                .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let registry = state.registry.clone();
    let access_db = state.access_db.clone();

    tokio::spawn(async move {
        crate::search::indexer::run_full_index(search_db, registry, access_db).await;
    });

    (StatusCode::OK, Json(json!({"success": true, "message": "Reindex started"}))).into_response()
}

/// GET /api/lane/admin/search/stats — index stats
pub async fn admin_stats(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let search_db = match &state.search_db {
        Some(db) => db,
        None => {
            return request_error("NOT_CONFIGURED", "Search is not available", None)
                .to_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    match search_db.stats() {
        Ok((schema, queries, endpoints)) => (
            StatusCode::OK,
            Json(json!({
                "schema_objects": schema,
                "queries": queries,
                "endpoints": endpoints,
            })),
        )
            .into_response(),
        Err(e) => {
            request_error("INTERNAL_ERROR", &e, None).to_response(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
