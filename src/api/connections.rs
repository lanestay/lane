use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

use crate::auth::access_control::StoredConnection;
use crate::auth::{authenticate, AuthResult};
use crate::db::ConnectionStatus;

use super::AppState;

// ============================================================================
// Helpers
// ============================================================================

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
// Request / Response types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateConnectionRequest {
    pub name: String,
    #[serde(rename = "type")]
    pub conn_type: String,
    pub host: String,
    pub port: Option<u16>,
    pub database: String,
    pub username: String,
    pub password: String,
    pub options_json: Option<String>,
    pub sslmode: Option<String>,
    pub is_default: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateConnectionRequest {
    #[serde(rename = "type")]
    pub conn_type: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub options_json: Option<String>,
    pub sslmode: Option<String>,
    pub is_default: Option<bool>,
    pub is_enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct TestConnectionRequest {
    #[serde(rename = "type")]
    pub conn_type: String,
    pub host: String,
    pub port: Option<u16>,
    pub database: String,
    pub username: String,
    pub password: String,
    pub options_json: Option<String>,
    pub sslmode: Option<String>,
}

#[derive(Debug, Serialize)]
struct ConnectionResponse {
    name: String,
    #[serde(rename = "type")]
    conn_type: String,
    host: String,
    port: u16,
    database: String,
    is_default: bool,
    is_enabled: bool,
    status: String,
    status_message: Option<String>,
}

fn stored_to_response(sc: &StoredConnection, status: &ConnectionStatus) -> ConnectionResponse {
    ConnectionResponse {
        name: sc.name.clone(),
        conn_type: sc.conn_type.clone(),
        host: sc.host.clone(),
        port: sc.port,
        database: sc.database_name.clone(),
        is_default: sc.is_default,
        is_enabled: sc.is_enabled,
        status: status.as_str().to_string(),
        status_message: status.message().map(|s| s.to_string()),
    }
}

fn default_port(conn_type: &str) -> u16 {
    match conn_type {
        "postgres" => 5432,
        "duckdb" => 0,
        "minio" => 9000,
        "clickhouse" => 8123,
        _ => 1433,
    }
}

// ============================================================================
// Handlers
// ============================================================================

/// GET /api/lane/admin/connections — list all connections (no passwords)
pub async fn list_admin_connections_handler(
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

    match db.list_connections_db() {
        Ok(conns) => {
            let list: Vec<ConnectionResponse> = conns
                .iter()
                .map(|sc| {
                    let status = state.registry.get_status(&sc.name);
                    stored_to_response(sc, &status)
                })
                .collect();
            (StatusCode::OK, Json(json!({ "connections": list }))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

/// POST /api/lane/admin/connections — create a new connection
pub async fn create_connection_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateConnectionRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let allowed_types = ["mssql", "postgres", "duckdb", "minio", "clickhouse"];
    if !allowed_types.contains(&body.conn_type.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "type must be 'mssql', 'postgres', 'duckdb', 'minio', or 'clickhouse'"})),
        )
            .into_response();
    }

    let stored = StoredConnection {
        name: body.name.clone(),
        conn_type: body.conn_type.clone(),
        host: body.host,
        port: body.port.unwrap_or_else(|| default_port(&body.conn_type)),
        database_name: body.database,
        username: body.username,
        password: body.password,
        options_json: body.options_json.unwrap_or_else(|| "{}".to_string()),
        sslmode: body.sslmode,
        is_default: body.is_default.unwrap_or(false),
        is_enabled: true,
    };

    // Save to DB first
    if let Err(e) = db.create_connection(&stored) {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response();
    }

    // MinIO connections go to storage registry, not database registry
    #[cfg(feature = "storage")]
    if stored.conn_type == "minio" {
        let nc = stored.to_named_connection();
        if let crate::config::ConnectionConfig::Minio(ref cfg) = nc.config {
            match crate::storage::StorageClient::new(cfg) {
                Ok(client) => {
                    state
                        .storage_registry
                        .register(stored.name.clone(), std::sync::Arc::new(client))
                        .await;
                }
                Err(e) => {
                    tracing::warn!(
                        "Storage client '{}' saved but creation failed: {:#}",
                        stored.name,
                        e
                    );
                }
            }
        }
        let status = ConnectionStatus::Connected;
        return (
            StatusCode::CREATED,
            Json(json!(stored_to_response(&stored, &status))),
        )
            .into_response();
    }

    // Try to create the backend pool
    let nc = stored.to_named_connection();
    match crate::create_backend(&nc).await {
        Ok(backend) => {
            if stored.is_default {
                state.registry.set_default(stored.name.clone());
            }
            state.registry.register(stored.name.clone(), backend);
            state
                .registry
                .set_status(&stored.name, ConnectionStatus::Connected);
        }
        Err(e) => {
            tracing::warn!("Connection '{}' saved but pool creation failed: {:#}", stored.name, e);
            state
                .registry
                .set_status(&stored.name, ConnectionStatus::Error(format!("{:#}", e)));
        }
    }

    let status = state.registry.get_status(&stored.name);
    (
        StatusCode::CREATED,
        Json(json!(stored_to_response(&stored, &status))),
    )
        .into_response()
}

/// PUT /api/lane/admin/connections/{name} — update a connection
pub async fn update_connection_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<UpdateConnectionRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    // Load existing
    let existing = match db.get_connection(&name) {
        Ok(c) => c,
        Err(e) => return (StatusCode::NOT_FOUND, Json(json!({"error": e}))).into_response(),
    };

    let updated = StoredConnection {
        name: name.clone(),
        conn_type: body.conn_type.unwrap_or(existing.conn_type),
        host: body.host.unwrap_or(existing.host),
        port: body.port.unwrap_or(existing.port),
        database_name: body.database.unwrap_or(existing.database_name),
        username: body.username.unwrap_or(existing.username),
        password: body.password.unwrap_or(existing.password),
        options_json: body.options_json.unwrap_or(existing.options_json),
        sslmode: body.sslmode.or(existing.sslmode),
        is_default: body.is_default.unwrap_or(existing.is_default),
        is_enabled: body.is_enabled.unwrap_or(existing.is_enabled),
    };

    if let Err(e) = db.update_connection(&name, &updated) {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response();
    }

    // Handle default change
    if updated.is_default {
        state.registry.set_default(name.clone());
    }

    // MinIO connections: update storage registry
    #[cfg(feature = "storage")]
    if updated.conn_type == "minio" {
        state.storage_registry.remove(&name).await;
        if updated.is_enabled {
            let nc = updated.to_named_connection();
            if let crate::config::ConnectionConfig::Minio(ref cfg) = nc.config {
                if let Ok(client) = crate::storage::StorageClient::new(cfg) {
                    state
                        .storage_registry
                        .register(name.clone(), std::sync::Arc::new(client))
                        .await;
                }
            }
        }
        let status = ConnectionStatus::Connected;
        return (StatusCode::OK, Json(json!(stored_to_response(&updated, &status)))).into_response();
    }

    // Handle enabled/disabled
    if !updated.is_enabled {
        state.registry.remove(&name);
    } else {
        // Pool-affecting fields may have changed: recreate
        state.registry.remove(&name);
        let nc = updated.to_named_connection();
        match crate::create_backend(&nc).await {
            Ok(backend) => {
                state.registry.register(name.clone(), backend);
                state
                    .registry
                    .set_status(&name, ConnectionStatus::Connected);
            }
            Err(e) => {
                tracing::warn!("Connection '{}' updated but pool creation failed: {:#}", name, e);
                state
                    .registry
                    .set_status(&name, ConnectionStatus::Error(format!("{:#}", e)));
            }
        }
    }

    let status = state.registry.get_status(&name);
    (StatusCode::OK, Json(json!(stored_to_response(&updated, &status)))).into_response()
}

/// DELETE /api/lane/admin/connections/{name}
pub async fn delete_connection_handler(
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

    if let Err(e) = db.delete_connection(&name) {
        return (StatusCode::NOT_FOUND, Json(json!({"error": e}))).into_response();
    }

    state.registry.remove(&name);
    #[cfg(feature = "storage")]
    state.storage_registry.remove(&name).await;
    (StatusCode::OK, Json(json!({"success": true}))).into_response()
}

/// POST /api/lane/admin/connections/{name}/test — test an existing saved connection
pub async fn test_existing_connection_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }

    // Try existing storage client first (MinIO)
    #[cfg(feature = "storage")]
    if let Some(client) = state.storage_registry.get(&name).await {
        return match client.health_check().await {
            Ok(()) => {
                (StatusCode::OK, Json(json!({"success": true, "message": "Storage connection is healthy"}))).into_response()
            }
            Err(e) => {
                let msg = format!("{:#}", e);
                (StatusCode::OK, Json(json!({"success": false, "message": msg}))).into_response()
            }
        };
    }

    // Try existing live backend first
    if let Some(backend) = state.registry.get(&name) {
        return match backend.health_check().await {
            Ok(()) => {
                state
                    .registry
                    .set_status(&name, ConnectionStatus::Connected);
                (StatusCode::OK, Json(json!({"success": true, "message": "Connection is healthy"}))).into_response()
            }
            Err(e) => {
                let msg = format!("{:#}", e);
                state
                    .registry
                    .set_status(&name, ConnectionStatus::Error(msg.clone()));
                (StatusCode::OK, Json(json!({"success": false, "message": msg}))).into_response()
            }
        };
    }

    // No live backend — try to create one from DB config
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let stored = match db.get_connection(&name) {
        Ok(c) => c,
        Err(e) => return (StatusCode::NOT_FOUND, Json(json!({"error": e}))).into_response(),
    };

    let nc = stored.to_named_connection();
    match crate::create_backend(&nc).await {
        Ok(backend) => {
            match backend.health_check().await {
                Ok(()) => {
                    // Register it since it works
                    state.registry.register(name.clone(), backend);
                    state.registry.set_status(&name, ConnectionStatus::Connected);
                    (StatusCode::OK, Json(json!({"success": true, "message": "Connection is healthy"}))).into_response()
                }
                Err(e) => {
                    let msg = format!("{:#}", e);
                    (StatusCode::OK, Json(json!({"success": false, "message": msg}))).into_response()
                }
            }
        }
        Err(e) => {
            let msg = format!("{:#}", e);
            (StatusCode::OK, Json(json!({"success": false, "message": msg}))).into_response()
        }
    }
}

/// POST /api/lane/admin/connections/test — test unsaved connection config
pub async fn test_inline_connection_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<TestConnectionRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }

    let allowed_types = ["mssql", "postgres", "duckdb", "minio", "clickhouse"];
    if !allowed_types.contains(&body.conn_type.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "type must be 'mssql', 'postgres', 'duckdb', 'minio', or 'clickhouse'"})),
        )
            .into_response();
    }

    let stored = StoredConnection {
        name: "__test__".to_string(),
        conn_type: body.conn_type.clone(),
        host: body.host,
        port: body.port.unwrap_or_else(|| default_port(&body.conn_type)),
        database_name: body.database,
        username: body.username,
        password: body.password,
        options_json: body.options_json.unwrap_or_else(|| "{}".to_string()),
        sslmode: body.sslmode,
        is_default: false,
        is_enabled: true,
    };

    // MinIO: test via StorageClient
    #[cfg(feature = "storage")]
    if stored.conn_type == "minio" {
        let nc = stored.to_named_connection();
        if let crate::config::ConnectionConfig::Minio(ref cfg) = nc.config {
            match crate::storage::StorageClient::new(cfg) {
                Ok(client) => {
                    return match client.health_check().await {
                        Ok(()) => (StatusCode::OK, Json(json!({"success": true, "message": "Storage connection successful"}))).into_response(),
                        Err(e) => (StatusCode::OK, Json(json!({"success": false, "message": format!("{:#}", e)}))).into_response(),
                    };
                }
                Err(e) => {
                    return (StatusCode::OK, Json(json!({"success": false, "message": format!("{:#}", e)}))).into_response();
                }
            }
        }
    }

    let nc = stored.to_named_connection();
    match crate::create_backend(&nc).await {
        Ok(backend) => {
            match backend.health_check().await {
                Ok(()) => (StatusCode::OK, Json(json!({"success": true, "message": "Connection successful"}))).into_response(),
                Err(e) => (StatusCode::OK, Json(json!({"success": false, "message": format!("{:#}", e)}))).into_response(),
            }
        }
        Err(e) => {
            (StatusCode::OK, Json(json!({"success": false, "message": format!("{:#}", e)}))).into_response()
        }
    }
}

/// GET /api/lane/connections/status — status map for all connections (any auth)
pub async fn connections_status_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let auth = authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": reason}))).into_response();
    }

    let infos = state.registry.list_connections();

    // Filter by connection access permissions
    let email = match &auth {
        AuthResult::SessionAccess { email, .. } => Some(email.as_str()),
        AuthResult::TokenAccess { email, .. } => Some(email.as_str()),
        _ => None,
    };
    let allowed = email.and_then(|e| {
        state.access_db.as_ref().and_then(|db| db.get_allowed_connections(e).ok().flatten())
    });

    let statuses: Vec<serde_json::Value> = infos
        .iter()
        .filter(|c| {
            match &allowed {
                Some(list) => list.iter().any(|a| a == &c.name),
                None => true,
            }
        })
        .map(|c| {
            json!({
                "name": c.name,
                "status": c.status,
                "status_message": c.status_message,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!({ "connections": statuses }))).into_response()
}

/// GET /api/lane/connections/health — pool stats + health history (any auth)
pub async fn connections_health_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let auth = authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": reason}))).into_response();
    }

    let infos = state.registry.list_connections();
    let pool_stats = state.registry.pool_stats_all();

    // Filter by connection access permissions
    let email = match &auth {
        AuthResult::SessionAccess { email, .. } => Some(email.as_str()),
        AuthResult::TokenAccess { email, .. } => Some(email.as_str()),
        _ => None,
    };
    let allowed = email.and_then(|e| {
        state.access_db.as_ref().and_then(|db| db.get_allowed_connections(e).ok().flatten())
    });

    let dialect_str = |d: crate::db::Dialect| match d {
        crate::db::Dialect::Mssql => "mssql",
        crate::db::Dialect::Postgres => "postgres",
        crate::db::Dialect::DuckDb => "duckdb",
        crate::db::Dialect::ClickHouse => "clickhouse",
    };

    let connections: Vec<serde_json::Value> = infos
        .iter()
        .filter(|c| match &allowed {
            Some(list) => list.iter().any(|a| a == &c.name),
            None => true,
        })
        .map(|c| {
            let ps = pool_stats.get(&c.name).and_then(|o| o.as_ref());
            let history = state
                .access_db
                .as_ref()
                .and_then(|db| db.get_health_history(&c.name, 24).ok())
                .unwrap_or_default();

            json!({
                "name": c.name,
                "dialect": dialect_str(c.dialect),
                "status": c.status,
                "status_message": c.status_message,
                "pool": ps,
                "history": history,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!({ "connections": connections }))).into_response()
}
