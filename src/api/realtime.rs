use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::auth::{self, AuthResult};
use crate::auth::access_control::{AccessControlDb, RealtimeWebhook};
use super::AppState;

/// Last time we checked for expired realtime tables (unix timestamp).
static LAST_REALTIME_CLEANUP: AtomicI64 = AtomicI64::new(0);

/// Auto-expire realtime tables after 1 hour when nobody is watching.
const REALTIME_MAX_AGE_SECS: i64 = 3600;
/// Only run the cleanup check once per minute.
const REALTIME_CLEANUP_INTERVAL_SECS: i64 = 60;

/// Remove expired realtime tables if enough time has passed since the last check.
fn maybe_cleanup_expired_realtime(state: &AppState) {
    let now = chrono::Utc::now().timestamp();
    let last = LAST_REALTIME_CLEANUP.load(Ordering::Relaxed);
    if now - last < REALTIME_CLEANUP_INTERVAL_SECS {
        return;
    }
    if LAST_REALTIME_CLEANUP.compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed).is_err() {
        return; // another thread got it
    }
    if let Some(ref access_db) = state.access_db {
        let _ = access_db.cleanup_expired_realtime_tables(REALTIME_MAX_AGE_SECS);
    }
}

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct RealtimeEvent {
    pub id: String,
    pub connection: String,
    pub database: String,
    pub table: String,
    pub query_type: String,
    pub row_count: Option<i64>,
    pub user: Option<String>,
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct SubscribeQuery {
    pub connection: Option<String>,
    pub database: String,
    pub table: String,
    pub token: Option<String>, // session token for EventSource (no custom headers)
}

#[derive(Debug, Deserialize)]
pub struct RealtimeTableRequest {
    pub connection: Option<String>,
    pub database: String,
    pub table: String,
}

// ============================================================================
// Webhook Cache
// ============================================================================

/// In-memory webhook cache keyed by (connection, database, table) -> Vec<WebhookConfig>.
/// Refreshed from DB when the dirty flag is set by admin operations.
pub struct WebhookCache {
    cache: RwLock<HashMap<(String, String, String), Vec<RealtimeWebhook>>>,
    dirty: AtomicBool,
    loaded: AtomicBool,
}

impl WebhookCache {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            dirty: AtomicBool::new(false),
            loaded: AtomicBool::new(false),
        }
    }

    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// Reload from DB if needed, then return webhooks matching the given key and event type.
    pub async fn get_matching(
        &self,
        access_db: &AccessControlDb,
        connection: &str,
        database: &str,
        table: &str,
        event_type: &str,
    ) -> Vec<RealtimeWebhook> {
        // Reload if dirty or never loaded
        if self.dirty.load(Ordering::Relaxed) || !self.loaded.load(Ordering::Relaxed) {
            self.reload(access_db).await;
        }

        let cache = self.cache.read().await;
        let key = (
            connection.to_lowercase(),
            database.to_lowercase(),
            table.to_lowercase(),
        );
        match cache.get(&key) {
            Some(hooks) => hooks
                .iter()
                .filter(|h| {
                    h.events
                        .split(',')
                        .any(|e| e.trim().eq_ignore_ascii_case(event_type))
                })
                .cloned()
                .collect(),
            None => vec![],
        }
    }

    async fn reload(&self, access_db: &AccessControlDb) {
        let all = access_db.get_all_realtime_webhooks_enabled();
        let mut map: HashMap<(String, String, String), Vec<RealtimeWebhook>> = HashMap::new();
        for hook in all {
            let key = (
                hook.connection_name.to_lowercase(),
                hook.database_name.to_lowercase(),
                hook.table_name.to_lowercase(),
            );
            map.entry(key).or_default().push(hook);
        }
        let mut cache = self.cache.write().await;
        *cache = map;
        self.dirty.store(false, Ordering::Relaxed);
        self.loaded.store(true, Ordering::Relaxed);
    }
}

// Global webhook cache — lazily initialized
static WEBHOOK_CACHE: std::sync::OnceLock<WebhookCache> = std::sync::OnceLock::new();

fn webhook_cache() -> &'static WebhookCache {
    WEBHOOK_CACHE.get_or_init(WebhookCache::new)
}

// ============================================================================
// Webhook Firing
// ============================================================================

/// Fire matching webhooks for a realtime event. Async, fire-and-forget via tokio::spawn.
fn fire_realtime_webhooks(
    access_db: Arc<AccessControlDb>,
    event: &RealtimeEvent,
) {
    let connection = event.connection.clone();
    let database = event.database.clone();
    let table = event.table.clone();
    let event_type = event.query_type.clone();
    let payload = json!({
        "event": &event.query_type,
        "connection": &event.connection,
        "database": &event.database,
        "table": &event.table,
        "row_count": &event.row_count,
        "user": &event.user,
        "timestamp": &event.timestamp,
        "data": &event.data,
    });

    tokio::spawn(async move {
        let hooks = webhook_cache()
            .get_matching(&access_db, &connection, &database, &table, &event_type)
            .await;
        if hooks.is_empty() {
            return;
        }

        let body = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(_) => return,
        };

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        for hook in hooks {
            let mut req = client
                .post(&hook.url)
                .header("Content-Type", "application/json")
                .header("X-Lane-Event", &event_type);

            // HMAC signature if secret is configured
            if let Some(ref secret) = hook.secret {
                use hmac::{Hmac, Mac};
                use sha2::Sha256;
                type HmacSha256 = Hmac<Sha256>;
                if let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) {
                    mac.update(&body);
                    let sig = hex::encode(mac.finalize().into_bytes());
                    req = req.header("X-Lane-Signature", format!("sha256={}", sig));
                }
            }

            let resp = req.body(body.clone()).send().await;
            if let Err(e) = resp {
                tracing::warn!("Realtime webhook to {} failed: {}", hook.url, e);
            }
        }
    });
}

// ============================================================================
// SQL Parsing Helpers
// ============================================================================

/// Extract the target table name from a write SQL statement.
/// Returns None for SELECT or unparseable queries.
pub fn extract_write_target_table(query: &str) -> Option<String> {
    let trimmed = query.trim();
    // Use a regex to match common write patterns
    let re = regex::Regex::new(
        r"(?ix)
        (?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE\s+TABLE|DROP\s+TABLE|MERGE\s+INTO)\s+
        (?:\[?[a-z0-9_]+\]?\.)* # optional schema like [dbo].
        \[?([a-z0-9_]+)\]?      # table name (with optional brackets)
        "
    ).ok()?;
    re.captures(trimmed).map(|caps| caps[1].to_string())
}

/// Classify the write type from the leading keyword.
pub fn classify_write_type(query: &str) -> &'static str {
    let upper = query.trim().to_uppercase();
    if upper.starts_with("INSERT") {
        "INSERT"
    } else if upper.starts_with("UPDATE") {
        "UPDATE"
    } else if upper.starts_with("DELETE") {
        "DELETE"
    } else if upper.starts_with("TRUNCATE") {
        "TRUNCATE"
    } else if upper.starts_with("DROP") {
        "DROP"
    } else if upper.starts_with("MERGE") {
        "MERGE"
    } else if upper.starts_with("ALTER") {
        "ALTER"
    } else if upper.starts_with("CREATE") {
        "CREATE"
    } else {
        "UNKNOWN"
    }
}

// ============================================================================
// Event Emission
// ============================================================================

/// Emit a realtime event if the target table has realtime enabled.
/// For use in HTTP handlers that have access to AppState.
pub fn try_emit_realtime_event(
    state: &AppState,
    connection: &str,
    database: &str,
    query: &str,
    row_count: Option<i64>,
    user_email: Option<&str>,
) {
    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => return,
    };
    try_emit_realtime_event_direct(
        &state.realtime_tx,
        access_db,
        connection,
        database,
        query,
        row_count,
        user_email,
    );
}

/// Emit a realtime event if the target table has realtime enabled.
/// For use in MCP tools that have broadcast::Sender + AccessControlDb directly.
pub fn try_emit_realtime_event_direct(
    tx: &broadcast::Sender<RealtimeEvent>,
    access_db: &AccessControlDb,
    connection: &str,
    database: &str,
    query: &str,
    row_count: Option<i64>,
    user_email: Option<&str>,
) {
    let table = match extract_write_target_table(query) {
        Some(t) => t,
        None => return,
    };

    if !access_db.is_realtime_enabled(connection, database, &table) {
        return;
    }

    let event = RealtimeEvent {
        id: uuid::Uuid::new_v4().to_string(),
        connection: connection.to_string(),
        database: database.to_string(),
        table,
        query_type: classify_write_type(query).to_string(),
        row_count,
        user: user_email.map(|s| s.to_string()),
        timestamp: chrono::Utc::now().to_rfc3339(),
        data: None,
    };

    // Best-effort send — don't care if nobody is listening
    // Note: webhooks not fired from the _direct path (MCP) — only from emit_realtime_event*
    let _ = tx.send(event);
}

/// Emit a realtime event for a known table and query type (no SQL parsing needed).
/// For use in REST API CRUD handlers where the table/action are already known.
pub fn emit_realtime_event(
    state: &AppState,
    connection: &str,
    database: &str,
    table: &str,
    query_type: &str,
    row_count: Option<i64>,
    user_email: Option<&str>,
) {
    if state.realtime_tx.receiver_count() == 0 {
        maybe_cleanup_expired_realtime(state);
        // Still check webhooks even with no SSE subscribers
    }

    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => return,
    };

    if !access_db.is_realtime_enabled(connection, database, table) {
        return;
    }

    let event = RealtimeEvent {
        id: uuid::Uuid::new_v4().to_string(),
        connection: connection.to_string(),
        database: database.to_string(),
        table: table.to_string(),
        query_type: query_type.to_string(),
        row_count,
        user: user_email.map(|s| s.to_string()),
        timestamp: chrono::Utc::now().to_rfc3339(),
        data: None,
    };

    fire_realtime_webhooks(Arc::clone(access_db), &event);
    let _ = state.realtime_tx.send(event);
}

/// Emit a realtime event with row data attached (for REST API CRUD handlers).
pub fn emit_realtime_event_with_data(
    state: &AppState,
    connection: &str,
    database: &str,
    table: &str,
    query_type: &str,
    row_count: Option<i64>,
    user_email: Option<&str>,
    data: Option<serde_json::Value>,
) {
    if state.realtime_tx.receiver_count() == 0 {
        maybe_cleanup_expired_realtime(state);
        // Still check webhooks even with no SSE subscribers
    }

    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => return,
    };

    if !access_db.is_realtime_enabled(connection, database, table) {
        return;
    }

    let event = RealtimeEvent {
        id: uuid::Uuid::new_v4().to_string(),
        connection: connection.to_string(),
        database: database.to_string(),
        table: table.to_string(),
        query_type: query_type.to_string(),
        row_count,
        user: user_email.map(|s| s.to_string()),
        timestamp: chrono::Utc::now().to_rfc3339(),
        data,
    };

    fire_realtime_webhooks(Arc::clone(access_db), &event);
    let _ = state.realtime_tx.send(event);
}

// ============================================================================
// SSE Subscribe Handler
// ============================================================================

pub async fn subscribe_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<SubscribeQuery>,
) -> Response {
    // Auth: any authenticated user
    // EventSource can't send custom headers, so also accept ?token= query param
    let auth = if let Some(ref token) = params.token {
        if let Some(ref access_db) = state.access_db {
            match access_db.validate_session(token) {
                Ok(info) => AuthResult::SessionAccess {
                    email: info.email,
                    is_admin: info.is_admin,
                },
                Err(_) => auth::authenticate(&headers, &state).await,
            }
        } else {
            auth::authenticate(&headers, &state).await
        }
    } else {
        auth::authenticate(&headers, &state).await
    };
    match &auth {
        AuthResult::Denied(reason) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": reason})),
            )
                .into_response();
        }
        _ => {}
    }

    // Check that realtime is enabled for this table
    let access_db = match state.access_db.as_ref() {
        Some(db) => db,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "Access control is not enabled"})),
            )
                .into_response();
        }
    };

    let connection = params.connection.as_deref().unwrap_or("default");
    if !access_db.is_realtime_enabled(connection, &params.database, &params.table) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "Realtime is not enabled for this table",
                "connection": connection,
                "database": &params.database,
                "table": &params.table,
            })),
        )
            .into_response();
    }

    let rx = state.realtime_tx.subscribe();
    let filter_connection = connection.to_lowercase();
    let filter_database = params.database.to_lowercase();
    let filter_table = params.table.to_lowercase();

    let stream = BroadcastStream::new(rx).filter_map(move |result| {
        match result {
            Ok(event) => {
                if event.connection.to_lowercase() == filter_connection
                    && event.database.to_lowercase() == filter_database
                    && event.table.to_lowercase() == filter_table
                {
                    let data = serde_json::to_string(&event).unwrap_or_default();
                    Some(Ok::<_, Infallible>(
                        Event::default()
                            .event("change")
                            .id(event.id.clone())
                            .data(data),
                    ))
                } else {
                    None
                }
            }
            Err(_) => {
                // Lagged receiver — silently skip missed events
                None
            }
        }
    });

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

// ============================================================================
// Admin Handlers
// ============================================================================

async fn check_admin_auth(headers: &HeaderMap, state: &AppState) -> Result<(), Response> {
    let auth = auth::authenticate(headers, state).await;
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

fn require_access_db(state: &AppState) -> Result<&AccessControlDb, Response> {
    state.access_db.as_deref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "Access control is not enabled"})),
        )
            .into_response()
    })
}

pub async fn enable_realtime_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<RealtimeTableRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let connection = body.connection.as_deref().unwrap_or("default");
    match db.enable_realtime(connection, &body.database, &body.table, None) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "message": format!("Realtime enabled for {}.{}.{}", connection, body.database, body.table),
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

pub async fn disable_realtime_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<RealtimeTableRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let connection = body.connection.as_deref().unwrap_or("default");
    match db.disable_realtime(connection, &body.database, &body.table) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "message": format!("Realtime disabled for {}.{}.{}", connection, body.database, body.table),
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

pub async fn list_realtime_tables_handler(
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

    match db.list_realtime_tables() {
        Ok(tables) => (StatusCode::OK, Json(json!(tables))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

// ============================================================================
// Webhook Admin Handlers
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct WebhookCreateRequest {
    pub connection: Option<String>,
    pub database: String,
    pub table: String,
    pub url: String,
    pub events: Option<Vec<String>>,
    pub secret: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WebhookUpdateRequest {
    pub url: Option<String>,
    pub events: Option<Vec<String>>,
    pub secret: Option<String>,
    pub is_enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct WebhookListQuery {
    pub connection: Option<String>,
    pub database: Option<String>,
    pub table: Option<String>,
}

pub async fn create_webhook_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<WebhookCreateRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let connection = body.connection.as_deref().unwrap_or("default");
    let events = body
        .events
        .as_ref()
        .map(|e| e.join(","))
        .unwrap_or_else(|| "INSERT,UPDATE,DELETE".to_string());

    match db.create_realtime_webhook(
        connection,
        &body.database,
        &body.table,
        &body.url,
        &events,
        body.secret.as_deref(),
        None,
    ) {
        Ok(hook) => {
            webhook_cache().mark_dirty();
            (StatusCode::CREATED, Json(json!(hook))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

pub async fn list_webhooks_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<WebhookListQuery>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match db.list_realtime_webhooks(
        params.connection.as_deref(),
        params.database.as_deref(),
        params.table.as_deref(),
    ) {
        Ok(hooks) => (StatusCode::OK, Json(json!(hooks))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

pub async fn update_webhook_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(body): Json<WebhookUpdateRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    // Fetch existing to merge partial updates
    let existing = match db.list_realtime_webhooks(None, None, None) {
        Ok(hooks) => match hooks.into_iter().find(|h| h.id == id) {
            Some(h) => h,
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "Webhook not found"})),
                )
                    .into_response()
            }
        },
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e})),
            )
                .into_response()
        }
    };

    let url = body.url.as_deref().unwrap_or(&existing.url);
    let events = body
        .events
        .as_ref()
        .map(|e| e.join(","))
        .unwrap_or(existing.events);
    let secret = if body.secret.is_some() {
        body.secret.as_deref()
    } else {
        existing.secret.as_deref()
    };
    let is_enabled = body.is_enabled.unwrap_or(existing.is_enabled);

    match db.update_realtime_webhook(id, url, &events, secret, is_enabled) {
        Ok(()) => {
            webhook_cache().mark_dirty();
            (
                StatusCode::OK,
                Json(json!({"success": true, "message": "Webhook updated"})),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

pub async fn delete_webhook_handler(
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

    match db.delete_realtime_webhook(id) {
        Ok(()) => {
            webhook_cache().mark_dirty();
            (
                StatusCode::OK,
                Json(json!({"success": true, "message": "Webhook deleted"})),
            )
                .into_response()
        }
        Err(e) => {
            let status = if e.contains("not found") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (status, Json(json!({"error": e}))).into_response()
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_write_target_table() {
        // INSERT INTO
        assert_eq!(
            extract_write_target_table("INSERT INTO Users (name) VALUES ('test')"),
            Some("Users".to_string())
        );
        assert_eq!(
            extract_write_target_table("INSERT INTO [dbo].[Users] (name) VALUES ('test')"),
            Some("Users".to_string())
        );
        assert_eq!(
            extract_write_target_table("insert into MyTable values (1)"),
            Some("MyTable".to_string())
        );

        // UPDATE
        assert_eq!(
            extract_write_target_table("UPDATE Users SET name = 'test'"),
            Some("Users".to_string())
        );
        assert_eq!(
            extract_write_target_table("UPDATE [dbo].[Users] SET name = 'test'"),
            Some("Users".to_string())
        );

        // DELETE FROM
        assert_eq!(
            extract_write_target_table("DELETE FROM Users WHERE id = 1"),
            Some("Users".to_string())
        );

        // TRUNCATE TABLE
        assert_eq!(
            extract_write_target_table("TRUNCATE TABLE Users"),
            Some("Users".to_string())
        );

        // DROP TABLE
        assert_eq!(
            extract_write_target_table("DROP TABLE [Users]"),
            Some("Users".to_string())
        );

        // MERGE INTO
        assert_eq!(
            extract_write_target_table("MERGE INTO Users USING source ON ..."),
            Some("Users".to_string())
        );

        // SELECT should return None
        assert_eq!(
            extract_write_target_table("SELECT * FROM Users"),
            None
        );

        // Schema-qualified
        assert_eq!(
            extract_write_target_table("INSERT INTO schema1.Orders (id) VALUES (1)"),
            Some("Orders".to_string())
        );
    }

    #[test]
    fn test_classify_write_type() {
        assert_eq!(classify_write_type("INSERT INTO Users VALUES (1)"), "INSERT");
        assert_eq!(classify_write_type("UPDATE Users SET x = 1"), "UPDATE");
        assert_eq!(classify_write_type("DELETE FROM Users"), "DELETE");
        assert_eq!(classify_write_type("TRUNCATE TABLE Users"), "TRUNCATE");
        assert_eq!(classify_write_type("DROP TABLE Users"), "DROP");
        assert_eq!(classify_write_type("MERGE INTO Users USING ..."), "MERGE");
        assert_eq!(classify_write_type("ALTER TABLE Users ADD x INT"), "ALTER");
        assert_eq!(classify_write_type("CREATE TABLE Foo (id INT)"), "CREATE");
        assert_eq!(classify_write_type("SELECT 1"), "UNKNOWN");
    }
}
