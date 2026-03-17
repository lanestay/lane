use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::auth::{self, AuthResult};
use crate::auth::access_control::AccessControlDb;
use super::AppState;

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
    };

    // Best-effort send — don't care if nobody is listening
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
    };

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
