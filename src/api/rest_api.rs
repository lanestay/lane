use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::{self, AuthResult};
use crate::auth::access_control::{AccessControlDb, PermAction};
use crate::db::Dialect;
use crate::query::QueryParams;
use crate::rest::filters::parse_rest_query;
use crate::rest::sql_builder;

use super::AppState;

// ============================================================================
// Route tree
// ============================================================================

pub fn rest_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        // Admin endpoints (static routes first)
        .route("/tables", get(admin_list_handler).post(admin_enable_handler).delete(admin_disable_handler))
        .route("/openapi.json", get(openapi_handler))
        // CRUD endpoints
        .route("/{connection}/{database}/{table}", get(list_handler).post(create_handler))
        .route("/{connection}/{database}/{table}/{id}", get(get_handler).put(update_handler).delete(delete_handler))
        .with_state(state)
}

// ============================================================================
// Auth helpers (mirror realtime.rs pattern)
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

/// Get the default schema for a dialect.
fn default_schema(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Postgres => "public",
        Dialect::DuckDb => "main",
        Dialect::Mssql => "dbo",
    }
}

/// Find the primary key column from describe_table results.
fn find_pk_column(columns: &[HashMap<String, Value>]) -> Option<String> {
    columns.iter().find_map(|col| {
        let is_pk = col.get("IS_PRIMARY_KEY")?.as_str()?;
        if is_pk == "YES" {
            col.get("COLUMN_NAME")?.as_str().map(|s| s.to_string())
        } else {
            None
        }
    })
}

/// Execute a SQL query against a resolved backend and return the raw rows.
async fn execute_sql(
    state: &AppState,
    connection: &str,
    database: &str,
    sql: &str,
) -> Result<Vec<HashMap<String, Value>>, Response> {
    let db = state.registry.resolve(Some(connection)).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("{}", e), "code": "INVALID_CONNECTION"})),
        )
            .into_response()
    })?;

    let params = QueryParams {
        database: database.to_string(),
        query: sql.to_string(),
        ..Default::default()
    };

    match db.execute_query(&params).await {
        Ok(result) => Ok(result.data),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("{:#}", e), "code": "EXEC_FAILED"})),
        )
            .into_response()),
    }
}

// ============================================================================
// Admin handlers
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct RestTableRequest {
    pub connection: String,
    pub database: String,
    pub schema: Option<String>,
    pub table: String,
}

pub async fn admin_list_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let access_db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match access_db.list_rest_tables() {
        Ok(tables) => (StatusCode::OK, Json(json!(tables))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

pub async fn admin_enable_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<RestTableRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let access_db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    // Resolve connection to get dialect for default schema
    let db = match state.registry.resolve(Some(&body.connection)) {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{}", e), "code": "INVALID_CONNECTION"})),
            )
                .into_response();
        }
    };
    let schema = body.schema.as_deref().unwrap_or_else(|| default_schema(db.dialect()));

    match access_db.enable_rest_table(&body.connection, &body.database, schema, &body.table, None) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "message": format!("REST enabled for {}.{}.{}.{}", body.connection, body.database, schema, body.table),
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

pub async fn admin_disable_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<RestTableRequest>,
) -> Response {
    if let Err(resp) = check_admin_auth(&headers, &state).await {
        return resp;
    }
    let access_db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let db = match state.registry.resolve(Some(&body.connection)) {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{}", e), "code": "INVALID_CONNECTION"})),
            )
                .into_response();
        }
    };
    let schema = body.schema.as_deref().unwrap_or_else(|| default_schema(db.dialect()));

    match access_db.disable_rest_table(&body.connection, &body.database, schema, &body.table) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "message": format!("REST disabled for {}.{}.{}.{}", body.connection, body.database, schema, body.table),
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

// ============================================================================
// CRUD handlers
// ============================================================================

/// GET /{connection}/{database}/{table} — List rows with filters
pub async fn list_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((connection, database, table)): Path<(String, String, String)>,
    Query(raw_params): Query<HashMap<String, String>>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    // Check REST is enabled
    let access_db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    // Check read permission
    if let Some(email) = extract_email(&auth) {
        if !access_db.check_table_permission_action(email, &database, &table, PermAction::Read) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Read access denied", "code": "FORBIDDEN"})),
            )
                .into_response();
        }
    } else if let AuthResult::ServiceAccountAccess { account_name } = &auth {
        if !access_db.check_sa_table_permission_action(account_name, &database, &table, PermAction::Read) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Read access denied", "code": "FORBIDDEN"})),
            )
                .into_response();
        }
    }

    let db = match state.registry.resolve(Some(&connection)) {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{}", e), "code": "INVALID_CONNECTION"})),
            )
                .into_response();
        }
    };
    let dialect = db.dialect();
    let schema = default_schema(dialect);

    if !access_db.is_rest_enabled(&connection, &database, schema, &table) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": format!("Table '{}.{}' is not enabled for REST access", schema, table),
                "code": "NOT_FOUND",
            })),
        )
            .into_response();
    }

    // Parse query params
    let rest_query = match parse_rest_query(&raw_params) {
        Ok(q) => q,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e, "code": "INVALID_QUERY"})),
            )
                .into_response();
        }
    };

    // Build and execute count query
    let count_sql = sql_builder::build_count(&table, schema, &rest_query.filters, dialect);
    let count_rows = match execute_sql(&state, &connection, &database, &count_sql).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let total = count_rows
        .first()
        .and_then(|r| r.get("total"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    // Build and execute select query
    let select_sql = sql_builder::build_select(&table, schema, &rest_query, dialect);
    let data = match execute_sql(&state, &connection, &database, &select_sql).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    let limit = rest_query.limit.unwrap_or(1000);
    let offset = rest_query.offset.unwrap_or(0);

    (
        StatusCode::OK,
        Json(json!({
            "data": data,
            "total": total,
            "limit": limit,
            "offset": offset,
        })),
    )
        .into_response()
}

/// GET /{connection}/{database}/{table}/{id} — Get single row by PK
pub async fn get_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((connection, database, table, id)): Path<(String, String, String, String)>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let access_db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    // Check read permission
    if let Some(email) = extract_email(&auth) {
        if !access_db.check_table_permission_action(email, &database, &table, PermAction::Read) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Read access denied", "code": "FORBIDDEN"})),
            )
                .into_response();
        }
    } else if let AuthResult::ServiceAccountAccess { account_name } = &auth {
        if !access_db.check_sa_table_permission_action(account_name, &database, &table, PermAction::Read) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Read access denied", "code": "FORBIDDEN"})),
            )
                .into_response();
        }
    }

    let db = match state.registry.resolve(Some(&connection)) {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{}", e), "code": "INVALID_CONNECTION"})),
            )
                .into_response();
        }
    };
    let dialect = db.dialect();
    let schema = default_schema(dialect);

    if !access_db.is_rest_enabled(&connection, &database, schema, &table) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Table '{}.{}' is not enabled for REST access", schema, table), "code": "NOT_FOUND"})),
        )
            .into_response();
    }

    // Get PK column
    let columns = match db.describe_table(&database, &table, schema).await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{:#}", e), "code": "DESCRIBE_FAILED"})),
            )
                .into_response();
        }
    };

    let pk_col = match find_pk_column(&columns) {
        Some(pk) => pk,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "No primary key found for this table. Single-row operations require a primary key.", "code": "NO_PRIMARY_KEY"})),
            )
                .into_response();
        }
    };

    let sql = sql_builder::build_select_by_pk(&table, schema, &pk_col, &id, dialect);
    let data = match execute_sql(&state, &connection, &database, &sql).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    match data.into_iter().next() {
        Some(row) => (StatusCode::OK, Json(json!({"data": row}))).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Row with {} = '{}' not found", pk_col, id), "code": "NOT_FOUND"})),
        )
            .into_response(),
    }
}

/// POST /{connection}/{database}/{table} — Insert row(s)
pub async fn create_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((connection, database, table)): Path<(String, String, String)>,
    Json(body): Json<Value>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let access_db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    // Check insert permission
    if let Some(email) = extract_email(&auth) {
        if !access_db.check_table_permission_action(email, &database, &table, PermAction::Insert) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Insert access denied", "code": "FORBIDDEN"})),
            )
                .into_response();
        }
    } else if let AuthResult::ServiceAccountAccess { account_name } = &auth {
        if !access_db.check_sa_table_permission_action(account_name, &database, &table, PermAction::Insert) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Insert access denied", "code": "FORBIDDEN"})),
            )
                .into_response();
        }
    }

    let db = match state.registry.resolve(Some(&connection)) {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{}", e), "code": "INVALID_CONNECTION"})),
            )
                .into_response();
        }
    };
    let dialect = db.dialect();
    let schema = default_schema(dialect);

    if !access_db.is_rest_enabled(&connection, &database, schema, &table) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Table '{}.{}' is not enabled for REST access", schema, table), "code": "NOT_FOUND"})),
        )
            .into_response();
    }

    // Parse body: single object or array of objects
    let objects = match &body {
        Value::Object(_) => vec![body.clone()],
        Value::Array(arr) => arr.clone(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Body must be a JSON object or array of objects", "code": "INVALID_BODY"})),
            )
                .into_response();
        }
    };

    if objects.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Empty body", "code": "INVALID_BODY"})),
        )
            .into_response();
    }

    // Extract columns from first object
    let columns: Vec<String> = match objects[0].as_object() {
        Some(obj) => obj.keys().cloned().collect(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Each item must be a JSON object", "code": "INVALID_BODY"})),
            )
                .into_response();
        }
    };

    // Build value rows
    let rows: Vec<Vec<Value>> = objects
        .iter()
        .map(|obj| {
            columns
                .iter()
                .map(|col| obj.get(col).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();

    let sql = sql_builder::build_insert(&table, schema, &columns, &rows, dialect);
    let data = match execute_sql(&state, &connection, &database, &sql).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    // Emit realtime event with row data for successful insert
    let row_count = data.len() as i64;
    let user = extract_email(&auth).or_else(|| {
        if let AuthResult::ServiceAccountAccess { account_name } = &auth {
            Some(account_name.as_str())
        } else { None }
    });
    let event_data = Some(if data.len() == 1 { serde_json::json!(data[0]) } else { serde_json::json!(data) });
    crate::api::realtime::emit_realtime_event_with_data(
        &state, &connection, &database, &table, "INSERT",
        Some(row_count), user, event_data,
    );

    let is_bulk = matches!(&body, Value::Array(_));
    if is_bulk {
        (StatusCode::CREATED, Json(json!({"data": data}))).into_response()
    } else {
        let row = data.into_iter().next().unwrap_or_default();
        (StatusCode::CREATED, Json(json!({"data": row}))).into_response()
    }
}

/// PUT /{connection}/{database}/{table}/{id} — Update row by PK
pub async fn update_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((connection, database, table, id)): Path<(String, String, String, String)>,
    Json(body): Json<Value>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let access_db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    if let Some(email) = extract_email(&auth) {
        if !access_db.check_table_permission_action(email, &database, &table, PermAction::Update) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Update access denied", "code": "FORBIDDEN"})),
            )
                .into_response();
        }
    } else if let AuthResult::ServiceAccountAccess { account_name } = &auth {
        if !access_db.check_sa_table_permission_action(account_name, &database, &table, PermAction::Update) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Update access denied", "code": "FORBIDDEN"})),
            )
                .into_response();
        }
    }

    let db = match state.registry.resolve(Some(&connection)) {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{}", e), "code": "INVALID_CONNECTION"})),
            )
                .into_response();
        }
    };
    let dialect = db.dialect();
    let schema = default_schema(dialect);

    if !access_db.is_rest_enabled(&connection, &database, schema, &table) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Table '{}.{}' is not enabled for REST access", schema, table), "code": "NOT_FOUND"})),
        )
            .into_response();
    }

    let updates = match body.as_object() {
        Some(obj) => obj.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Body must be a JSON object", "code": "INVALID_BODY"})),
            )
                .into_response();
        }
    };

    if updates.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "No fields to update", "code": "INVALID_BODY"})),
        )
            .into_response();
    }

    // Get PK column
    let columns = match db.describe_table(&database, &table, schema).await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{:#}", e), "code": "DESCRIBE_FAILED"})),
            )
                .into_response();
        }
    };

    let pk_col = match find_pk_column(&columns) {
        Some(pk) => pk,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "No primary key found for this table", "code": "NO_PRIMARY_KEY"})),
            )
                .into_response();
        }
    };

    let sql = sql_builder::build_update(&table, schema, &pk_col, &id, &updates, dialect);
    let data = match execute_sql(&state, &connection, &database, &sql).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    match data.into_iter().next() {
        Some(row) => {
            // Emit realtime event with data for successful update
            crate::api::realtime::emit_realtime_event_with_data(
                &state, &connection, &database, &table, "UPDATE",
                Some(1), extract_email(&auth), Some(serde_json::json!(&row)),
            );
            (StatusCode::OK, Json(json!({"data": row}))).into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Row with {} = '{}' not found", pk_col, id), "code": "NOT_FOUND"})),
        )
            .into_response(),
    }
}

/// DELETE /{connection}/{database}/{table}/{id} — Delete row by PK
pub async fn delete_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((connection, database, table, id)): Path<(String, String, String, String)>,
) -> Response {
    let auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let access_db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    if let Some(email) = extract_email(&auth) {
        if !access_db.check_table_permission_action(email, &database, &table, PermAction::Delete) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Delete access denied", "code": "FORBIDDEN"})),
            )
                .into_response();
        }
    } else if let AuthResult::ServiceAccountAccess { account_name } = &auth {
        if !access_db.check_sa_table_permission_action(account_name, &database, &table, PermAction::Delete) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "Delete access denied", "code": "FORBIDDEN"})),
            )
                .into_response();
        }
    }

    let db = match state.registry.resolve(Some(&connection)) {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{}", e), "code": "INVALID_CONNECTION"})),
            )
                .into_response();
        }
    };
    let dialect = db.dialect();
    let schema = default_schema(dialect);

    if !access_db.is_rest_enabled(&connection, &database, schema, &table) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("Table '{}.{}' is not enabled for REST access", schema, table), "code": "NOT_FOUND"})),
        )
            .into_response();
    }

    let columns = match db.describe_table(&database, &table, schema).await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{:#}", e), "code": "DESCRIBE_FAILED"})),
            )
                .into_response();
        }
    };

    let pk_col = match find_pk_column(&columns) {
        Some(pk) => pk,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "No primary key found for this table", "code": "NO_PRIMARY_KEY"})),
            )
                .into_response();
        }
    };

    let sql = sql_builder::build_delete(&table, schema, &pk_col, &id, dialect);
    match execute_sql(&state, &connection, &database, &sql).await {
        Ok(_) => {
            // Emit realtime event with deleted row identifier
            crate::api::realtime::emit_realtime_event_with_data(
                &state, &connection, &database, &table, "DELETE",
                Some(1), extract_email(&auth),
                Some(serde_json::json!({ pk_col.clone(): id.clone() })),
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(resp) => resp,
    }
}

// ============================================================================
// OpenAPI handler
// ============================================================================

pub async fn openapi_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let _auth = match check_auth(&headers, &state).await {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let access_db = match require_access_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let spec = crate::rest::openapi::generate_openapi(&state.registry, access_db).await;

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        Json(spec),
    )
        .into_response()
}

// ============================================================================
// Helpers
// ============================================================================

fn extract_email(auth: &AuthResult) -> Option<&str> {
    match auth {
        AuthResult::SessionAccess { email, .. } => Some(email.as_str()),
        AuthResult::TokenAccess { email, .. } => Some(email.as_str()),
        _ => None,
    }
}
