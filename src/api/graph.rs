use axum::{
    extract::{Path, Query, State},
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
// Types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateNodeRequest {
    pub connection_name: String,
    pub database_name: String,
    #[serde(default)]
    pub schema_name: Option<String>,
    #[serde(default)]
    pub table_name: Option<String>,
    #[serde(default)]
    pub node_type: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateEdgeRequest {
    // By node ID
    #[serde(default)]
    pub source_node_id: Option<i64>,
    #[serde(default)]
    pub target_node_id: Option<i64>,
    // By identity (auto-creates nodes)
    #[serde(default)]
    pub source_connection: Option<String>,
    #[serde(default)]
    pub source_database: Option<String>,
    #[serde(default)]
    pub source_schema: Option<String>,
    #[serde(default)]
    pub source_table: Option<String>,
    #[serde(default)]
    pub target_connection: Option<String>,
    #[serde(default)]
    pub target_database: Option<String>,
    #[serde(default)]
    pub target_schema: Option<String>,
    #[serde(default)]
    pub target_table: Option<String>,
    // Edge properties
    pub edge_type: String,
    #[serde(default)]
    pub source_columns: Option<String>,
    #[serde(default)]
    pub target_columns: Option<String>,
    #[serde(default)]
    pub metadata: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TraverseRequest {
    #[serde(default)]
    pub node_id: Option<i64>,
    #[serde(default)]
    pub connection_name: Option<String>,
    #[serde(default)]
    pub database_name: Option<String>,
    #[serde(default)]
    pub schema_name: Option<String>,
    #[serde(default)]
    pub table_name: Option<String>,
    #[serde(default)]
    pub max_depth: Option<usize>,
    #[serde(default)]
    pub edge_types: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SeedRequest {
    #[serde(default)]
    pub connection_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListNodesQuery {
    #[serde(default)]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListEdgesQuery {
    #[serde(default)]
    pub edge_type: Option<String>,
}

// ============================================================================
// Helpers
// ============================================================================

fn require_graph_db(state: &AppState) -> Result<&crate::graph::GraphDb, Response> {
    state.graph_db.as_deref().ok_or_else(|| {
        request_error("NOT_CONFIGURED", "Graph database is not available", None)
            .to_response(StatusCode::SERVICE_UNAVAILABLE)
    })
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

fn extract_auth_email(auth: &AuthResult) -> Option<&str> {
    match auth {
        AuthResult::SessionAccess { email, .. } => Some(email.as_str()),
        AuthResult::TokenAccess { email, .. } => Some(email.as_str()),
        _ => None,
    }
}

// ============================================================================
// Node handlers
// ============================================================================

/// POST /api/lane/admin/graph/nodes
pub async fn create_node_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateNodeRequest>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let graph_db = match require_graph_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let schema = body.schema_name.as_deref().unwrap_or("");
    let table = body.table_name.as_deref().unwrap_or("");
    let node_type = body.node_type.as_deref().unwrap_or("table");

    match graph_db.upsert_graph_node(
        &body.connection_name,
        &body.database_name,
        schema,
        table,
        node_type,
        body.label.as_deref(),
    ) {
        Ok(id) => {
            match graph_db.get_graph_node(id) {
                Ok(Some(node)) => (StatusCode::CREATED, Json(json!(node))).into_response(),
                _ => (StatusCode::CREATED, Json(json!({"id": id}))).into_response(),
            }
        }
        Err(e) => {
            request_error("CREATE_FAILED", &e, None).to_response(StatusCode::BAD_REQUEST)
        }
    }
}

/// GET /api/lane/admin/graph/nodes
pub async fn list_nodes_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ListNodesQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let graph_db = match require_graph_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match graph_db.list_graph_nodes(query.connection.as_deref()) {
        Ok(nodes) => (StatusCode::OK, Json(json!(nodes))).into_response(),
        Err(e) => request_error("QUERY_FAILED", &e, None)
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// DELETE /api/lane/admin/graph/nodes/{id}
pub async fn delete_node_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let graph_db = match require_graph_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match graph_db.delete_graph_node(id) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true}))).into_response(),
        Err(e) => {
            request_error("DELETE_FAILED", &e, None).to_response(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// ============================================================================
// Edge handlers
// ============================================================================

/// POST /api/lane/admin/graph/edges
pub async fn create_edge_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateEdgeRequest>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let graph_db = match require_graph_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let created_by = extract_auth_email(&auth).map(|s| s.to_string());

    // Resolve source node ID
    let source_id = if let Some(id) = body.source_node_id {
        id
    } else if let (Some(conn), Some(db)) = (&body.source_connection, &body.source_database) {
        match graph_db.upsert_graph_node(
            conn,
            db,
            body.source_schema.as_deref().unwrap_or(""),
            body.source_table.as_deref().unwrap_or(""),
            "table",
            None,
        ) {
            Ok(id) => id,
            Err(e) => {
                return request_error("CREATE_FAILED", &format!("Source node: {}", e), None)
                    .to_response(StatusCode::BAD_REQUEST);
            }
        }
    } else {
        return request_error(
            "INVALID_REQUEST",
            "Provide source_node_id or (source_connection + source_database)",
            None,
        )
        .to_response(StatusCode::BAD_REQUEST);
    };

    // Resolve target node ID
    let target_id = if let Some(id) = body.target_node_id {
        id
    } else if let (Some(conn), Some(db)) = (&body.target_connection, &body.target_database) {
        match graph_db.upsert_graph_node(
            conn,
            db,
            body.target_schema.as_deref().unwrap_or(""),
            body.target_table.as_deref().unwrap_or(""),
            "table",
            None,
        ) {
            Ok(id) => id,
            Err(e) => {
                return request_error("CREATE_FAILED", &format!("Target node: {}", e), None)
                    .to_response(StatusCode::BAD_REQUEST);
            }
        }
    } else {
        return request_error(
            "INVALID_REQUEST",
            "Provide target_node_id or (target_connection + target_database)",
            None,
        )
        .to_response(StatusCode::BAD_REQUEST);
    };

    match graph_db.create_graph_edge(
        source_id,
        target_id,
        &body.edge_type,
        body.source_columns.as_deref(),
        body.target_columns.as_deref(),
        body.metadata.as_deref(),
        created_by.as_deref(),
    ) {
        Ok(id) => (StatusCode::CREATED, Json(json!({"id": id, "source_node_id": source_id, "target_node_id": target_id}))).into_response(),
        Err(e) => {
            request_error("CREATE_FAILED", &e, None).to_response(StatusCode::BAD_REQUEST)
        }
    }
}

/// GET /api/lane/admin/graph/edges
pub async fn list_edges_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ListEdgesQuery>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let graph_db = match require_graph_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match graph_db.list_graph_edges(query.edge_type.as_deref()) {
        Ok(edges) => (StatusCode::OK, Json(json!(edges))).into_response(),
        Err(e) => request_error("QUERY_FAILED", &e, None)
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// DELETE /api/lane/admin/graph/edges/{id}
pub async fn delete_edge_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let graph_db = match require_graph_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    match graph_db.delete_graph_edge(id) {
        Ok(()) => (StatusCode::OK, Json(json!({"success": true}))).into_response(),
        Err(e) => {
            request_error("DELETE_FAILED", &e, None).to_response(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// ============================================================================
// Seed handler
// ============================================================================

/// POST /api/lane/admin/graph/seed
pub async fn seed_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SeedRequest>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let Err(resp) = require_admin(&auth) {
        return resp;
    }

    let graph_db = match require_graph_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    let registry = &state.registry;
    let connection_names = registry.connection_names();

    let names: Vec<&str> = if let Some(ref filter) = body.connection_name {
        if connection_names.contains(filter) {
            vec![filter.as_str()]
        } else {
            return request_error(
                "NOT_FOUND",
                &format!("Connection '{}' not found", filter),
                None,
            )
            .to_response(StatusCode::NOT_FOUND);
        }
    } else {
        connection_names.iter().map(|s| s.as_str()).collect()
    };

    // Clear existing auto-seeded edges before re-seeding
    let cleared = graph_db
        .delete_graph_edges_by_type("join_key")
        .unwrap_or(0);

    let mut total_edges = 0usize;
    let mut connections_processed = 0usize;
    let mut errors: Vec<String> = Vec::new();

    for conn_name in &names {
        let db = match registry.get(conn_name) {
            Some(db) => db,
            None => continue,
        };

        // List databases
        let databases = match db.list_databases().await {
            Ok(dbs) => dbs,
            Err(e) => {
                errors.push(format!("{}: list_databases failed: {}", conn_name, e));
                continue;
            }
        };

        for db_row in &databases {
            let db_name = db_row
                .get("database_name")
                .or_else(|| db_row.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if db_name.is_empty() {
                continue;
            }

            // List schemas
            let schema_rows = match db.list_schemas(db_name).await {
                Ok(s) => s,
                Err(_) => vec![{
                    let mut m = std::collections::HashMap::new();
                    m.insert(
                        "schema_name".to_string(),
                        serde_json::Value::String("dbo".to_string()),
                    );
                    m
                }],
            };

            for schema_row in &schema_rows {
                let schema = schema_row
                    .get("schema_name")
                    .or_else(|| schema_row.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("dbo");

                // List tables
                let tables = match db.list_tables(db_name, schema).await {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                for table in &tables {
                    let table_name = table
                        .get("TABLE_NAME")
                        .or_else(|| table.get("table_name"))
                        .or_else(|| table.get("name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");

                    if table_name.is_empty() {
                        continue;
                    }

                    match db.get_foreign_keys(db_name, table_name, schema).await {
                        Ok(fks) if !fks.is_empty() => {
                            match graph_db.seed_graph_from_fks(conn_name, db_name, &fks) {
                                Ok(n) => total_edges += n,
                                Err(e) => errors.push(format!(
                                    "{}.{}.{}: seed failed: {}",
                                    conn_name, db_name, table_name, e
                                )),
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        connections_processed += 1;
    }

    (
        StatusCode::OK,
        Json(json!({
            "success": true,
            "connections_processed": connections_processed,
            "edges_cleared": cleared,
            "edges_seeded": total_edges,
            "errors": errors,
        })),
    )
        .into_response()
}

// ============================================================================
// Traverse handler
// ============================================================================

/// POST /api/lane/graph/traverse
pub async fn traverse_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<TraverseRequest>,
) -> Response {
    // Any authenticated user can traverse
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None).to_response(StatusCode::UNAUTHORIZED);
    }

    let graph_db = match require_graph_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    // Resolve start node
    let start_id = if let Some(id) = body.node_id {
        id
    } else if let (Some(conn), Some(db_name)) = (&body.connection_name, &body.database_name) {
        let schema = body.schema_name.as_deref().unwrap_or("");
        let table = body.table_name.as_deref().unwrap_or("");
        match graph_db.find_graph_node(conn, db_name, schema, table) {
            Ok(Some(node)) => node.id,
            Ok(None) => {
                return request_error(
                    "NOT_FOUND",
                    "No graph node matches the given identity",
                    None,
                )
                .to_response(StatusCode::NOT_FOUND);
            }
            Err(e) => {
                return request_error("QUERY_FAILED", &e, None)
                    .to_response(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        return request_error(
            "INVALID_REQUEST",
            "Provide node_id or (connection_name + database_name)",
            None,
        )
        .to_response(StatusCode::BAD_REQUEST);
    };

    // Parse edge type filter
    let edge_type_strings: Vec<String> = body
        .edge_types
        .as_deref()
        .map(|s| s.split(',').map(|t| t.trim().to_string()).collect())
        .unwrap_or_default();
    let edge_types: Option<Vec<&str>> = if edge_type_strings.is_empty() {
        None
    } else {
        Some(edge_type_strings.iter().map(|s| s.as_str()).collect())
    };

    match graph_db.graph_traverse(start_id, body.max_depth, edge_types.as_deref()) {
        Ok(result) => (StatusCode::OK, Json(json!(result))).into_response(),
        Err(e) => request_error("TRAVERSAL_FAILED", &e, None)
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
