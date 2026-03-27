use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
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

#[derive(Debug, Deserialize)]
pub struct GraphPlanRequest {
    pub tables: Vec<GraphPlanTable>,
    #[serde(default)]
    pub row_limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct GraphPlanTable {
    #[serde(default)]
    pub connection: Option<String>,
    pub database: String,
    #[serde(default)]
    pub schema: Option<String>,
    pub table: String,
}

#[derive(Debug, Serialize)]
pub struct GraphPlan {
    pub imports: Vec<PlanImportStep>,
    pub join_query: String,
    pub path_description: String,
}

#[derive(Debug, Serialize)]
pub struct PlanImportStep {
    pub connection: String,
    pub database: String,
    pub query: String,
    pub workspace_table: String,
    pub columns: Vec<String>,
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

// ============================================================================
// Plan handler
// ============================================================================

fn sanitize_ws_name(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c.to_ascii_lowercase() } else { '_' })
        .collect()
}

fn build_select_query(
    dialect: crate::db::Dialect,
    schema: &str,
    table: &str,
    row_limit: Option<i64>,
) -> String {
    match dialect {
        crate::db::Dialect::Mssql => {
            let schema_esc = schema.replace(']', "]]");
            let table_esc = table.replace(']', "]]");
            if let Some(limit) = row_limit {
                format!("SELECT TOP {} * FROM [{}].[{}]", limit, schema_esc, table_esc)
            } else {
                format!("SELECT * FROM [{}].[{}]", schema_esc, table_esc)
            }
        }
        _ => {
            let schema_esc = schema.replace('"', "\"\"");
            let table_esc = table.replace('"', "\"\"");
            if let Some(limit) = row_limit {
                format!("SELECT * FROM \"{}\".\"{}\" LIMIT {}", schema_esc, table_esc, limit)
            } else {
                format!("SELECT * FROM \"{}\".\"{}\"\n", schema_esc, table_esc)
            }
        }
    }
}

/// Build a complete execution plan for combining multiple tables.
pub async fn build_graph_plan(
    registry: &crate::db::ConnectionRegistry,
    join_path: crate::graph::JoinPath,
    row_limit: Option<i64>,
) -> Result<GraphPlan, String> {
    // Build workspace table names and detect collisions
    let mut ws_names: HashMap<i64, String> = HashMap::new();
    let mut name_counts: HashMap<String, usize> = HashMap::new();

    // First pass: generate base names
    for node in &join_path.nodes {
        let base = format!("{}_{}", sanitize_ws_name(&node.connection_name), sanitize_ws_name(&node.table_name));
        *name_counts.entry(base.clone()).or_insert(0) += 1;
    }

    // Second pass: add database prefix on collision
    for node in &join_path.nodes {
        let base = format!("{}_{}", sanitize_ws_name(&node.connection_name), sanitize_ws_name(&node.table_name));
        let name = if name_counts.get(&base).copied().unwrap_or(0) > 1 {
            format!("{}_{}_{}", sanitize_ws_name(&node.connection_name), sanitize_ws_name(&node.database_name), sanitize_ws_name(&node.table_name))
        } else {
            base
        };
        ws_names.insert(node.id, name);
    }

    // Describe tables and build import steps
    let mut imports: Vec<PlanImportStep> = Vec::new();
    for node in &join_path.nodes {
        let db = registry.get(&node.connection_name).ok_or_else(|| {
            format!("Connection '{}' not found", node.connection_name)
        })?;

        let columns = match db.describe_table(&node.database_name, &node.table_name, &node.schema_name).await {
            Ok(cols) => cols
                .iter()
                .filter_map(|c| c.get("COLUMN_NAME").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .collect::<Vec<_>>(),
            Err(_) => vec![], // table might not be accessible, still include in plan
        };

        let query = build_select_query(db.dialect(), &node.schema_name, &node.table_name, row_limit);
        let ws_table = ws_names.get(&node.id).cloned().unwrap_or_default();

        imports.push(PlanImportStep {
            connection: node.connection_name.clone(),
            database: node.database_name.clone(),
            query: query.trim().to_string(),
            workspace_table: ws_table,
            columns,
        });
    }

    // Build join SQL (DuckDB dialect — workspace is always DuckDB)
    let mut join_sql = String::new();
    if imports.is_empty() {
        return Ok(GraphPlan {
            imports,
            join_query: String::new(),
            path_description: String::new(),
        });
    }

    // Node ID -> workspace table name + alias
    let mut node_alias: HashMap<i64, (String, String)> = HashMap::new();
    for (i, node) in join_path.nodes.iter().enumerate() {
        let ws_name = ws_names.get(&node.id).cloned().unwrap_or_default();
        let alias = format!("t{}", i);
        node_alias.insert(node.id, (ws_name, alias));
    }

    // SELECT * FROM first table
    let (first_ws, first_alias) = node_alias.get(&join_path.nodes[0].id).unwrap();
    join_sql.push_str(&format!("SELECT *\nFROM \"{}\" AS {}", first_ws, first_alias));

    // Track which nodes have been joined
    let mut joined: HashSet<i64> = HashSet::new();
    joined.insert(join_path.nodes[0].id);

    // For each edge, add a LEFT JOIN
    for edge in &join_path.edges {
        let src_id = edge.source.id;
        let tgt_id = edge.target.id;

        // Figure out which side is already joined and which is new
        let (existing_id, new_id, left_cols, right_cols) = if joined.contains(&src_id) && !joined.contains(&tgt_id) {
            (src_id, tgt_id, &edge.source_columns, &edge.target_columns)
        } else if joined.contains(&tgt_id) && !joined.contains(&src_id) {
            (tgt_id, src_id, &edge.target_columns, &edge.source_columns)
        } else if joined.contains(&src_id) && joined.contains(&tgt_id) {
            continue; // both already joined, skip duplicate edge
        } else {
            continue; // neither joined yet — shouldn't happen with ordered edges
        };

        let (_, existing_alias) = node_alias.get(&existing_id).unwrap();
        let (new_ws, new_alias) = node_alias.get(&new_id).unwrap();

        join_sql.push_str(&format!("\nLEFT JOIN \"{}\" AS {}", new_ws, new_alias));

        // Build ON clause from column mappings
        if let (Some(l_cols), Some(r_cols)) = (left_cols, right_cols) {
            let conditions: Vec<String> = l_cols
                .iter()
                .zip(r_cols.iter())
                .map(|(l, r)| format!("{}.\"{}\" = {}.\"{}\"", existing_alias, l, new_alias, r))
                .collect();
            if !conditions.is_empty() {
                join_sql.push_str(&format!("\n  ON {}", conditions.join(" AND ")));
            }
        }

        joined.insert(new_id);
    }

    // Build path description
    let mut path_parts: Vec<String> = Vec::new();
    for (i, node) in join_path.nodes.iter().enumerate() {
        let mut part = node.table_name.clone();
        if i > 0 {
            // Find the edge that connects this node
            if let Some(edge) = join_path.edges.iter().find(|e| e.source.id == node.id || e.target.id == node.id) {
                let src_cols = edge.source_columns.as_ref().map(|c| c.join(",")).unwrap_or_default();
                let tgt_cols = edge.target_columns.as_ref().map(|c| c.join(",")).unwrap_or_default();
                part = format!("{} ({}→{})", node.table_name, src_cols, tgt_cols);
            }
        }
        path_parts.push(part);
    }
    let path_description = path_parts.join(" → ");

    Ok(GraphPlan {
        imports,
        join_query: join_sql,
        path_description,
    })
}

/// POST /api/lane/graph/plan
pub async fn plan_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<GraphPlanRequest>,
) -> Response {
    let auth = auth::authenticate(&headers, &state).await;
    if let AuthResult::Denied(reason) = &auth {
        return request_error("UNAUTHORIZED", reason, None).to_response(StatusCode::UNAUTHORIZED);
    }

    let graph_db = match require_graph_db(&state) {
        Ok(db) => db,
        Err(resp) => return resp,
    };

    if body.tables.is_empty() {
        return request_error("INVALID_REQUEST", "At least one table is required", None)
            .to_response(StatusCode::BAD_REQUEST);
    }

    // Resolve each table to a node ID
    let default_conn = state.registry.default_name();
    let mut node_ids: Vec<i64> = Vec::new();

    for t in &body.tables {
        let conn = t.connection.as_deref().unwrap_or(&default_conn);
        let schema = t.schema.as_deref().unwrap_or("");
        match graph_db.find_graph_node(conn, &t.database, schema, &t.table) {
            Ok(Some(node)) => node_ids.push(node.id),
            Ok(None) => {
                return request_error(
                    "NOT_FOUND",
                    &format!("No graph node for {}/{}/{}.{}", conn, t.database, schema, t.table),
                    Some("Seed the graph or create the node first"),
                )
                .to_response(StatusCode::NOT_FOUND);
            }
            Err(e) => {
                return request_error("QUERY_FAILED", &e, None)
                    .to_response(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    // Find join paths
    let join_path = match graph_db.find_join_paths(&node_ids) {
        Ok(p) => p,
        Err(e) => {
            return request_error("PLAN_FAILED", &e, Some("Ensure edges exist between the requested tables"))
                .to_response(StatusCode::BAD_REQUEST);
        }
    };

    // Build the plan
    match build_graph_plan(&state.registry, join_path, body.row_limit).await {
        Ok(plan) => (StatusCode::OK, Json(json!(plan))).into_response(),
        Err(e) => request_error("PLAN_FAILED", &e, None)
            .to_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
