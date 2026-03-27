use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNode {
    pub id: i64,
    pub connection_name: String,
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub node_type: String,
    pub label: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphEdge {
    pub id: i64,
    pub source_node_id: i64,
    pub target_node_id: i64,
    pub edge_type: String,
    pub source_columns: Option<String>,
    pub target_columns: Option<String>,
    pub metadata: Option<String>,
    pub created_by: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphEdgeExpanded {
    pub id: i64,
    pub edge_type: String,
    pub source: GraphNode,
    pub target: GraphNode,
    pub source_columns: Option<Vec<String>>,
    pub target_columns: Option<Vec<String>>,
    pub metadata: Option<serde_json::Value>,
    pub created_by: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraversalResult {
    pub start_node: GraphNode,
    pub reachable: Vec<TraversalPath>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraversalPath {
    pub node: GraphNode,
    pub depth: usize,
    pub edges: Vec<GraphEdgeExpanded>,
}

/// A path connecting multiple requested tables through the graph.
#[derive(Debug, Clone, Serialize)]
pub struct JoinPath {
    /// All nodes in the join chain, ordered for import (hub first, then by BFS depth).
    /// Includes both requested tables and any intermediate tables needed.
    pub nodes: Vec<GraphNode>,
    /// Unique edges connecting the nodes, in traversal order.
    pub edges: Vec<GraphEdgeExpanded>,
}

// ============================================================================
// GraphDb
// ============================================================================

pub struct GraphDb {
    conn: Mutex<Connection>,
}

impl GraphDb {
    pub fn new(path: &str, key: &str) -> Result<Self, String> {
        let conn =
            Connection::open(path).map_err(|e| format!("Failed to open graph DB: {}", e))?;

        conn.pragma_update(None, "key", key)
            .map_err(|e| format!("Failed to set SQLCipher key: {}", e))?;

        conn.execute_batch("SELECT count(*) FROM sqlite_master;")
            .map_err(|e| format!("SQLCipher key verification failed (wrong key?): {}", e))?;

        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| format!("Failed to set WAL mode: {}", e))?;

        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(|e| format!("Failed to enable foreign keys: {}", e))?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS graph_nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_name TEXT NOT NULL,
                database_name TEXT NOT NULL,
                schema_name TEXT NOT NULL DEFAULT '',
                table_name TEXT NOT NULL DEFAULT '',
                node_type TEXT NOT NULL DEFAULT 'table',
                label TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(connection_name, database_name, schema_name, table_name)
            );

            CREATE TABLE IF NOT EXISTS graph_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_node_id INTEGER NOT NULL,
                target_node_id INTEGER NOT NULL,
                edge_type TEXT NOT NULL DEFAULT 'join_key',
                source_columns TEXT,
                target_columns TEXT,
                metadata TEXT DEFAULT '{}',
                created_by TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(source_node_id, target_node_id, edge_type, source_columns, target_columns),
                FOREIGN KEY (source_node_id) REFERENCES graph_nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (target_node_id) REFERENCES graph_nodes(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_graph_edges_source ON graph_edges(source_node_id);
            CREATE INDEX IF NOT EXISTS idx_graph_edges_target ON graph_edges(target_node_id);
            CREATE INDEX IF NOT EXISTS idx_graph_edges_type ON graph_edges(edge_type);
            ",
        )
        .map_err(|e| format!("Failed to create graph tables: {}", e))?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    // ========================================================================
    // Node CRUD
    // ========================================================================

    /// Get or create a graph node. Returns the node ID.
    pub fn upsert_graph_node(
        &self,
        connection_name: &str,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
        node_type: &str,
        label: Option<&str>,
    ) -> Result<i64, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT OR IGNORE INTO graph_nodes (connection_name, database_name, schema_name, table_name, node_type, label)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![connection_name, database_name, schema_name, table_name, node_type, label],
        )
        .map_err(|e| format!("Failed to upsert node: {}", e))?;

        let id: i64 = conn
            .query_row(
                "SELECT id FROM graph_nodes WHERE connection_name = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4",
                params![connection_name, database_name, schema_name, table_name],
                |row| row.get(0),
            )
            .map_err(|e| format!("Failed to get node id: {}", e))?;

        Ok(id)
    }

    pub fn get_graph_node(&self, id: i64) -> Result<Option<GraphNode>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn
            .prepare("SELECT id, connection_name, database_name, schema_name, table_name, node_type, label, created_at FROM graph_nodes WHERE id = ?1")
            .map_err(|e| format!("Prepare error: {}", e))?;

        let node = stmt
            .query_row(params![id], |row| {
                Ok(GraphNode {
                    id: row.get(0)?,
                    connection_name: row.get(1)?,
                    database_name: row.get(2)?,
                    schema_name: row.get(3)?,
                    table_name: row.get(4)?,
                    node_type: row.get(5)?,
                    label: row.get(6)?,
                    created_at: row.get(7)?,
                })
            })
            .ok();

        Ok(node)
    }

    pub fn find_graph_node(
        &self,
        connection_name: &str,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<GraphNode>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, connection_name, database_name, schema_name, table_name, node_type, label, created_at
                 FROM graph_nodes
                 WHERE connection_name = ?1 AND database_name = ?2 AND schema_name = ?3 AND table_name = ?4",
            )
            .map_err(|e| format!("Prepare error: {}", e))?;

        let node = stmt
            .query_row(
                params![connection_name, database_name, schema_name, table_name],
                |row| {
                    Ok(GraphNode {
                        id: row.get(0)?,
                        connection_name: row.get(1)?,
                        database_name: row.get(2)?,
                        schema_name: row.get(3)?,
                        table_name: row.get(4)?,
                        node_type: row.get(5)?,
                        label: row.get(6)?,
                        created_at: row.get(7)?,
                    })
                },
            )
            .ok();

        Ok(node)
    }

    pub fn list_graph_nodes(
        &self,
        connection_filter: Option<&str>,
    ) -> Result<Vec<GraphNode>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let (sql, filter_params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) =
            if let Some(cn) = connection_filter {
                (
                    "SELECT id, connection_name, database_name, schema_name, table_name, node_type, label, created_at
                     FROM graph_nodes WHERE connection_name = ?1 ORDER BY connection_name, database_name, schema_name, table_name"
                        .to_string(),
                    vec![Box::new(cn.to_string())],
                )
            } else {
                (
                    "SELECT id, connection_name, database_name, schema_name, table_name, node_type, label, created_at
                     FROM graph_nodes ORDER BY connection_name, database_name, schema_name, table_name"
                        .to_string(),
                    vec![],
                )
            };

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("Prepare error: {}", e))?;

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            filter_params.iter().map(|p| p.as_ref()).collect();

        let nodes = stmt
            .query_map(params_refs.as_slice(), |row| {
                Ok(GraphNode {
                    id: row.get(0)?,
                    connection_name: row.get(1)?,
                    database_name: row.get(2)?,
                    schema_name: row.get(3)?,
                    table_name: row.get(4)?,
                    node_type: row.get(5)?,
                    label: row.get(6)?,
                    created_at: row.get(7)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(nodes)
    }

    pub fn delete_graph_node(&self, id: i64) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute("DELETE FROM graph_nodes WHERE id = ?1", params![id])
            .map_err(|e| format!("Failed to delete node: {}", e))?;
        Ok(())
    }

    // ========================================================================
    // Edge CRUD
    // ========================================================================

    pub fn create_graph_edge(
        &self,
        source_node_id: i64,
        target_node_id: i64,
        edge_type: &str,
        source_columns: Option<&str>,
        target_columns: Option<&str>,
        metadata: Option<&str>,
        created_by: Option<&str>,
    ) -> Result<i64, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO graph_edges (source_node_id, target_node_id, edge_type, source_columns, target_columns, metadata, created_by)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                source_node_id,
                target_node_id,
                edge_type,
                source_columns,
                target_columns,
                metadata.unwrap_or("{}"),
                created_by,
            ],
        )
        .map_err(|e| format!("Failed to create edge: {}", e))?;

        Ok(conn.last_insert_rowid())
    }

    /// Create an edge, ignoring duplicates. Returns the edge ID (new or existing).
    pub fn create_graph_edge_or_ignore(
        &self,
        source_node_id: i64,
        target_node_id: i64,
        edge_type: &str,
        source_columns: Option<&str>,
        target_columns: Option<&str>,
        metadata: Option<&str>,
        created_by: Option<&str>,
    ) -> Result<i64, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT OR IGNORE INTO graph_edges (source_node_id, target_node_id, edge_type, source_columns, target_columns, metadata, created_by)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                source_node_id,
                target_node_id,
                edge_type,
                source_columns,
                target_columns,
                metadata.unwrap_or("{}"),
                created_by,
            ],
        )
        .map_err(|e| format!("Failed to create edge: {}", e))?;

        // Return the ID (either newly inserted or existing)
        let id: i64 = conn
            .query_row(
                "SELECT id FROM graph_edges WHERE source_node_id = ?1 AND target_node_id = ?2 AND edge_type = ?3 AND source_columns IS ?4 AND target_columns IS ?5",
                params![source_node_id, target_node_id, edge_type, source_columns, target_columns],
                |row| row.get(0),
            )
            .map_err(|e| format!("Failed to get edge id: {}", e))?;

        Ok(id)
    }

    pub fn list_graph_edges(
        &self,
        edge_type_filter: Option<&str>,
    ) -> Result<Vec<GraphEdgeExpanded>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let (sql, filter_params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) =
            if let Some(et) = edge_type_filter {
                (
                    "SELECT e.id, e.edge_type, e.source_columns, e.target_columns, e.metadata, e.created_by, e.created_at,
                            s.id, s.connection_name, s.database_name, s.schema_name, s.table_name, s.node_type, s.label, s.created_at,
                            t.id, t.connection_name, t.database_name, t.schema_name, t.table_name, t.node_type, t.label, t.created_at
                     FROM graph_edges e
                     JOIN graph_nodes s ON e.source_node_id = s.id
                     JOIN graph_nodes t ON e.target_node_id = t.id
                     WHERE e.edge_type = ?1
                     ORDER BY e.id".to_string(),
                    vec![Box::new(et.to_string())],
                )
            } else {
                (
                    "SELECT e.id, e.edge_type, e.source_columns, e.target_columns, e.metadata, e.created_by, e.created_at,
                            s.id, s.connection_name, s.database_name, s.schema_name, s.table_name, s.node_type, s.label, s.created_at,
                            t.id, t.connection_name, t.database_name, t.schema_name, t.table_name, t.node_type, t.label, t.created_at
                     FROM graph_edges e
                     JOIN graph_nodes s ON e.source_node_id = s.id
                     JOIN graph_nodes t ON e.target_node_id = t.id
                     ORDER BY e.id".to_string(),
                    vec![],
                )
            };

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("Prepare error: {}", e))?;

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            filter_params.iter().map(|p| p.as_ref()).collect();

        let edges = stmt
            .query_map(params_refs.as_slice(), |row| {
                Ok(Self::row_to_expanded_edge(row))
            })
            .map_err(|e| format!("Query error: {}", e))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(edges)
    }

    pub fn list_graph_edges_for_node(&self, node_id: i64) -> Result<Vec<GraphEdge>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, source_node_id, target_node_id, edge_type, source_columns, target_columns, metadata, created_by, created_at
                 FROM graph_edges
                 WHERE source_node_id = ?1 OR target_node_id = ?1",
            )
            .map_err(|e| format!("Prepare error: {}", e))?;

        let edges = stmt
            .query_map(params![node_id], |row| {
                Ok(GraphEdge {
                    id: row.get(0)?,
                    source_node_id: row.get(1)?,
                    target_node_id: row.get(2)?,
                    edge_type: row.get(3)?,
                    source_columns: row.get(4)?,
                    target_columns: row.get(5)?,
                    metadata: row.get(6)?,
                    created_by: row.get(7)?,
                    created_at: row.get(8)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(edges)
    }

    pub fn delete_graph_edge(&self, id: i64) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute("DELETE FROM graph_edges WHERE id = ?1", params![id])
            .map_err(|e| format!("Failed to delete edge: {}", e))?;
        Ok(())
    }

    pub fn delete_graph_edges_by_type(&self, edge_type: &str) -> Result<usize, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let count = conn
            .execute(
                "DELETE FROM graph_edges WHERE edge_type = ?1",
                params![edge_type],
            )
            .map_err(|e| format!("Failed to delete edges: {}", e))?;
        Ok(count)
    }

    // ========================================================================
    // Traversal
    // ========================================================================

    /// BFS traversal from a starting node. Returns all reachable nodes and the paths to them.
    pub fn graph_traverse(
        &self,
        start_node_id: i64,
        max_depth: Option<usize>,
        edge_types: Option<&[&str]>,
    ) -> Result<TraversalResult, String> {
        let start_node = self
            .get_graph_node(start_node_id)?
            .ok_or_else(|| format!("Start node {} not found", start_node_id))?;

        let max = max_depth.unwrap_or(5).min(10);
        let mut queue: VecDeque<(i64, usize, Vec<GraphEdgeExpanded>)> = VecDeque::new();
        let mut visited: HashSet<i64> = HashSet::new();
        let mut reachable: Vec<TraversalPath> = Vec::new();

        queue.push_back((start_node_id, 0, vec![]));
        visited.insert(start_node_id);

        while let Some((current_id, depth, path)) = queue.pop_front() {
            if depth >= max {
                continue;
            }

            let edges = self.list_graph_edges_for_node(current_id)?;

            for edge in edges {
                // Filter by edge type if specified
                if let Some(types) = edge_types {
                    if !types.contains(&edge.edge_type.as_str()) {
                        continue;
                    }
                }

                let neighbor_id = if edge.source_node_id == current_id {
                    edge.target_node_id
                } else {
                    edge.source_node_id
                };

                if visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);

                let neighbor_node = match self.get_graph_node(neighbor_id)? {
                    Some(n) => n,
                    None => continue,
                };

                // Build expanded edge for the path
                let source_node = self.get_graph_node(edge.source_node_id)?;
                let target_node = self.get_graph_node(edge.target_node_id)?;
                if source_node.is_none() || target_node.is_none() {
                    continue;
                }

                let expanded = GraphEdgeExpanded {
                    id: edge.id,
                    edge_type: edge.edge_type.clone(),
                    source: source_node.unwrap(),
                    target: target_node.unwrap(),
                    source_columns: edge
                        .source_columns
                        .as_deref()
                        .map(|s| s.split(',').map(|c| c.trim().to_string()).collect()),
                    target_columns: edge
                        .target_columns
                        .as_deref()
                        .map(|s| s.split(',').map(|c| c.trim().to_string()).collect()),
                    metadata: edge
                        .metadata
                        .as_deref()
                        .and_then(|s| serde_json::from_str(s).ok()),
                    created_by: edge.created_by.clone(),
                    created_at: edge.created_at.clone(),
                };

                let mut new_path = path.clone();
                new_path.push(expanded);

                reachable.push(TraversalPath {
                    node: neighbor_node,
                    depth: depth + 1,
                    edges: new_path.clone(),
                });

                queue.push_back((neighbor_id, depth + 1, new_path));
            }
        }

        Ok(TraversalResult {
            start_node,
            reachable,
        })
    }

    // ========================================================================
    // Join Path Finding
    // ========================================================================

    /// Find the shortest paths connecting multiple tables through the graph.
    /// Uses the first table as a hub and BFS to find paths to all others.
    /// Returns an ordered list of nodes (including intermediates) and edges.
    pub fn find_join_paths(&self, node_ids: &[i64]) -> Result<JoinPath, String> {
        if node_ids.is_empty() {
            return Err("At least one node ID is required".to_string());
        }

        // Single table — no join needed
        if node_ids.len() == 1 {
            let node = self
                .get_graph_node(node_ids[0])?
                .ok_or_else(|| format!("Node {} not found", node_ids[0]))?;
            return Ok(JoinPath {
                nodes: vec![node],
                edges: vec![],
            });
        }

        // Hub = first node. BFS from hub to find all others.
        let hub_id = node_ids[0];
        let hub_node = self
            .get_graph_node(hub_id)?
            .ok_or_else(|| format!("Hub node {} not found", hub_id))?;

        let traversal = self.graph_traverse(hub_id, Some(10), None)?;

        // Collect paths to each requested target
        let mut all_edges: Vec<GraphEdgeExpanded> = Vec::new();
        let mut all_node_ids: HashSet<i64> = HashSet::new();
        all_node_ids.insert(hub_id);

        for &target_id in &node_ids[1..] {
            // Verify target exists
            let target_node = self
                .get_graph_node(target_id)?
                .ok_or_else(|| format!("Node {} not found", target_id))?;

            // Find target in traversal results
            let path = traversal
                .reachable
                .iter()
                .find(|p| p.node.id == target_id)
                .ok_or_else(|| {
                    format!(
                        "No path found between {}.{} and {}.{}",
                        hub_node.connection_name, hub_node.table_name,
                        target_node.connection_name, target_node.table_name,
                    )
                })?;

            // Collect all nodes and edges from this path
            for edge in &path.edges {
                all_node_ids.insert(edge.source.id);
                all_node_ids.insert(edge.target.id);
            }
            all_edges.extend(path.edges.clone());
        }

        // Deduplicate edges by ID
        let mut seen_edge_ids: HashSet<i64> = HashSet::new();
        let unique_edges: Vec<GraphEdgeExpanded> = all_edges
            .into_iter()
            .filter(|e| seen_edge_ids.insert(e.id))
            .collect();

        // Build ordered node list: hub first, then others by BFS depth
        let mut ordered_nodes: Vec<GraphNode> = vec![hub_node];
        // Sort remaining by their depth in the traversal (closest first)
        let mut remaining: Vec<&TraversalPath> = traversal
            .reachable
            .iter()
            .filter(|p| all_node_ids.contains(&p.node.id) && p.node.id != hub_id)
            .collect();
        remaining.sort_by_key(|p| p.depth);
        for path in remaining {
            ordered_nodes.push(path.node.clone());
        }

        Ok(JoinPath {
            nodes: ordered_nodes,
            edges: unique_edges,
        })
    }

    // ========================================================================
    // FK Seeding
    // ========================================================================

    /// Bulk upsert edges from FK metadata. Returns count of new edges created.
    pub fn seed_graph_from_fks(
        &self,
        connection_name: &str,
        database_name: &str,
        fks: &[crate::db::ForeignKeyInfo],
    ) -> Result<usize, String> {
        let mut count = 0;

        for fk in fks {
            let source_id = self.upsert_graph_node(
                connection_name,
                database_name,
                &fk.from_schema,
                &fk.from_table,
                "table",
                None,
            )?;

            let target_id = self.upsert_graph_node(
                connection_name,
                database_name,
                &fk.to_schema,
                &fk.to_table,
                "table",
                None,
            )?;

            let src_cols = fk.from_columns.join(",");
            let tgt_cols = fk.to_columns.join(",");

            let result = self.create_graph_edge_or_ignore(
                source_id,
                target_id,
                "join_key",
                Some(&src_cols),
                Some(&tgt_cols),
                None,
                Some("auto_seed"),
            );

            if result.is_ok() {
                count += 1;
            }
        }

        Ok(count)
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    #[allow(dead_code)]
    fn row_to_expanded_edge(row: &rusqlite::Row) -> GraphEdgeExpanded {
        let source_columns: Option<String> = row.get(2).ok();
        let target_columns: Option<String> = row.get(3).ok();
        let metadata_str: Option<String> = row.get(4).ok();

        GraphEdgeExpanded {
            id: row.get(0).unwrap_or(0),
            edge_type: row.get(1).unwrap_or_default(),
            source_columns: source_columns
                .as_deref()
                .map(|s| s.split(',').map(|c| c.trim().to_string()).collect()),
            target_columns: target_columns
                .as_deref()
                .map(|s| s.split(',').map(|c| c.trim().to_string()).collect()),
            metadata: metadata_str
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok()),
            created_by: row.get(5).ok(),
            created_at: row.get(6).unwrap_or_default(),
            source: GraphNode {
                id: row.get(7).unwrap_or(0),
                connection_name: row.get(8).unwrap_or_default(),
                database_name: row.get(9).unwrap_or_default(),
                schema_name: row.get(10).unwrap_or_default(),
                table_name: row.get(11).unwrap_or_default(),
                node_type: row.get(12).unwrap_or_default(),
                label: row.get(13).ok(),
                created_at: row.get(14).unwrap_or_default(),
            },
            target: GraphNode {
                id: row.get(15).unwrap_or(0),
                connection_name: row.get(16).unwrap_or_default(),
                database_name: row.get(17).unwrap_or_default(),
                schema_name: row.get(18).unwrap_or_default(),
                table_name: row.get(19).unwrap_or_default(),
                node_type: row.get(20).unwrap_or_default(),
                label: row.get(21).ok(),
                created_at: row.get(22).unwrap_or_default(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> GraphDb {
        GraphDb::new(":memory:", "test-key-12345").unwrap()
    }

    #[test]
    fn upsert_and_find_node() {
        let db = test_db();
        let id = db
            .upsert_graph_node("mssql", "ACEDW", "dbo", "Users", "table", None)
            .unwrap();
        assert!(id > 0);

        // Upsert again returns same ID
        let id2 = db
            .upsert_graph_node("mssql", "ACEDW", "dbo", "Users", "table", None)
            .unwrap();
        assert_eq!(id, id2);

        // Find by identity
        let node = db
            .find_graph_node("mssql", "ACEDW", "dbo", "Users")
            .unwrap()
            .unwrap();
        assert_eq!(node.id, id);
        assert_eq!(node.connection_name, "mssql");
        assert_eq!(node.table_name, "Users");

        // Get by ID
        let node2 = db.get_graph_node(id).unwrap().unwrap();
        assert_eq!(node2.table_name, "Users");
    }

    #[test]
    fn list_nodes_with_filter() {
        let db = test_db();
        db.upsert_graph_node("mssql", "ACEDW", "dbo", "Users", "table", None)
            .unwrap();
        db.upsert_graph_node("mssql", "ACEDW", "dbo", "Orders", "table", None)
            .unwrap();
        db.upsert_graph_node("postgres", "analytics", "public", "events", "table", None)
            .unwrap();

        let all = db.list_graph_nodes(None).unwrap();
        assert_eq!(all.len(), 3);

        let mssql_only = db.list_graph_nodes(Some("mssql")).unwrap();
        assert_eq!(mssql_only.len(), 2);

        let pg_only = db.list_graph_nodes(Some("postgres")).unwrap();
        assert_eq!(pg_only.len(), 1);
        assert_eq!(pg_only[0].table_name, "events");
    }

    #[test]
    fn delete_node_cascades_edges() {
        let db = test_db();
        let a = db
            .upsert_graph_node("mssql", "db1", "dbo", "A", "table", None)
            .unwrap();
        let b = db
            .upsert_graph_node("mssql", "db1", "dbo", "B", "table", None)
            .unwrap();
        db.create_graph_edge(a, b, "join_key", Some("id"), Some("a_id"), None, None)
            .unwrap();

        assert_eq!(db.list_graph_edges(None).unwrap().len(), 1);

        db.delete_graph_node(a).unwrap();

        // Edge should be gone via CASCADE
        assert_eq!(db.list_graph_edges(None).unwrap().len(), 0);
    }

    #[test]
    fn create_and_list_edges() {
        let db = test_db();
        let a = db
            .upsert_graph_node("mssql", "db1", "dbo", "Users", "table", None)
            .unwrap();
        let b = db
            .upsert_graph_node("pg", "db2", "public", "orders", "table", None)
            .unwrap();

        let edge_id = db
            .create_graph_edge(
                a,
                b,
                "join_key",
                Some("user_id"),
                Some("customer_id"),
                None,
                Some("admin@test.com"),
            )
            .unwrap();
        assert!(edge_id > 0);

        let edges = db.list_graph_edges(None).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].edge_type, "join_key");
        assert_eq!(edges[0].source.table_name, "Users");
        assert_eq!(edges[0].target.table_name, "orders");
        assert_eq!(
            edges[0].source_columns,
            Some(vec!["user_id".to_string()])
        );
        assert_eq!(
            edges[0].target_columns,
            Some(vec!["customer_id".to_string()])
        );

        // Filter by edge type
        let join_only = db.list_graph_edges(Some("join_key")).unwrap();
        assert_eq!(join_only.len(), 1);
        let other = db.list_graph_edges(Some("derives_from")).unwrap();
        assert_eq!(other.len(), 0);
    }

    #[test]
    fn edge_duplicate_ignored() {
        let db = test_db();
        let a = db
            .upsert_graph_node("mssql", "db1", "dbo", "A", "table", None)
            .unwrap();
        let b = db
            .upsert_graph_node("mssql", "db1", "dbo", "B", "table", None)
            .unwrap();

        let id1 = db
            .create_graph_edge_or_ignore(a, b, "join_key", Some("id"), Some("a_id"), None, None)
            .unwrap();
        let id2 = db
            .create_graph_edge_or_ignore(a, b, "join_key", Some("id"), Some("a_id"), None, None)
            .unwrap();
        assert_eq!(id1, id2);
        assert_eq!(db.list_graph_edges(None).unwrap().len(), 1);
    }

    #[test]
    fn traverse_linear_chain() {
        // A -> B -> C
        let db = test_db();
        let a = db
            .upsert_graph_node("conn", "db", "dbo", "A", "table", None)
            .unwrap();
        let b = db
            .upsert_graph_node("conn", "db", "dbo", "B", "table", None)
            .unwrap();
        let c = db
            .upsert_graph_node("conn", "db", "dbo", "C", "table", None)
            .unwrap();
        db.create_graph_edge(a, b, "join_key", Some("id"), Some("a_id"), None, None)
            .unwrap();
        db.create_graph_edge(b, c, "join_key", Some("id"), Some("b_id"), None, None)
            .unwrap();

        let result = db.graph_traverse(a, Some(5), None).unwrap();
        assert_eq!(result.start_node.table_name, "A");
        assert_eq!(result.reachable.len(), 2);

        // B at depth 1
        let b_path = result.reachable.iter().find(|p| p.node.table_name == "B").unwrap();
        assert_eq!(b_path.depth, 1);
        assert_eq!(b_path.edges.len(), 1);

        // C at depth 2
        let c_path = result.reachable.iter().find(|p| p.node.table_name == "C").unwrap();
        assert_eq!(c_path.depth, 2);
        assert_eq!(c_path.edges.len(), 2);
    }

    #[test]
    fn traverse_respects_max_depth() {
        // A -> B -> C, but max_depth=1 should only find B
        let db = test_db();
        let a = db
            .upsert_graph_node("conn", "db", "dbo", "A", "table", None)
            .unwrap();
        let b = db
            .upsert_graph_node("conn", "db", "dbo", "B", "table", None)
            .unwrap();
        let c = db
            .upsert_graph_node("conn", "db", "dbo", "C", "table", None)
            .unwrap();
        db.create_graph_edge(a, b, "join_key", None, None, None, None)
            .unwrap();
        db.create_graph_edge(b, c, "join_key", None, None, None, None)
            .unwrap();

        let result = db.graph_traverse(a, Some(1), None).unwrap();
        assert_eq!(result.reachable.len(), 1);
        assert_eq!(result.reachable[0].node.table_name, "B");
    }

    #[test]
    fn traverse_filters_edge_types() {
        // A -join_key-> B, A -derives_from-> C
        let db = test_db();
        let a = db
            .upsert_graph_node("conn", "db", "dbo", "A", "table", None)
            .unwrap();
        let b = db
            .upsert_graph_node("conn", "db", "dbo", "B", "table", None)
            .unwrap();
        let c = db
            .upsert_graph_node("conn", "db", "dbo", "C", "table", None)
            .unwrap();
        db.create_graph_edge(a, b, "join_key", None, None, None, None)
            .unwrap();
        db.create_graph_edge(a, c, "derives_from", None, None, None, None)
            .unwrap();

        // Only follow join_key
        let result = db
            .graph_traverse(a, Some(5), Some(&["join_key"]))
            .unwrap();
        assert_eq!(result.reachable.len(), 1);
        assert_eq!(result.reachable[0].node.table_name, "B");

        // Only follow derives_from
        let result = db
            .graph_traverse(a, Some(5), Some(&["derives_from"]))
            .unwrap();
        assert_eq!(result.reachable.len(), 1);
        assert_eq!(result.reachable[0].node.table_name, "C");

        // Follow all
        let result = db.graph_traverse(a, Some(5), None).unwrap();
        assert_eq!(result.reachable.len(), 2);
    }

    #[test]
    fn traverse_bidirectional() {
        // A -> B: traversing from B should still find A
        let db = test_db();
        let a = db
            .upsert_graph_node("conn", "db", "dbo", "A", "table", None)
            .unwrap();
        let b = db
            .upsert_graph_node("conn", "db", "dbo", "B", "table", None)
            .unwrap();
        db.create_graph_edge(a, b, "join_key", None, None, None, None)
            .unwrap();

        let result = db.graph_traverse(b, Some(5), None).unwrap();
        assert_eq!(result.reachable.len(), 1);
        assert_eq!(result.reachable[0].node.table_name, "A");
    }

    #[test]
    fn traverse_handles_cycles() {
        // A -> B -> C -> A (cycle)
        let db = test_db();
        let a = db
            .upsert_graph_node("conn", "db", "dbo", "A", "table", None)
            .unwrap();
        let b = db
            .upsert_graph_node("conn", "db", "dbo", "B", "table", None)
            .unwrap();
        let c = db
            .upsert_graph_node("conn", "db", "dbo", "C", "table", None)
            .unwrap();
        db.create_graph_edge(a, b, "join_key", None, None, None, None)
            .unwrap();
        db.create_graph_edge(b, c, "join_key", None, None, None, None)
            .unwrap();
        db.create_graph_edge(c, a, "join_key", None, None, None, None)
            .unwrap();

        // Should not infinite loop — visited set prevents revisiting
        let result = db.graph_traverse(a, Some(10), None).unwrap();
        assert_eq!(result.reachable.len(), 2); // B and C, not A again
    }

    #[test]
    fn traverse_cross_connection() {
        // mssql.Users -> postgres.orders (cross-connection edge)
        let db = test_db();
        let users = db
            .upsert_graph_node("mssql", "ACEDW", "dbo", "Users", "table", None)
            .unwrap();
        let orders = db
            .upsert_graph_node("postgres", "analytics", "public", "orders", "table", None)
            .unwrap();
        db.create_graph_edge(
            users,
            orders,
            "join_key",
            Some("user_id"),
            Some("customer_id"),
            None,
            Some("admin"),
        )
        .unwrap();

        let result = db.graph_traverse(users, Some(3), None).unwrap();
        assert_eq!(result.reachable.len(), 1);
        assert_eq!(result.reachable[0].node.connection_name, "postgres");
        assert_eq!(result.reachable[0].node.table_name, "orders");
        assert_eq!(result.reachable[0].edges.len(), 1);
        assert_eq!(
            result.reachable[0].edges[0].source_columns,
            Some(vec!["user_id".to_string()])
        );
    }

    #[test]
    fn seed_from_fks() {
        let db = test_db();
        let fks = vec![
            crate::db::ForeignKeyInfo {
                constraint_name: "FK_Orders_Users".to_string(),
                from_schema: "dbo".to_string(),
                from_table: "Orders".to_string(),
                from_columns: vec!["user_id".to_string()],
                to_schema: "dbo".to_string(),
                to_table: "Users".to_string(),
                to_columns: vec!["id".to_string()],
            },
            crate::db::ForeignKeyInfo {
                constraint_name: "FK_OrderItems_Orders".to_string(),
                from_schema: "dbo".to_string(),
                from_table: "OrderItems".to_string(),
                from_columns: vec!["order_id".to_string()],
                to_schema: "dbo".to_string(),
                to_table: "Orders".to_string(),
                to_columns: vec!["id".to_string()],
            },
        ];

        let count = db.seed_graph_from_fks("mssql", "ACEDW", &fks).unwrap();
        assert_eq!(count, 2);

        // Should have created 3 nodes (Users, Orders, OrderItems)
        let nodes = db.list_graph_nodes(None).unwrap();
        assert_eq!(nodes.len(), 3);

        // Should have 2 edges
        let edges = db.list_graph_edges(None).unwrap();
        assert_eq!(edges.len(), 2);

        // Re-seeding is idempotent
        let count2 = db.seed_graph_from_fks("mssql", "ACEDW", &fks).unwrap();
        assert_eq!(count2, 2); // still returns 2 (or_ignore succeeds)
        assert_eq!(db.list_graph_edges(None).unwrap().len(), 2);
    }

    #[test]
    fn traverse_nonexistent_node() {
        let db = test_db();
        let result = db.graph_traverse(999, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn delete_edges_by_type() {
        let db = test_db();
        let a = db
            .upsert_graph_node("conn", "db", "dbo", "A", "table", None)
            .unwrap();
        let b = db
            .upsert_graph_node("conn", "db", "dbo", "B", "table", None)
            .unwrap();
        let c = db
            .upsert_graph_node("conn", "db", "dbo", "C", "table", None)
            .unwrap();
        db.create_graph_edge(a, b, "join_key", None, None, None, None)
            .unwrap();
        db.create_graph_edge(a, c, "derives_from", None, None, None, None)
            .unwrap();

        let deleted = db.delete_graph_edges_by_type("join_key").unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(db.list_graph_edges(None).unwrap().len(), 1);
        assert_eq!(db.list_graph_edges(None).unwrap()[0].edge_type, "derives_from");
    }

    #[test]
    fn plan_linear_chain() {
        // A -> B -> C, request A + C — B should be included as intermediate
        let db = test_db();
        let a = db.upsert_graph_node("conn", "db", "dbo", "A", "table", None).unwrap();
        let b = db.upsert_graph_node("conn", "db", "dbo", "B", "table", None).unwrap();
        let c = db.upsert_graph_node("conn", "db", "dbo", "C", "table", None).unwrap();
        db.create_graph_edge(a, b, "join_key", Some("id"), Some("a_id"), None, None).unwrap();
        db.create_graph_edge(b, c, "join_key", Some("id"), Some("b_id"), None, None).unwrap();

        let plan = db.find_join_paths(&[a, c]).unwrap();
        assert_eq!(plan.nodes.len(), 3); // A, B, C
        assert_eq!(plan.edges.len(), 2);
        assert_eq!(plan.nodes[0].table_name, "A"); // hub first
    }

    #[test]
    fn plan_two_tables() {
        // A -> B direct edge
        let db = test_db();
        let a = db.upsert_graph_node("conn", "db", "dbo", "A", "table", None).unwrap();
        let b = db.upsert_graph_node("conn", "db", "dbo", "B", "table", None).unwrap();
        db.create_graph_edge(a, b, "join_key", Some("id"), Some("a_id"), None, None).unwrap();

        let plan = db.find_join_paths(&[a, b]).unwrap();
        assert_eq!(plan.nodes.len(), 2);
        assert_eq!(plan.edges.len(), 1);
    }

    #[test]
    fn plan_unreachable() {
        // A -> B, C isolated — requesting A + C should fail
        let db = test_db();
        let a = db.upsert_graph_node("conn", "db", "dbo", "A", "table", None).unwrap();
        let _b = db.upsert_graph_node("conn", "db", "dbo", "B", "table", None).unwrap();
        let c = db.upsert_graph_node("conn", "db", "dbo", "C", "table", None).unwrap();
        db.create_graph_edge(a, _b, "join_key", None, None, None, None).unwrap();

        let result = db.find_join_paths(&[a, c]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No path found"));
    }

    #[test]
    fn plan_single_table() {
        let db = test_db();
        let a = db.upsert_graph_node("conn", "db", "dbo", "A", "table", None).unwrap();

        let plan = db.find_join_paths(&[a]).unwrap();
        assert_eq!(plan.nodes.len(), 1);
        assert_eq!(plan.edges.len(), 0);
    }
}
