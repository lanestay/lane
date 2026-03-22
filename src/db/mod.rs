pub mod metadata;

#[cfg(feature = "mssql")]
pub mod mssql;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "duckdb_backend")]
pub mod duckdb_backend;

#[cfg(feature = "clickhouse_backend")]
pub mod clickhouse_backend;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::Serialize;

use crate::query::{QueryParams, QueryResult};

/// Type alias for a streaming chunk: either a Bytes payload or an IO error.
pub type StreamChunk = Result<bytes::Bytes, std::io::Error>;

/// Database dialect — used for dialect-aware SQL generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum Dialect {
    Mssql,
    Postgres,
    DuckDb,
    ClickHouse,
}

/// Live connection status.
#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    Connected,
    Error(String),
    Unknown,
}

impl ConnectionStatus {
    pub fn as_str(&self) -> &str {
        match self {
            ConnectionStatus::Connected => "connected",
            ConnectionStatus::Error(_) => "error",
            ConnectionStatus::Unknown => "unknown",
        }
    }

    pub fn message(&self) -> Option<&str> {
        match self {
            ConnectionStatus::Error(msg) => Some(msg.as_str()),
            _ => None,
        }
    }
}

/// Connection pool statistics.
#[derive(Debug, Clone, Serialize)]
pub struct PoolStats {
    pub total_connections: u32,
    pub idle_connections: u32,
    pub active_connections: u32,
    pub max_size: u32,
}

/// Foreign key relationship info for cross-table navigation.
#[derive(Debug, Clone, Serialize)]
pub struct ForeignKeyInfo {
    pub constraint_name: String,
    pub from_schema: String,
    pub from_table: String,
    pub from_columns: Vec<String>,
    pub to_schema: String,
    pub to_table: String,
    pub to_columns: Vec<String>,
}

/// Abstraction over database backends (MSSQL, Postgres, etc.)
#[async_trait]
pub trait DatabaseBackend: Send + Sync {
    /// Execute a query and return structured results
    async fn execute_query(&self, params: &QueryParams) -> Result<QueryResult>;

    /// Validate a query without executing (dry run)
    async fn validate_query(&self, database: &str, query: &str) -> Result<(), String>;

    /// List all databases accessible to this connection
    async fn list_databases(&self) -> Result<Vec<HashMap<String, Value>>>;

    /// List schemas in a database
    async fn list_schemas(&self, database: &str) -> Result<Vec<HashMap<String, Value>>>;

    /// List tables in a schema
    async fn list_tables(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>>;

    /// Describe a table's columns
    async fn describe_table(
        &self,
        database: &str,
        table: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>>;

    /// Execute a query and stream rows as NDJSON chunks through the channel.
    async fn execute_query_streaming(
        &self,
        _params: &QueryParams,
        _tx: tokio::sync::mpsc::Sender<StreamChunk>,
    ) -> Result<()> {
        anyhow::bail!("Streaming not supported by this backend")
    }

    /// Return the SQL dialect this backend uses.
    fn dialect(&self) -> Dialect {
        Dialect::Mssql
    }

    /// Return the default database for this connection.
    fn default_database(&self) -> &str;

    /// Health check — verifies the connection is alive.
    async fn health_check(&self) -> Result<()> {
        self.list_databases().await.map(|_| ())
    }

    /// List currently running queries on this connection.
    async fn list_active_queries(&self) -> Result<Vec<HashMap<String, Value>>> {
        anyhow::bail!("list_active_queries not supported by this backend")
    }

    /// Kill a running query by its process/session ID.
    async fn kill_query(&self, _process_id: i64) -> Result<()> {
        anyhow::bail!("kill_query not supported by this backend")
    }

    /// Return pool utilization statistics.
    fn pool_stats(&self) -> Option<PoolStats> {
        None
    }

    /// Get foreign key relationships for a table (both outgoing and incoming).
    async fn get_foreign_keys(
        &self,
        _database: &str,
        _table: &str,
        _schema: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        Ok(Vec::new())
    }

    /// List views in a database schema.
    async fn list_views(
        &self,
        _database: &str,
        _schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        Ok(Vec::new())
    }

    /// List stored procedures and functions in a database schema.
    async fn list_routines(
        &self,
        _database: &str,
        _schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        Ok(Vec::new())
    }

    /// Get the SQL definition of a view, procedure, or function.
    async fn get_object_definition(
        &self,
        _database: &str,
        _schema: &str,
        _name: &str,
        _object_type: &str,
    ) -> Result<Option<HashMap<String, Value>>> {
        Ok(None)
    }

    /// List triggers defined on a table.
    async fn list_triggers(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        Ok(Vec::new())
    }

    /// Get the SQL definition of a trigger.
    async fn get_trigger_definition(
        &self,
        _database: &str,
        _schema: &str,
        _name: &str,
    ) -> Result<Option<HashMap<String, Value>>> {
        Ok(None)
    }

    /// List views, procedures, and functions that reference a table.
    async fn get_related_objects(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        Ok(Vec::new())
    }

    /// List Row-Level Security policies on a table.
    async fn list_rls_policies(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        Ok(Vec::new())
    }

    /// Get RLS status for a table (enabled, forced, policy count).
    async fn get_rls_status(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
    ) -> Result<Option<HashMap<String, Value>>> {
        Ok(None)
    }

    /// Generate SQL for an RLS action (enable/disable, create/drop policy).
    async fn generate_rls_sql(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
        _action: &str,
        _params: &HashMap<String, String>,
    ) -> Result<String> {
        anyhow::bail!("RLS not supported by this backend")
    }
}

// ============================================================================
// Connection Registry
// ============================================================================

/// Registry of named database connections (thread-safe, interior mutability).
pub struct ConnectionRegistry {
    connections: RwLock<HashMap<String, Arc<dyn DatabaseBackend>>>,
    status: RwLock<HashMap<String, ConnectionStatus>>,
    default_name: RwLock<String>,
}

impl ConnectionRegistry {
    pub fn new(default_name: String) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            status: RwLock::new(HashMap::new()),
            default_name: RwLock::new(default_name),
        }
    }

    /// Register a named connection.
    pub fn register(&self, name: String, backend: Arc<dyn DatabaseBackend>) {
        self.status
            .write()
            .unwrap()
            .insert(name.clone(), ConnectionStatus::Unknown);
        self.connections.write().unwrap().insert(name, backend);
    }

    /// Remove a named connection and its status.
    pub fn remove(&self, name: &str) {
        self.connections.write().unwrap().remove(name);
        self.status.write().unwrap().remove(name);
    }

    /// Get a connection by name.
    #[allow(dead_code)]
    pub fn get(&self, name: &str) -> Option<Arc<dyn DatabaseBackend>> {
        self.connections.read().unwrap().get(name).cloned()
    }

    /// Get the default connection.
    #[allow(dead_code)]
    pub fn get_default(&self) -> Option<Arc<dyn DatabaseBackend>> {
        let default = self.default_name.read().unwrap().clone();
        self.connections.read().unwrap().get(&default).cloned()
    }

    /// Get a connection by name, falling back to the default if name is None.
    /// Returns an error if the connection is not found.
    pub fn resolve(&self, name: Option<&str>) -> Result<Arc<dyn DatabaseBackend>> {
        let default = self.default_name.read().unwrap().clone();
        let key = name.unwrap_or(&default);
        let conns = self.connections.read().unwrap();
        conns
            .get(key)
            .cloned()
            .ok_or_else(|| {
                let available: Vec<&str> = conns.keys().map(|s| s.as_str()).collect();
                anyhow::anyhow!(
                    "Connection '{}' not found. Available: [{}]",
                    key,
                    available.join(", ")
                )
            })
    }

    /// List all connection names.
    pub fn list_connections(&self) -> Vec<ConnectionInfo> {
        let conns = self.connections.read().unwrap();
        let statuses = self.status.read().unwrap();
        let default = self.default_name.read().unwrap().clone();
        conns
            .keys()
            .map(|name| {
                let backend = &conns[name];
                let st = statuses.get(name).cloned().unwrap_or(ConnectionStatus::Unknown);
                ConnectionInfo {
                    name: name.clone(),
                    is_default: *name == default,
                    dialect: backend.dialect(),
                    default_database: backend.default_database().to_string(),
                    status: st.as_str().to_string(),
                    status_message: st.message().map(|s| s.to_string()),
                }
            })
            .collect()
    }

    /// The name of the default connection.
    #[allow(dead_code)]
    pub fn default_name(&self) -> String {
        self.default_name.read().unwrap().clone()
    }

    /// Update the default connection name.
    pub fn set_default(&self, name: String) {
        *self.default_name.write().unwrap() = name;
    }

    /// Set status for a connection.
    pub fn set_status(&self, name: &str, st: ConnectionStatus) {
        self.status
            .write()
            .unwrap()
            .insert(name.to_string(), st);
    }

    /// Get status for a connection.
    #[allow(dead_code)]
    pub fn get_status(&self, name: &str) -> ConnectionStatus {
        self.status
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .unwrap_or(ConnectionStatus::Unknown)
    }

    /// Get all connection names (for health check iteration).
    pub fn connection_names(&self) -> Vec<String> {
        self.connections.read().unwrap().keys().cloned().collect()
    }

    /// Get pool stats for all connections.
    pub fn pool_stats_all(&self) -> HashMap<String, Option<PoolStats>> {
        let conns = self.connections.read().unwrap();
        conns
            .iter()
            .map(|(name, backend)| (name.clone(), backend.pool_stats()))
            .collect()
    }
}

/// Info about a registered connection.
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub name: String,
    pub is_default: bool,
    pub dialect: Dialect,
    pub default_database: String,
    pub status: String,
    pub status_message: Option<String>,
}
