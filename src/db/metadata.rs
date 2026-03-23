use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

use super::{ConnectionRegistry, DatabaseBackend, Dialect, ForeignKeyInfo};

/// Connection metadata returned by list_connections.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionMeta {
    pub name: String,
    pub is_default: bool,
    #[serde(rename = "type")]
    pub connection_type: &'static str,
    pub default_database: String,
    pub status: String,
    pub status_message: Option<String>,
}

/// List all registered connections with metadata.
pub fn list_connections(registry: &ConnectionRegistry) -> Vec<ConnectionMeta> {
    registry
        .list_connections()
        .into_iter()
        .map(|info| ConnectionMeta {
            name: info.name,
            is_default: info.is_default,
            connection_type: match info.dialect {
                Dialect::Mssql => "mssql",
                Dialect::Postgres => "postgres",
                Dialect::DuckDb => "duckdb",
                Dialect::ClickHouse => "clickhouse",
            },
            default_database: info.default_database,
            status: info.status,
            status_message: info.status_message,
        })
        .collect()
}

/// List all databases on a connection.
pub async fn list_databases(
    db: &dyn DatabaseBackend,
) -> Result<Vec<HashMap<String, Value>>> {
    db.list_databases().await
}

/// List schemas in a database.
pub async fn list_schemas(
    db: &dyn DatabaseBackend,
    database: &str,
) -> Result<Vec<HashMap<String, Value>>> {
    db.list_schemas(database).await
}

/// List tables in a schema.
pub async fn list_tables(
    db: &dyn DatabaseBackend,
    database: &str,
    schema: &str,
) -> Result<Vec<HashMap<String, Value>>> {
    db.list_tables(database, schema).await
}

/// Describe a table's columns.
pub async fn describe_table(
    db: &dyn DatabaseBackend,
    database: &str,
    table: &str,
    schema: &str,
) -> Result<Vec<HashMap<String, Value>>> {
    db.describe_table(database, table, schema).await
}

/// Get foreign key relationships for a table.
pub async fn get_foreign_keys(
    db: &dyn DatabaseBackend,
    database: &str,
    table: &str,
    schema: &str,
) -> Result<Vec<ForeignKeyInfo>> {
    db.get_foreign_keys(database, table, schema).await
}

/// List views in a database schema.
pub async fn list_views(
    db: &dyn DatabaseBackend,
    database: &str,
    schema: &str,
) -> Result<Vec<HashMap<String, Value>>> {
    db.list_views(database, schema).await
}

/// List stored procedures and functions in a database schema.
pub async fn list_routines(
    db: &dyn DatabaseBackend,
    database: &str,
    schema: &str,
) -> Result<Vec<HashMap<String, Value>>> {
    db.list_routines(database, schema).await
}

/// Get the SQL definition of a view, procedure, or function.
pub async fn get_object_definition(
    db: &dyn DatabaseBackend,
    database: &str,
    schema: &str,
    name: &str,
    object_type: &str,
) -> Result<Option<HashMap<String, Value>>> {
    db.get_object_definition(database, schema, name, object_type).await
}

/// List triggers defined on a table.
pub async fn list_triggers(
    db: &dyn DatabaseBackend,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<Vec<HashMap<String, Value>>> {
    db.list_triggers(database, schema, table).await
}

/// Get the SQL definition of a trigger.
pub async fn get_trigger_definition(
    db: &dyn DatabaseBackend,
    database: &str,
    schema: &str,
    name: &str,
) -> Result<Option<HashMap<String, Value>>> {
    db.get_trigger_definition(database, schema, name).await
}

/// List views, procedures, and functions that reference a table.
pub async fn get_related_objects(
    db: &dyn DatabaseBackend,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<Vec<HashMap<String, Value>>> {
    db.get_related_objects(database, schema, table).await
}

/// List Row-Level Security policies on a table.
pub async fn list_rls_policies(
    db: &dyn DatabaseBackend,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<Vec<HashMap<String, Value>>> {
    db.list_rls_policies(database, schema, table).await
}

/// Get RLS status for a table.
pub async fn get_rls_status(
    db: &dyn DatabaseBackend,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<Option<HashMap<String, Value>>> {
    db.get_rls_status(database, schema, table).await
}

/// Generate SQL for an RLS action.
pub async fn generate_rls_sql(
    db: &dyn DatabaseBackend,
    database: &str,
    schema: &str,
    table: &str,
    action: &str,
    params: &HashMap<String, String>,
) -> Result<String> {
    db.generate_rls_sql(database, schema, table, action, params).await
}
