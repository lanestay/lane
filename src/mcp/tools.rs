use std::sync::Arc;
use std::time::Duration;

use rmcp::{schemars, tool, tool_router};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use serde::Deserialize;
use serde_json::json;

use crate::api::FileCache;
use crate::api::approvals::{ApprovalDecision, ApprovalRegistry, PendingApproval};
use crate::auth::access_control::{AccessControlDb, SqlMode};
use crate::db::ConnectionRegistry;
#[cfg(feature = "duckdb_backend")]
use crate::db::DatabaseBackend as _;
use crate::db::metadata;
#[cfg(feature = "duckdb_backend")]
use crate::import::{InferredColumn, InferredType, sql_gen};
use crate::query::QueryParams;
use crate::query::validation::{is_ddl_query, is_read_only_safe, wrap_exec_sql};

/// Detect write/destructive SQL statements (inverse of read-only safe).
fn is_write_query(query: &str) -> bool {
    !is_read_only_safe(query)
}

/// Detect read-only SQL statements.
fn is_read_query(query: &str) -> bool {
    is_read_only_safe(query)
}

// ============================================================================
// Tool Parameter Structs
// ============================================================================

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExecuteSqlReadParams {
    #[schemars(description = "Database name. Uses server default if omitted.")]
    pub database: Option<String>,

    #[schemars(description = "SQL query to execute (SELECT, WITH, sp_help, sp_columns only)")]
    pub query: String,

    #[schemars(
        description = "PII handling mode: none=raw data, scrub=replace with <type> placeholder"
    )]
    pub pii_mode: Option<String>,

    #[schemars(description = "Enable pagination for large result sets (requires ORDER BY)")]
    pub pagination: Option<bool>,

    #[schemars(description = "Include column metadata (name, type) in response")]
    pub include_metadata: Option<bool>,

    #[schemars(
        description = "Output format. Set to \"xlsx\" to export results as an Excel file and return a single-use download URL instead of inline data. Download link expires after 5 minutes."
    )]
    #[serde(rename = "outputFormat")]
    pub output_format: Option<String>,

    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExecuteSqlWriteParams {
    #[schemars(description = "Database name. Uses server default if omitted.")]
    pub database: Option<String>,

    #[schemars(description = "SQL query to execute (any valid T-SQL including INSERT, UPDATE, DELETE, CREATE, etc.)")]
    pub query: String,

    #[schemars(
        description = "PII handling mode: none=raw data, scrub=replace with <type> placeholder"
    )]
    pub pii_mode: Option<String>,

    #[schemars(
        description = "Wrap query in EXEC sp_executesql for DDL execution (CREATE PROCEDURE, ALTER, DROP). Auto-escapes quotes."
    )]
    pub exec_sql: Option<bool>,

    #[schemars(
        description = "Skip SQL validation before execution. Required for DDL statements that fail PARSEONLY validation."
    )]
    pub skip_validation: Option<bool>,

    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExecuteSqlDryRunParams {
    #[schemars(description = "Database name")]
    pub database: String,

    #[schemars(description = "SQL query to validate (will NOT be executed)")]
    pub query: String,

    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListTablesParams {
    #[schemars(description = "Database name")]
    pub database: String,

    #[schemars(description = "Schema name (defaults to 'dbo')")]
    pub schema: Option<String>,

    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DescribeTableParams {
    #[schemars(description = "Database name")]
    pub database: String,

    #[schemars(description = "Table name")]
    pub table: String,

    #[schemars(description = "Schema name (defaults to 'dbo')")]
    pub schema: Option<String>,

    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetObjectDefinitionParams {
    #[schemars(description = "Database name")]
    pub database: String,

    #[schemars(description = "Object name (view, procedure, or function)")]
    pub name: String,

    #[schemars(description = "Object type: 'view', 'materialized_view', 'procedure', or 'function'")]
    pub object_type: String,

    #[schemars(description = "Schema name (defaults to 'dbo' for MSSQL, 'public' for Postgres)")]
    pub schema: Option<String>,

    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TableObjectParams {
    #[schemars(description = "Database name")]
    pub database: String,

    #[schemars(description = "Table name")]
    pub table: String,

    #[schemars(description = "Schema name (defaults to 'dbo' for MSSQL, 'public' for Postgres)")]
    pub schema: Option<String>,

    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TriggerDefinitionParams {
    #[schemars(description = "Database name")]
    pub database: String,

    #[schemars(description = "Trigger name")]
    pub name: String,

    #[schemars(description = "Schema name (defaults to 'dbo' for MSSQL, 'public' for Postgres)")]
    pub schema: Option<String>,

    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GenerateRlsSqlParams {
    #[schemars(description = "Database name")]
    pub database: String,

    #[schemars(description = "Table name")]
    pub table: String,

    #[schemars(description = "RLS action: Postgres: 'enable_rls', 'disable_rls', 'force_rls', 'no_force_rls', 'create_policy', 'drop_policy'. MSSQL: 'enable_policy', 'disable_policy', 'create_policy', 'drop_policy'.")]
    pub action: String,

    #[schemars(description = "Policy name (required for create/drop/enable/disable policy)")]
    pub policy_name: Option<String>,

    #[schemars(description = "SQL command the policy applies to: ALL, SELECT, INSERT, UPDATE, DELETE (Postgres only)")]
    pub command: Option<String>,

    #[schemars(description = "Whether the policy is permissive ('true') or restrictive ('false'). Defaults to 'true'. (Postgres only)")]
    pub permissive: Option<String>,

    #[schemars(description = "Comma-separated list of roles the policy applies to (Postgres only, defaults to PUBLIC)")]
    pub roles: Option<String>,

    #[schemars(description = "USING expression for the policy (Postgres only)")]
    pub using_expr: Option<String>,

    #[schemars(description = "WITH CHECK expression for the policy (Postgres only)")]
    pub with_check_expr: Option<String>,

    #[schemars(description = "Predicate type: FILTER or BLOCK (MSSQL only)")]
    pub predicate_type: Option<String>,

    #[schemars(description = "Predicate function name (MSSQL only)")]
    pub predicate_function: Option<String>,

    #[schemars(description = "Predicate function arguments (MSSQL only)")]
    pub predicate_args: Option<String>,

    #[schemars(description = "Schema name (defaults to 'dbo' for MSSQL, 'public' for Postgres)")]
    pub schema: Option<String>,

    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct NavigateTablesParams {
    #[schemars(description = "Database name")]
    pub database: String,

    #[schemars(description = "Table name to navigate from")]
    pub table: String,

    #[schemars(description = "Schema name (defaults to 'dbo' for MSSQL, 'public' for Postgres)")]
    pub schema: Option<String>,

    #[schemars(description = "WHERE clause to filter source rows (e.g. \"id = 42\"). Omit to get relationship structure only without data.")]
    pub filter: Option<String>,

    #[schemars(description = "Max rows per related table (default 10, max 100)")]
    pub row_limit: Option<i64>,

    #[schemars(description = "FK direction: 'both' (default), 'outgoing' (this table references), or 'incoming' (references this table)")]
    pub direction: Option<String>,

    #[schemars(description = "PII handling mode: none | scrub")]
    pub pii_mode: Option<String>,

    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListDatabasesParams {
    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,
}

#[cfg_attr(not(feature = "duckdb_backend"), allow(dead_code))]
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WorkspaceImportQueryParams {
    #[schemars(description = "Named connection to execute the source query against")]
    pub connection: Option<String>,

    #[schemars(description = "Database name on the source connection")]
    pub database: Option<String>,

    #[schemars(description = "SQL query to execute on the source connection. Results will be imported into the workspace.")]
    pub query: String,

    #[schemars(description = "Name for the destination table in the workspace. Must be a valid SQL identifier.")]
    pub table_name: String,

    #[schemars(description = "What to do if the table already exists: 'replace' (drop and recreate) or 'fail' (return error). Defaults to 'fail'.")]
    pub if_exists: Option<String>,
}

#[cfg_attr(not(feature = "duckdb_backend"), allow(dead_code))]
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WorkspaceQueryParams {
    #[schemars(description = "SQL query to execute against the workspace DuckDB. Can query any imported tables.")]
    pub query: String,

    #[schemars(description = "Include column metadata (name, type) in response")]
    pub include_metadata: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WorkspaceListTablesParams {}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WorkspaceClearParams {}

#[cfg_attr(not(feature = "duckdb_backend"), allow(dead_code))]
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WorkspaceExportToTableParams {
    #[schemars(description = "DuckDB SQL query to select data from the workspace")]
    pub source_query: String,

    #[schemars(description = "Named connection to use. Uses default connection if omitted.")]
    pub connection: Option<String>,

    #[schemars(description = "Database name")]
    pub database: String,

    #[schemars(description = "Target table name")]
    pub table: String,

    #[schemars(description = "Schema name (defaults to 'dbo' for MSSQL, 'public' for Postgres)")]
    pub schema: Option<String>,

    #[schemars(description = "What to do if the table already exists: 'replace' (drop and recreate), 'append' (insert into existing), or 'fail' (return error). Defaults to 'fail'.")]
    pub if_exists: Option<String>,
}

// Storage tool params
#[cfg_attr(not(feature = "storage"), allow(dead_code))]
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StorageListBucketsParams {
    #[schemars(description = "Named storage connection to use. Uses first available if omitted.")]
    pub connection: Option<String>,
}

#[cfg_attr(not(feature = "storage"), allow(dead_code))]
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StorageListObjectsParams {
    #[schemars(description = "Named storage connection to use. Uses first available if omitted.")]
    pub connection: Option<String>,

    #[schemars(description = "Bucket name")]
    pub bucket: String,

    #[schemars(description = "Prefix to filter objects (e.g. 'folder/' to list folder contents)")]
    pub prefix: Option<String>,
}

#[cfg_attr(not(feature = "storage"), allow(dead_code))]
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StorageUploadParams {
    #[schemars(description = "Named storage connection to use. Uses first available if omitted.")]
    pub connection: Option<String>,

    #[schemars(description = "Bucket name")]
    pub bucket: String,

    #[schemars(description = "Object key (path) to upload to")]
    pub key: String,

    #[schemars(description = "Content to upload. Prefix with 'base64:' for binary content, otherwise treated as UTF-8 text.")]
    pub content: String,

    #[schemars(description = "Content-Type header (e.g. 'text/plain', 'application/json'). Auto-detected if omitted.")]
    pub content_type: Option<String>,
}

#[cfg_attr(any(not(feature = "storage"), not(feature = "duckdb_backend")), allow(dead_code))]
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StorageDownloadToWorkspaceParams {
    #[schemars(description = "Named storage connection to use. Uses first available if omitted.")]
    pub connection: Option<String>,

    #[schemars(description = "Bucket name")]
    pub bucket: String,

    #[schemars(description = "Object key (path) to download")]
    pub key: String,

    #[schemars(description = "Table name in workspace. Auto-generated from filename if omitted.")]
    pub table_name: Option<String>,
}

#[cfg_attr(not(feature = "storage"), allow(dead_code))]
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StorageGetUrlParams {
    #[schemars(description = "Named storage connection to use. Uses first available if omitted.")]
    pub connection: Option<String>,

    #[schemars(description = "Bucket name")]
    pub bucket: String,

    #[schemars(description = "Object key (path)")]
    pub key: String,

    #[schemars(description = "URL expiry in seconds (default: 3600)")]
    pub expiry_secs: Option<u32>,
}

#[cfg_attr(not(feature = "storage"), allow(dead_code))]
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StorageExportQueryParams {
    #[schemars(description = "Named database connection to query. Uses default if omitted.")]
    pub connection: Option<String>,

    #[schemars(description = "Database name. Uses server default if omitted.")]
    pub database: Option<String>,

    #[schemars(description = "SQL query to execute (SELECT only)")]
    pub query: String,

    #[schemars(description = "Named storage connection to upload to. Uses first available if omitted.")]
    pub storage_connection: Option<String>,

    #[schemars(description = "Bucket to upload to")]
    pub bucket: String,

    #[schemars(description = "Object key (path) for the exported file, e.g. 'exports/results.csv'")]
    pub key: String,

    #[schemars(description = "Export format: csv, json, xlsx. Auto-detected from key extension if omitted.")]
    pub format: Option<String>,
}

#[cfg_attr(any(not(feature = "storage"), not(feature = "duckdb_backend")), allow(dead_code))]
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WorkspaceExportToStorageParams {
    #[schemars(description = "SQL query to execute against the workspace")]
    pub query: String,

    #[schemars(description = "Named storage connection to upload to. Uses first available if omitted.")]
    pub storage_connection: Option<String>,

    #[schemars(description = "Bucket to upload to")]
    pub bucket: String,

    #[schemars(description = "Object key (path) for the exported file, e.g. 'exports/workspace_data.parquet'")]
    pub key: String,

    #[schemars(description = "Export format: csv, json, parquet. Auto-detected from key extension if omitted.")]
    pub format: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExecuteEndpointParams {
    #[schemars(description = "Name of the saved endpoint to execute")]
    pub name: String,

    #[schemars(description = "Parameters to pass to the endpoint as a JSON object (e.g. {\"region\": \"US\", \"min_year\": \"2020\"})")]
    pub parameters: Option<std::collections::HashMap<String, String>>,
}

// ============================================================================
// User Context for per-token MCP auth
// ============================================================================

#[derive(Clone)]
pub struct UserContext {
    pub email: String,
    pub token_prefix: String,
    pub access_db: Arc<AccessControlDb>,
    pub pii_mode: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchSchemaParams {
    #[schemars(description = "Search query (e.g. table name, column name, keyword)")]
    pub query: String,

    #[schemars(description = "Max results to return (default: 10)")]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchQueryParams {
    #[schemars(description = "Search query (e.g. SQL keyword, table name)")]
    pub query: String,

    #[schemars(description = "Max results to return (default: 10)")]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchEndpointParams {
    #[schemars(description = "Search query (e.g. endpoint name, description keyword)")]
    pub query: String,

    #[schemars(description = "Max results to return (default: 10)")]
    pub limit: Option<usize>,
}

// ============================================================================
// MCP Server Struct
// ============================================================================

#[derive(Clone)]
pub struct BatchQueryMcp {
    pub tool_router: ToolRouter<Self>,
    pub registry: Arc<ConnectionRegistry>,
    pub user_context: Option<UserContext>,
    pub approval_registry: Option<Arc<ApprovalRegistry>>,
    pub downloads: Option<FileCache>,
    pub realtime_tx: Option<tokio::sync::broadcast::Sender<crate::api::realtime::RealtimeEvent>>,
    #[cfg(feature = "duckdb_backend")]
    pub workspace_db: Option<Arc<crate::db::duckdb_backend::DuckDbBackend>>,
    #[cfg(feature = "duckdb_backend")]
    #[cfg_attr(not(feature = "storage"), allow(dead_code))]
    pub workspace_dir: Option<std::path::PathBuf>,
    #[cfg(feature = "storage")]
    pub storage_registry: Option<Arc<crate::storage::StorageRegistry>>,
    pub search_db: Option<Arc<crate::search::db::SearchDb>>,
}

impl BatchQueryMcp {
    pub fn new(
        registry: Arc<ConnectionRegistry>,
        user_context: Option<UserContext>,
        approval_registry: Option<Arc<ApprovalRegistry>>,
        downloads: Option<FileCache>,
        realtime_tx: Option<tokio::sync::broadcast::Sender<crate::api::realtime::RealtimeEvent>>,
        #[cfg(feature = "duckdb_backend")] workspace_db: Option<Arc<crate::db::duckdb_backend::DuckDbBackend>>,
        #[cfg(feature = "duckdb_backend")] workspace_dir: Option<std::path::PathBuf>,
        #[cfg(feature = "storage")] storage_registry: Option<Arc<crate::storage::StorageRegistry>>,
        search_db: Option<Arc<crate::search::db::SearchDb>>,
    ) -> Self {
        Self {
            tool_router: Self::tool_router(),
            registry,
            user_context,
            approval_registry,
            downloads,
            realtime_tx,
            #[cfg(feature = "duckdb_backend")]
            workspace_db,
            #[cfg(feature = "duckdb_backend")]
            workspace_dir,
            #[cfg(feature = "storage")]
            storage_registry,
            search_db,
        }
    }

    /// Check if the current user has permission for the given database and action.
    /// Returns Ok(()) if allowed, Err(json_string) if denied.
    fn check_permission(&self, database: &str, is_write: bool) -> Result<(), String> {
        let ctx = match &self.user_context {
            None => return Ok(()), // System access — no restrictions
            Some(ctx) => ctx,
        };
        // Check if MCP access is enabled for this user
        if !ctx.access_db.is_mcp_enabled(&ctx.email) {
            return Err(json!({
                "error": true,
                "message": "MCP access is disabled for your account"
            }).to_string());
        }
        if !ctx.access_db.check_permission(&ctx.email, database, is_write) {
            let action = if is_write { "write" } else { "read" };
            ctx.access_db.log_access(
                Some(&ctx.token_prefix),
                Some(&ctx.email),
                None,
                Some(database),
                Some(action),
                "denied",
                Some(&format!("MCP: no {} permission for '{}'", action, database)),
            );
            return Err(json!({
                "error": true,
                "message": format!("Permission denied: no {} access to database '{}'", action, database)
            }).to_string());
        }
        let action = if is_write { "write" } else { "read" };
        ctx.access_db.log_access(
            Some(&ctx.token_prefix),
            Some(&ctx.email),
            None,
            Some(database),
            Some(action),
            "allowed",
            Some("MCP tool call"),
        );
        Ok(())
    }

    /// Resolve a connection from the registry, returning a JSON error string on failure.
    /// Also checks connection-level access permissions.
    fn resolve_connection(&self, name: Option<&str>) -> Result<Arc<dyn crate::db::DatabaseBackend>, String> {
        // Check connection access before resolving
        if let Some(ref ctx) = self.user_context {
            let conn_name = name.unwrap_or_else(|| {
                // Leak a string to get a &str from default_name() - safe because it's a short-lived reference
                // We just need the name for the access check
                ""
            });
            if !conn_name.is_empty() && !ctx.access_db.check_connection_access(&ctx.email, conn_name) {
                return Err(json!({
                    "error": true,
                    "message": format!("Access denied to connection '{}'", conn_name)
                }).to_string());
            }
        }

        let backend = self.registry.resolve(name).map_err(|e| {
            json!({
                "error": true,
                "message": format!("{}", e)
            }).to_string()
        })?;

        // If no explicit name was given, check access to the default connection
        if name.is_none() {
            if let Some(ref ctx) = self.user_context {
                let default_name = self.registry.default_name();
                if !ctx.access_db.check_connection_access(&ctx.email, &default_name) {
                    return Err(json!({
                        "error": true,
                        "message": format!("Access denied to connection '{}'", default_name)
                    }).to_string());
                }
            }
        }

        Ok(backend)
    }

    /// Resolve a storage connection from the registry, returning `(name, client)`.
    /// Also checks connection-level access if user context is present.
    #[cfg(feature = "storage")]
    async fn resolve_storage(&self, connection: Option<&str>) -> Result<(String, Arc<crate::storage::StorageClient>), String> {
        let reg = match &self.storage_registry {
            Some(r) => r,
            None => return Err(json!({"error": true, "message": "Storage is not configured on this server"}).to_string()),
        };
        let result = if let Some(name) = connection {
            match reg.get(name).await {
                Some(client) => Ok((name.to_string(), client)),
                None => Err(json!({"error": true, "message": format!("Storage connection '{}' not found", name)}).to_string()),
            }
        } else {
            let names = reg.list_names().await;
            if let Some(name) = names.first() {
                match reg.get(name).await {
                    Some(client) => Ok((name.clone(), client)),
                    None => Err(json!({"error": true, "message": "No storage connections available"}).to_string()),
                }
            } else {
                Err(json!({"error": true, "message": "No storage connections configured"}).to_string())
            }
        };

        // Check connection-level access
        if let Ok((ref name, _)) = result {
            if let Some(ref ctx) = self.user_context {
                if !ctx.access_db.check_connection_access(&ctx.email, name) {
                    return Err(json!({
                        "error": true,
                        "message": format!("Access denied to storage connection '{}'", name)
                    }).to_string());
                }
            }
        }

        result
    }

    /// Check bucket-level storage permission for the current user.
    #[cfg(feature = "storage")]
    fn check_storage_permission(&self, connection: &str, bucket: &str, action: crate::auth::access_control::StoragePermAction) -> Result<(), String> {
        let ctx = match &self.user_context {
            Some(c) => c,
            None => return Ok(()), // No user context = system access = allow all
        };
        if !ctx.access_db.check_storage_access(&ctx.email, connection, bucket, action) {
            let action_str = match action {
                crate::auth::access_control::StoragePermAction::Read => "read",
                crate::auth::access_control::StoragePermAction::Write => "write",
                crate::auth::access_control::StoragePermAction::Delete => "delete",
            };
            return Err(json!({
                "error": true,
                "message": format!("Storage {} access denied for bucket '{}'", action_str, bucket)
            }).to_string());
        }
        Ok(())
    }

    /// Log a storage operation to the audit log.
    #[cfg(feature = "storage")]
    fn log_storage_op(&self, connection: &str, action: &str, details: &str) {
        if let Some(ref ctx) = self.user_context {
            ctx.access_db.log_access(
                Some(&ctx.token_prefix),
                Some(&ctx.email),
                None,
                Some(connection),
                Some("storage"),
                action,
                Some(details),
            );
        }
    }

    /// Check if the current user's SQL mode allows the given query.
    fn check_sql_mode(&self, query: &str) -> Result<(), String> {
        let ctx = match &self.user_context {
            Some(c) => c,
            None => return Ok(()), // No user context = system/API key = allow all
        };
        let mode = ctx.access_db.get_sql_mode(&ctx.email);
        match mode {
            SqlMode::None => Err(json!({
                "error": true,
                "message": "Raw SQL access is disabled. Use the REST API tools instead."
            }).to_string()),
            SqlMode::ReadOnly => {
                if !is_read_query(query) {
                    Err(json!({
                        "error": true,
                        "message": "Read-only SQL mode. Only SELECT queries are allowed."
                    }).to_string())
                } else {
                    Ok(())
                }
            }
            SqlMode::Supervised | SqlMode::Confirmed => {
                if is_ddl_query(query) {
                    Err(json!({
                        "error": true,
                        "message": format!(
                            "DDL requires human review. Open the SQL Editor to execute this statement.\n\nSQL:\n{}\n\nEditor: /ui?sql={}",
                            query,
                            simple_url_encode(query)
                        )
                    }).to_string())
                } else {
                    Ok(())
                }
            }
            SqlMode::Full => Ok(()),
        }
    }

    /// Check if the current user is in supervised mode and needs approval for writes.
    fn needs_approval(&self) -> bool {
        self.user_context
            .as_ref()
            .map(|ctx| matches!(ctx.access_db.get_sql_mode(&ctx.email), SqlMode::Supervised | SqlMode::Confirmed))
            .unwrap_or(false)
    }

    /// Require at least the given SqlMode level. Returns Err(json_string) if denied.
    /// For write operations to external systems, use SqlMode::Supervised.
    fn require_min_sql_mode(&self, min_mode: SqlMode, context: &str) -> Result<(), String> {
        let ctx = match &self.user_context {
            None => return Ok(()), // System access — no restrictions
            Some(ctx) => ctx,
        };
        let mode = ctx.access_db.get_sql_mode(&ctx.email);
        let level = |m: &SqlMode| match m {
            SqlMode::None => 0,
            SqlMode::ReadOnly => 1,
            SqlMode::Supervised => 2,
            SqlMode::Confirmed => 3,
            SqlMode::Full => 4,
        };
        if level(&mode) < level(&min_mode) {
            return Err(json!({
                "error": true,
                "message": format!(
                    "{} requires at least {:?} SQL access mode. Contact an admin to upgrade your access.",
                    context, min_mode
                )
            }).to_string());
        }
        Ok(())
    }

    /// Queue an operation for human approval and block until decided (5 min timeout).
    async fn await_approval(
        &self,
        tool_name: &str,
        sql_statements: Vec<String>,
        target_connection: &str,
        target_database: &str,
        context: &str,
    ) -> Result<(), String> {
        let registry = self
            .approval_registry
            .as_ref()
            .ok_or("Approval system not available")?;
        let email = self
            .user_context
            .as_ref()
            .ok_or("No user context")?
            .email
            .clone();

        let approval = PendingApproval::new(
            email.clone(),
            tool_name,
            sql_statements.clone(),
            target_connection,
            target_database,
            context,
        );
        let id = approval.id.clone();

        let max_pending = self
            .user_context
            .as_ref()
            .map(|ctx| ctx.access_db.get_max_pending_approvals(&ctx.email))
            .unwrap_or(6);
        let rx = registry.submit(approval, max_pending).await?;

        // Record in audit DB
        if let Some(ref ctx) = self.user_context {
            let sql_preview = sql_statements.first().map(|s| s.as_str());
            let _ = ctx.access_db.record_approval_request(
                &id,
                &ctx.email,
                tool_name,
                target_connection,
                target_database,
                sql_preview,
            );
        }

        // Fire team webhooks (async, non-blocking)
        if let Some(ref ctx) = self.user_context {
            let base_url = std::env::var("LANE_BASE_URL").unwrap_or_default();
            crate::api::approvals::fire_webhooks(
                Some(&ctx.access_db),
                &email,
                tool_name,
                target_connection,
                target_database,
                &base_url,
            );
        }

        tracing::info!(
            tool = tool_name,
            approval_id = %id,
            user = %email,
            "Awaiting human approval"
        );

        // Block waiting for decision (5 min timeout)
        match tokio::time::timeout(Duration::from_secs(300), rx).await {
            Ok(Ok(ApprovalDecision::Approved)) => {
                if let Some(ref ctx) = self.user_context {
                    let _ = ctx
                        .access_db
                        .record_approval_decision(&id, "approved", None);
                }
                tracing::info!(approval_id = %id, "Approval granted");
                Ok(())
            }
            Ok(Ok(ApprovalDecision::Rejected { reason })) => {
                if let Some(ref ctx) = self.user_context {
                    let _ = ctx
                        .access_db
                        .record_approval_decision(&id, "rejected", Some(&reason));
                }
                tracing::info!(approval_id = %id, reason = %reason, "Approval rejected");
                Err(format!("Rejected: {}", reason))
            }
            Ok(Err(_)) => {
                if let Some(ref ctx) = self.user_context {
                    let _ = ctx
                        .access_db
                        .record_approval_decision(&id, "cancelled", None);
                }
                Err("Approval was cancelled".into())
            }
            Err(_) => {
                registry.remove(&id).await;
                if let Some(ref ctx) = self.user_context {
                    let _ = ctx
                        .access_db
                        .record_approval_decision(&id, "timeout", None);
                }
                Err("Approval timed out after 5 minutes".into())
            }
        }
    }
}

fn simple_url_encode(s: &str) -> String {
    let mut result = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(b as char);
            }
            _ => result.push_str(&format!("%{:02X}", b)),
        }
    }
    result
}

// ============================================================================
// Tool Implementations
// ============================================================================

#[tool_router]
impl BatchQueryMcp {
    #[tool(
        description = "Execute a READ-ONLY SQL query (SELECT, WITH, sp_help, sp_columns, etc.). Rejects any write operations. Safe for auto-approval. IMPORTANT: Before querying a table, ALWAYS call describe_table first to understand its schema and column names. Do not guess column names."
    )]
    async fn execute_sql_read(
        &self,
        Parameters(params): Parameters<ExecuteSqlReadParams>,
    ) -> String {
        tracing::info!(tool = "execute_sql_read", ?params, "Tool called");

        // Enforce read-only: reject write queries
        if is_write_query(&params.query) {
            return json!({
                "error": true,
                "message": "Write operations not allowed in execute_sql_read. Use execute_sql_write instead.",
                "blocked_query": &params.query[..params.query.len().min(100)]
            })
            .to_string();
        }

        // Also reject queries that don't look like reads
        if !is_read_query(&params.query) {
            return json!({
                "error": true,
                "message": "Query does not appear to be a read operation. Use execute_sql_write for non-SELECT queries.",
                "blocked_query": &params.query[..params.query.len().min(100)]
            })
            .to_string();
        }

        // SQL mode gate
        if let Err(msg) = self.check_sql_mode(&params.query) {
            return msg;
        }

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        let database = params
            .database
            .unwrap_or_else(|| db.default_database().to_string());

        // Permission check
        if let Err(e) = self.check_permission(&database, false) {
            return e;
        }

        let mut qp = QueryParams {
            database,
            query: params.query,
            pagination: params.pagination.unwrap_or(false),
            include_metadata: params.include_metadata.unwrap_or(false),
            pii_mode: params.pii_mode,
            ..Default::default()
        };

        // Build enriched PII processor using resolution chain
        if let Some(ref ctx) = self.user_context {
            let pii_ctx = crate::query::PiiContext {
                token_pii_mode: ctx.pii_mode.clone(),
                email: Some(ctx.email.clone()),
                is_full_access: false,
            };
            if let Some(processor) = crate::query::build_enriched_pii_processor(
                &qp,
                Some(&ctx.access_db),
                params.connection.as_deref(),
                &pii_ctx,
            ) {
                qp.pii_processor_override = Some(processor);
            }
        }

        match db.execute_query(&qp).await {
            Ok(result) => {
                if params.output_format.as_deref() == Some("xlsx") {
                    #[cfg(feature = "xlsx")]
                    {
                        let downloads = match &self.downloads {
                            Some(d) => d,
                            None => return json!({
                                "error": true,
                                "message": "xlsx export is not available in stdio mode (no HTTP server to serve downloads)"
                            }).to_string(),
                        };
                        match crate::export::xlsx::query_result_to_xlsx(&result) {
                            Ok(bytes) => {
                                let id = uuid::Uuid::new_v4().to_string();
                                let total_rows = result.total_rows;
                                let execution_time_ms = result.execution_time_ms;
                                downloads.write().await.insert(
                                    id.clone(),
                                    crate::api::CachedFile {
                                        bytes,
                                        content_type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".to_string(),
                                        filename: "results.xlsx".to_string(),
                                        created_at: std::time::Instant::now(),
                                    },
                                );
                                json!({
                                    "success": true,
                                    "total_rows": total_rows,
                                    "execution_time_ms": execution_time_ms,
                                    "download_url": format!("/api/lane/download/{}", id),
                                    "note": "Single-use link. Expires after 5 minutes."
                                }).to_string()
                            }
                            Err(e) => json!({"error": format!("xlsx export failed: {:#}", e)}).to_string(),
                        }
                    }
                    #[cfg(not(feature = "xlsx"))]
                    {
                        json!({"error": "xlsx export is not enabled on this server"}).to_string()
                    }
                } else {
                    serde_json::to_string_pretty(&result)
                        .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string())
                }
            }
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "Execute a SQL query that modifies data (INSERT, UPDATE, DELETE, DROP, CREATE PROCEDURE, etc.). DESTRUCTIVE - requires user confirmation. IMPORTANT: Before modifying a table, ALWAYS call describe_table first to understand its schema, column names, and constraints. Do not guess column names or types."
    )]
    async fn execute_sql_write(
        &self,
        Parameters(params): Parameters<ExecuteSqlWriteParams>,
    ) -> String {
        tracing::info!(tool = "execute_sql_write", ?params, "Tool called");

        // For supervised users: route through approval instead of blocking
        if self.needs_approval() {
            let db = match self.resolve_connection(params.connection.as_deref()) {
                Ok(db) => db,
                Err(e) => return e,
            };
            let database = params
                .database
                .clone()
                .unwrap_or_else(|| db.default_database().to_string());
            let conn_name = params
                .connection
                .clone()
                .unwrap_or_else(|| self.registry.default_name());
            let context = format!("Write query on {}.{}", conn_name, database);
            if let Err(msg) = self
                .await_approval(
                    "execute_sql_write",
                    vec![params.query.clone()],
                    &conn_name,
                    &database,
                    &context,
                )
                .await
            {
                return json!({"error": true, "message": msg}).to_string();
            }
            // Approval granted — fall through to execute
        } else {
            // SQL mode gate (blocks None, ReadOnly, and DDL for non-supervised)
            if let Err(msg) = self.check_sql_mode(&params.query) {
                return msg;
            }
        }

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        let database = params
            .database
            .unwrap_or_else(|| db.default_database().to_string());

        // Permission check
        if let Err(e) = self.check_permission(&database, true) {
            return e;
        }

        // Optionally wrap in sp_executesql for DDL execution
        let query = if params.exec_sql.unwrap_or(false) {
            wrap_exec_sql(&params.query)
        } else {
            params.query.clone()
        };

        // Validate unless explicitly skipped (DDL statements fail PARSEONLY)
        if !params.skip_validation.unwrap_or(false) {
            if let Err(e) = db.validate_query(&database, &query).await {
                return json!({
                    "error": true,
                    "message": format!("Validation failed: {}", e),
                    "hint": "Add skip_validation=true for DDL statements that fail PARSEONLY validation."
                })
                .to_string();
            }
        }

        let mut qp = QueryParams {
            database,
            query,
            pii_mode: params.pii_mode,
            ..Default::default()
        };

        // Build enriched PII processor using resolution chain
        if let Some(ref ctx) = self.user_context {
            let pii_ctx = crate::query::PiiContext {
                token_pii_mode: ctx.pii_mode.clone(),
                email: Some(ctx.email.clone()),
                is_full_access: false,
            };
            if let Some(processor) = crate::query::build_enriched_pii_processor(
                &qp,
                Some(&ctx.access_db),
                params.connection.as_deref(),
                &pii_ctx,
            ) {
                qp.pii_processor_override = Some(processor);
            }
        }

        match db.execute_query(&qp).await {
            Ok(result) => {
                // Emit realtime event for successful writes
                if let Some(ref tx) = self.realtime_tx {
                    if let Some(ref ctx) = self.user_context {
                        let conn = params.connection.clone()
                            .unwrap_or_else(|| self.registry.default_name());
                        crate::api::realtime::try_emit_realtime_event_direct(
                            tx,
                            &ctx.access_db,
                            &conn,
                            &qp.database,
                            &params.query,
                            Some(result.total_rows),
                            Some(&ctx.email),
                        );
                    }
                }
                serde_json::to_string_pretty(&result)
                    .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string())
            }
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "Validate a SQL query without executing it. Useful for checking syntax before running complex queries. Returns whether the query is syntactically valid."
    )]
    async fn execute_sql_dry_run(
        &self,
        Parameters(params): Parameters<ExecuteSqlDryRunParams>,
    ) -> String {
        tracing::info!(tool = "execute_sql_dry_run", ?params, "Tool called");

        // SQL mode gate
        if let Err(msg) = self.check_sql_mode(&params.query) {
            return msg;
        }

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        // Permission check (read access needed for validation)
        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        match db.validate_query(&params.database, &params.query).await {
            Ok(()) => json!({
                "valid": true,
                "message": "Query syntax is valid"
            })
            .to_string(),
            Err(e) => json!({
                "valid": false,
                "message": e
            })
            .to_string(),
        }
    }

    #[tool(
        description = "List all tables in a database schema. Returns table names with row counts."
    )]
    async fn list_tables(
        &self,
        Parameters(params): Parameters<ListTablesParams>,
    ) -> String {
        tracing::info!(tool = "list_tables", ?params, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        // Permission check
        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);

        match metadata::list_tables(db.as_ref(), &params.database, schema).await {
            Ok(tables) => {
                // Filter tables by table-level permissions
                let filtered = if let Some(ref ctx) = self.user_context {
                    tables
                        .into_iter()
                        .filter(|row| {
                            let table_name = row
                                .get("TABLE_NAME")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            ctx.access_db.check_table_permission(
                                &ctx.email,
                                &params.database,
                                table_name,
                                false,
                            )
                        })
                        .collect()
                } else {
                    tables
                };
                serde_json::to_string_pretty(&filtered)
                    .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string())
            }
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "Get column information for a specific table including data types, nullability, and primary keys. ALWAYS call this before querying or modifying a table."
    )]
    async fn describe_table(
        &self,
        Parameters(params): Parameters<DescribeTableParams>,
    ) -> String {
        tracing::info!(tool = "describe_table", ?params, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        // Permission check
        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);

        match metadata::describe_table(db.as_ref(), &params.database, &params.table, schema).await {
            Ok(columns) => {
                // Also fetch FK info to include in response
                let fks = metadata::get_foreign_keys(db.as_ref(), &params.database, &params.table, schema)
                    .await
                    .unwrap_or_default();
                let result = json!({
                    "columns": columns,
                    "foreign_keys": fks,
                });
                serde_json::to_string_pretty(&result)
                    .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string())
            }
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "List views in a database schema. Returns view names and types (VIEW, MATERIALIZED VIEW)."
    )]
    async fn list_views(
        &self,
        Parameters(params): Parameters<ListTablesParams>,
    ) -> String {
        tracing::info!(tool = "list_views", ?params, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);

        match metadata::list_views(db.as_ref(), &params.database, schema).await {
            Ok(views) => serde_json::to_string_pretty(&views)
                .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string()),
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "List stored procedures and functions in a database schema. Returns names with routine types (PROCEDURE, FUNCTION, SCALAR_FUNCTION, etc.)."
    )]
    async fn list_routines(
        &self,
        Parameters(params): Parameters<ListTablesParams>,
    ) -> String {
        tracing::info!(tool = "list_routines", ?params, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);

        match metadata::list_routines(db.as_ref(), &params.database, schema).await {
            Ok(routines) => serde_json::to_string_pretty(&routines)
                .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string()),
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "Get the SQL definition of a view, stored procedure, or function. Returns the CREATE statement and parameter info."
    )]
    async fn get_object_definition(
        &self,
        Parameters(params): Parameters<GetObjectDefinitionParams>,
    ) -> String {
        tracing::info!(tool = "get_object_definition", ?params, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);

        match metadata::get_object_definition(db.as_ref(), &params.database, schema, &params.name, &params.object_type).await {
            Ok(Some(def)) => serde_json::to_string_pretty(&def)
                .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string()),
            Ok(None) => json!({"error": "Object definition not found (may be encrypted)"}).to_string(),
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "List triggers defined on a table. Returns trigger names, events (INSERT/UPDATE/DELETE), timing, and enabled status."
    )]
    async fn list_triggers(
        &self,
        Parameters(params): Parameters<TableObjectParams>,
    ) -> String {
        tracing::info!(tool = "list_triggers", ?params, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);

        match metadata::list_triggers(db.as_ref(), &params.database, schema, &params.table).await {
            Ok(triggers) => serde_json::to_string_pretty(&triggers)
                .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string()),
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "Get the SQL definition of a trigger. Returns the CREATE TRIGGER statement."
    )]
    async fn get_trigger_definition(
        &self,
        Parameters(params): Parameters<TriggerDefinitionParams>,
    ) -> String {
        tracing::info!(tool = "get_trigger_definition", ?params, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);

        match metadata::get_trigger_definition(db.as_ref(), &params.database, schema, &params.name).await {
            Ok(Some(def)) => serde_json::to_string_pretty(&def)
                .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string()),
            Ok(None) => json!({"error": "Trigger definition not found"}).to_string(),
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "List views, procedures, and functions that reference a table. Shows objects that depend on or use the specified table."
    )]
    async fn get_related_objects(
        &self,
        Parameters(params): Parameters<TableObjectParams>,
    ) -> String {
        tracing::info!(tool = "get_related_objects", ?params, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);

        match metadata::get_related_objects(db.as_ref(), &params.database, schema, &params.table).await {
            Ok(objects) => serde_json::to_string_pretty(&objects)
                .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string()),
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "List Row-Level Security policies on a table. Returns policy names, commands, expressions, and roles. Works with Postgres (pg_policy) and MSSQL (sys.security_policies)."
    )]
    async fn list_rls_policies(
        &self,
        Parameters(params): Parameters<TableObjectParams>,
    ) -> String {
        tracing::info!(tool = "list_rls_policies", ?params, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);

        match metadata::list_rls_policies(db.as_ref(), &params.database, schema, &params.table).await {
            Ok(policies) => serde_json::to_string_pretty(&policies)
                .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string()),
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "Get Row-Level Security status for a table. Postgres: returns rls_enabled and rls_forced booleans. MSSQL: returns policy_count and enabled_count."
    )]
    async fn get_rls_status(
        &self,
        Parameters(params): Parameters<TableObjectParams>,
    ) -> String {
        tracing::info!(tool = "get_rls_status", ?params, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);

        match metadata::get_rls_status(db.as_ref(), &params.database, schema, &params.table).await {
            Ok(status) => serde_json::to_string_pretty(&status)
                .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string()),
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "Generate SQL for an RLS action without executing it. Returns the SQL string for review. Use execute_sql_write with skip_validation=true to run the generated SQL. Postgres actions: enable_rls, disable_rls, force_rls, no_force_rls, create_policy, drop_policy. MSSQL actions: enable_policy, disable_policy, create_policy, drop_policy."
    )]
    async fn generate_rls_sql(
        &self,
        Parameters(params): Parameters<GenerateRlsSqlParams>,
    ) -> String {
        tracing::info!(tool = "generate_rls_sql", action = %params.action, ?params, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        if matches!(db.dialect(), crate::db::Dialect::DuckDb | crate::db::Dialect::ClickHouse) {
            return json!({"error": "RLS not supported for this connection type"}).to_string();
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);

        let mut rls_params = std::collections::HashMap::new();
        if let Some(v) = &params.policy_name { rls_params.insert("policy_name".to_string(), v.clone()); }
        if let Some(v) = &params.command { rls_params.insert("command".to_string(), v.clone()); }
        if let Some(v) = &params.permissive { rls_params.insert("permissive".to_string(), v.clone()); }
        if let Some(v) = &params.roles { rls_params.insert("roles".to_string(), v.clone()); }
        if let Some(v) = &params.using_expr { rls_params.insert("using_expr".to_string(), v.clone()); }
        if let Some(v) = &params.with_check_expr { rls_params.insert("with_check_expr".to_string(), v.clone()); }
        if let Some(v) = &params.predicate_type { rls_params.insert("predicate_type".to_string(), v.clone()); }
        if let Some(v) = &params.predicate_function { rls_params.insert("predicate_function".to_string(), v.clone()); }
        if let Some(v) = &params.predicate_args { rls_params.insert("predicate_args".to_string(), v.clone()); }

        match metadata::generate_rls_sql(db.as_ref(), &params.database, schema, &params.table, &params.action, &rls_params).await {
            Ok(sql) => json!({
                "sql": sql,
                "instructions": "Review the generated SQL, then execute via execute_sql_write with skip_validation=true."
            }).to_string(),
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "Navigate foreign key relationships from a table. Discovers all FK connections (outgoing and incoming) and optionally fetches related rows. Without a filter, returns the relationship graph structure. With a filter (e.g. \"id = 42\"), fetches matching source rows and follows FK links to retrieve related rows from connected tables. Use direction to limit to 'outgoing' (references FROM this table) or 'incoming' (references TO this table)."
    )]
    async fn navigate_tables(
        &self,
        Parameters(params): Parameters<NavigateTablesParams>,
    ) -> String {
        tracing::info!(tool = "navigate_tables", table = %params.table, ?params.filter, "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        // Permission check on source table
        if let Err(e) = self.check_permission(&params.database, false) {
            return e;
        }

        let default_schema = match db.dialect() {
            crate::db::Dialect::Postgres => "public",
            crate::db::Dialect::DuckDb => "main",
            crate::db::Dialect::ClickHouse => "default",
            _ => "dbo",
        };
        let schema = params.schema.as_deref().unwrap_or(default_schema);
        let direction = params.direction.as_deref().unwrap_or("both");
        let row_limit = params.row_limit.unwrap_or(10).min(100).max(1);

        // Check table-level permission on source
        if let Some(ref ctx) = self.user_context {
            if !ctx.access_db.check_table_permission(&ctx.email, &params.database, &params.table, false) {
                return json!({"error": true, "message": format!("Permission denied on table '{}'", params.table)}).to_string();
            }
        }

        // Get FK relationships
        let all_fks = match metadata::get_foreign_keys(db.as_ref(), &params.database, &params.table, schema).await {
            Ok(fks) => fks,
            Err(e) => return json!({"error": format!("Failed to get foreign keys: {:#}", e)}).to_string(),
        };

        if all_fks.is_empty() {
            return json!({
                "table": params.table,
                "schema": schema,
                "database": params.database,
                "relationships": [],
                "message": "No foreign key relationships found for this table."
            }).to_string();
        }

        // Classify FKs into outgoing (this table's column references another) and incoming
        let mut outgoing = Vec::new();
        let mut incoming = Vec::new();
        for fk in &all_fks {
            if fk.from_table.eq_ignore_ascii_case(&params.table)
                && fk.from_schema.eq_ignore_ascii_case(schema)
            {
                outgoing.push(fk);
            }
            if fk.to_table.eq_ignore_ascii_case(&params.table)
                && fk.to_schema.eq_ignore_ascii_case(schema)
            {
                incoming.push(fk);
            }
        }

        // Filter by direction
        let (use_outgoing, use_incoming) = match direction {
            "outgoing" => (true, false),
            "incoming" => (false, true),
            _ => (true, true),
        };

        // No filter? Return structure only (with column lists for related tables)
        if params.filter.is_none() {
            // Collect unique related tables and fetch their columns
            let mut column_cache: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
            let mut tables_to_describe: Vec<(String, String)> = Vec::new(); // (schema, table)
            if use_outgoing {
                for fk in &outgoing {
                    let key = format!("{}.{}", fk.to_schema, fk.to_table);
                    if !column_cache.contains_key(&key) {
                        column_cache.insert(key, Vec::new());
                        tables_to_describe.push((fk.to_schema.clone(), fk.to_table.clone()));
                    }
                }
            }
            if use_incoming {
                for fk in &incoming {
                    let key = format!("{}.{}", fk.from_schema, fk.from_table);
                    if !column_cache.contains_key(&key) {
                        column_cache.insert(key, Vec::new());
                        tables_to_describe.push((fk.from_schema.clone(), fk.from_table.clone()));
                    }
                }
            }
            for (tbl_schema, tbl_name) in &tables_to_describe {
                if let Ok(cols) = metadata::describe_table(db.as_ref(), &params.database, tbl_name, tbl_schema).await {
                    let col_names: Vec<String> = cols.iter()
                        .filter_map(|c| c.get("column_name").or_else(|| c.get("COLUMN_NAME"))
                            .and_then(|v| v.as_str()).map(|s| s.to_string()))
                        .collect();
                    let key = format!("{}.{}", tbl_schema, tbl_name);
                    column_cache.insert(key, col_names);
                }
            }

            let mut relationships = Vec::new();
            if use_outgoing {
                for fk in &outgoing {
                    let key = format!("{}.{}", fk.to_schema, fk.to_table);
                    let mut rel = json!({
                        "direction": "outgoing",
                        "constraint_name": fk.constraint_name,
                        "from_columns": fk.from_columns,
                        "related_table": &key,
                        "related_columns": fk.to_columns,
                    });
                    if let Some(cols) = column_cache.get(&key) {
                        if !cols.is_empty() {
                            rel["related_table_columns"] = json!(cols);
                        }
                    }
                    relationships.push(rel);
                }
            }
            if use_incoming {
                for fk in &incoming {
                    let key = format!("{}.{}", fk.from_schema, fk.from_table);
                    let mut rel = json!({
                        "direction": "incoming",
                        "constraint_name": fk.constraint_name,
                        "related_table": &key,
                        "related_columns": fk.from_columns,
                        "referenced_columns": fk.to_columns,
                    });
                    if let Some(cols) = column_cache.get(&key) {
                        if !cols.is_empty() {
                            rel["related_table_columns"] = json!(cols);
                        }
                    }
                    relationships.push(rel);
                }
            }
            return json!({
                "table": params.table,
                "schema": schema,
                "database": params.database,
                "relationships": relationships,
                "note": "No filter provided. Showing relationship structure only with column names for related tables. Add a filter (e.g. \"id = 42\") to fetch related rows."
            }).to_string();
        }

        let filter = params.filter.as_deref().unwrap();
        let dialect = db.dialect();

        // Fetch source rows
        let source_query = build_nav_select_query(dialect, schema, &params.table, filter, row_limit);
        let source_qp = QueryParams {
            database: params.database.clone(),
            query: source_query,
            pii_mode: params.pii_mode.clone(),
            ..Default::default()
        };

        let source_result = match db.execute_query(&source_qp).await {
            Ok(r) => r,
            Err(e) => return json!({"error": format!("Failed to query source table: {:#}", e)}).to_string(),
        };

        if source_result.data.is_empty() {
            return json!({
                "table": params.table,
                "schema": schema,
                "database": params.database,
                "source_rows": [],
                "relationships": [],
                "message": format!("No rows matched filter: {}", filter)
            }).to_string();
        }

        // Navigate relationships
        let mut relationships = Vec::new();

        // Outgoing: this table's FK columns → referenced table's PK columns
        if use_outgoing {
            for fk in &outgoing {
                // Check permission on related table
                if let Some(ref ctx) = self.user_context {
                    if !ctx.access_db.check_table_permission(&ctx.email, &params.database, &fk.to_table, false) {
                        relationships.push(json!({
                            "direction": "outgoing",
                            "constraint_name": fk.constraint_name,
                            "related_table": format!("{}.{}", fk.to_schema, fk.to_table),
                            "error": "Permission denied on related table"
                        }));
                        continue;
                    }
                }

                let fk_filter = build_fk_filter(&source_result.data, &fk.from_columns, &fk.to_columns);
                if fk_filter.is_empty() {
                    continue;
                }

                let related_query = build_nav_select_query(dialect, &fk.to_schema, &fk.to_table, &fk_filter, row_limit);
                let related_qp = QueryParams {
                    database: params.database.clone(),
                    query: related_query,
                    pii_mode: params.pii_mode.clone(),
                    ..Default::default()
                };

                match db.execute_query(&related_qp).await {
                    Ok(result) => {
                        relationships.push(json!({
                            "direction": "outgoing",
                            "constraint_name": fk.constraint_name,
                            "from_columns": fk.from_columns,
                            "related_table": format!("{}.{}", fk.to_schema, fk.to_table),
                            "related_columns": fk.to_columns,
                            "rows": result.data,
                            "row_count": result.data.len(),
                        }));
                    }
                    Err(e) => {
                        relationships.push(json!({
                            "direction": "outgoing",
                            "constraint_name": fk.constraint_name,
                            "related_table": format!("{}.{}", fk.to_schema, fk.to_table),
                            "error": format!("{:#}", e)
                        }));
                    }
                }
            }
        }

        // Incoming: other tables' FK columns → this table's PK columns
        if use_incoming {
            for fk in &incoming {
                if let Some(ref ctx) = self.user_context {
                    if !ctx.access_db.check_table_permission(&ctx.email, &params.database, &fk.from_table, false) {
                        relationships.push(json!({
                            "direction": "incoming",
                            "constraint_name": fk.constraint_name,
                            "related_table": format!("{}.{}", fk.from_schema, fk.from_table),
                            "error": "Permission denied on related table"
                        }));
                        continue;
                    }
                }

                // For incoming: source rows have to_columns (PK), query from_table using from_columns
                let fk_filter = build_fk_filter(&source_result.data, &fk.to_columns, &fk.from_columns);
                if fk_filter.is_empty() {
                    continue;
                }

                let related_query = build_nav_select_query(dialect, &fk.from_schema, &fk.from_table, &fk_filter, row_limit);
                let related_qp = QueryParams {
                    database: params.database.clone(),
                    query: related_query,
                    pii_mode: params.pii_mode.clone(),
                    ..Default::default()
                };

                match db.execute_query(&related_qp).await {
                    Ok(result) => {
                        relationships.push(json!({
                            "direction": "incoming",
                            "constraint_name": fk.constraint_name,
                            "related_table": format!("{}.{}", fk.from_schema, fk.from_table),
                            "related_columns": fk.from_columns,
                            "referenced_columns": fk.to_columns,
                            "rows": result.data,
                            "row_count": result.data.len(),
                        }));
                    }
                    Err(e) => {
                        relationships.push(json!({
                            "direction": "incoming",
                            "constraint_name": fk.constraint_name,
                            "related_table": format!("{}.{}", fk.from_schema, fk.from_table),
                            "error": format!("{:#}", e)
                        }));
                    }
                }
            }
        }

        json!({
            "table": params.table,
            "schema": schema,
            "database": params.database,
            "source_rows": source_result.data,
            "source_row_count": source_result.data.len(),
            "relationships": relationships,
        }).to_string()
    }

    #[tool(description = "List all accessible databases on the server.")]
    async fn list_databases(
        &self,
        Parameters(params): Parameters<ListDatabasesParams>,
    ) -> String {
        tracing::info!(tool = "list_databases", "Tool called");

        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        match metadata::list_databases(db.as_ref()).await {
            Ok(databases) => {
                // Filter to only databases the user has read permission for
                let filtered = if let Some(ref ctx) = self.user_context {
                    databases
                        .into_iter()
                        .filter(|row| {
                            row.get("name")
                                .and_then(|v| v.as_str())
                                .map(|name| ctx.access_db.check_permission(&ctx.email, name, false))
                                .unwrap_or(false)
                        })
                        .collect()
                } else {
                    databases
                };
                serde_json::to_string_pretty(&filtered)
                    .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string())
            }
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(description = "List all named database connections available on this server.")]
    async fn list_connections(&self) -> String {
        tracing::info!(tool = "list_connections", "Tool called");

        let mut connections = metadata::list_connections(&self.registry);

        // Filter by connection access permissions
        if let Some(ref ctx) = self.user_context {
            if let Ok(Some(allowed)) = ctx.access_db.get_allowed_connections(&ctx.email) {
                connections.retain(|c| allowed.iter().any(|a| a == &c.name));
            }
        }

        serde_json::to_string_pretty(&connections)
            .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string())
    }

    #[tool(
        description = "Get help documentation for the lane MCP server including available tools, options, and usage examples."
    )]
    async fn get_api_help(&self) -> String {
        tracing::info!(tool = "get_api_help", "Tool called");

        json!({
            "service": "lane MCP",
            "tools": {
                "execute_sql_read": {
                    "description": "Execute a READ-ONLY SQL query (SELECT, WITH, sp_help, sp_columns). Safe for auto-approval.",
                    "params": {
                        "database": "Database name (optional, uses server default)",
                        "query": "SQL SELECT query",
                        "pii_mode": "none | scrub",
                        "pagination": "Enable pagination (requires ORDER BY)",
                        "include_metadata": "Include column metadata in response",
                        "outputFormat": "Set to \"xlsx\" to export as Excel and return a download URL",
                        "connection": "Named connection (optional, uses default)"
                    }
                },
                "execute_sql_write": {
                    "description": "Execute a SQL query that modifies data. DESTRUCTIVE - requires user confirmation.",
                    "params": {
                        "database": "Database name (optional, uses server default)",
                        "query": "Any valid SQL",
                        "pii_mode": "none | scrub",
                        "exec_sql": "Wrap in sp_executesql for DDL (CREATE PROCEDURE, etc.)",
                        "skip_validation": "Skip PARSEONLY validation (needed for DDL)",
                        "connection": "Named connection (optional, uses default)"
                    }
                },
                "execute_sql_dry_run": {
                    "description": "Validate SQL syntax without executing",
                    "params": {
                        "database": "Database name",
                        "query": "SQL query to validate",
                        "connection": "Named connection (optional, uses default)"
                    }
                },
                "list_tables": {
                    "description": "List all tables in a schema with row counts",
                    "params": {
                        "database": "Database name",
                        "schema": "Schema name (default: dbo)",
                        "connection": "Named connection (optional, uses default)"
                    }
                },
                "describe_table": {
                    "description": "Get column info for a table (types, nullability, keys, foreign keys)",
                    "params": {
                        "database": "Database name",
                        "table": "Table name",
                        "schema": "Schema name (default: dbo)",
                        "connection": "Named connection (optional, uses default)"
                    }
                },
                "navigate_tables": {
                    "description": "Navigate foreign key relationships from a table. Discovers FK connections and optionally fetches related rows.",
                    "params": {
                        "database": "Database name",
                        "table": "Table name to navigate from",
                        "schema": "Schema name (default: dbo)",
                        "filter": "WHERE clause (e.g. \"id = 42\"). Omit for structure only.",
                        "row_limit": "Max rows per related table (default 10, max 100)",
                        "direction": "both | outgoing | incoming",
                        "pii_mode": "none | scrub",
                        "connection": "Named connection (optional, uses default)"
                    }
                },
                "list_databases": {
                    "description": "List all accessible databases",
                    "params": {
                        "connection": "Named connection (optional, uses default)"
                    }
                },
                "list_connections": {
                    "description": "List all named database connections available on this server",
                    "params": {}
                },
                "get_api_help": {
                    "description": "This help documentation",
                    "params": {}
                }
            },
            "pii_modes": {
                "none": "Return raw data (default)",
                "scrub": "Replace PII with <type> placeholder text"
            },
            "tips": [
                "Always call describe_table before querying or modifying a table.",
                "Use execute_sql_dry_run to validate complex queries before running them.",
                "Use exec_sql=true and skip_validation=true for DDL statements (CREATE PROCEDURE, ALTER, DROP).",
                "Use pagination=true with ORDER BY for large result sets.",
                "execute_sql_read is safe for auto-approval; execute_sql_write requires user confirmation.",
                "Use navigate_tables to discover and follow foreign key relationships between tables. Omit filter for structure, add filter to fetch related rows.",
                "Use list_connections to see available named connections, then pass connection='name' to other tools.",
                "Use workspace_import_query to import query results into the local DuckDB workspace for cross-database analysis.",
                "Use workspace_query to run SQL across imported workspace tables.",
                "Use workspace_list_tables to see what's in the workspace, workspace_clear to reset.",
                "Use workspace_export_to_table to write workspace query results back to an external database table."
            ],
            "excel_export": {
                "description": "Excel (.xlsx) export is available via the REST API, not MCP tools.",
                "how_to": "POST /api/lane with outputFormat: \"xlsx\". Returns JSON with a single-use download_url. GET that URL to download the file.",
                "details": "Each result set becomes a separate sheet. Column types are preserved (numbers, booleans, strings). NULL values are empty cells. Arrays/objects are serialized as JSON strings. Download links expire after 5 minutes."
            }
        })
        .to_string()
    }

    // ========================================================================
    // Workspace Tools (DuckDB)
    // ========================================================================

    #[tool(
        description = "Import query results from a source database connection into the local DuckDB workspace as a named table. \
        Enables cross-database analytics by letting you query data from different sources together. \
        IMPORTANT: ALWAYS call describe_table on the source table first to learn exact column names before writing your query. \
        Example workflow: import sales data from MSSQL, import inventory from Postgres, then JOIN them in workspace_query."
    )]
    async fn workspace_import_query(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<WorkspaceImportQueryParams>,
    ) -> String {
        tracing::info!(tool = "workspace_import_query", table = %params.table_name, "Tool called");

        #[cfg(not(feature = "duckdb_backend"))]
        {
            return json!({"error": true, "message": "Workspace feature (duckdb_backend) is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "duckdb_backend")]
        {
            self.workspace_import_query_impl(params).await
        }
    }

    #[tool(
        description = "Execute a SQL query against the local DuckDB workspace. Use this to query, join, and analyze data \
        that was imported via workspace_import_query. Supports full DuckDB SQL syntax including JOINs, aggregations, \
        window functions, CTEs, etc."
    )]
    async fn workspace_query(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<WorkspaceQueryParams>,
    ) -> String {
        tracing::info!(tool = "workspace_query", "Tool called");

        #[cfg(not(feature = "duckdb_backend"))]
        {
            return json!({"error": true, "message": "Workspace feature (duckdb_backend) is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "duckdb_backend")]
        {
            self.workspace_query_impl(params).await
        }
    }

    #[tool(
        description = "List all tables currently in the DuckDB workspace, including row counts, column counts, and when they were imported."
    )]
    async fn workspace_list_tables(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<WorkspaceListTablesParams>,
    ) -> String {
        tracing::info!(tool = "workspace_list_tables", "Tool called");

        #[cfg(not(feature = "duckdb_backend"))]
        {
            return json!({"error": true, "message": "Workspace feature (duckdb_backend) is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "duckdb_backend")]
        {
            self.workspace_list_tables_impl().await
        }
    }

    #[tool(
        description = "Clear all tables from the DuckDB workspace. Drops every imported table and resets the metadata. This is irreversible."
    )]
    async fn workspace_clear(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<WorkspaceClearParams>,
    ) -> String {
        tracing::info!(tool = "workspace_clear", "Tool called");

        #[cfg(not(feature = "duckdb_backend"))]
        {
            return json!({"error": true, "message": "Workspace feature (duckdb_backend) is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "duckdb_backend")]
        {
            self.workspace_clear_impl().await
        }
    }

    #[tool(
        description = "Export workspace query results to an external database table. Performs server-side data transfer without sending data through the LLM. Supports 'fail' (default), 'replace', or 'append' modes via if_exists."
    )]
    async fn workspace_export_to_table(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<WorkspaceExportToTableParams>,
    ) -> String {
        tracing::info!(tool = "workspace_export_to_table", table = %params.table, "Tool called");

        #[cfg(not(feature = "duckdb_backend"))]
        {
            return json!({"error": true, "message": "Workspace feature (duckdb_backend) is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "duckdb_backend")]
        {
            self.workspace_export_to_table_impl(params).await
        }
    }

    // ========================================================================
    // Storage Tools (MinIO/S3)
    // ========================================================================

    #[tool(
        description = "List all buckets on a storage (MinIO/S3) connection."
    )]
    async fn storage_list_buckets(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<StorageListBucketsParams>,
    ) -> String {
        tracing::info!(tool = "storage_list_buckets", "Tool called");

        #[cfg(not(feature = "storage"))]
        {
            return json!({"error": true, "message": "Storage feature is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "storage")]
        {
            let (conn_name, client) = match self.resolve_storage(params.connection.as_deref()).await {
                Ok(r) => r,
                Err(e) => return e,
            };
            match client.list_buckets().await {
                Ok(buckets) => json!({
                    "connection": conn_name,
                    "buckets": buckets,
                    "count": buckets.len()
                }).to_string(),
                Err(e) => json!({"error": true, "message": format!("{:#}", e)}).to_string(),
            }
        }
    }

    #[tool(
        description = "List objects in a storage (MinIO/S3) bucket. Optionally filter by prefix (folder path)."
    )]
    async fn storage_list_objects(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<StorageListObjectsParams>,
    ) -> String {
        tracing::info!(tool = "storage_list_objects", bucket = %params.bucket, "Tool called");

        #[cfg(not(feature = "storage"))]
        {
            return json!({"error": true, "message": "Storage feature is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "storage")]
        {
            let (conn_name, client) = match self.resolve_storage(params.connection.as_deref()).await {
                Ok(r) => r,
                Err(e) => return e,
            };
            if let Err(e) = self.check_storage_permission(&conn_name, &params.bucket, crate::auth::access_control::StoragePermAction::Read) {
                return e;
            }
            match client.list_objects(&params.bucket, params.prefix.as_deref(), Some("/")).await {
                Ok(objects) => json!({
                    "connection": conn_name,
                    "bucket": params.bucket,
                    "objects": objects,
                    "count": objects.len()
                }).to_string(),
                Err(e) => json!({"error": true, "message": format!("{:#}", e)}).to_string(),
            }
        }
    }

    #[tool(
        description = "Upload content to a storage (MinIO/S3) bucket. Content is UTF-8 text by default; prefix with 'base64:' for binary data."
    )]
    async fn storage_upload(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<StorageUploadParams>,
    ) -> String {
        tracing::info!(tool = "storage_upload", bucket = %params.bucket, key = %params.key, "Tool called");

        if let Err(e) = self.require_min_sql_mode(SqlMode::Supervised, "storage_upload") {
            return e;
        }

        #[cfg(not(feature = "storage"))]
        {
            return json!({"error": true, "message": "Storage feature is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "storage")]
        {
            let (conn_name, client) = match self.resolve_storage(params.connection.as_deref()).await {
                Ok(r) => r,
                Err(e) => return e,
            };

            if let Err(e) = self.check_storage_permission(&conn_name, &params.bucket, crate::auth::access_control::StoragePermAction::Write) {
                return e;
            }

            // Approval flow for supervised/confirmed users
            if self.needs_approval() {
                let context = format!("Upload to {}/{}/{}", conn_name, params.bucket, params.key);
                if let Err(msg) = self
                    .await_approval(
                        "storage_upload",
                        vec![format!("PUT {}/{}", params.bucket, params.key)],
                        &conn_name,
                        &params.bucket,
                        &context,
                    )
                    .await
                {
                    return json!({"error": true, "message": msg}).to_string();
                }
            }

            let data = if let Some(b64) = params.content.strip_prefix("base64:") {
                use base64::Engine;
                match base64::engine::general_purpose::STANDARD.decode(b64) {
                    Ok(bytes) => bytes,
                    Err(e) => return json!({"error": true, "message": format!("Invalid base64 content: {}", e)}).to_string(),
                }
            } else {
                params.content.into_bytes()
            };

            match client.upload_object(&params.bucket, &params.key, &data, params.content_type.as_deref()).await {
                Ok(()) => {
                    self.log_storage_op(&conn_name, "upload_object", &format!("{}/{}", params.bucket, params.key));
                    json!({
                        "success": true,
                        "bucket": params.bucket,
                        "key": params.key,
                        "size": data.len()
                    }).to_string()
                }
                Err(e) => json!({"error": true, "message": format!("{:#}", e)}).to_string(),
            }
        }
    }

    #[tool(
        description = "Download an object from storage and load it into the DuckDB workspace as a table. \
        Supports CSV, TSV, Parquet, JSON, JSONL, XLSX files. \
        The table can then be queried with workspace_query."
    )]
    async fn storage_download_to_workspace(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<StorageDownloadToWorkspaceParams>,
    ) -> String {
        tracing::info!(tool = "storage_download_to_workspace", bucket = %params.bucket, key = %params.key, "Tool called");

        #[cfg(not(feature = "storage"))]
        {
            return json!({"error": true, "message": "Storage feature is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "storage")]
        {
            self.storage_download_to_workspace_impl(params).await
        }
    }

    #[tool(
        description = "Execute a saved named endpoint. Endpoints are pre-defined queries with optional parameters, created by admins. Use list_endpoints to see available endpoints."
    )]
    async fn execute_endpoint(
        &self,
        Parameters(params): Parameters<ExecuteEndpointParams>,
    ) -> String {
        tracing::info!(tool = "execute_endpoint", name = %params.name, "Tool called");

        let ctx = match &self.user_context {
            None => {
                // System access — need access_db for endpoints
                return json!({"error": true, "message": "Endpoints require access control to be enabled"}).to_string();
            }
            Some(ctx) => ctx,
        };

        // Check MCP enabled
        if !ctx.access_db.is_mcp_enabled(&ctx.email) {
            return json!({"error": true, "message": "MCP access is disabled for your account"}).to_string();
        }

        // Load endpoint
        let endpoint = match ctx.access_db.get_endpoint(&params.name) {
            Ok(Some(ep)) => ep,
            Ok(None) => return json!({"error": true, "message": format!("Endpoint '{}' not found", params.name)}).to_string(),
            Err(e) => return json!({"error": true, "message": e}).to_string(),
        };

        // Check endpoint access
        if !ctx.access_db.check_endpoint_access(&ctx.email, &params.name) {
            return json!({"error": true, "message": format!("Access denied to endpoint '{}'", params.name)}).to_string();
        }

        // Check connection access
        if !ctx.access_db.check_connection_access(&ctx.email, &endpoint.connection_name) {
            return json!({"error": true, "message": format!("Access denied to connection '{}'", endpoint.connection_name)}).to_string();
        }

        // Resolve connection
        let db = match self.registry.resolve(Some(&endpoint.connection_name)) {
            Ok(db) => db,
            Err(e) => return json!({"error": true, "message": format!("{}", e)}).to_string(),
        };

        // Substitute parameters
        let param_defs = crate::api::endpoints::parse_param_defs(endpoint.parameters.as_deref());
        let values = params.parameters.unwrap_or_default();
        let query = match crate::api::endpoints::substitute_parameters(&endpoint.query, &values, &param_defs) {
            Ok(q) => q,
            Err(e) => return json!({"error": true, "message": e}).to_string(),
        };

        // Enforce read-only
        if !crate::query::validation::is_select_like(&query) {
            return json!({"error": true, "message": "Endpoints can only execute read-only queries (SELECT/WITH)"}).to_string();
        }

        // Apply default row limit (10k)
        let limited_query = crate::query::validation::apply_row_limit_dialect(&query, 10_000, db.dialect());

        // Execute
        let qp = QueryParams {
            database: endpoint.database_name.clone(),
            query: limited_query,
            pagination: false,
            include_metadata: true,
            ..Default::default()
        };

        ctx.access_db.log_access(
            Some(&ctx.token_prefix),
            Some(&ctx.email),
            None,
            Some(&endpoint.database_name),
            Some("read"),
            "allowed",
            Some(&format!("MCP: execute_endpoint '{}'", params.name)),
        );

        match db.execute_query(&qp).await {
            Ok(result) => serde_json::to_string_pretty(&result)
                .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string()),
            Err(e) => json!({"error": format!("{:#}", e)}).to_string(),
        }
    }

    #[tool(
        description = "List all named endpoints available to you. Endpoints are saved queries with optional parameters."
    )]
    async fn list_endpoints(&self) -> String {
        tracing::info!(tool = "list_endpoints", "Tool called");

        let ctx = match &self.user_context {
            None => return json!({"error": true, "message": "Endpoints require access control"}).to_string(),
            Some(ctx) => ctx,
        };

        if !ctx.access_db.is_mcp_enabled(&ctx.email) {
            return json!({"error": true, "message": "MCP access is disabled for your account"}).to_string();
        }

        let all = match ctx.access_db.list_endpoints() {
            Ok(eps) => eps,
            Err(e) => return json!({"error": true, "message": e}).to_string(),
        };

        let visible: Vec<_> = all
            .into_iter()
            .filter(|ep| ctx.access_db.check_endpoint_access(&ctx.email, &ep.name))
            .map(|ep| {
                json!({
                    "name": ep.name,
                    "connection_name": ep.connection_name,
                    "database_name": ep.database_name,
                    "description": ep.description,
                    "parameters": ep.parameters.as_deref()
                        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()),
                })
            })
            .collect();

        serde_json::to_string_pretty(&visible)
            .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string())
    }

    #[tool(
        description = "Generate a presigned GET URL for a storage object. The URL allows temporary unauthenticated download access."
    )]
    async fn storage_get_url(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<StorageGetUrlParams>,
    ) -> String {
        tracing::info!(tool = "storage_get_url", bucket = %params.bucket, key = %params.key, "Tool called");

        #[cfg(not(feature = "storage"))]
        {
            return json!({"error": true, "message": "Storage feature is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "storage")]
        {
            let (conn_name, client) = match self.resolve_storage(params.connection.as_deref()).await {
                Ok(r) => r,
                Err(e) => return e,
            };
            if let Err(e) = self.check_storage_permission(&conn_name, &params.bucket, crate::auth::access_control::StoragePermAction::Read) {
                return e;
            }
            let _ = &conn_name; // suppress unused warning
            let expiry = params.expiry_secs.unwrap_or(3600);
            match client.presign_get_url(&params.bucket, &params.key, expiry).await {
                Ok(url) => json!({
                    "success": true,
                    "url": url,
                    "bucket": params.bucket,
                    "key": params.key,
                    "expiry_secs": expiry
                }).to_string(),
                Err(e) => json!({"error": true, "message": format!("{:#}", e)}).to_string(),
            }
        }
    }

    #[tool(
        description = "Export SQL query results directly to a storage (MinIO/S3) bucket as CSV, JSON, or XLSX. \
        Runs the query against a database connection and uploads the results to storage."
    )]
    async fn storage_export_query(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<StorageExportQueryParams>,
    ) -> String {
        tracing::info!(tool = "storage_export_query", bucket = %params.bucket, key = %params.key, "Tool called");

        if let Err(e) = self.require_min_sql_mode(SqlMode::Supervised, "storage_export_query") {
            return e;
        }

        #[cfg(not(feature = "storage"))]
        {
            return json!({"error": true, "message": "Storage feature is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "storage")]
        {
            let (conn_name, client) = match self.resolve_storage(params.storage_connection.as_deref()).await {
                Ok(r) => r,
                Err(e) => return e,
            };

            if let Err(e) = self.check_storage_permission(&conn_name, &params.bucket, crate::auth::access_control::StoragePermAction::Write) {
                return e;
            }

            // Approval flow
            if self.needs_approval() {
                let context = format!("Export query results to {}/{}/{}", conn_name, params.bucket, params.key);
                if let Err(msg) = self
                    .await_approval(
                        "storage_export_query",
                        vec![format!("QUERY -> PUT {}/{}", params.bucket, params.key)],
                        &conn_name,
                        &params.bucket,
                        &context,
                    )
                    .await
                {
                    return json!({"error": true, "message": msg}).to_string();
                }
            }

            // Infer format
            let fmt = match crate::export::infer_export_format(&params.key, params.format.as_deref()) {
                Ok(f) => f,
                Err(e) => return json!({"error": true, "message": e}).to_string(),
            };

            // Cannot export Parquet from regular queries
            if fmt == crate::export::ExportFormat::Parquet {
                return json!({"error": true, "message": "Parquet export is only supported from workspace queries. Use csv, json, or xlsx."}).to_string();
            }

            // Resolve DB connection and execute query
            let db = match self.registry.resolve(params.connection.as_deref()) {
                Ok(db) => db,
                Err(e) => return json!({"error": true, "message": format!("{}", e)}).to_string(),
            };

            // Check DB connection access
            if let Err(e) = self.check_permission(&db.default_database(), false) {
                return e;
            }

            let qp = QueryParams {
                database: params.database.unwrap_or_else(|| db.default_database().to_string()),
                query: params.query.clone(),
                pagination: false,
                include_metadata: true,
                ..Default::default()
            };

            let result = match db.execute_query(&qp).await {
                Ok(r) => r,
                Err(e) => return json!({"error": true, "message": format!("Query failed: {:#}", e)}).to_string(),
            };

            let row_count = result.total_rows;

            let data = match fmt {
                crate::export::ExportFormat::Csv => {
                    match crate::export::csv::query_result_to_csv(&result) {
                        Ok(d) => d,
                        Err(e) => return json!({"error": true, "message": format!("CSV conversion failed: {:#}", e)}).to_string(),
                    }
                }
                crate::export::ExportFormat::Json => {
                    match serde_json::to_vec_pretty(&result.data) {
                        Ok(d) => d,
                        Err(e) => return json!({"error": true, "message": format!("JSON conversion failed: {}", e)}).to_string(),
                    }
                }
                #[cfg(feature = "xlsx")]
                crate::export::ExportFormat::Xlsx => {
                    match crate::export::xlsx::query_result_to_xlsx(&result) {
                        Ok(d) => d,
                        Err(e) => return json!({"error": true, "message": format!("XLSX conversion failed: {:#}", e)}).to_string(),
                    }
                }
                crate::export::ExportFormat::Parquet => unreachable!(),
            };

            let size = data.len();

            match client.upload_object(&params.bucket, &params.key, &data, Some(fmt.content_type())).await {
                Ok(()) => {
                    self.log_storage_op(&conn_name, "export_query", &format!("{}/{} ({})", params.bucket, params.key, fmt.as_str()));
                    json!({
                        "success": true,
                        "bucket": params.bucket,
                        "key": params.key,
                        "size": size,
                        "row_count": row_count,
                        "format": fmt.as_str()
                    }).to_string()
                }
                Err(e) => json!({"error": true, "message": format!("Upload failed: {:#}", e)}).to_string(),
            }
        }
    }

    #[tool(
        description = "Export workspace (DuckDB) query results to a storage (MinIO/S3) bucket. \
        Supports CSV, JSON, and Parquet formats. Parquet uses DuckDB's native COPY for efficiency."
    )]
    async fn workspace_export_to_storage(
        &self,
        #[allow(unused_variables)]
        Parameters(params): Parameters<WorkspaceExportToStorageParams>,
    ) -> String {
        tracing::info!(tool = "workspace_export_to_storage", bucket = %params.bucket, key = %params.key, "Tool called");

        if let Err(e) = self.require_min_sql_mode(SqlMode::Supervised, "workspace_export_to_storage") {
            return e;
        }

        #[cfg(not(feature = "storage"))]
        {
            return json!({"error": true, "message": "Storage feature is not enabled on this server"}).to_string();
        }

        #[cfg(feature = "storage")]
        {
            self.workspace_export_to_storage_impl(params).await
        }
    }

    #[tool(description = "Search database schema (tables, views, columns) across all connections. Uses full-text search with stemming.")]
    async fn search_schema(
        &self,
        Parameters(params): Parameters<SearchSchemaParams>,
    ) -> String {
        tracing::info!(tool = "search_schema", query = %params.query, "Tool called");

        let search_db = match &self.search_db {
            Some(db) => db,
            None => return json!({"error": true, "message": "Search is not available on this server"}).to_string(),
        };

        let limit = params.limit.unwrap_or(10).min(50);
        let results = search_db.search_schema(&params.query, limit);

        json!({
            "success": true,
            "results": results,
            "total": results.len()
        }).to_string()
    }

    #[tool(description = "Search query history across all users. Uses full-text search with stemming.")]
    async fn search_queries(
        &self,
        Parameters(params): Parameters<SearchQueryParams>,
    ) -> String {
        tracing::info!(tool = "search_queries", query = %params.query, "Tool called");

        let search_db = match &self.search_db {
            Some(db) => db,
            None => return json!({"error": true, "message": "Search is not available on this server"}).to_string(),
        };

        let limit = params.limit.unwrap_or(10).min(50);
        let results = search_db.search_queries(&params.query, None, limit);

        json!({
            "success": true,
            "results": results,
            "total": results.len()
        }).to_string()
    }

    #[tool(description = "Search named data endpoints by name, description, or query content. Uses full-text search with stemming.")]
    async fn search_endpoints(
        &self,
        Parameters(params): Parameters<SearchEndpointParams>,
    ) -> String {
        tracing::info!(tool = "search_endpoints", query = %params.query, "Tool called");

        let search_db = match &self.search_db {
            Some(db) => db,
            None => return json!({"error": true, "message": "Search is not available on this server"}).to_string(),
        };

        let limit = params.limit.unwrap_or(10).min(50);
        let results = search_db.search_endpoints(&params.query, limit);

        json!({
            "success": true,
            "results": results,
            "total": results.len()
        }).to_string()
    }
}

// ============================================================================
// Workspace Tool Implementations (feature-gated)
// ============================================================================

#[cfg(feature = "duckdb_backend")]
impl BatchQueryMcp {
    async fn workspace_import_query_impl(&self, params: WorkspaceImportQueryParams) -> String {
        let ws = match &self.workspace_db {
            Some(ws) => ws,
            None => return json!({"error": true, "message": "Workspace is not available on this server"}).to_string(),
        };

        // MCP-specific permission checks
        if let Some(conn_name) = params.connection.as_deref() {
            if let Some(ref ctx) = self.user_context {
                if !ctx.access_db.check_connection_access(&ctx.email, conn_name) {
                    return json!({"error": true, "message": format!("Access denied to connection '{}'", conn_name)}).to_string();
                }
            }
        }

        if let Some(ref db_name) = params.database {
            if let Err(e) = self.check_permission(db_name, false) {
                return e;
            }
        }

        match crate::api::workspace::do_workspace_import(
            &self.registry,
            ws,
            params.connection.as_deref(),
            params.database.as_deref(),
            &params.query,
            &params.table_name,
            params.if_exists.as_deref(),
        )
        .await
        {
            Ok(result) => json!({
                "success": true,
                "table_name": result.table_name,
                "row_count": result.row_count,
                "column_count": result.column_count,
                "columns": result.columns
            }).to_string(),
            Err(e) => json!({"error": true, "message": e}).to_string(),
        }
    }

    async fn workspace_query_impl(&self, params: WorkspaceQueryParams) -> String {
        let ws = match &self.workspace_db {
            Some(ws) => ws,
            None => return json!({"error": true, "message": "Workspace is not available on this server"}).to_string(),
        };

        let qp = QueryParams {
            database: String::new(),
            query: params.query,
            include_metadata: params.include_metadata.unwrap_or(false),
            ..Default::default()
        };

        match ws.execute_query(&qp).await {
            Ok(result) => serde_json::to_string_pretty(&result)
                .unwrap_or_else(|e| json!({"error": e.to_string()}).to_string()),
            Err(e) => json!({"error": true, "message": format!("{:#}", e)}).to_string(),
        }
    }

    async fn workspace_list_tables_impl(&self) -> String {
        let ws = match &self.workspace_db {
            Some(ws) => ws,
            None => return json!({"error": true, "message": "Workspace is not available on this server"}).to_string(),
        };

        match ws.query_rows("SELECT table_name, original_filename, uploaded_at, row_count, column_count FROM __workspace_meta ORDER BY table_name").await {
            Ok(tables) => {
                json!({
                    "tables": tables,
                    "count": tables.len()
                }).to_string()
            }
            Err(e) => json!({"error": true, "message": format!("{:#}", e)}).to_string(),
        }
    }

    async fn workspace_clear_impl(&self) -> String {
        if let Err(e) = self.require_min_sql_mode(SqlMode::Supervised, "workspace_clear") {
            return e;
        }

        let ws = match &self.workspace_db {
            Some(ws) => ws,
            None => return json!({"error": true, "message": "Workspace is not available on this server"}).to_string(),
        };

        // Get list of tables to drop
        let tables = match ws.query_rows("SELECT table_name FROM __workspace_meta").await {
            Ok(t) => t,
            Err(e) => return json!({"error": true, "message": format!("{:#}", e)}).to_string(),
        };

        let mut dropped = 0;
        for row in &tables {
            if let Some(name) = row.get("table_name").and_then(|v| v.as_str()) {
                if ws.execute_sql(&format!("DROP TABLE IF EXISTS \"{}\"", name)).await.is_ok() {
                    dropped += 1;
                }
            }
        }

        let _ = ws.execute_sql("DELETE FROM __workspace_meta").await;

        json!({
            "success": true,
            "tables_dropped": dropped
        }).to_string()
    }

    async fn workspace_export_to_table_impl(&self, params: WorkspaceExportToTableParams) -> String {
        let ws = match &self.workspace_db {
            Some(ws) => ws,
            None => return json!({"error": true, "message": "Workspace is not available on this server"}).to_string(),
        };

        // 1. Execute source query against workspace with metadata
        let qp = QueryParams {
            database: String::new(),
            query: params.source_query,
            include_metadata: true,
            ..Default::default()
        };

        let result = match ws.execute_query(&qp).await {
            Ok(r) => r,
            Err(e) => return json!({"error": true, "message": format!("Workspace query failed: {:#}", e)}).to_string(),
        };

        // 2. Extract columns from metadata
        let columns_meta = match &result.metadata {
            Some(meta) if !meta.columns.is_empty() => &meta.columns,
            _ => return json!({"error": true, "message": "Workspace query returned no column metadata"}).to_string(),
        };

        // 3. If empty result, return early
        if result.data.is_empty() {
            return json!({
                "success": true,
                "table": params.table,
                "row_count": 0,
                "column_count": columns_meta.len(),
                "message": "Source query returned no rows — no table created"
            }).to_string();
        }

        // 4. Resolve target connection
        let db = match self.resolve_connection(params.connection.as_deref()) {
            Ok(db) => db,
            Err(e) => return e,
        };

        let dialect = db.dialect();

        // Cannot export back to DuckDB
        if matches!(dialect, crate::db::Dialect::DuckDb) {
            return json!({"error": true, "message": "Cannot export to a DuckDB connection — use workspace_query instead"}).to_string();
        }

        // 5. Permission check (write)
        if let Err(e) = self.check_permission(&params.database, true) {
            return e;
        }

        // 5b. sql_mode check — this tool autonomously writes to external databases
        //     (CREATE TABLE, DROP TABLE, INSERT). Supervised users go through approval.
        let if_exists = params.if_exists.as_deref().unwrap_or("fail");
        if let Some(ref ctx) = self.user_context {
            let mode = ctx.access_db.get_sql_mode(&ctx.email);
            if matches!(mode, SqlMode::Supervised | SqlMode::Confirmed) {
                // Build a preview of the SQL that will execute
                let conn_name = params
                    .connection
                    .clone()
                    .unwrap_or_else(|| self.registry.default_name());
                let context = format!(
                    "Export workspace data to {}.{}.{}",
                    conn_name, params.database, params.table
                );
                let preview_sql = format!(
                    "-- workspace_export_to_table: {} rows to {}.{} (if_exists={})",
                    result.data.len(),
                    params.database,
                    params.table,
                    if_exists,
                );
                if let Err(msg) = self
                    .await_approval(
                        "workspace_export_to_table",
                        vec![preview_sql],
                        &conn_name,
                        &params.database,
                        &context,
                    )
                    .await
                {
                    return json!({"error": true, "message": msg}).to_string();
                }
                // Approved — continue execution
            } else if mode != SqlMode::Full {
                return json!({
                    "error": true,
                    "message": "workspace_export_to_table requires 'full', 'confirmed', or 'supervised' SQL mode. \
                               This tool writes autonomously to external databases. \
                               Ask an admin to upgrade your SQL mode, or use the SQL Editor for manual export."
                }).to_string();
            }
        }

        // 6. Default schema
        let schema = params.schema.unwrap_or_else(|| {
            match dialect {
                crate::db::Dialect::Postgres => "public".to_string(),
                crate::db::Dialect::ClickHouse => "default".to_string(),
                _ => "dbo".to_string(),
            }
        });

        // 7. Build InferredColumn vec from workspace columns
        let inferred_columns: Vec<InferredColumn> = columns_meta.iter().map(|col| {
            let sql_type = map_duckdb_type_to_dialect(&col.data_type, dialect);
            let inferred_type = duckdb_type_to_inferred(&col.data_type);
            InferredColumn {
                name: col.name.clone(),
                inferred_type,
                sql_type,
                nullable: true,
            }
        }).collect();

        // 8. Handle if_exists
        let target_qp_base = QueryParams {
            database: params.database.clone(),
            query: String::new(),
            ..Default::default()
        };

        match if_exists {
            "fail" => {
                // Check if table exists
                let check_sql = format!(
                    "SELECT COUNT(*) AS cnt FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}'",
                    schema, params.table
                );
                let check_qp = QueryParams { query: check_sql, ..target_qp_base.clone() };
                if let Ok(check_result) = db.execute_query(&check_qp).await {
                    if let Some(row) = check_result.data.first() {
                        let count = row.get("cnt")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0);
                        if count > 0 {
                            return json!({"error": true, "message": format!("Table '{}.{}' already exists. Use if_exists='replace' to overwrite or 'append' to add rows.", schema, params.table)}).to_string();
                        }
                    }
                }
                // Create table
                let create_sql = sql_gen::generate_create_table(&inferred_columns, &schema, &params.table, dialect);
                let create_qp = QueryParams { query: create_sql, ..target_qp_base.clone() };
                if let Err(e) = db.execute_query(&create_qp).await {
                    return json!({"error": true, "message": format!("Failed to create table: {:#}", e)}).to_string();
                }
            }
            "replace" => {
                // Drop and recreate
                let drop_sql = format!(
                    "DROP TABLE IF EXISTS {}.{}",
                    sql_gen::escape_identifier(&schema, dialect),
                    sql_gen::escape_identifier(&params.table, dialect)
                );
                let drop_qp = QueryParams { query: drop_sql, ..target_qp_base.clone() };
                if let Err(e) = db.execute_query(&drop_qp).await {
                    return json!({"error": true, "message": format!("Failed to drop existing table: {:#}", e)}).to_string();
                }
                let create_sql = sql_gen::generate_create_table(&inferred_columns, &schema, &params.table, dialect);
                let create_qp = QueryParams { query: create_sql, ..target_qp_base.clone() };
                if let Err(e) = db.execute_query(&create_qp).await {
                    return json!({"error": true, "message": format!("Failed to create table: {:#}", e)}).to_string();
                }
            }
            "append" => {
                // No DDL — just insert into existing table
            }
            other => {
                return json!({"error": true, "message": format!("Invalid if_exists value '{}'. Use 'fail', 'replace', or 'append'.", other)}).to_string();
            }
        }

        // 9. Convert rows to Vec<Vec<Option<String>>> preserving column order
        let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(result.data.len());
        for row_map in &result.data {
            let row: Vec<Option<String>> = columns_meta.iter().map(|col| {
                match row_map.get(&col.name) {
                    None | Some(serde_json::Value::Null) => None,
                    Some(serde_json::Value::String(s)) => Some(s.clone()),
                    Some(serde_json::Value::Bool(b)) => Some(b.to_string()),
                    Some(v) => Some(v.to_string()),
                }
            }).collect();
            rows.push(row);
        }

        // 10. Batch INSERT (500 rows per batch)
        let total_rows = rows.len();
        let mut inserted = 0usize;
        let mut batch_count = 0usize;

        for chunk in rows.chunks(500) {
            let insert_sql = sql_gen::generate_insert_batch(&inferred_columns, chunk, &schema, &params.table, dialect);
            let insert_qp = QueryParams { query: insert_sql, ..target_qp_base.clone() };
            if let Err(e) = db.execute_query(&insert_qp).await {
                return json!({
                    "error": true,
                    "message": format!("Batch insert failed after {} rows: {:#}", inserted, e),
                    "rows_inserted_before_failure": inserted,
                    "table": params.table
                }).to_string();
            }
            inserted += chunk.len();
            batch_count += 1;
        }

        json!({
            "success": true,
            "table": format!("{}.{}", schema, params.table),
            "row_count": total_rows,
            "column_count": inferred_columns.len(),
            "batches": batch_count
        }).to_string()
    }
}

// ============================================================================
// Storage Tool Implementations (feature-gated)
// ============================================================================

#[cfg(all(feature = "storage", feature = "duckdb_backend"))]
impl BatchQueryMcp {
    async fn storage_download_to_workspace_impl(&self, params: StorageDownloadToWorkspaceParams) -> String {
        let (conn_name, client) = match self.resolve_storage(params.connection.as_deref()).await {
            Ok(r) => r,
            Err(e) => return e,
        };
        if let Err(e) = self.check_storage_permission(&conn_name, &params.bucket, crate::auth::access_control::StoragePermAction::Read) {
            return e;
        }

        let ws_dir = match &self.workspace_dir {
            Some(d) => d,
            None => return json!({"error": true, "message": "Workspace directory not available"}).to_string(),
        };
        let ws_db = match &self.workspace_db {
            Some(d) => d,
            None => return json!({"error": true, "message": "DuckDB workspace not available"}).to_string(),
        };

        // Download the file
        let data = match client.download_object(&params.bucket, &params.key).await {
            Ok(d) => d,
            Err(e) => return json!({"error": true, "message": format!("Download failed: {:#}", e)}).to_string(),
        };

        // Write file to workspace dir
        let filename = params.key.rsplit('/').next().unwrap_or(&params.key);
        let file_path = ws_dir.join(filename);
        if let Err(e) = tokio::fs::write(&file_path, &data).await {
            return json!({"error": true, "message": format!("Failed to write file: {}", e)}).to_string();
        }

        // Determine table name
        let table_name = params.table_name.unwrap_or_else(|| sanitize_table_name(filename));

        // Load into DuckDB based on extension
        let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
        let path_str = file_path.to_string_lossy();

        let create_sql = match ext.as_str() {
            "csv" | "tsv" => format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_csv_auto('{}')",
                table_name, path_str
            ),
            "parquet" => format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_parquet('{}')",
                table_name, path_str
            ),
            "json" | "jsonl" | "ndjson" => format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_json_auto('{}')",
                table_name, path_str
            ),
            "xlsx" | "xls" => format!(
                "INSTALL spatial; LOAD spatial; CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM st_read('{}')",
                table_name, path_str
            ),
            _ => {
                return json!({"error": true, "message": format!("Unsupported file type for workspace import: .{}", ext)}).to_string();
            }
        };

        match ws_db.execute_sql(&create_sql).await {
            Ok(_) => {
                let count_sql = format!("SELECT COUNT(*) FROM \"{}\"", table_name);
                let row_count = ws_db.query_count(&count_sql).await.unwrap_or(0);

                // Update workspace metadata
                let meta_sql = format!(
                    "INSERT OR REPLACE INTO __workspace_meta (table_name, original_filename, row_count, column_count) \
                     VALUES ('{}', '{}', {}, (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{}'))",
                    table_name, filename, row_count, table_name
                );
                let _ = ws_db.execute_sql(&meta_sql).await;

                json!({
                    "success": true,
                    "table_name": table_name,
                    "filename": filename,
                    "bucket": params.bucket,
                    "key": params.key,
                    "row_count": row_count
                }).to_string()
            }
            Err(e) => {
                json!({"error": true, "message": format!("Failed to load into DuckDB: {:#}", e)}).to_string()
            }
        }
    }
}

#[cfg(all(feature = "storage", feature = "duckdb_backend"))]
impl BatchQueryMcp {
    async fn workspace_export_to_storage_impl(&self, params: WorkspaceExportToStorageParams) -> String {
        let (conn_name, client) = match self.resolve_storage(params.storage_connection.as_deref()).await {
            Ok(r) => r,
            Err(e) => return e,
        };
        if let Err(e) = self.check_storage_permission(&conn_name, &params.bucket, crate::auth::access_control::StoragePermAction::Write) {
            return e;
        }

        // Approval flow
        if self.needs_approval() {
            let context = format!("Export workspace query to {}/{}/{}", conn_name, params.bucket, params.key);
            if let Err(msg) = self
                .await_approval(
                    "workspace_export_to_storage",
                    vec![format!("WORKSPACE QUERY -> PUT {}/{}", params.bucket, params.key)],
                    &conn_name,
                    &params.bucket,
                    &context,
                )
                .await
            {
                return json!({"error": true, "message": msg}).to_string();
            }
        }

        let fmt = match crate::export::infer_export_format(&params.key, params.format.as_deref()) {
            Ok(f) => f,
            Err(e) => return json!({"error": true, "message": e}).to_string(),
        };

        let ws_db = match &self.workspace_db {
            Some(d) => d,
            None => return json!({"error": true, "message": "DuckDB workspace not available"}).to_string(),
        };

        let (data, row_count) = match fmt {
            crate::export::ExportFormat::Parquet => {
                let ws_dir = match &self.workspace_dir {
                    Some(d) => d,
                    None => return json!({"error": true, "message": "Workspace directory not available"}).to_string(),
                };

                let temp_path = ws_dir.join(format!("_export_tmp_{}.parquet", uuid::Uuid::new_v4()));
                let temp_path_str = temp_path.to_string_lossy().replace('\'', "''");

                let copy_sql = format!(
                    "COPY ({}) TO '{}' (FORMAT PARQUET)",
                    params.query, temp_path_str
                );

                if let Err(e) = ws_db.execute_sql(&copy_sql).await {
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    return json!({"error": true, "message": format!("Parquet export failed: {:#}", e)}).to_string();
                }

                let bytes = match tokio::fs::read(&temp_path).await {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tokio::fs::remove_file(&temp_path).await;
                        return json!({"error": true, "message": format!("Failed to read parquet file: {}", e)}).to_string();
                    }
                };
                let _ = tokio::fs::remove_file(&temp_path).await;

                let count_sql = format!("SELECT COUNT(*) FROM ({})", params.query);
                let row_count = ws_db.query_count(&count_sql).await.unwrap_or(0);

                (bytes, row_count)
            }
            _ => {
                let qp = QueryParams {
                    database: "workspace".to_string(),
                    query: params.query.clone(),
                    pagination: false,
                    include_metadata: true,
                    ..Default::default()
                };

                let result = match ws_db.execute_query(&qp).await {
                    Ok(r) => r,
                    Err(e) => return json!({"error": true, "message": format!("Workspace query failed: {:#}", e)}).to_string(),
                };

                let row_count = result.total_rows;

                let bytes = match fmt {
                    crate::export::ExportFormat::Csv => {
                        match crate::export::csv::query_result_to_csv(&result) {
                            Ok(d) => d,
                            Err(e) => return json!({"error": true, "message": format!("CSV conversion failed: {:#}", e)}).to_string(),
                        }
                    }
                    crate::export::ExportFormat::Json => {
                        match serde_json::to_vec_pretty(&result.data) {
                            Ok(d) => d,
                            Err(e) => return json!({"error": true, "message": format!("JSON conversion failed: {}", e)}).to_string(),
                        }
                    }
                    #[cfg(feature = "xlsx")]
                    crate::export::ExportFormat::Xlsx => {
                        match crate::export::xlsx::query_result_to_xlsx(&result) {
                            Ok(d) => d,
                            Err(e) => return json!({"error": true, "message": format!("XLSX conversion failed: {:#}", e)}).to_string(),
                        }
                    }
                    crate::export::ExportFormat::Parquet => unreachable!(),
                };

                (bytes, row_count)
            }
        };

        let size = data.len();

        match client.upload_object(&params.bucket, &params.key, &data, Some(fmt.content_type())).await {
            Ok(()) => {
                self.log_storage_op(&conn_name, "workspace_export", &format!("{}/{} ({})", params.bucket, params.key, fmt.as_str()));
                json!({
                    "success": true,
                    "bucket": params.bucket,
                    "key": params.key,
                    "size": size,
                    "row_count": row_count,
                    "format": fmt.as_str()
                }).to_string()
            }
            Err(e) => json!({"error": true, "message": format!("Upload failed: {:#}", e)}).to_string(),
        }
    }
}

#[cfg(all(feature = "storage", not(feature = "duckdb_backend")))]
impl BatchQueryMcp {
    async fn storage_download_to_workspace_impl(&self, _params: StorageDownloadToWorkspaceParams) -> String {
        json!({"error": true, "message": "storage_download_to_workspace requires both 'storage' and 'duckdb_backend' features"}).to_string()
    }
    async fn workspace_export_to_storage_impl(&self, _params: WorkspaceExportToStorageParams) -> String {
        json!({"error": true, "message": "workspace_export_to_storage requires both 'storage' and 'duckdb_backend' features"}).to_string()
    }
}

#[cfg(all(feature = "storage", feature = "duckdb_backend"))]
fn sanitize_table_name(filename: &str) -> String {
    let stem = filename.rsplit('.').skip(1).collect::<Vec<_>>();
    let stem = if stem.is_empty() {
        filename
    } else {
        &stem.into_iter().rev().collect::<Vec<_>>().join(".")
    };

    let sanitized: String = stem
        .to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect();

    let mut result = String::new();
    let mut prev_underscore = false;
    for c in sanitized.chars() {
        if c == '_' {
            if !prev_underscore && !result.is_empty() {
                result.push(c);
            }
            prev_underscore = true;
        } else {
            prev_underscore = false;
            result.push(c);
        }
    }
    let result = result.trim_end_matches('_').to_string();

    if result.is_empty() || result.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
        format!("t_{}", result)
    } else {
        result
    }
}

// ============================================================================
// Type mapping helpers for workspace export
// ============================================================================

/// Map a DuckDB column type back to a dialect-specific SQL type for export.
#[cfg(feature = "duckdb_backend")]
fn map_duckdb_type_to_dialect(duckdb_type: &str, dialect: crate::db::Dialect) -> String {
    let upper = duckdb_type.to_uppercase();
    match dialect {
        crate::db::Dialect::Mssql => match upper.as_str() {
            "TINYINT" | "SMALLINT" | "INTEGER" | "INT" | "BIGINT" | "HUGEINT"
            | "UTINYINT" | "USMALLINT" | "UINTEGER" | "UBIGINT" => "BIGINT".to_string(),
            "FLOAT" | "REAL" => "FLOAT".to_string(),
            "DOUBLE" | "DOUBLE PRECISION" => "FLOAT".to_string(),
            "BOOLEAN" | "BOOL" => "BIT".to_string(),
            "DATE" => "DATE".to_string(),
            "TIME" => "TIME".to_string(),
            "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "DATETIME2".to_string(),
            "BLOB" => "VARBINARY(MAX)".to_string(),
            _ if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") => upper.clone(),
            _ => "NVARCHAR(MAX)".to_string(),
        },
        crate::db::Dialect::Postgres => match upper.as_str() {
            "TINYINT" | "SMALLINT" | "INTEGER" | "INT" | "BIGINT" | "HUGEINT"
            | "UTINYINT" | "USMALLINT" | "UINTEGER" | "UBIGINT" => "BIGINT".to_string(),
            "FLOAT" | "REAL" => "DOUBLE PRECISION".to_string(),
            "DOUBLE" | "DOUBLE PRECISION" => "DOUBLE PRECISION".to_string(),
            "BOOLEAN" | "BOOL" => "BOOLEAN".to_string(),
            "DATE" => "DATE".to_string(),
            "TIME" => "TIME".to_string(),
            "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "TIMESTAMP".to_string(),
            "BLOB" => "BYTEA".to_string(),
            _ if upper.starts_with("DECIMAL") => upper.replace("DECIMAL", "NUMERIC"),
            _ if upper.starts_with("NUMERIC") => upper.clone(),
            _ => "TEXT".to_string(),
        },
        crate::db::Dialect::ClickHouse => match upper.as_str() {
            "TINYINT" | "SMALLINT" | "INTEGER" | "INT" | "BIGINT" | "HUGEINT"
            | "UTINYINT" | "USMALLINT" | "UINTEGER" | "UBIGINT" => "Int64".to_string(),
            "FLOAT" | "REAL" => "Float64".to_string(),
            "DOUBLE" | "DOUBLE PRECISION" => "Float64".to_string(),
            "BOOLEAN" | "BOOL" => "Bool".to_string(),
            "DATE" => "Date".to_string(),
            "TIME" => "String".to_string(),
            "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "DateTime".to_string(),
            "BLOB" => "String".to_string(),
            _ if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") => upper.clone(),
            _ => "String".to_string(),
        },
        crate::db::Dialect::DuckDb => upper.clone(), // shouldn't happen, guarded above
    }
}

/// Map a DuckDB type string to an InferredType for escape_value to handle correctly.
#[cfg(feature = "duckdb_backend")]
fn duckdb_type_to_inferred(duckdb_type: &str) -> InferredType {
    let upper = duckdb_type.to_uppercase();
    match upper.as_str() {
        "BOOLEAN" | "BOOL" => InferredType::Boolean,
        "TINYINT" | "SMALLINT" | "INTEGER" | "INT" | "BIGINT" | "HUGEINT"
        | "UTINYINT" | "USMALLINT" | "UINTEGER" | "UBIGINT" => InferredType::Integer,
        "FLOAT" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" => InferredType::Float,
        "DATE" => InferredType::Date,
        "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => InferredType::DateTime,
        _ if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") => InferredType::Float,
        _ => InferredType::Text,
    }
}

// ============================================================================
// navigate_tables helper functions
// ============================================================================

/// Build a dialect-aware SELECT query for navigation.
fn build_nav_select_query(
    dialect: crate::db::Dialect,
    schema: &str,
    table: &str,
    filter: &str,
    limit: i64,
) -> String {
    let qualified = format!(
        "[{}].[{}]",
        schema.replace(']', "]]"),
        table.replace(']', "]]")
    );
    match dialect {
        crate::db::Dialect::Mssql => {
            format!("SELECT TOP {} * FROM {} WHERE {}", limit, qualified, filter)
        }
        _ => {
            // Postgres / DuckDB use LIMIT
            let qualified_pg = format!(
                "\"{}\".\"{}\"",
                schema.replace('"', "\"\""),
                table.replace('"', "\"\"")
            );
            format!("SELECT * FROM {} WHERE {} LIMIT {}", qualified_pg, filter, limit)
        }
    }
}

/// Build a WHERE filter to follow FK relationships from source rows.
///
/// For single-column FKs: `target_col IN (val1, val2, ...)`
/// For composite FKs: `(target_col1 = v1 AND target_col2 = v2) OR ...`
fn build_fk_filter(
    source_rows: &[std::collections::HashMap<String, serde_json::Value>],
    source_columns: &[String],
    target_columns: &[String],
) -> String {
    if source_columns.is_empty() || target_columns.is_empty() || source_rows.is_empty() {
        return String::new();
    }

    if source_columns.len() == 1 {
        // Single-column FK: use IN clause
        let src_col = &source_columns[0];
        let tgt_col = &target_columns[0];
        let mut values: Vec<String> = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for row in source_rows {
            if let Some(val) = row.get(src_col) {
                if !val.is_null() {
                    let formatted = format_value_for_sql(val);
                    if seen.insert(formatted.clone()) {
                        values.push(formatted);
                    }
                }
            }
        }

        if values.is_empty() {
            return String::new();
        }

        format!("[{}] IN ({})", tgt_col, values.join(", "))
    } else {
        // Composite FK: use OR of AND conditions
        let mut conditions: Vec<String> = Vec::new();

        for row in source_rows {
            let mut parts: Vec<String> = Vec::new();
            let mut all_present = true;

            for (src_col, tgt_col) in source_columns.iter().zip(target_columns.iter()) {
                if let Some(val) = row.get(src_col) {
                    if val.is_null() {
                        all_present = false;
                        break;
                    }
                    parts.push(format!("[{}] = {}", tgt_col, format_value_for_sql(val)));
                } else {
                    all_present = false;
                    break;
                }
            }

            if all_present && !parts.is_empty() {
                conditions.push(format!("({})", parts.join(" AND ")));
            }
        }

        if conditions.is_empty() {
            return String::new();
        }

        conditions.join(" OR ")
    }
}

/// Format a JSON value for use in a SQL literal.
fn format_value_for_sql(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => if *b { "1".to_string() } else { "0".to_string() },
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        serde_json::Value::Null => "NULL".to_string(),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}
