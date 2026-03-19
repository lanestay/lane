use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use chrono::{NaiveDateTime, Utc};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlMode {
    None,
    ReadOnly,
    Supervised,
    Confirmed,
    Full,
}

impl SqlMode {
    pub fn from_db(s: &str) -> Self {
        match s {
            "read_only" => Self::ReadOnly,
            "supervised" => Self::Supervised,
            "confirmed" => Self::Confirmed,
            "full" => Self::Full,
            _ => Self::None,
        }
    }
    #[allow(dead_code)]
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ReadOnly => "read_only",
            Self::Supervised => "supervised",
            Self::Confirmed => "confirmed",
            Self::Full => "full",
        }
    }
    #[allow(dead_code)]
    pub fn allows_read(&self) -> bool {
        !matches!(self, Self::None)
    }
    #[allow(dead_code)]
    pub fn allows_dml(&self) -> bool {
        matches!(self, Self::Supervised | Self::Confirmed | Self::Full)
    }
    #[allow(dead_code)]
    pub fn allows_ddl(&self) -> bool {
        matches!(self, Self::Full)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenInfo {
    pub token_prefix: String,
    pub email: String,
    pub label: Option<String>,
    pub expires_at: Option<String>,
    pub is_active: bool,
    pub pii_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UserInfo {
    pub email: String,
    pub display_name: Option<String>,
    pub is_admin: bool,
    pub is_enabled: bool,
    pub mcp_enabled: bool,
    pub pii_mode: Option<String>,
    pub sql_mode: String,
    pub max_pending_approvals: Option<u32>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Permission {
    pub id: i64,
    pub email: String,
    pub database_name: String,
    pub table_pattern: String,
    pub can_read: bool,
    pub can_write: bool,
    pub can_update: bool,
    pub can_delete: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenRecord {
    pub token_prefix: String,
    pub email: String,
    pub label: Option<String>,
    pub expires_at: Option<String>,
    pub is_active: bool,
    pub created_at: String,
    pub pii_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RealtimeTableEntry {
    pub connection_name: String,
    pub database_name: String,
    pub table_name: String,
    pub enabled_by: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RestTableEntry {
    pub connection_name: String,
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub enabled_by: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditEntry {
    pub id: i64,
    pub token_prefix: Option<String>,
    pub email: Option<String>,
    pub source_ip: Option<String>,
    pub database_name: Option<String>,
    pub query_type: Option<String>,
    pub action: Option<String>,
    pub details: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionInfo {
    pub email: String,
    pub is_admin: bool,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryHistoryEntry {
    pub id: i64,
    pub email: String,
    pub connection_name: Option<String>,
    pub database_name: Option<String>,
    pub sql_text: String,
    pub execution_time_ms: Option<i64>,
    pub row_count: Option<i64>,
    pub is_success: bool,
    pub error_message: Option<String>,
    pub is_favorite: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredConnection {
    pub name: String,
    pub conn_type: String,
    pub host: String,
    pub port: u16,
    pub database_name: String,
    pub username: String,
    pub password: String,
    #[serde(default = "default_options_json")]
    pub options_json: String,
    pub sslmode: Option<String>,
    #[serde(default)]
    pub is_default: bool,
    #[serde(default = "default_true")]
    pub is_enabled: bool,
}

fn default_options_json() -> String {
    "{}".to_string()
}

fn default_true() -> bool {
    true
}

impl StoredConnection {
    pub fn to_named_connection(&self) -> crate::config::NamedConnection {
        use crate::config::*;
        let config = match self.conn_type.as_str() {
            "postgres" => ConnectionConfig::Postgres(PostgresConnectionConfig {
                host: self.host.clone(),
                port: self.port,
                database: self.database_name.clone(),
                user: self.username.clone(),
                password: self.password.clone(),
                sslmode: self.sslmode.clone(),
            }),
            #[cfg(feature = "duckdb_backend")]
            "duckdb" => {
                let read_only: Option<bool> = serde_json::from_str(&self.options_json)
                    .ok()
                    .and_then(|v: serde_json::Value| v.get("read_only").and_then(|r| r.as_bool()));
                ConnectionConfig::DuckDb(DuckDbConnectionConfig {
                    path: self.host.clone(),
                    read_only,
                })
            }
            #[cfg(feature = "storage")]
            "minio" => {
                let opts: serde_json::Value =
                    serde_json::from_str(&self.options_json).unwrap_or_default();
                let region = opts
                    .get("region")
                    .and_then(|v| v.as_str())
                    .unwrap_or("us-east-1")
                    .to_string();
                let path_style = opts
                    .get("path_style")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                ConnectionConfig::Minio(MinioConnectionConfig {
                    endpoint: self.host.clone(),
                    port: self.port,
                    access_key: self.username.clone(),
                    secret_key: self.password.clone(),
                    region,
                    path_style,
                })
            }
            _ => {
                let options: DbOptions =
                    serde_json::from_str(&self.options_json).unwrap_or_default();
                ConnectionConfig::Mssql(MssqlConnectionConfig {
                    server: self.host.clone(),
                    port: self.port,
                    database: self.database_name.clone(),
                    user: self.username.clone(),
                    password: self.password.clone(),
                    options,
                })
            }
        };
        NamedConnection {
            name: self.name.clone(),
            config,
        }
    }

    pub fn from_named_connection(nc: &crate::config::NamedConnection) -> Self {
        use crate::config::ConnectionConfig;
        match &nc.config {
            ConnectionConfig::Mssql(c) => StoredConnection {
                name: nc.name.clone(),
                conn_type: "mssql".to_string(),
                host: c.server.clone(),
                port: c.port,
                database_name: c.database.clone(),
                username: c.user.clone(),
                password: c.password.clone(),
                options_json: serde_json::to_string(&c.options).unwrap_or_else(|_| "{}".to_string()),
                sslmode: None,
                is_default: false,
                is_enabled: true,
            },
            ConnectionConfig::Postgres(c) => StoredConnection {
                name: nc.name.clone(),
                conn_type: "postgres".to_string(),
                host: c.host.clone(),
                port: c.port,
                database_name: c.database.clone(),
                username: c.user.clone(),
                password: c.password.clone(),
                options_json: "{}".to_string(),
                sslmode: c.sslmode.clone(),
                is_default: false,
                is_enabled: true,
            },
            #[cfg(feature = "duckdb_backend")]
            ConnectionConfig::DuckDb(c) => {
                let options = if c.read_only == Some(true) {
                    r#"{"read_only":true}"#.to_string()
                } else {
                    "{}".to_string()
                };
                StoredConnection {
                    name: nc.name.clone(),
                    conn_type: "duckdb".to_string(),
                    host: c.path.clone(),
                    port: 0,
                    database_name: c.path.clone(),
                    username: String::new(),
                    password: String::new(),
                    options_json: options,
                    sslmode: None,
                    is_default: false,
                    is_enabled: true,
                }
            }
            #[cfg(feature = "storage")]
            ConnectionConfig::Minio(c) => {
                let options = serde_json::json!({
                    "region": c.region,
                    "path_style": c.path_style,
                });
                StoredConnection {
                    name: nc.name.clone(),
                    conn_type: "minio".to_string(),
                    host: c.endpoint.clone(),
                    port: c.port,
                    database_name: String::new(),
                    username: c.access_key.clone(),
                    password: c.secret_key.clone(),
                    options_json: options.to_string(),
                    sslmode: None,
                    is_default: false,
                    is_enabled: true,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiiRule {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub regex_pattern: String,
    pub replacement_text: String,
    pub entity_kind: String,
    pub is_builtin: bool,
    pub is_enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiiColumn {
    pub id: i64,
    pub connection_name: String,
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
    pub pii_type: String,
    pub custom_replacement: Option<String>,
    pub created_at: String,
}

#[cfg(feature = "storage")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageColumnLink {
    pub id: i64,
    pub connection_name: String,
    pub database_name: String,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub column_name: String,
    pub storage_connection: String,
    pub bucket_name: String,
    pub key_prefix: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    pub name: String,
    pub connection_name: String,
    pub database_name: String,
    pub query: String,
    pub description: Option<String>,
    pub parameters: Option<String>,
    pub created_by: Option<String>,
    pub updated_at: String,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub email: String,
    pub display_name: Option<String>,
    pub is_admin: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateUserRequest {
    pub display_name: Option<String>,
    pub is_admin: Option<bool>,
    pub is_enabled: Option<bool>,
    pub mcp_enabled: Option<bool>,
    pub pii_mode: Option<String>,
    pub sql_mode: Option<String>,
    pub max_pending_approvals: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct SetPermissionsRequest {
    pub email: String,
    pub permissions: Vec<PermissionEntry>,
}

#[derive(Debug, Deserialize)]
pub struct PermissionEntry {
    pub database_name: String,
    pub table_pattern: Option<String>,
    pub can_read: Option<bool>,
    pub can_write: Option<bool>,
    pub can_update: Option<bool>,
    pub can_delete: Option<bool>,
}

/// Fine-grained permission action type.
pub enum PermAction {
    Read,
    Insert,
    Update,
    Delete,
}

/// Storage permission action type.
#[cfg(feature = "storage")]
#[derive(Debug, Clone, Copy)]
pub enum StoragePermAction {
    Read,
    Write,
    Delete,
}

/// Storage permission record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoragePermission {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub identity: String,
    pub connection_name: String,
    #[serde(default = "default_star")]
    pub bucket_pattern: String,
    #[serde(default = "default_true")]
    pub can_read: bool,
    #[serde(default)]
    pub can_write: bool,
    #[serde(default)]
    pub can_delete: bool,
}

fn default_star() -> String {
    "*".to_string()
}

/// Input entry for setting storage permissions (from API).
#[derive(Debug, Deserialize)]
pub struct StoragePermissionEntry {
    pub connection_name: String,
    pub bucket_pattern: Option<String>,
    pub can_read: Option<bool>,
    pub can_write: Option<bool>,
    pub can_delete: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct SetStoragePermissionsRequest {
    pub email: String,
    pub permissions: Vec<StoragePermissionEntry>,
}

#[derive(Debug, Deserialize)]
pub struct SetSaStoragePermissionsRequest {
    pub name: String,
    pub permissions: Vec<StoragePermissionEntry>,
}

#[derive(Debug, Deserialize)]
pub struct GenerateTokenRequest {
    pub email: String,
    pub label: Option<String>,
    /// Hours until expiry. None = never expires.
    pub expires_hours: Option<u64>,
    pub pii_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceAccountInfo {
    pub name: String,
    pub description: Option<String>,
    pub api_key_prefix: String,
    pub sql_mode: String,
    pub is_enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateServiceAccountRequest {
    pub name: String,
    pub description: Option<String>,
    pub sql_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateServiceAccountRequest {
    pub description: Option<String>,
    pub sql_mode: Option<String>,
    pub is_enabled: Option<bool>,
}

// ============================================================================
// Database
// ============================================================================

pub struct AccessControlDb {
    conn: Mutex<Connection>,
}

impl AccessControlDb {
    /// Open (or create) the encrypted SQLite database.
    pub fn new(path: &str, key: &str) -> Result<Self, String> {
        let conn = Connection::open(path).map_err(|e| format!("Failed to open SQLite: {}", e))?;

        // Set the encryption key
        conn.pragma_update(None, "key", key)
            .map_err(|e| format!("Failed to set SQLCipher key: {}", e))?;

        // Verify the key works by reading from the DB
        conn.execute_batch("SELECT count(*) FROM sqlite_master;")
            .map_err(|e| format!("SQLCipher key verification failed (wrong key?): {}", e))?;

        // Enable WAL mode for concurrent reads
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| format!("Failed to set WAL mode: {}", e))?;

        // Enable foreign keys
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(|e| format!("Failed to enable foreign keys: {}", e))?;

        // Create tables if they don't exist
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS users (
                email TEXT PRIMARY KEY,
                display_name TEXT,
                is_admin INTEGER DEFAULT 0,
                is_enabled INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS tokens (
                token TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                label TEXT,
                expires_at TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (email) REFERENCES users(email) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                database_name TEXT NOT NULL,
                table_pattern TEXT DEFAULT '*',
                can_read INTEGER DEFAULT 1,
                can_write INTEGER DEFAULT 0,
                UNIQUE(email, database_name, table_pattern),
                FOREIGN KEY (email) REFERENCES users(email) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS access_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_prefix TEXT,
                email TEXT,
                source_ip TEXT,
                database_name TEXT,
                query_type TEXT,
                action TEXT,
                details TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS system_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                session_token TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                expires_at TEXT NOT NULL,
                last_active_at TEXT DEFAULT (datetime('now')),
                ip_address TEXT,
                user_agent TEXT,
                FOREIGN KEY (email) REFERENCES users(email) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS oauth_states (
                state TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                pkce_verifier TEXT NOT NULL,
                redirect_uri TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS connections (
                name TEXT PRIMARY KEY,
                conn_type TEXT NOT NULL CHECK(conn_type IN ('mssql', 'postgres', 'duckdb', 'minio')),
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                database_name TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                options_json TEXT DEFAULT '{}',
                sslmode TEXT,
                is_default INTEGER DEFAULT 0,
                is_enabled INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS query_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                connection_name TEXT,
                database_name TEXT,
                sql_text TEXT NOT NULL,
                execution_time_ms INTEGER,
                row_count INTEGER,
                is_success INTEGER DEFAULT 1,
                error_message TEXT,
                is_favorite INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (email) REFERENCES users(email) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS connection_permissions (
                email TEXT NOT NULL,
                connection_name TEXT NOT NULL,
                UNIQUE(email, connection_name),
                FOREIGN KEY (email) REFERENCES users(email) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS pii_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                regex_pattern TEXT NOT NULL,
                replacement_text TEXT NOT NULL,
                entity_kind TEXT NOT NULL,
                is_builtin INTEGER DEFAULT 0,
                is_enabled INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS pii_columns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_name TEXT NOT NULL,
                database_name TEXT NOT NULL,
                schema_name TEXT NOT NULL DEFAULT 'dbo',
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                pii_type TEXT NOT NULL DEFAULT 'auto',
                custom_replacement TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(connection_name, database_name, schema_name, table_name, column_name)
            );

            CREATE TABLE IF NOT EXISTS storage_column_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_name TEXT NOT NULL,
                database_name TEXT NOT NULL,
                schema_name TEXT,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                storage_connection TEXT NOT NULL,
                bucket_name TEXT NOT NULL,
                key_prefix TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(connection_name, database_name, table_name, column_name)
            );

            CREATE TABLE IF NOT EXISTS realtime_tables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_name TEXT NOT NULL,
                database_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                enabled_by TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(connection_name, database_name, table_name)
            );

            CREATE TABLE IF NOT EXISTS rest_tables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_name TEXT NOT NULL,
                database_name TEXT NOT NULL,
                schema_name TEXT NOT NULL DEFAULT 'dbo',
                table_name TEXT NOT NULL,
                enabled_by TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(connection_name, database_name, schema_name, table_name)
            );

            CREATE TABLE IF NOT EXISTS service_accounts (
                name TEXT PRIMARY KEY,
                description TEXT,
                api_key TEXT NOT NULL UNIQUE,
                sql_mode TEXT NOT NULL DEFAULT 'full',
                is_enabled INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS service_account_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL,
                database_name TEXT NOT NULL,
                table_pattern TEXT DEFAULT '*',
                can_read INTEGER DEFAULT 1,
                can_write INTEGER DEFAULT 0,
                can_update INTEGER DEFAULT 0,
                can_delete INTEGER DEFAULT 0,
                UNIQUE(account_name, database_name, table_pattern),
                FOREIGN KEY (account_name) REFERENCES service_accounts(name) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS service_account_connections (
                account_name TEXT NOT NULL,
                connection_name TEXT NOT NULL,
                UNIQUE(account_name, connection_name),
                FOREIGN KEY (account_name) REFERENCES service_accounts(name) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS connection_health_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_name TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                checked_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_health_hist_conn_time
                ON connection_health_history(connection_name, checked_at);

            CREATE TABLE IF NOT EXISTS teams (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                webhook_url TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                team_id TEXT NOT NULL,
                name TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(team_id, name),
                FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS team_members (
                team_id TEXT NOT NULL,
                email TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'member',
                UNIQUE(team_id, email),
                FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE,
                FOREIGN KEY (email) REFERENCES users(email) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS project_members (
                project_id TEXT NOT NULL,
                email TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'member',
                UNIQUE(project_id, email),
                FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
                FOREIGN KEY (email) REFERENCES users(email) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS storage_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                connection_name TEXT NOT NULL,
                bucket_pattern TEXT NOT NULL DEFAULT '*',
                can_read INTEGER DEFAULT 1,
                can_write INTEGER DEFAULT 0,
                can_delete INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(email, connection_name, bucket_pattern),
                FOREIGN KEY (email) REFERENCES users(email) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sa_storage_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL,
                connection_name TEXT NOT NULL,
                bucket_pattern TEXT NOT NULL DEFAULT '*',
                can_read INTEGER DEFAULT 1,
                can_write INTEGER DEFAULT 0,
                can_delete INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(account_name, connection_name, bucket_pattern),
                FOREIGN KEY (account_name) REFERENCES service_accounts(name) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS endpoints (
                name TEXT PRIMARY KEY,
                connection_name TEXT NOT NULL,
                database_name TEXT NOT NULL,
                query TEXT NOT NULL,
                description TEXT,
                parameters TEXT,
                created_by TEXT,
                updated_at TEXT DEFAULT (datetime('now')),
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS endpoint_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                endpoint_name TEXT NOT NULL,
                UNIQUE(email, endpoint_name),
                FOREIGN KEY (email) REFERENCES users(email) ON DELETE CASCADE,
                FOREIGN KEY (endpoint_name) REFERENCES endpoints(name) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sa_endpoint_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL,
                endpoint_name TEXT NOT NULL,
                UNIQUE(account_name, endpoint_name),
                FOREIGN KEY (account_name) REFERENCES service_accounts(name) ON DELETE CASCADE,
                FOREIGN KEY (endpoint_name) REFERENCES endpoints(name) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS email_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                code_hash TEXT NOT NULL,
                attempts INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                consumed_at TEXT,
                FOREIGN KEY (email) REFERENCES users(email) ON DELETE CASCADE
            );
            ",
        )
        .map_err(|e| format!("Failed to create tables: {}", e))?;

        // Migrate: add password_hash and phone columns if missing
        let has_password_hash: bool = conn
            .prepare("PRAGMA table_info(users)")
            .and_then(|mut stmt| {
                let cols: Vec<String> = stmt
                    .query_map([], |row| row.get::<_, String>(1))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
                Ok(cols.contains(&"password_hash".to_string()))
            })
            .unwrap_or(false);

        if !has_password_hash {
            conn.execute_batch("ALTER TABLE users ADD COLUMN password_hash TEXT;")
                .map_err(|e| format!("Failed to add password_hash column: {}", e))?;
        }

        let has_phone: bool = conn
            .prepare("PRAGMA table_info(users)")
            .and_then(|mut stmt| {
                let cols: Vec<String> = stmt
                    .query_map([], |row| row.get::<_, String>(1))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
                Ok(cols.contains(&"phone".to_string()))
            })
            .unwrap_or(false);

        if !has_phone {
            conn.execute_batch("ALTER TABLE users ADD COLUMN phone TEXT;")
                .map_err(|e| format!("Failed to add phone column: {}", e))?;
        }

        // Migrate: add mcp_enabled column if missing
        let has_mcp_enabled: bool = conn
            .prepare("PRAGMA table_info(users)")
            .and_then(|mut stmt| {
                let cols: Vec<String> = stmt
                    .query_map([], |row| row.get::<_, String>(1))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
                Ok(cols.contains(&"mcp_enabled".to_string()))
            })
            .unwrap_or(false);

        if !has_mcp_enabled {
            conn.execute_batch("ALTER TABLE users ADD COLUMN mcp_enabled INTEGER NOT NULL DEFAULT 1;")
                .map_err(|e| format!("Failed to add mcp_enabled column: {}", e))?;
        }

        // Migration: add pii_mode column to tokens
        let has_token_pii_mode = conn
            .prepare("PRAGMA table_info(tokens)")
            .and_then(|mut stmt| {
                let cols: Vec<String> = stmt
                    .query_map([], |row| row.get::<_, String>(1))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
                Ok(cols.contains(&"pii_mode".to_string()))
            })
            .unwrap_or(false);

        if !has_token_pii_mode {
            conn.execute_batch("ALTER TABLE tokens ADD COLUMN pii_mode TEXT;")
                .map_err(|e| format!("Failed to add pii_mode column to tokens: {}", e))?;
        }

        // Migration: add pii_mode column to users
        let has_user_pii_mode = conn
            .prepare("PRAGMA table_info(users)")
            .and_then(|mut stmt| {
                let cols: Vec<String> = stmt
                    .query_map([], |row| row.get::<_, String>(1))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
                Ok(cols.contains(&"pii_mode".to_string()))
            })
            .unwrap_or(false);

        if !has_user_pii_mode {
            conn.execute_batch("ALTER TABLE users ADD COLUMN pii_mode TEXT;")
                .map_err(|e| format!("Failed to add pii_mode column to users: {}", e))?;
        }

        // Migration: add raw_sql_enabled column to users
        {
            let has_raw_sql: bool = conn
                .prepare("PRAGMA table_info(users)")
                .and_then(|mut stmt| {
                    let cols: Vec<String> = stmt
                        .query_map([], |row| row.get::<_, String>(1))
                        .unwrap()
                        .filter_map(|r| r.ok())
                        .collect();
                    Ok(cols.contains(&"raw_sql_enabled".to_string()))
                })
                .unwrap_or(false);

            if !has_raw_sql {
                conn.execute_batch("ALTER TABLE users ADD COLUMN raw_sql_enabled INTEGER NOT NULL DEFAULT 0;")
                    .map_err(|e| format!("Failed to add raw_sql_enabled column: {}", e))?;
            }
        }

        // Migration: add can_update and can_delete columns to permissions
        {
            let perm_cols: Vec<String> = conn
                .prepare("PRAGMA table_info(permissions)")
                .and_then(|mut stmt| {
                    Ok(stmt
                        .query_map([], |row| row.get::<_, String>(1))
                        .unwrap()
                        .filter_map(|r| r.ok())
                        .collect())
                })
                .unwrap_or_default();

            if !perm_cols.contains(&"can_update".to_string()) {
                conn.execute_batch("ALTER TABLE permissions ADD COLUMN can_update INTEGER DEFAULT 0;")
                    .map_err(|e| format!("Failed to add can_update column: {}", e))?;
                // Backfill: existing can_write=1 rows get can_update=1
                conn.execute_batch("UPDATE permissions SET can_update = 1 WHERE can_write = 1;")
                    .map_err(|e| format!("Failed to backfill can_update: {}", e))?;
            }

            if !perm_cols.contains(&"can_delete".to_string()) {
                conn.execute_batch("ALTER TABLE permissions ADD COLUMN can_delete INTEGER DEFAULT 0;")
                    .map_err(|e| format!("Failed to add can_delete column: {}", e))?;
                // Backfill: existing can_write=1 rows get can_delete=1
                conn.execute_batch("UPDATE permissions SET can_delete = 1 WHERE can_write = 1;")
                    .map_err(|e| format!("Failed to backfill can_delete: {}", e))?;
            }
        }

        // Migration: widen connections CHECK constraint to include 'duckdb'
        // SQLite can't ALTER CHECK constraints, so recreate the table if needed.
        {
            let has_duckdb_check: bool = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='connections'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .map(|sql| sql.contains("duckdb"))
                .unwrap_or(true); // if table doesn't exist, skip (will be created fresh)

            if !has_duckdb_check {
                conn.execute_batch(
                    "
                    ALTER TABLE connections RENAME TO connections_old;
                    CREATE TABLE connections (
                        name TEXT PRIMARY KEY,
                        conn_type TEXT NOT NULL CHECK(conn_type IN ('mssql', 'postgres', 'duckdb')),
                        host TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        database_name TEXT NOT NULL,
                        username TEXT NOT NULL,
                        password TEXT NOT NULL,
                        options_json TEXT DEFAULT '{}',
                        sslmode TEXT,
                        is_default INTEGER DEFAULT 0,
                        is_enabled INTEGER DEFAULT 1,
                        created_at TEXT DEFAULT (datetime('now')),
                        updated_at TEXT DEFAULT (datetime('now'))
                    );
                    INSERT INTO connections SELECT * FROM connections_old;
                    DROP TABLE connections_old;
                    ",
                )
                .map_err(|e| format!("Failed to migrate connections table for duckdb: {}", e))?;
            }
        }

        // Migration: widen connections CHECK constraint to include 'minio'
        {
            let has_minio_check: bool = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='connections'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .map(|sql| sql.contains("minio"))
                .unwrap_or(true);

            if !has_minio_check {
                conn.execute_batch(
                    "
                    ALTER TABLE connections RENAME TO connections_old;
                    CREATE TABLE connections (
                        name TEXT PRIMARY KEY,
                        conn_type TEXT NOT NULL CHECK(conn_type IN ('mssql', 'postgres', 'duckdb', 'minio')),
                        host TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        database_name TEXT NOT NULL,
                        username TEXT NOT NULL,
                        password TEXT NOT NULL,
                        options_json TEXT DEFAULT '{}',
                        sslmode TEXT,
                        is_default INTEGER DEFAULT 0,
                        is_enabled INTEGER DEFAULT 1,
                        created_at TEXT DEFAULT (datetime('now')),
                        updated_at TEXT DEFAULT (datetime('now'))
                    );
                    INSERT INTO connections SELECT * FROM connections_old;
                    DROP TABLE connections_old;
                    ",
                )
                .map_err(|e| format!("Failed to migrate connections table for minio: {}", e))?;
            }
        }

        // Migration: add sql_mode column and backfill from raw_sql_enabled
        {
            let has_sql_mode: bool = conn
                .prepare("PRAGMA table_info(users)")
                .and_then(|mut stmt| {
                    let cols: Vec<String> = stmt
                        .query_map([], |row| row.get::<_, String>(1))
                        .unwrap()
                        .filter_map(|r| r.ok())
                        .collect();
                    Ok(cols.contains(&"sql_mode".to_string()))
                })
                .unwrap_or(false);

            if !has_sql_mode {
                conn.execute_batch(
                    "ALTER TABLE users ADD COLUMN sql_mode TEXT NOT NULL DEFAULT 'none';
                     UPDATE users SET sql_mode = 'full' WHERE raw_sql_enabled = 1;
                     UPDATE users SET sql_mode = 'supervised' WHERE is_admin = 1 AND sql_mode = 'none';",
                )
                .map_err(|e| format!("Failed to add sql_mode column: {}", e))?;
            }
        }

        // Migration: create approval_audit table
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS approval_audit (
                id TEXT PRIMARY KEY,
                user_email TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                target_connection TEXT,
                target_database TEXT,
                sql_preview TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                reason TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                decided_at TEXT
            );"
        ).map_err(|e| format!("Failed to create approval_audit table: {}", e))?;

        // Migration: add max_pending_approvals column to users
        {
            let has_col: bool = conn
                .prepare("PRAGMA table_info(users)")
                .and_then(|mut stmt| {
                    let cols: Vec<String> = stmt
                        .query_map([], |row| row.get::<_, String>(1))
                        .unwrap()
                        .filter_map(|r| r.ok())
                        .collect();
                    Ok(cols.contains(&"max_pending_approvals".to_string()))
                })
                .unwrap_or(false);

            if !has_col {
                conn.execute_batch(
                    "ALTER TABLE users ADD COLUMN max_pending_approvals INTEGER;"
                )
                .map_err(|e| format!("Failed to add max_pending_approvals column: {}", e))?;
            }
        }

        // Migration: rename supervised → confirmed to preserve self-approve behavior
        {
            let already_migrated: bool = conn
                .query_row(
                    "SELECT value FROM system_config WHERE key = 'confirmed_migration'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .map(|v| v == "true")
                .unwrap_or(false);

            if !already_migrated {
                conn.execute_batch(
                    "UPDATE users SET sql_mode = 'confirmed' WHERE sql_mode = 'supervised';
                     UPDATE service_accounts SET sql_mode = 'confirmed' WHERE sql_mode = 'supervised';
                     INSERT INTO system_config (key, value) VALUES ('confirmed_migration', 'true')
                       ON CONFLICT(key) DO UPDATE SET value = 'true';",
                )
                .map_err(|e| format!("Failed to migrate supervised→confirmed: {}", e))?;
            }
        }

        let db = Self {
            conn: Mutex::new(conn),
        };

        // Seed built-in PII rules
        db.seed_builtin_pii_rules();

        Ok(db)
    }

    // ========================================================================
    // Token validation
    // ========================================================================

    /// Validate a token and return info about it.
    /// Returns Err if the token is not found, expired, inactive, or user disabled.
    pub fn validate_token(&self, token: &str) -> Result<TokenInfo, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let result = conn.query_row(
            "SELECT t.token, t.email, t.label, t.expires_at, t.is_active, u.is_enabled, t.pii_mode
             FROM tokens t
             JOIN users u ON t.email = u.email
             WHERE t.token = ?1",
            params![token],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, bool>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            },
        );

        match result {
            Err(rusqlite::Error::QueryReturnedNoRows) => Err("Token not found".to_string()),
            Err(e) => Err(format!("Database error: {}", e)),
            Ok((full_token, email, label, expires_at, is_active, is_enabled, pii_mode)) => {
                if !is_active {
                    return Err("Token has been revoked".to_string());
                }
                if !is_enabled {
                    return Err("User account is disabled".to_string());
                }

                // Check expiry
                if let Some(ref exp) = expires_at {
                    if let Ok(exp_dt) = NaiveDateTime::parse_from_str(exp, "%Y-%m-%d %H:%M:%S") {
                        if Utc::now().naive_utc() > exp_dt {
                            return Err("Token expired".to_string());
                        }
                    }
                }

                Ok(TokenInfo {
                    token_prefix: full_token[..8.min(full_token.len())].to_string(),
                    email,
                    label,
                    expires_at,
                    is_active,
                    pii_mode,
                })
            }
        }
    }

    // ========================================================================
    // Permission checking
    // ========================================================================

    /// Check if a user has permission for a given database and query type.
    pub fn check_permission(&self, email: &str, database: &str, is_write: bool) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        // Check for wildcard database permission first, then specific database
        let result = conn.query_row(
            "SELECT can_read, can_write FROM permissions
             WHERE email = ?1 AND (database_name = ?2 OR database_name = '*')
             ORDER BY CASE WHEN database_name = '*' THEN 1 ELSE 0 END
             LIMIT 1",
            params![email, database],
            |row| Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?)),
        );

        match result {
            Ok((can_read, can_write)) => {
                if is_write {
                    can_write
                } else {
                    can_read
                }
            }
            Err(_) => false,
        }
    }

    /// Check if a user has permission for a given database + table and query type.
    /// Matches table_pattern against the table name:
    ///   - `*` matches all tables
    ///   - Exact match (case-insensitive)
    ///   - Suffix `*` means prefix match (e.g. `Orders*` matches `OrderDetails`)
    pub fn check_table_permission(&self, email: &str, database: &str, table: &str, is_write: bool) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        // Get all permission rows for (email, database) and (email, *)
        let mut stmt = match conn.prepare(
            "SELECT table_pattern, can_read, can_write FROM permissions
             WHERE email = ?1 AND (database_name = ?2 OR database_name = '*')",
        ) {
            Ok(s) => s,
            Err(_) => return false,
        };

        let rows = match stmt.query_map(params![email, database], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, bool>(2)?,
            ))
        }) {
            Ok(r) => r,
            Err(_) => return false,
        };

        for row in rows {
            if let Ok((pattern, can_read, can_write)) = row {
                let access = if is_write { can_write } else { can_read };
                if !access {
                    continue;
                }

                // Match the pattern against the table name
                if pattern == "*" {
                    return true;
                }
                if pattern.ends_with('*') {
                    let prefix = &pattern[..pattern.len() - 1];
                    if table.to_lowercase().starts_with(&prefix.to_lowercase()) {
                        return true;
                    }
                } else if pattern.eq_ignore_ascii_case(table) {
                    return true;
                }
            }
        }

        false
    }

    /// Check if a user has permission for a given database + table and a specific action.
    /// Same pattern as `check_table_permission()` but checks the appropriate column
    /// based on the `PermAction` variant.
    pub fn check_table_permission_action(
        &self,
        email: &str,
        database: &str,
        table: &str,
        action: PermAction,
    ) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        let mut stmt = match conn.prepare(
            "SELECT table_pattern, can_read, can_write, can_update, can_delete FROM permissions
             WHERE email = ?1 AND (database_name = ?2 OR database_name = '*')",
        ) {
            Ok(s) => s,
            Err(_) => return false,
        };

        let rows = match stmt.query_map(params![email, database], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, bool>(4)?,
            ))
        }) {
            Ok(r) => r,
            Err(_) => return false,
        };

        for row in rows {
            if let Ok((pattern, can_read, can_write, can_update, can_delete)) = row {
                let access = match action {
                    PermAction::Read => can_read,
                    PermAction::Insert => can_write,
                    PermAction::Update => can_update,
                    PermAction::Delete => can_delete,
                };
                if !access {
                    continue;
                }

                if pattern == "*" {
                    return true;
                }
                if pattern.ends_with('*') {
                    let prefix = &pattern[..pattern.len() - 1];
                    if table.to_lowercase().starts_with(&prefix.to_lowercase()) {
                        return true;
                    }
                } else if pattern.eq_ignore_ascii_case(table) {
                    return true;
                }
            }
        }

        false
    }

    /// Get the SQL mode for a user.
    pub fn get_sql_mode(&self, email: &str) -> SqlMode {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return SqlMode::None,
        };

        conn.query_row(
            "SELECT sql_mode FROM users WHERE email = ?1 AND is_enabled = 1",
            params![email],
            |row| row.get::<_, String>(0),
        )
        .map(|s| SqlMode::from_db(&s))
        .unwrap_or(SqlMode::None)
    }

    /// Get the max pending approvals limit for a user (default 6).
    pub fn get_max_pending_approvals(&self, email: &str) -> u32 {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return 6,
        };

        conn.query_row(
            "SELECT max_pending_approvals FROM users WHERE email = ?1 AND is_enabled = 1",
            params![email],
            |row| row.get::<_, Option<u32>>(0),
        )
        .unwrap_or(None)
        .unwrap_or(6)
    }

    /// Check if a user has raw SQL access enabled (any mode other than None).
    #[allow(dead_code)]
    pub fn is_raw_sql_enabled(&self, email: &str) -> bool {
        self.get_sql_mode(email) != SqlMode::None
    }

    /// Check if a user exists and is enabled.
    pub fn user_exists(&self, email: &str) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        conn.query_row(
            "SELECT COUNT(*) > 0 FROM users WHERE email = ?1 AND is_enabled = 1",
            params![email],
            |row| row.get::<_, bool>(0),
        )
        .unwrap_or(false)
    }

    /// Check if a user is an admin.
    pub fn is_admin(&self, email: &str) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        conn.query_row(
            "SELECT is_admin FROM users WHERE email = ?1 AND is_enabled = 1",
            params![email],
            |row| row.get::<_, bool>(0),
        )
        .unwrap_or(false)
    }

    /// Check if MCP access is enabled for a user.
    pub fn is_mcp_enabled(&self, email: &str) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        conn.query_row(
            "SELECT mcp_enabled FROM users WHERE email = ?1 AND is_enabled = 1",
            params![email],
            |row| row.get::<_, bool>(0),
        )
        .unwrap_or(false)
    }

    /// Get the user-level PII mode setting.
    pub fn get_user_pii_mode(&self, email: &str) -> Result<Option<String>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        conn.query_row(
            "SELECT pii_mode FROM users WHERE email = ?1",
            params![email],
            |row| row.get::<_, Option<String>>(0),
        )
        .map_err(|e| format!("Database error: {}", e))
    }

    // ========================================================================
    // Token management
    // ========================================================================

    /// Generate a new token for a user.
    /// `expires_hours`: Some(n) = expires in n hours, None = never expires.
    /// Returns the full token string (caller must show it once to user).
    pub fn generate_token(
        &self,
        email: &str,
        label: Option<&str>,
        expires_hours: Option<u64>,
        pii_mode: Option<&str>,
    ) -> Result<String, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        // Verify user exists
        let user_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM users WHERE email = ?1",
                params![email],
                |row| row.get(0),
            )
            .map_err(|e| format!("Database error: {}", e))?;

        if !user_exists {
            return Err(format!("User '{}' not found", email));
        }

        // Generate a random 64-char hex token
        let token = generate_random_hex(32); // 32 bytes = 64 hex chars

        let expires_at = expires_hours.map(|h| {
            let exp = Utc::now().naive_utc() + chrono::Duration::hours(h as i64);
            exp.format("%Y-%m-%d %H:%M:%S").to_string()
        });

        conn.execute(
            "INSERT INTO tokens (token, email, label, expires_at, pii_mode) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![token, email, label, expires_at, pii_mode],
        )
        .map_err(|e| format!("Failed to create token: {}", e))?;

        Ok(token)
    }

    /// Revoke a token (by prefix or full token).
    pub fn revoke_token(&self, token_or_prefix: &str) -> Result<u64, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let affected = if token_or_prefix.len() <= 16 {
            // Treat as prefix
            conn.execute(
                "UPDATE tokens SET is_active = 0 WHERE token LIKE ?1 || '%' AND is_active = 1",
                params![token_or_prefix],
            )
        } else {
            conn.execute(
                "UPDATE tokens SET is_active = 0 WHERE token = ?1 AND is_active = 1",
                params![token_or_prefix],
            )
        }
        .map_err(|e| format!("Failed to revoke token: {}", e))?;

        Ok(affected as u64)
    }

    /// List tokens, optionally filtered by email.
    pub fn list_tokens(&self, email: Option<&str>) -> Result<Vec<TokenRecord>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut records = Vec::new();

        if let Some(email) = email {
            let mut stmt = conn
                .prepare(
                    "SELECT substr(token, 1, 8), email, label, expires_at, is_active, created_at, pii_mode
                     FROM tokens WHERE email = ?1 ORDER BY created_at DESC",
                )
                .map_err(|e| format!("Query error: {}", e))?;

            let rows = stmt
                .query_map(params![email], |row| {
                    Ok(TokenRecord {
                        token_prefix: row.get(0)?,
                        email: row.get(1)?,
                        label: row.get(2)?,
                        expires_at: row.get(3)?,
                        is_active: row.get(4)?,
                        created_at: row.get(5)?,
                        pii_mode: row.get(6)?,
                    })
                })
                .map_err(|e| format!("Query error: {}", e))?;

            for row in rows {
                records.push(row.map_err(|e| format!("Row error: {}", e))?);
            }
        } else {
            let mut stmt = conn
                .prepare(
                    "SELECT substr(token, 1, 8), email, label, expires_at, is_active, created_at, pii_mode
                     FROM tokens ORDER BY created_at DESC",
                )
                .map_err(|e| format!("Query error: {}", e))?;

            let rows = stmt
                .query_map([], |row| {
                    Ok(TokenRecord {
                        token_prefix: row.get(0)?,
                        email: row.get(1)?,
                        label: row.get(2)?,
                        expires_at: row.get(3)?,
                        is_active: row.get(4)?,
                        created_at: row.get(5)?,
                        pii_mode: row.get(6)?,
                    })
                })
                .map_err(|e| format!("Query error: {}", e))?;

            for row in rows {
                records.push(row.map_err(|e| format!("Row error: {}", e))?);
            }
        }

        Ok(records)
    }

    // ========================================================================
    // User management
    // ========================================================================

    /// Create a new user. Returns error if user already exists.
    pub fn create_user(
        &self,
        email: &str,
        display_name: Option<&str>,
        is_admin: bool,
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        conn.execute(
            "INSERT INTO users (email, display_name, is_admin) VALUES (?1, ?2, ?3)",
            params![email, display_name, is_admin],
        )
        .map_err(|e| format!("Failed to create user: {}", e))?;

        Ok(())
    }

    /// Update an existing user.
    pub fn update_user(
        &self,
        email: &str,
        display_name: Option<&str>,
        is_admin: Option<bool>,
        is_enabled: Option<bool>,
        mcp_enabled: Option<bool>,
        pii_mode: Option<&str>,
        sql_mode: Option<&str>,
        max_pending_approvals: Option<Option<u32>>,
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        // Build dynamic update
        let mut sets = vec!["updated_at = datetime('now')"];
        let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(name) = display_name {
            sets.push("display_name = ?");
            values.push(Box::new(name.to_string()));
        }
        if let Some(admin) = is_admin {
            sets.push("is_admin = ?");
            values.push(Box::new(admin));
        }
        if let Some(enabled) = is_enabled {
            sets.push("is_enabled = ?");
            values.push(Box::new(enabled));
        }
        if let Some(mcp) = mcp_enabled {
            sets.push("mcp_enabled = ?");
            values.push(Box::new(mcp));
        }
        if let Some(pii) = pii_mode {
            sets.push("pii_mode = ?");
            // Store empty string / "inherit" as NULL
            if pii.is_empty() || pii == "inherit" {
                values.push(Box::new(None::<String>));
            } else {
                values.push(Box::new(pii.to_string()));
            }
        }
        if let Some(mode) = sql_mode {
            sets.push("sql_mode = ?");
            values.push(Box::new(mode.to_string()));
        }
        if let Some(max_pending) = max_pending_approvals {
            sets.push("max_pending_approvals = ?");
            values.push(Box::new(max_pending.map(|v| v as i64)));
        }

        // Re-number placeholders
        let mut numbered_sets = Vec::new();
        let mut param_idx = 1;
        for s in &sets {
            if s.contains('?') {
                numbered_sets.push(s.replace('?', &format!("?{}", param_idx)));
                param_idx += 1;
            } else {
                numbered_sets.push(s.to_string());
            }
        }
        values.push(Box::new(email.to_string()));

        let sql = format!(
            "UPDATE users SET {} WHERE email = ?{}",
            numbered_sets.join(", "),
            param_idx
        );

        let params: Vec<&dyn rusqlite::types::ToSql> = values.iter().map(|v| v.as_ref()).collect();

        conn.execute(&sql, params.as_slice())
            .map_err(|e| format!("Failed to update user: {}", e))?;

        Ok(())
    }

    /// Delete a user (cascades to tokens and permissions).
    pub fn delete_user(&self, email: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let affected = conn
            .execute("DELETE FROM users WHERE email = ?1", params![email])
            .map_err(|e| format!("Failed to delete user: {}", e))?;

        if affected == 0 {
            return Err(format!("User '{}' not found", email));
        }

        Ok(())
    }

    /// List all users.
    pub fn list_users(&self) -> Result<Vec<UserInfo>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut stmt = conn
            .prepare(
                "SELECT email, display_name, is_admin, is_enabled, created_at, updated_at, mcp_enabled, pii_mode, sql_mode, max_pending_approvals
                 FROM users ORDER BY email",
            )
            .map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(UserInfo {
                    email: row.get(0)?,
                    display_name: row.get(1)?,
                    is_admin: row.get(2)?,
                    is_enabled: row.get(3)?,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
                    mcp_enabled: row.get(6)?,
                    pii_mode: row.get(7)?,
                    sql_mode: row.get::<_, Option<String>>(8)?.unwrap_or_else(|| "none".to_string()),
                    max_pending_approvals: row.get::<_, Option<u32>>(9)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        let mut users = Vec::new();
        for row in rows {
            users.push(row.map_err(|e| format!("Row error: {}", e))?);
        }

        Ok(users)
    }

    // ========================================================================
    // Permission management
    // ========================================================================

    /// Set permissions for a user (replaces all existing permissions).
    pub fn set_permissions(
        &self,
        email: &str,
        permissions: &[PermissionEntry],
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        // Delete existing permissions
        conn.execute("DELETE FROM permissions WHERE email = ?1", params![email])
            .map_err(|e| format!("Failed to clear permissions: {}", e))?;

        // Insert new permissions
        for p in permissions {
            let table_pattern = p.table_pattern.as_deref().unwrap_or("*");
            let can_read = p.can_read.unwrap_or(true);
            let can_write = p.can_write.unwrap_or(false);
            let can_update = p.can_update.unwrap_or(can_write);
            let can_delete = p.can_delete.unwrap_or(can_write);

            conn.execute(
                "INSERT INTO permissions (email, database_name, table_pattern, can_read, can_write, can_update, can_delete)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![email, p.database_name, table_pattern, can_read, can_write, can_update, can_delete],
            )
            .map_err(|e| format!("Failed to insert permission: {}", e))?;
        }

        Ok(())
    }

    /// Get permissions for a user.
    pub fn get_permissions(&self, email: &str) -> Result<Vec<Permission>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut stmt = conn
            .prepare(
                "SELECT id, email, database_name, table_pattern, can_read, can_write, can_update, can_delete
                 FROM permissions WHERE email = ?1",
            )
            .map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map(params![email], |row| {
                Ok(Permission {
                    id: row.get(0)?,
                    email: row.get(1)?,
                    database_name: row.get(2)?,
                    table_pattern: row.get(3)?,
                    can_read: row.get(4)?,
                    can_write: row.get(5)?,
                    can_update: row.get(6)?,
                    can_delete: row.get(7)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        let mut perms = Vec::new();
        for row in rows {
            perms.push(row.map_err(|e| format!("Row error: {}", e))?);
        }

        Ok(perms)
    }

    // ========================================================================
    // System config
    // ========================================================================

    pub fn get_config(&self, key: &str) -> Result<Option<String>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        match conn.query_row(
            "SELECT value FROM system_config WHERE key = ?1",
            params![key],
            |row| row.get::<_, String>(0),
        ) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(format!("Config read error: {}", e)),
        }
    }

    pub fn set_config(&self, key: &str, value: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO system_config (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )
        .map_err(|e| format!("Config write error: {}", e))?;
        Ok(())
    }

    // ========================================================================
    // Password management
    // ========================================================================

    pub fn set_password(&self, email: &str, password: &str) -> Result<(), String> {
        // Generate a random salt using /dev/urandom
        let salt_bytes = generate_random_hex(16); // 16 bytes = 32 hex chars
        let salt = SaltString::from_b64(&salt_bytes)
            .map_err(|e| format!("Salt generation error: {}", e))?;
        let argon2 = Argon2::default();
        let hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| format!("Password hash error: {}", e))?
            .to_string();

        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let affected = conn
            .execute(
                "UPDATE users SET password_hash = ?1, updated_at = datetime('now') WHERE email = ?2",
                params![hash, email],
            )
            .map_err(|e| format!("Failed to set password: {}", e))?;

        if affected == 0 {
            return Err(format!("User '{}' not found", email));
        }
        Ok(())
    }

    pub fn verify_password(&self, email: &str, password: &str) -> Result<bool, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let result = conn.query_row(
            "SELECT password_hash, is_enabled FROM users WHERE email = ?1",
            params![email],
            |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, bool>(1)?)),
        );

        match result {
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
            Err(e) => Err(format!("Database error: {}", e)),
            Ok((None, _)) => Err("No password set for this account".to_string()),
            Ok((_, false)) => Err("Account is disabled".to_string()),
            Ok((Some(hash_str), true)) => {
                let parsed = PasswordHash::new(&hash_str)
                    .map_err(|e| format!("Invalid stored hash: {}", e))?;
                Ok(Argon2::default()
                    .verify_password(password.as_bytes(), &parsed)
                    .is_ok())
            }
        }
    }

    // ========================================================================
    // Session management
    // ========================================================================

    pub fn create_session(
        &self,
        email: &str,
        ip: Option<&str>,
        user_agent: Option<&str>,
        hours: u64,
    ) -> Result<String, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let token = generate_random_hex(32);
        let expires_at = (Utc::now().naive_utc() + chrono::Duration::hours(hours as i64))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        conn.execute(
            "INSERT INTO sessions (session_token, email, expires_at, ip_address, user_agent)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![token, email, expires_at, ip, user_agent],
        )
        .map_err(|e| format!("Failed to create session: {}", e))?;

        Ok(token)
    }

    pub fn validate_session(&self, token: &str) -> Result<SessionInfo, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let result = conn.query_row(
            "SELECT s.email, s.expires_at, u.is_admin, u.is_enabled, u.display_name
             FROM sessions s
             JOIN users u ON s.email = u.email
             WHERE s.session_token = ?1",
            params![token],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        );

        match result {
            Err(rusqlite::Error::QueryReturnedNoRows) => Err("Invalid session".to_string()),
            Err(e) => Err(format!("Database error: {}", e)),
            Ok((email, expires_at, is_admin, is_enabled, display_name)) => {
                if !is_enabled {
                    return Err("Account is disabled".to_string());
                }

                if let Ok(exp_dt) =
                    NaiveDateTime::parse_from_str(&expires_at, "%Y-%m-%d %H:%M:%S")
                {
                    if Utc::now().naive_utc() > exp_dt {
                        // Clean up expired session
                        let _ = conn.execute(
                            "DELETE FROM sessions WHERE session_token = ?1",
                            params![token],
                        );
                        return Err("Session expired".to_string());
                    }
                }

                // Update last_active_at
                let _ = conn.execute(
                    "UPDATE sessions SET last_active_at = datetime('now') WHERE session_token = ?1",
                    params![token],
                );

                Ok(SessionInfo {
                    email,
                    is_admin,
                    display_name,
                })
            }
        }
    }

    pub fn delete_session(&self, token: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "DELETE FROM sessions WHERE session_token = ?1",
            params![token],
        )
        .map_err(|e| format!("Failed to delete session: {}", e))?;
        Ok(())
    }

    pub fn delete_user_sessions(&self, email: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "DELETE FROM sessions WHERE email = ?1",
            params![email],
        )
        .map_err(|e| format!("Failed to delete sessions: {}", e))?;
        Ok(())
    }

    pub fn cleanup_expired_sessions(&self) -> Result<u64, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let affected = conn
            .execute(
                "DELETE FROM sessions WHERE expires_at < datetime('now')",
                [],
            )
            .map_err(|e| format!("Cleanup error: {}", e))?;
        Ok(affected as u64)
    }

    // ========================================================================
    // OAuth state management
    // ========================================================================

    pub fn store_oauth_state(
        &self,
        state: &str,
        provider: &str,
        pkce_verifier: &str,
        redirect_uri: &str,
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO oauth_states (state, provider, pkce_verifier, redirect_uri) VALUES (?1, ?2, ?3, ?4)",
            params![state, provider, pkce_verifier, redirect_uri],
        )
        .map_err(|e| format!("Failed to store OAuth state: {}", e))?;
        Ok(())
    }

    pub fn consume_oauth_state(&self, state: &str) -> Result<(String, String, String), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let result = conn
            .query_row(
                "SELECT provider, pkce_verifier, redirect_uri, created_at FROM oauth_states WHERE state = ?1",
                params![state],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .map_err(|e| format!("OAuth state not found: {}", e))?;

        // Delete the state (one-time use)
        conn.execute("DELETE FROM oauth_states WHERE state = ?1", params![state])
            .map_err(|e| format!("Failed to delete OAuth state: {}", e))?;

        // Check expiration (10 minutes)
        let created_at = NaiveDateTime::parse_from_str(&result.3, "%Y-%m-%d %H:%M:%S")
            .map_err(|e| format!("Failed to parse created_at: {}", e))?;
        let age = Utc::now().naive_utc() - created_at;
        if age.num_minutes() > 10 {
            return Err("OAuth state expired".to_string());
        }

        Ok((result.0, result.1, result.2))
    }

    pub fn cleanup_expired_oauth_states(&self) -> Result<u64, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let affected = conn
            .execute(
                "DELETE FROM oauth_states WHERE created_at < datetime('now', '-10 minutes')",
                [],
            )
            .map_err(|e| format!("OAuth state cleanup error: {}", e))?;
        Ok(affected as u64)
    }

    // ========================================================================
    // Email code login
    // ========================================================================

    /// Store a hashed email login code.
    pub fn store_email_code(&self, email: &str, code_hash: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO email_codes (email, code_hash) VALUES (?1, ?2)",
            params![email, code_hash],
        )
        .map_err(|e| format!("Failed to store email code: {}", e))?;
        Ok(())
    }

    /// Count codes sent to this email in the last hour (rate limit).
    pub fn count_recent_email_codes(&self, email: &str) -> u32 {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return 0,
        };
        conn.query_row(
            "SELECT COUNT(*) FROM email_codes WHERE email = ?1 AND created_at > datetime('now', '-1 hour')",
            params![email],
            |row| row.get::<_, u32>(0),
        )
        .unwrap_or(0)
    }

    /// Verify an email code. Returns true if valid (unconsumed, <10min old, <3 attempts).
    /// On success, marks as consumed. On failure, increments attempts.
    pub fn verify_email_code(&self, email: &str, code_hash: &str) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        // Find the most recent unconsumed code for this email that matches the hash
        let result = conn.query_row(
            "SELECT id, attempts, created_at FROM email_codes
             WHERE email = ?1 AND code_hash = ?2 AND consumed_at IS NULL AND attempts < 3
             ORDER BY created_at DESC LIMIT 1",
            params![email, code_hash],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i32>(1)?, row.get::<_, String>(2)?)),
        );

        match result {
            Ok((id, _attempts, created_at)) => {
                // Check expiration (10 minutes)
                if let Ok(dt) = NaiveDateTime::parse_from_str(&created_at, "%Y-%m-%d %H:%M:%S") {
                    if (Utc::now().naive_utc() - dt).num_minutes() > 10 {
                        return false;
                    }
                }
                // Mark consumed
                let _ = conn.execute(
                    "UPDATE email_codes SET consumed_at = datetime('now') WHERE id = ?1",
                    params![id],
                );
                true
            }
            Err(_) => {
                // Increment attempts on the most recent unconsumed code for this email
                let _ = conn.execute(
                    "UPDATE email_codes SET attempts = attempts + 1
                     WHERE id = (
                         SELECT id FROM email_codes
                         WHERE email = ?1 AND consumed_at IS NULL
                         ORDER BY created_at DESC LIMIT 1
                     )",
                    params![email],
                );
                false
            }
        }
    }

    /// Delete expired email codes (older than 10 minutes).
    pub fn cleanup_expired_email_codes(&self) -> Result<u64, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let affected = conn
            .execute(
                "DELETE FROM email_codes WHERE created_at < datetime('now', '-10 minutes')",
                [],
            )
            .map_err(|e| format!("Email code cleanup error: {}", e))?;
        Ok(affected as u64)
    }

    // ========================================================================
    // Connection management
    // ========================================================================

    pub fn create_connection(&self, conn: &StoredConnection) -> Result<(), String> {
        let db = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        db.execute(
            "INSERT INTO connections (name, conn_type, host, port, database_name, username, password, options_json, sslmode, is_default, is_enabled)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                conn.name, conn.conn_type, conn.host, conn.port as i64,
                conn.database_name, conn.username, conn.password,
                conn.options_json, conn.sslmode, conn.is_default, conn.is_enabled,
            ],
        )
        .map_err(|e| format!("Failed to create connection: {}", e))?;

        // If this is the new default, clear default on all others
        if conn.is_default {
            db.execute(
                "UPDATE connections SET is_default = 0 WHERE name != ?1",
                params![conn.name],
            )
            .map_err(|e| format!("Failed to update default: {}", e))?;
        }
        Ok(())
    }

    pub fn update_connection(&self, name: &str, conn: &StoredConnection) -> Result<(), String> {
        let db = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let affected = db.execute(
            "UPDATE connections SET conn_type = ?1, host = ?2, port = ?3, database_name = ?4,
             username = ?5, password = ?6, options_json = ?7, sslmode = ?8,
             is_default = ?9, is_enabled = ?10, updated_at = datetime('now')
             WHERE name = ?11",
            params![
                conn.conn_type, conn.host, conn.port as i64,
                conn.database_name, conn.username, conn.password,
                conn.options_json, conn.sslmode, conn.is_default, conn.is_enabled,
                name,
            ],
        )
        .map_err(|e| format!("Failed to update connection: {}", e))?;
        if affected == 0 {
            return Err(format!("Connection '{}' not found", name));
        }
        if conn.is_default {
            db.execute(
                "UPDATE connections SET is_default = 0 WHERE name != ?1",
                params![name],
            )
            .map_err(|e| format!("Failed to update default: {}", e))?;
        }
        Ok(())
    }

    pub fn delete_connection(&self, name: &str) -> Result<(), String> {
        let db = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let affected = db.execute("DELETE FROM connections WHERE name = ?1", params![name])
            .map_err(|e| format!("Failed to delete connection: {}", e))?;
        if affected == 0 {
            return Err(format!("Connection '{}' not found", name));
        }
        Ok(())
    }

    pub fn list_connections_db(&self) -> Result<Vec<StoredConnection>, String> {
        let db = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = db
            .prepare(
                "SELECT name, conn_type, host, port, database_name, username, password,
                        options_json, sslmode, is_default, is_enabled
                 FROM connections ORDER BY name",
            )
            .map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(StoredConnection {
                    name: row.get(0)?,
                    conn_type: row.get(1)?,
                    host: row.get(2)?,
                    port: row.get::<_, i64>(3)? as u16,
                    database_name: row.get(4)?,
                    username: row.get(5)?,
                    password: row.get(6)?,
                    options_json: row.get::<_, Option<String>>(7)?.unwrap_or_else(|| "{}".to_string()),
                    sslmode: row.get(8)?,
                    is_default: row.get(9)?,
                    is_enabled: row.get(10)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        let mut conns = Vec::new();
        for row in rows {
            conns.push(row.map_err(|e| format!("Row error: {}", e))?);
        }
        Ok(conns)
    }

    pub fn get_connection(&self, name: &str) -> Result<StoredConnection, String> {
        let db = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        db.query_row(
            "SELECT name, conn_type, host, port, database_name, username, password,
                    options_json, sslmode, is_default, is_enabled
             FROM connections WHERE name = ?1",
            params![name],
            |row| {
                Ok(StoredConnection {
                    name: row.get(0)?,
                    conn_type: row.get(1)?,
                    host: row.get(2)?,
                    port: row.get::<_, i64>(3)? as u16,
                    database_name: row.get(4)?,
                    username: row.get(5)?,
                    password: row.get(6)?,
                    options_json: row.get::<_, Option<String>>(7)?.unwrap_or_else(|| "{}".to_string()),
                    sslmode: row.get(8)?,
                    is_default: row.get(9)?,
                    is_enabled: row.get(10)?,
                })
            },
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => format!("Connection '{}' not found", name),
            _ => format!("Database error: {}", e),
        })
    }

    pub fn connections_seeded(&self) -> bool {
        self.get_config("connections_seeded")
            .ok()
            .flatten()
            .map(|v| v == "true")
            .unwrap_or(false)
    }

    pub fn mark_connections_seeded(&self) -> Result<(), String> {
        self.set_config("connections_seeded", "true")
    }

    // ========================================================================
    // Setup flow
    // ========================================================================

    pub fn needs_setup(&self) -> Result<bool, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
            .map_err(|e| format!("Database error: {}", e))?;
        Ok(count == 0)
    }

    pub fn setup_admin(
        &self,
        email: &str,
        display_name: Option<&str>,
        password: &str,
        phone: Option<&str>,
    ) -> Result<(), String> {
        if !self.needs_setup()? {
            return Err("Setup already completed — users exist".to_string());
        }

        // Create admin user
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO users (email, display_name, is_admin, phone) VALUES (?1, ?2, 1, ?3)",
            params![email, display_name, phone],
        )
        .map_err(|e| format!("Failed to create admin: {}", e))?;

        // Set wildcard permissions
        conn.execute(
            "INSERT INTO permissions (email, database_name, table_pattern, can_read, can_write)
             VALUES (?1, '*', '*', 1, 1)",
            params![email],
        )
        .map_err(|e| format!("Failed to set permissions: {}", e))?;

        drop(conn);

        // Hash and store password
        self.set_password(email, password)?;

        Ok(())
    }

    // ========================================================================
    // Audit logging
    // ========================================================================

    pub fn log_access(
        &self,
        token_prefix: Option<&str>,
        email: Option<&str>,
        source_ip: Option<&str>,
        database_name: Option<&str>,
        query_type: Option<&str>,
        action: &str,
        details: Option<&str>,
    ) {
        if let Ok(conn) = self.conn.lock() {
            let _ = conn.execute(
                "INSERT INTO access_log (token_prefix, email, source_ip, database_name, query_type, action, details)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![token_prefix, email, source_ip, database_name, query_type, action, details],
            );
        }
    }

    /// Query audit log with optional filters.
    pub fn query_audit_log(
        &self,
        email: Option<&str>,
        action: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AuditEntry>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut conditions = Vec::new();
        let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut param_idx = 1;

        if let Some(email) = email {
            conditions.push(format!("email = ?{}", param_idx));
            values.push(Box::new(email.to_string()));
            param_idx += 1;
        }
        if let Some(action) = action {
            conditions.push(format!("action = ?{}", param_idx));
            values.push(Box::new(action.to_string()));
            param_idx += 1;
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        values.push(Box::new(limit as i64));

        let sql = format!(
            "SELECT id, token_prefix, email, source_ip, database_name, query_type, action, details, created_at
             FROM access_log {} ORDER BY id DESC LIMIT ?{}",
            where_clause, param_idx
        );

        let params: Vec<&dyn rusqlite::types::ToSql> = values.iter().map(|v| v.as_ref()).collect();

        let mut stmt = conn.prepare(&sql).map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map(params.as_slice(), |row| {
                Ok(AuditEntry {
                    id: row.get(0)?,
                    token_prefix: row.get(1)?,
                    email: row.get(2)?,
                    source_ip: row.get(3)?,
                    database_name: row.get(4)?,
                    query_type: row.get(5)?,
                    action: row.get(6)?,
                    details: row.get(7)?,
                    created_at: row.get(8)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        let mut entries = Vec::new();
        for row in rows {
            entries.push(row.map_err(|e| format!("Row error: {}", e))?);
        }

        Ok(entries)
    }

    // ========================================================================
    // Query History
    // ========================================================================

    #[allow(clippy::too_many_arguments)]
    pub fn log_query_history(
        &self,
        email: &str,
        connection_name: Option<&str>,
        database_name: Option<&str>,
        sql_text: &str,
        execution_time_ms: Option<i64>,
        row_count: Option<i64>,
        is_success: bool,
        error_message: Option<&str>,
    ) {
        let conn = self.conn.lock().unwrap();
        // Insert the entry
        let _ = conn.execute(
            "INSERT INTO query_history (email, connection_name, database_name, sql_text, execution_time_ms, row_count, is_success, error_message)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                email,
                connection_name,
                database_name,
                sql_text,
                execution_time_ms,
                row_count,
                is_success as i32,
                error_message,
            ],
        );
        // Prune old entries (keep max 1000 non-favorite per user)
        let _ = conn.execute(
            "DELETE FROM query_history WHERE id IN (
                SELECT id FROM query_history
                WHERE email = ?1 AND is_favorite = 0
                ORDER BY created_at DESC
                LIMIT -1 OFFSET 1000
            )",
            params![email],
        );
    }

    pub fn list_query_history(
        &self,
        email: &str,
        limit: i64,
        offset: i64,
        search: Option<&str>,
        favorites_only: bool,
    ) -> Result<Vec<QueryHistoryEntry>, String> {
        let conn = self.conn.lock().unwrap();
        let mut sql = String::from(
            "SELECT id, email, connection_name, database_name, sql_text, execution_time_ms, row_count, is_success, error_message, is_favorite, created_at
             FROM query_history WHERE email = ?1",
        );
        if favorites_only {
            sql.push_str(" AND is_favorite = 1");
        }
        if search.is_some() {
            sql.push_str(" AND sql_text LIKE ?4");
        }
        sql.push_str(" ORDER BY created_at DESC LIMIT ?2 OFFSET ?3");

        let search_pattern = search.map(|s| format!("%{}%", s));

        let mut stmt = conn.prepare(&sql).map_err(|e| format!("Prepare error: {}", e))?;

        fn read_row(row: &rusqlite::Row) -> rusqlite::Result<QueryHistoryEntry> {
            Ok(QueryHistoryEntry {
                id: row.get(0)?,
                email: row.get(1)?,
                connection_name: row.get(2)?,
                database_name: row.get(3)?,
                sql_text: row.get(4)?,
                execution_time_ms: row.get(5)?,
                row_count: row.get(6)?,
                is_success: row.get::<_, i32>(7)? != 0,
                error_message: row.get(8)?,
                is_favorite: row.get::<_, i32>(9)? != 0,
                created_at: row.get(10)?,
            })
        }

        let rows: Vec<QueryHistoryEntry> = if let Some(ref pattern) = search_pattern {
            stmt.query_map(params![email, limit, offset, pattern], read_row)
                .map_err(|e| format!("Query error: {}", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Row error: {}", e))?
        } else {
            stmt.query_map(params![email, limit, offset], read_row)
                .map_err(|e| format!("Query error: {}", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Row error: {}", e))?
        };

        Ok(rows)
    }

    pub fn toggle_favorite(&self, id: i64, email: &str, is_favorite: bool) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        let affected = conn
            .execute(
                "UPDATE query_history SET is_favorite = ?1 WHERE id = ?2 AND email = ?3",
                params![is_favorite as i32, id, email],
            )
            .map_err(|e| format!("Update error: {}", e))?;
        if affected == 0 {
            return Err("History entry not found".to_string());
        }
        Ok(())
    }

    pub fn delete_history_entry(&self, id: i64, email: &str) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        let affected = conn
            .execute(
                "DELETE FROM query_history WHERE id = ?1 AND email = ?2",
                params![id, email],
            )
            .map_err(|e| format!("Delete error: {}", e))?;
        if affected == 0 {
            return Err("History entry not found".to_string());
        }
        Ok(())
    }

    // ========================================================================
    // Service Accounts
    // ========================================================================

    /// Create a new service account. Returns the full API key (shown once).
    pub fn create_service_account(
        &self,
        name: &str,
        description: Option<&str>,
        sql_mode: Option<&str>,
    ) -> Result<String, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let api_key = format!("sa_{}", generate_random_hex(32));
        let mode = sql_mode.unwrap_or("full");

        conn.execute(
            "INSERT INTO service_accounts (name, description, api_key, sql_mode) VALUES (?1, ?2, ?3, ?4)",
            params![name, description, api_key, mode],
        )
        .map_err(|e| format!("Failed to create service account: {}", e))?;

        Ok(api_key)
    }

    /// Validate a service account key. Returns info if key is valid and account is enabled.
    pub fn validate_service_account_key(&self, key: &str) -> Result<ServiceAccountInfo, String> {
        if !key.starts_with("sa_") {
            return Err("Not a service account key".to_string());
        }

        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        conn.query_row(
            "SELECT name, description, api_key, sql_mode, is_enabled, created_at, updated_at
             FROM service_accounts WHERE api_key = ?1 AND is_enabled = 1",
            params![key],
            |row| {
                let full_key: String = row.get(2)?;
                Ok(ServiceAccountInfo {
                    name: row.get(0)?,
                    description: row.get(1)?,
                    api_key_prefix: full_key[..8.min(full_key.len())].to_string(),
                    sql_mode: row.get(3)?,
                    is_enabled: row.get(4)?,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            },
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => "Invalid or disabled service account key".to_string(),
            _ => format!("Database error: {}", e),
        })
    }

    /// List all service accounts.
    pub fn list_service_accounts(&self) -> Result<Vec<ServiceAccountInfo>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut stmt = conn
            .prepare(
                "SELECT name, description, api_key, sql_mode, is_enabled, created_at, updated_at
                 FROM service_accounts ORDER BY name",
            )
            .map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map([], |row| {
                let full_key: String = row.get(2)?;
                Ok(ServiceAccountInfo {
                    name: row.get(0)?,
                    description: row.get(1)?,
                    api_key_prefix: full_key[..8.min(full_key.len())].to_string(),
                    sql_mode: row.get(3)?,
                    is_enabled: row.get(4)?,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        let mut accounts = Vec::new();
        for row in rows {
            accounts.push(row.map_err(|e| format!("Row error: {}", e))?);
        }
        Ok(accounts)
    }

    /// Update a service account.
    pub fn update_service_account(
        &self,
        name: &str,
        description: Option<&str>,
        sql_mode: Option<&str>,
        is_enabled: Option<bool>,
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut sets = vec!["updated_at = datetime('now')"];
        let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(desc) = description {
            sets.push("description = ?");
            values.push(Box::new(desc.to_string()));
        }
        if let Some(mode) = sql_mode {
            sets.push("sql_mode = ?");
            values.push(Box::new(mode.to_string()));
        }
        if let Some(enabled) = is_enabled {
            sets.push("is_enabled = ?");
            values.push(Box::new(enabled));
        }

        let mut numbered_sets = Vec::new();
        let mut param_idx = 1;
        for s in &sets {
            if s.contains('?') {
                numbered_sets.push(s.replace('?', &format!("?{}", param_idx)));
                param_idx += 1;
            } else {
                numbered_sets.push(s.to_string());
            }
        }
        values.push(Box::new(name.to_string()));

        let sql = format!(
            "UPDATE service_accounts SET {} WHERE name = ?{}",
            numbered_sets.join(", "),
            param_idx
        );

        let params: Vec<&dyn rusqlite::types::ToSql> = values.iter().map(|v| v.as_ref()).collect();

        let affected = conn.execute(&sql, params.as_slice())
            .map_err(|e| format!("Failed to update service account: {}", e))?;

        if affected == 0 {
            return Err(format!("Service account '{}' not found", name));
        }

        Ok(())
    }

    /// Delete a service account (cascades to permissions and connections).
    pub fn delete_service_account(&self, name: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let affected = conn
            .execute("DELETE FROM service_accounts WHERE name = ?1", params![name])
            .map_err(|e| format!("Failed to delete service account: {}", e))?;

        if affected == 0 {
            return Err(format!("Service account '{}' not found", name));
        }

        Ok(())
    }

    /// Rotate a service account's API key. Returns the new key.
    pub fn rotate_service_account_key(&self, name: &str) -> Result<String, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let new_key = format!("sa_{}", generate_random_hex(32));

        let affected = conn
            .execute(
                "UPDATE service_accounts SET api_key = ?1, updated_at = datetime('now') WHERE name = ?2",
                params![new_key, name],
            )
            .map_err(|e| format!("Failed to rotate key: {}", e))?;

        if affected == 0 {
            return Err(format!("Service account '{}' not found", name));
        }

        Ok(new_key)
    }

    /// Get the SQL mode for a service account.
    pub fn get_sa_sql_mode(&self, name: &str) -> SqlMode {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return SqlMode::None,
        };

        conn.query_row(
            "SELECT sql_mode FROM service_accounts WHERE name = ?1 AND is_enabled = 1",
            params![name],
            |row| row.get::<_, String>(0),
        )
        .map(|s| SqlMode::from_db(&s))
        .unwrap_or(SqlMode::None)
    }

    /// Check if a service account has permission for a given database and query type.
    pub fn check_sa_permission(&self, name: &str, database: &str, is_write: bool) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        let result = conn.query_row(
            "SELECT can_read, can_write FROM service_account_permissions
             WHERE account_name = ?1 AND (database_name = ?2 OR database_name = '*')
             ORDER BY CASE WHEN database_name = '*' THEN 1 ELSE 0 END
             LIMIT 1",
            params![name, database],
            |row| Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?)),
        );

        match result {
            Ok((can_read, can_write)) => {
                if is_write { can_write } else { can_read }
            }
            Err(_) => false,
        }
    }

    /// Check if a service account has permission for a specific database+table+action.
    pub fn check_sa_table_permission_action(
        &self,
        name: &str,
        database: &str,
        table: &str,
        action: PermAction,
    ) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        let mut stmt = match conn.prepare(
            "SELECT table_pattern, can_read, can_write, can_update, can_delete FROM service_account_permissions
             WHERE account_name = ?1 AND (database_name = ?2 OR database_name = '*')",
        ) {
            Ok(s) => s,
            Err(_) => return false,
        };

        let rows = match stmt.query_map(params![name, database], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, bool>(4)?,
            ))
        }) {
            Ok(r) => r,
            Err(_) => return false,
        };

        for row in rows {
            if let Ok((pattern, can_read, can_write, can_update, can_delete)) = row {
                let access = match action {
                    PermAction::Read => can_read,
                    PermAction::Insert => can_write,
                    PermAction::Update => can_update,
                    PermAction::Delete => can_delete,
                };
                if !access {
                    continue;
                }

                if pattern == "*" {
                    return true;
                }
                if pattern.ends_with('*') {
                    let prefix = &pattern[..pattern.len() - 1];
                    if table.to_lowercase().starts_with(&prefix.to_lowercase()) {
                        return true;
                    }
                } else if pattern.eq_ignore_ascii_case(table) {
                    return true;
                }
            }
        }

        false
    }

    /// Check if a service account has access to a specific connection.
    pub fn check_sa_connection_access(&self, name: &str, connection_name: &str) -> bool {
        match self.get_sa_allowed_connections(name) {
            Ok(None) => true,
            Ok(Some(allowed)) => allowed.iter().any(|c| c == connection_name),
            Err(_) => true,
        }
    }

    /// Get allowed connections for a service account.
    pub fn get_sa_allowed_connections(&self, name: &str) -> Result<Option<Vec<String>>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn
            .prepare("SELECT connection_name FROM service_account_connections WHERE account_name = ?1")
            .map_err(|e| format!("Query error: {}", e))?;
        let names: Vec<String> = stmt
            .query_map(params![name], |row| row.get(0))
            .map_err(|e| format!("Query error: {}", e))?
            .filter_map(|r| r.ok())
            .collect();
        if names.is_empty() {
            Ok(None)
        } else {
            Ok(Some(names))
        }
    }

    /// Set connection permissions for a service account.
    pub fn set_sa_connection_permissions(&self, name: &str, connections: &[String]) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "DELETE FROM service_account_connections WHERE account_name = ?1",
            params![name],
        )
        .map_err(|e| format!("Delete error: {}", e))?;

        for cn in connections {
            conn.execute(
                "INSERT INTO service_account_connections (account_name, connection_name) VALUES (?1, ?2)",
                params![name, cn],
            )
            .map_err(|e| format!("Insert error: {}", e))?;
        }
        Ok(())
    }

    /// Get permissions for a service account.
    pub fn get_sa_permissions(&self, name: &str) -> Result<Vec<Permission>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut stmt = conn
            .prepare(
                "SELECT id, account_name, database_name, table_pattern, can_read, can_write, can_update, can_delete
                 FROM service_account_permissions WHERE account_name = ?1 ORDER BY database_name",
            )
            .map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map(params![name], |row| {
                Ok(Permission {
                    id: row.get(0)?,
                    email: row.get(1)?, // reuse `email` field for account_name
                    database_name: row.get(2)?,
                    table_pattern: row.get(3)?,
                    can_read: row.get(4)?,
                    can_write: row.get(5)?,
                    can_update: row.get(6)?,
                    can_delete: row.get(7)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        let mut perms = Vec::new();
        for row in rows {
            perms.push(row.map_err(|e| format!("Row error: {}", e))?);
        }
        Ok(perms)
    }

    /// Set permissions for a service account (replace all).
    pub fn set_sa_permissions(&self, name: &str, permissions: &[PermissionEntry]) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        conn.execute(
            "DELETE FROM service_account_permissions WHERE account_name = ?1",
            params![name],
        )
        .map_err(|e| format!("Delete error: {}", e))?;

        for perm in permissions {
            let table_pattern = perm.table_pattern.as_deref().unwrap_or("*");
            let can_read = perm.can_read.unwrap_or(true);
            let can_write = perm.can_write.unwrap_or(false);
            let can_update = perm.can_update.unwrap_or(false);
            let can_delete = perm.can_delete.unwrap_or(false);

            conn.execute(
                "INSERT INTO service_account_permissions (account_name, database_name, table_pattern, can_read, can_write, can_update, can_delete)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![name, perm.database_name, table_pattern, can_read, can_write, can_update, can_delete],
            )
            .map_err(|e| format!("Insert error: {}", e))?;
        }

        Ok(())
    }

    // ========================================================================
    // Connection-level access control
    // ========================================================================

    /// Get allowed connections for a user.
    /// Returns None if unrestricted (no rows), Some(list) if restricted.
    pub fn get_allowed_connections(&self, email: &str) -> Result<Option<Vec<String>>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn
            .prepare("SELECT connection_name FROM connection_permissions WHERE email = ?1")
            .map_err(|e| format!("Query error: {}", e))?;
        let names: Vec<String> = stmt
            .query_map(params![email], |row| row.get(0))
            .map_err(|e| format!("Query error: {}", e))?
            .filter_map(|r| r.ok())
            .collect();
        if names.is_empty() {
            Ok(None)
        } else {
            Ok(Some(names))
        }
    }

    /// Set connection permissions for a user.
    /// Empty slice = unrestricted (remove all rows).
    pub fn set_connection_permissions(&self, email: &str, connections: &[String]) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "DELETE FROM connection_permissions WHERE email = ?1",
            params![email],
        )
        .map_err(|e| format!("Delete error: {}", e))?;

        for name in connections {
            conn.execute(
                "INSERT INTO connection_permissions (email, connection_name) VALUES (?1, ?2)",
                params![email, name],
            )
            .map_err(|e| format!("Insert error: {}", e))?;
        }
        Ok(())
    }

    /// Check if a user has access to a specific connection.
    /// Returns true if unrestricted or if the connection is in the allowed list.
    pub fn check_connection_access(&self, email: &str, connection_name: &str) -> bool {
        match self.get_allowed_connections(email) {
            Ok(None) => true, // unrestricted
            Ok(Some(allowed)) => allowed.iter().any(|c| c == connection_name),
            Err(_) => true, // on error, fail open
        }
    }

    // ========================================================================
    // Storage Permissions
    // ========================================================================

    /// Check if a user has storage access for a given connection + bucket + action.
    /// No rows for the user+connection = unrestricted (fail-open, same as connection_permissions).
    #[cfg(feature = "storage")]
    pub fn check_storage_access(
        &self,
        email: &str,
        connection_name: &str,
        bucket: &str,
        action: StoragePermAction,
    ) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return true, // fail open
        };

        let mut stmt = match conn.prepare(
            "SELECT bucket_pattern, can_read, can_write, can_delete FROM storage_permissions
             WHERE email = ?1 AND connection_name = ?2",
        ) {
            Ok(s) => s,
            Err(_) => return true,
        };

        let rows: Vec<_> = match stmt.query_map(params![email, connection_name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, bool>(3)?,
            ))
        }) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => return true,
        };

        // No rows = unrestricted
        if rows.is_empty() {
            return true;
        }

        for (pattern, can_read, can_write, can_delete) in rows {
            let access = match action {
                StoragePermAction::Read => can_read,
                StoragePermAction::Write => can_write,
                StoragePermAction::Delete => can_delete,
            };
            if !access {
                continue;
            }

            if pattern == "*" {
                return true;
            }
            if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len() - 1];
                if bucket.to_lowercase().starts_with(&prefix.to_lowercase()) {
                    return true;
                }
            } else if pattern.eq_ignore_ascii_case(bucket) {
                return true;
            }
        }

        false
    }

    /// Check if a service account has storage access for a given connection + bucket + action.
    #[cfg(feature = "storage")]
    pub fn check_sa_storage_access(
        &self,
        account_name: &str,
        connection_name: &str,
        bucket: &str,
        action: StoragePermAction,
    ) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return true,
        };

        let mut stmt = match conn.prepare(
            "SELECT bucket_pattern, can_read, can_write, can_delete FROM sa_storage_permissions
             WHERE account_name = ?1 AND connection_name = ?2",
        ) {
            Ok(s) => s,
            Err(_) => return true,
        };

        let rows: Vec<_> = match stmt.query_map(params![account_name, connection_name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, bool>(3)?,
            ))
        }) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => return true,
        };

        if rows.is_empty() {
            return true;
        }

        for (pattern, can_read, can_write, can_delete) in rows {
            let access = match action {
                StoragePermAction::Read => can_read,
                StoragePermAction::Write => can_write,
                StoragePermAction::Delete => can_delete,
            };
            if !access {
                continue;
            }

            if pattern == "*" {
                return true;
            }
            if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len() - 1];
                if bucket.to_lowercase().starts_with(&prefix.to_lowercase()) {
                    return true;
                }
            } else if pattern.eq_ignore_ascii_case(bucket) {
                return true;
            }
        }

        false
    }

    /// Get storage permissions for a user.
    pub fn get_storage_permissions(&self, email: &str) -> Result<Vec<StoragePermission>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut stmt = conn
            .prepare(
                "SELECT id, email, connection_name, bucket_pattern, can_read, can_write, can_delete
                 FROM storage_permissions WHERE email = ?1 ORDER BY connection_name",
            )
            .map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map(params![email], |row| {
                Ok(StoragePermission {
                    id: row.get(0)?,
                    identity: row.get(1)?,
                    connection_name: row.get(2)?,
                    bucket_pattern: row.get(3)?,
                    can_read: row.get(4)?,
                    can_write: row.get(5)?,
                    can_delete: row.get(6)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        let mut perms = Vec::new();
        for row in rows {
            perms.push(row.map_err(|e| format!("Row error: {}", e))?);
        }
        Ok(perms)
    }

    /// Set storage permissions for a user (replace all).
    pub fn set_storage_permissions(
        &self,
        email: &str,
        permissions: &[StoragePermissionEntry],
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        conn.execute("DELETE FROM storage_permissions WHERE email = ?1", params![email])
            .map_err(|e| format!("Delete error: {}", e))?;

        for p in permissions {
            let bucket_pattern = p.bucket_pattern.as_deref().unwrap_or("*");
            let can_read = p.can_read.unwrap_or(true);
            let can_write = p.can_write.unwrap_or(false);
            let can_delete = p.can_delete.unwrap_or(false);

            conn.execute(
                "INSERT INTO storage_permissions (email, connection_name, bucket_pattern, can_read, can_write, can_delete)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![email, p.connection_name, bucket_pattern, can_read, can_write, can_delete],
            )
            .map_err(|e| format!("Insert error: {}", e))?;
        }

        Ok(())
    }

    /// Get storage permissions for a service account.
    pub fn get_sa_storage_permissions(&self, name: &str) -> Result<Vec<StoragePermission>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut stmt = conn
            .prepare(
                "SELECT id, account_name, connection_name, bucket_pattern, can_read, can_write, can_delete
                 FROM sa_storage_permissions WHERE account_name = ?1 ORDER BY connection_name",
            )
            .map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map(params![name], |row| {
                Ok(StoragePermission {
                    id: row.get(0)?,
                    identity: row.get(1)?,
                    connection_name: row.get(2)?,
                    bucket_pattern: row.get(3)?,
                    can_read: row.get(4)?,
                    can_write: row.get(5)?,
                    can_delete: row.get(6)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        let mut perms = Vec::new();
        for row in rows {
            perms.push(row.map_err(|e| format!("Row error: {}", e))?);
        }
        Ok(perms)
    }

    /// Set storage permissions for a service account (replace all).
    pub fn set_sa_storage_permissions(
        &self,
        name: &str,
        permissions: &[StoragePermissionEntry],
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        conn.execute("DELETE FROM sa_storage_permissions WHERE account_name = ?1", params![name])
            .map_err(|e| format!("Delete error: {}", e))?;

        for p in permissions {
            let bucket_pattern = p.bucket_pattern.as_deref().unwrap_or("*");
            let can_read = p.can_read.unwrap_or(true);
            let can_write = p.can_write.unwrap_or(false);
            let can_delete = p.can_delete.unwrap_or(false);

            conn.execute(
                "INSERT INTO sa_storage_permissions (account_name, connection_name, bucket_pattern, can_read, can_write, can_delete)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![name, p.connection_name, bucket_pattern, can_read, can_write, can_delete],
            )
            .map_err(|e| format!("Insert error: {}", e))?;
        }

        Ok(())
    }

    // ========================================================================
    // PII Rules
    // ========================================================================

    /// Seed built-in PII rules (SSN, CC, Email, Phone) if they don't exist yet.
    pub fn seed_builtin_pii_rules(&self) {
        let builtins = [
            ("SSN", "Social Security Number", r"(?:^|[^0-9])(\d{3}-\d{2}-\d{4}|\d{9})(?:$|[^0-9])", "<ssn>", "ssn"),
            ("Credit Card", "Credit card number", r"(?:^|[^0-9])((?:\d[ -]?){12,18}\d)(?:$|[^0-9])", "<credit_card>", "credit_card"),
            ("Email", "Email address", r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", "<email_address>", "email"),
            ("Phone", "Phone number", r"(?:^|[^0-9])((?:\+?1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4})(?:$|[^0-9])", "<phone_number>", "phone"),
        ];

        if let Ok(conn) = self.conn.lock() {
            for (name, desc, pattern, replacement, kind) in &builtins {
                let _ = conn.execute(
                    "INSERT OR IGNORE INTO pii_rules (name, description, regex_pattern, replacement_text, entity_kind, is_builtin)
                     VALUES (?1, ?2, ?3, ?4, ?5, 1)",
                    params![name, desc, pattern, replacement, kind],
                );
            }
        }
    }

    pub fn list_pii_rules(&self) -> Result<Vec<PiiRule>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, name, description, regex_pattern, replacement_text, entity_kind, is_builtin, is_enabled, created_at, updated_at
                 FROM pii_rules ORDER BY is_builtin DESC, name",
            )
            .map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(PiiRule {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    regex_pattern: row.get(3)?,
                    replacement_text: row.get(4)?,
                    entity_kind: row.get(5)?,
                    is_builtin: row.get::<_, i32>(6)? != 0,
                    is_enabled: row.get::<_, i32>(7)? != 0,
                    created_at: row.get(8)?,
                    updated_at: row.get(9)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Row error: {}", e))
    }

    pub fn get_enabled_pii_rules(&self) -> Result<Vec<PiiRule>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, name, description, regex_pattern, replacement_text, entity_kind, is_builtin, is_enabled, created_at, updated_at
                 FROM pii_rules WHERE is_enabled = 1",
            )
            .map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(PiiRule {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    regex_pattern: row.get(3)?,
                    replacement_text: row.get(4)?,
                    entity_kind: row.get(5)?,
                    is_builtin: row.get::<_, i32>(6)? != 0,
                    is_enabled: row.get::<_, i32>(7)? != 0,
                    created_at: row.get(8)?,
                    updated_at: row.get(9)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Row error: {}", e))
    }

    pub fn create_pii_rule(
        &self,
        name: &str,
        description: Option<&str>,
        regex_pattern: &str,
        replacement_text: &str,
        entity_kind: &str,
    ) -> Result<i64, String> {
        // Validate regex
        regex::Regex::new(regex_pattern)
            .map_err(|e| format!("Invalid regex pattern: {}", e))?;

        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO pii_rules (name, description, regex_pattern, replacement_text, entity_kind)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![name, description, regex_pattern, replacement_text, entity_kind],
        )
        .map_err(|e| format!("Insert error: {}", e))?;

        Ok(conn.last_insert_rowid())
    }

    pub fn update_pii_rule(
        &self,
        id: i64,
        name: Option<&str>,
        description: Option<&str>,
        regex_pattern: Option<&str>,
        replacement_text: Option<&str>,
        entity_kind: Option<&str>,
        is_enabled: Option<bool>,
    ) -> Result<(), String> {
        // Validate regex if provided
        if let Some(pattern) = regex_pattern {
            regex::Regex::new(pattern)
                .map_err(|e| format!("Invalid regex pattern: {}", e))?;
        }

        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut sets = Vec::new();
        let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut idx = 1;

        if let Some(v) = name {
            sets.push(format!("name = ?{}", idx));
            values.push(Box::new(v.to_string()));
            idx += 1;
        }
        if let Some(v) = description {
            sets.push(format!("description = ?{}", idx));
            values.push(Box::new(v.to_string()));
            idx += 1;
        }
        if let Some(v) = regex_pattern {
            sets.push(format!("regex_pattern = ?{}", idx));
            values.push(Box::new(v.to_string()));
            idx += 1;
        }
        if let Some(v) = replacement_text {
            sets.push(format!("replacement_text = ?{}", idx));
            values.push(Box::new(v.to_string()));
            idx += 1;
        }
        if let Some(v) = entity_kind {
            sets.push(format!("entity_kind = ?{}", idx));
            values.push(Box::new(v.to_string()));
            idx += 1;
        }
        if let Some(v) = is_enabled {
            sets.push(format!("is_enabled = ?{}", idx));
            values.push(Box::new(v as i32));
            idx += 1;
        }

        if sets.is_empty() {
            return Ok(());
        }

        sets.push(format!("updated_at = datetime('now')"));
        values.push(Box::new(id));

        let sql = format!(
            "UPDATE pii_rules SET {} WHERE id = ?{}",
            sets.join(", "),
            idx
        );

        let params: Vec<&dyn rusqlite::types::ToSql> = values.iter().map(|v| v.as_ref()).collect();
        let affected = conn
            .execute(&sql, params.as_slice())
            .map_err(|e| format!("Update error: {}", e))?;

        if affected == 0 {
            return Err("PII rule not found".to_string());
        }
        Ok(())
    }

    pub fn delete_pii_rule(&self, id: i64) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        // Check if built-in
        let is_builtin: bool = conn
            .query_row(
                "SELECT is_builtin FROM pii_rules WHERE id = ?1",
                params![id],
                |row| row.get::<_, i32>(0).map(|v| v != 0),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => "PII rule not found".to_string(),
                _ => format!("Query error: {}", e),
            })?;

        if is_builtin {
            return Err("Cannot delete built-in PII rules. Disable them instead.".to_string());
        }

        conn.execute("DELETE FROM pii_rules WHERE id = ?1", params![id])
            .map_err(|e| format!("Delete error: {}", e))?;

        Ok(())
    }

    // ========================================================================
    // PII Columns
    // ========================================================================

    pub fn list_pii_columns(
        &self,
        connection: Option<&str>,
        database: Option<&str>,
    ) -> Result<Vec<PiiColumn>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut conditions = Vec::new();
        let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut idx = 1;

        if let Some(c) = connection {
            conditions.push(format!("connection_name = ?{}", idx));
            values.push(Box::new(c.to_string()));
            idx += 1;
        }
        if let Some(d) = database {
            conditions.push(format!("database_name = ?{}", idx));
            values.push(Box::new(d.to_string()));
            idx += 1;
        }
        let _ = idx;

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            "SELECT id, connection_name, database_name, schema_name, table_name, column_name, pii_type, custom_replacement, created_at
             FROM pii_columns {} ORDER BY connection_name, database_name, table_name, column_name",
            where_clause
        );

        let params: Vec<&dyn rusqlite::types::ToSql> = values.iter().map(|v| v.as_ref()).collect();
        let mut stmt = conn.prepare(&sql).map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map(params.as_slice(), |row| {
                Ok(PiiColumn {
                    id: row.get(0)?,
                    connection_name: row.get(1)?,
                    database_name: row.get(2)?,
                    schema_name: row.get(3)?,
                    table_name: row.get(4)?,
                    column_name: row.get(5)?,
                    pii_type: row.get(6)?,
                    custom_replacement: row.get(7)?,
                    created_at: row.get(8)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Row error: {}", e))
    }

    pub fn set_pii_column(
        &self,
        connection: &str,
        database: &str,
        schema: &str,
        table: &str,
        column: &str,
        pii_type: &str,
        custom_replacement: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO pii_columns (connection_name, database_name, schema_name, table_name, column_name, pii_type, custom_replacement)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(connection_name, database_name, schema_name, table_name, column_name)
             DO UPDATE SET pii_type = excluded.pii_type, custom_replacement = excluded.custom_replacement",
            params![connection, database, schema, table, column, pii_type, custom_replacement],
        )
        .map_err(|e| format!("Upsert error: {}", e))?;
        Ok(())
    }

    pub fn remove_pii_column(&self, id: i64) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let affected = conn
            .execute("DELETE FROM pii_columns WHERE id = ?1", params![id])
            .map_err(|e| format!("Delete error: {}", e))?;
        if affected == 0 {
            return Err("PII column tag not found".to_string());
        }
        Ok(())
    }

    pub fn get_pii_columns_for_query(
        &self,
        connection: &str,
        database: &str,
    ) -> Result<Vec<PiiColumn>, String> {
        self.list_pii_columns(Some(connection), Some(database))
    }

    // ========================================================================
    // Storage Column Links
    // ========================================================================

    #[cfg(feature = "storage")]
    pub fn list_storage_column_links(
        &self,
        connection: Option<&str>,
        database: Option<&str>,
    ) -> Result<Vec<StorageColumnLink>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let mut conditions = Vec::new();
        let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut idx = 1;

        if let Some(c) = connection {
            conditions.push(format!("connection_name = ?{}", idx));
            values.push(Box::new(c.to_string()));
            idx += 1;
        }
        if let Some(d) = database {
            conditions.push(format!("database_name = ?{}", idx));
            values.push(Box::new(d.to_string()));
            idx += 1;
        }
        let _ = idx;

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            "SELECT id, connection_name, database_name, schema_name, table_name, column_name, storage_connection, bucket_name, key_prefix, created_at
             FROM storage_column_links {} ORDER BY connection_name, database_name, table_name, column_name",
            where_clause
        );

        let params: Vec<&dyn rusqlite::types::ToSql> = values.iter().map(|v| v.as_ref()).collect();
        let mut stmt = conn.prepare(&sql).map_err(|e| format!("Query error: {}", e))?;

        let rows = stmt
            .query_map(params.as_slice(), |row| {
                Ok(StorageColumnLink {
                    id: row.get(0)?,
                    connection_name: row.get(1)?,
                    database_name: row.get(2)?,
                    schema_name: row.get(3)?,
                    table_name: row.get(4)?,
                    column_name: row.get(5)?,
                    storage_connection: row.get(6)?,
                    bucket_name: row.get(7)?,
                    key_prefix: row.get(8)?,
                    created_at: row.get(9)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Row error: {}", e))
    }

    #[cfg(feature = "storage")]
    pub fn set_storage_column_link(
        &self,
        connection_name: &str,
        database_name: &str,
        schema_name: Option<&str>,
        table_name: &str,
        column_name: &str,
        storage_connection: &str,
        bucket_name: &str,
        key_prefix: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO storage_column_links (connection_name, database_name, schema_name, table_name, column_name, storage_connection, bucket_name, key_prefix)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(connection_name, database_name, table_name, column_name)
             DO UPDATE SET schema_name = excluded.schema_name, storage_connection = excluded.storage_connection, bucket_name = excluded.bucket_name, key_prefix = excluded.key_prefix",
            params![connection_name, database_name, schema_name, table_name, column_name, storage_connection, bucket_name, key_prefix],
        )
        .map_err(|e| format!("Upsert error: {}", e))?;
        Ok(())
    }

    #[cfg(feature = "storage")]
    pub fn remove_storage_column_link(&self, id: i64) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let affected = conn
            .execute("DELETE FROM storage_column_links WHERE id = ?1", params![id])
            .map_err(|e| format!("Delete error: {}", e))?;
        if affected == 0 {
            return Err("Storage column link not found".to_string());
        }
        Ok(())
    }

    // ========================================================================
    // Realtime Tables
    // ========================================================================

    pub fn is_realtime_enabled(&self, connection: &str, database: &str, table: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT 1 FROM realtime_tables WHERE connection_name = ?1 COLLATE NOCASE AND database_name = ?2 COLLATE NOCASE AND table_name = ?3 COLLATE NOCASE",
            params![connection, database, table],
            |_| Ok(()),
        ).is_ok()
    }

    pub fn enable_realtime(&self, connection: &str, database: &str, table: &str, enabled_by: Option<&str>) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO realtime_tables (connection_name, database_name, table_name, enabled_by) VALUES (?1, ?2, ?3, ?4)",
            params![connection, database, table, enabled_by],
        ).map_err(|e| format!("Failed to enable realtime: {}", e))?;
        Ok(())
    }

    /// Remove all realtime tables older than the given duration.
    pub fn cleanup_expired_realtime_tables(&self, max_age_secs: i64) -> Result<u64, String> {
        let conn = self.conn.lock().unwrap();
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(max_age_secs);
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();
        let deleted = conn.execute(
            "DELETE FROM realtime_tables WHERE created_at < ?1",
            params![cutoff_str],
        ).map_err(|e| format!("Failed to cleanup realtime tables: {}", e))?;
        Ok(deleted as u64)
    }

    pub fn disable_realtime(&self, connection: &str, database: &str, table: &str) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM realtime_tables WHERE connection_name = ?1 COLLATE NOCASE AND database_name = ?2 COLLATE NOCASE AND table_name = ?3 COLLATE NOCASE",
            params![connection, database, table],
        ).map_err(|e| format!("Failed to disable realtime: {}", e))?;
        Ok(())
    }

    pub fn list_realtime_tables(&self) -> Result<Vec<RealtimeTableEntry>, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT connection_name, database_name, table_name, enabled_by, created_at FROM realtime_tables ORDER BY connection_name, database_name, table_name")
            .map_err(|e| format!("Failed to prepare query: {}", e))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(RealtimeTableEntry {
                    connection_name: row.get(0)?,
                    database_name: row.get(1)?,
                    table_name: row.get(2)?,
                    enabled_by: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })
            .map_err(|e| format!("Failed to query: {}", e))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to collect: {}", e))
    }

    // ========================================================================
    // REST Tables
    // ========================================================================

    pub fn is_rest_enabled(&self, connection: &str, database: &str, schema: &str, table: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT 1 FROM rest_tables WHERE connection_name = ?1 COLLATE NOCASE AND database_name = ?2 COLLATE NOCASE AND schema_name = ?3 COLLATE NOCASE AND table_name = ?4 COLLATE NOCASE",
            params![connection, database, schema, table],
            |_| Ok(()),
        ).is_ok()
    }

    pub fn enable_rest_table(&self, connection: &str, database: &str, schema: &str, table: &str, enabled_by: Option<&str>) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO rest_tables (connection_name, database_name, schema_name, table_name, enabled_by) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![connection, database, schema, table, enabled_by],
        ).map_err(|e| format!("Failed to enable REST table: {}", e))?;
        Ok(())
    }

    pub fn disable_rest_table(&self, connection: &str, database: &str, schema: &str, table: &str) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM rest_tables WHERE connection_name = ?1 COLLATE NOCASE AND database_name = ?2 COLLATE NOCASE AND schema_name = ?3 COLLATE NOCASE AND table_name = ?4 COLLATE NOCASE",
            params![connection, database, schema, table],
        ).map_err(|e| format!("Failed to disable REST table: {}", e))?;
        Ok(())
    }

    pub fn list_rest_tables(&self) -> Result<Vec<RestTableEntry>, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT connection_name, database_name, schema_name, table_name, enabled_by, created_at FROM rest_tables ORDER BY connection_name, database_name, schema_name, table_name")
            .map_err(|e| format!("Failed to prepare query: {}", e))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(RestTableEntry {
                    connection_name: row.get(0)?,
                    database_name: row.get(1)?,
                    schema_name: row.get(2)?,
                    table_name: row.get(3)?,
                    enabled_by: row.get(4)?,
                    created_at: row.get(5)?,
                })
            })
            .map_err(|e| format!("Failed to query: {}", e))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to collect: {}", e))
    }
}

// ============================================================================
// Connection Health History
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct HealthHistoryEntry {
    pub status: String,
    pub error_message: Option<String>,
    pub checked_at: String,
}

impl AccessControlDb {
    /// Record a health check result.
    pub fn record_health_check(
        &self,
        connection_name: &str,
        status: &str,
        error_message: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO connection_health_history (connection_name, status, error_message) VALUES (?1, ?2, ?3)",
            params![connection_name, status, error_message],
        )
        .map_err(|e| format!("Failed to record health check: {}", e))?;
        Ok(())
    }

    /// Get health history for a connection within the last N hours.
    pub fn get_health_history(
        &self,
        connection_name: &str,
        hours: u32,
    ) -> Result<Vec<HealthHistoryEntry>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn
            .prepare(
                "SELECT status, error_message, checked_at FROM connection_health_history \
                 WHERE connection_name = ?1 AND checked_at >= datetime('now', ?2) \
                 ORDER BY checked_at ASC",
            )
            .map_err(|e| format!("Failed to prepare: {}", e))?;

        let cutoff = format!("-{} hours", hours);
        let rows = stmt
            .query_map(params![connection_name, cutoff], |row| {
                Ok(HealthHistoryEntry {
                    status: row.get(0)?,
                    error_message: row.get(1)?,
                    checked_at: row.get(2)?,
                })
            })
            .map_err(|e| format!("Failed to query: {}", e))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to collect: {}", e))
    }

    /// Delete health history entries older than N hours.
    pub fn prune_health_history(&self, retain_hours: u32) -> Result<usize, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let cutoff = format!("-{} hours", retain_hours);
        let deleted = conn
            .execute(
                "DELETE FROM connection_health_history WHERE checked_at < datetime('now', ?1)",
                params![cutoff],
            )
            .map_err(|e| format!("Failed to prune: {}", e))?;
        Ok(deleted)
    }
}

// ============================================================================
// Approval Audit Trail
// ============================================================================

impl AccessControlDb {
    /// Record a new approval request in the audit trail.
    pub fn record_approval_request(
        &self,
        id: &str,
        user_email: &str,
        tool_name: &str,
        target_connection: &str,
        target_database: &str,
        sql_preview: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO approval_audit (id, user_email, tool_name, target_connection, target_database, sql_preview, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending')",
            params![id, user_email, tool_name, target_connection, target_database, sql_preview],
        )
        .map_err(|e| format!("Failed to record approval request: {}", e))?;
        Ok(())
    }

    /// Record an approval decision (approved/rejected/timeout).
    pub fn record_approval_decision(
        &self,
        id: &str,
        status: &str,
        reason: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "UPDATE approval_audit SET status = ?1, reason = ?2, decided_at = datetime('now') WHERE id = ?3",
            params![status, reason, id],
        )
        .map_err(|e| format!("Failed to record approval decision: {}", e))?;
        Ok(())
    }
}

// ============================================================================
// Teams, Projects & Membership
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct TeamInfo {
    pub id: String,
    pub name: String,
    pub webhook_url: Option<String>,
    pub member_count: i64,
    pub project_count: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectInfo {
    pub id: String,
    pub team_id: String,
    pub name: String,
    pub member_count: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamMemberInfo {
    pub email: String,
    pub role: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectMemberInfo {
    pub email: String,
    pub role: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UserTeamMembership {
    pub team_id: String,
    pub team_name: String,
    pub role: String,
}

impl AccessControlDb {
    // --- Teams CRUD ---

    pub fn create_team(&self, name: &str, webhook_url: Option<&str>) -> Result<String, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO teams (id, name, webhook_url) VALUES (?1, ?2, ?3)",
            params![id, name, webhook_url],
        )
        .map_err(|e| format!("Failed to create team: {}", e))?;
        Ok(id)
    }

    pub fn update_team(&self, id: &str, name: Option<&str>, webhook_url: Option<Option<&str>>) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        if let Some(name) = name {
            conn.execute("UPDATE teams SET name = ?1 WHERE id = ?2", params![name, id])
                .map_err(|e| format!("Failed to update team name: {}", e))?;
        }
        if let Some(wh) = webhook_url {
            conn.execute("UPDATE teams SET webhook_url = ?1 WHERE id = ?2", params![wh, id])
                .map_err(|e| format!("Failed to update team webhook: {}", e))?;
        }
        Ok(())
    }

    pub fn delete_team(&self, id: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute("DELETE FROM teams WHERE id = ?1", params![id])
            .map_err(|e| format!("Failed to delete team: {}", e))?;
        Ok(())
    }

    pub fn list_teams(&self) -> Result<Vec<TeamInfo>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT t.id, t.name, t.webhook_url, t.created_at,
                    (SELECT COUNT(*) FROM team_members tm WHERE tm.team_id = t.id) AS member_count,
                    (SELECT COUNT(*) FROM projects p WHERE p.team_id = t.id) AS project_count
             FROM teams t ORDER BY t.name"
        ).map_err(|e| format!("Failed to list teams: {}", e))?;
        let rows = stmt.query_map([], |row| {
            Ok(TeamInfo {
                id: row.get(0)?,
                name: row.get(1)?,
                webhook_url: row.get(2)?,
                created_at: row.get(3)?,
                member_count: row.get(4)?,
                project_count: row.get(5)?,
            })
        }).map_err(|e| format!("Failed to list teams: {}", e))?;
        let mut teams = Vec::new();
        for row in rows {
            teams.push(row.map_err(|e| format!("Row error: {}", e))?);
        }
        Ok(teams)
    }

    #[allow(dead_code)]
    pub fn get_team(&self, id: &str) -> Result<Option<TeamInfo>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let result = conn.query_row(
            "SELECT t.id, t.name, t.webhook_url, t.created_at,
                    (SELECT COUNT(*) FROM team_members tm WHERE tm.team_id = t.id) AS member_count,
                    (SELECT COUNT(*) FROM projects p WHERE p.team_id = t.id) AS project_count
             FROM teams t WHERE t.id = ?1",
            params![id],
            |row| Ok(TeamInfo {
                id: row.get(0)?,
                name: row.get(1)?,
                webhook_url: row.get(2)?,
                created_at: row.get(3)?,
                member_count: row.get(4)?,
                project_count: row.get(5)?,
            }),
        );
        match result {
            Ok(t) => Ok(Some(t)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(format!("Failed to get team: {}", e)),
        }
    }

    // --- Team Members ---

    pub fn add_team_member(&self, team_id: &str, email: &str, role: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO team_members (team_id, email, role) VALUES (?1, ?2, ?3)
             ON CONFLICT(team_id, email) DO UPDATE SET role = ?3",
            params![team_id, email, role],
        )
        .map_err(|e| format!("Failed to add team member: {}", e))?;
        Ok(())
    }

    pub fn remove_team_member(&self, team_id: &str, email: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "DELETE FROM team_members WHERE team_id = ?1 AND email = ?2",
            params![team_id, email],
        )
        .map_err(|e| format!("Failed to remove team member: {}", e))?;
        Ok(())
    }

    pub fn set_team_member_role(&self, team_id: &str, email: &str, role: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "UPDATE team_members SET role = ?1 WHERE team_id = ?2 AND email = ?3",
            params![role, team_id, email],
        )
        .map_err(|e| format!("Failed to set team member role: {}", e))?;
        Ok(())
    }

    pub fn list_team_members(&self, team_id: &str) -> Result<Vec<TeamMemberInfo>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT tm.email, tm.role, u.display_name
             FROM team_members tm
             JOIN users u ON tm.email = u.email
             WHERE tm.team_id = ?1
             ORDER BY tm.role DESC, tm.email"
        ).map_err(|e| format!("Failed to list team members: {}", e))?;
        let rows = stmt.query_map(params![team_id], |row| {
            Ok(TeamMemberInfo {
                email: row.get(0)?,
                role: row.get(1)?,
                display_name: row.get(2)?,
            })
        }).map_err(|e| format!("Failed to list team members: {}", e))?;
        let mut members = Vec::new();
        for row in rows {
            members.push(row.map_err(|e| format!("Row error: {}", e))?);
        }
        Ok(members)
    }

    // --- Projects CRUD ---

    pub fn create_project(&self, team_id: &str, name: &str) -> Result<String, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO projects (id, team_id, name) VALUES (?1, ?2, ?3)",
            params![id, team_id, name],
        )
        .map_err(|e| format!("Failed to create project: {}", e))?;
        Ok(id)
    }

    pub fn update_project(&self, id: &str, name: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute("UPDATE projects SET name = ?1 WHERE id = ?2", params![name, id])
            .map_err(|e| format!("Failed to update project: {}", e))?;
        Ok(())
    }

    pub fn delete_project(&self, id: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute("DELETE FROM projects WHERE id = ?1", params![id])
            .map_err(|e| format!("Failed to delete project: {}", e))?;
        Ok(())
    }

    pub fn list_projects(&self, team_id: &str) -> Result<Vec<ProjectInfo>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT p.id, p.team_id, p.name, p.created_at,
                    (SELECT COUNT(*) FROM project_members pm WHERE pm.project_id = p.id) AS member_count
             FROM projects p WHERE p.team_id = ?1 ORDER BY p.name"
        ).map_err(|e| format!("Failed to list projects: {}", e))?;
        let rows = stmt.query_map(params![team_id], |row| {
            Ok(ProjectInfo {
                id: row.get(0)?,
                team_id: row.get(1)?,
                name: row.get(2)?,
                created_at: row.get(3)?,
                member_count: row.get(4)?,
            })
        }).map_err(|e| format!("Failed to list projects: {}", e))?;
        let mut projects = Vec::new();
        for row in rows {
            projects.push(row.map_err(|e| format!("Row error: {}", e))?);
        }
        Ok(projects)
    }

    // --- Project Members ---

    pub fn add_project_member(&self, project_id: &str, email: &str, role: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO project_members (project_id, email, role) VALUES (?1, ?2, ?3)
             ON CONFLICT(project_id, email) DO UPDATE SET role = ?3",
            params![project_id, email, role],
        )
        .map_err(|e| format!("Failed to add project member: {}", e))?;
        Ok(())
    }

    pub fn remove_project_member(&self, project_id: &str, email: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "DELETE FROM project_members WHERE project_id = ?1 AND email = ?2",
            params![project_id, email],
        )
        .map_err(|e| format!("Failed to remove project member: {}", e))?;
        Ok(())
    }

    pub fn set_project_member_role(&self, project_id: &str, email: &str, role: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "UPDATE project_members SET role = ?1 WHERE project_id = ?2 AND email = ?3",
            params![role, project_id, email],
        )
        .map_err(|e| format!("Failed to set project member role: {}", e))?;
        Ok(())
    }

    pub fn list_project_members(&self, project_id: &str) -> Result<Vec<ProjectMemberInfo>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT pm.email, pm.role, u.display_name
             FROM project_members pm
             JOIN users u ON pm.email = u.email
             WHERE pm.project_id = ?1
             ORDER BY pm.role DESC, pm.email"
        ).map_err(|e| format!("Failed to list project members: {}", e))?;
        let rows = stmt.query_map(params![project_id], |row| {
            Ok(ProjectMemberInfo {
                email: row.get(0)?,
                role: row.get(1)?,
                display_name: row.get(2)?,
            })
        }).map_err(|e| format!("Failed to list project members: {}", e))?;
        let mut members = Vec::new();
        for row in rows {
            members.push(row.map_err(|e| format!("Row error: {}", e))?);
        }
        Ok(members)
    }

    // --- Delegation queries ---

    /// Get all teams a user belongs to (with their role in each).
    pub fn get_user_teams(&self, email: &str) -> Result<Vec<UserTeamMembership>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn.prepare(
            "SELECT t.id, t.name, tm.role
             FROM team_members tm
             JOIN teams t ON tm.team_id = t.id
             WHERE tm.email = ?1
             ORDER BY t.name"
        ).map_err(|e| format!("Failed to get user teams: {}", e))?;
        let rows = stmt.query_map(params![email], |row| {
            Ok(UserTeamMembership {
                team_id: row.get(0)?,
                team_name: row.get(1)?,
                role: row.get(2)?,
            })
        }).map_err(|e| format!("Failed to get user teams: {}", e))?;
        let mut memberships = Vec::new();
        for row in rows {
            memberships.push(row.map_err(|e| format!("Row error: {}", e))?);
        }
        Ok(memberships)
    }

    /// Check if `approver_email` can approve requests from `requester_email`.
    /// True if: approver is org admin, team_lead in requester's team,
    /// project_lead in requester's project, or self.
    pub fn can_approve(&self, approver_email: &str, requester_email: &str) -> bool {
        if approver_email == requester_email {
            return true;
        }
        if self.is_admin(approver_email) {
            return true;
        }
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };
        // Check if approver is team_lead in any team the requester belongs to
        let is_team_lead: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM team_members tl
             JOIN team_members tr ON tl.team_id = tr.team_id
             WHERE tl.email = ?1 AND tl.role = 'team_lead'
               AND tr.email = ?2",
            params![approver_email, requester_email],
            |row| row.get(0),
        ).unwrap_or(false);
        if is_team_lead {
            return true;
        }
        // Check if approver is project_lead in any project the requester belongs to
        let is_project_lead: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM project_members pl
             JOIN project_members pr ON pl.project_id = pr.project_id
             WHERE pl.email = ?1 AND pl.role = 'project_lead'
               AND pr.email = ?2",
            params![approver_email, requester_email],
            |row| row.get(0),
        ).unwrap_or(false);
        is_project_lead
    }

    /// Get all emails this user can approve for (based on team_lead/project_lead roles).
    #[allow(dead_code)]
    pub fn get_approvable_emails(&self, approver_email: &str) -> Vec<String> {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return vec![],
        };
        let mut emails = std::collections::HashSet::new();
        // Always can approve own
        emails.insert(approver_email.to_string());
        // If admin, return empty (caller should handle admin=see all)
        // Team lead: all members of teams where this user is team_lead
        if let Ok(mut stmt) = conn.prepare(
            "SELECT DISTINCT tr.email FROM team_members tl
             JOIN team_members tr ON tl.team_id = tr.team_id
             WHERE tl.email = ?1 AND tl.role = 'team_lead'"
        ) {
            if let Ok(rows) = stmt.query_map(params![approver_email], |row| row.get::<_, String>(0)) {
                for row in rows.flatten() {
                    emails.insert(row);
                }
            }
        }
        // Project lead: all members of projects where this user is project_lead
        if let Ok(mut stmt) = conn.prepare(
            "SELECT DISTINCT pr.email FROM project_members pl
             JOIN project_members pr ON pl.project_id = pr.project_id
             WHERE pl.email = ?1 AND pl.role = 'project_lead'"
        ) {
            if let Ok(rows) = stmt.query_map(params![approver_email], |row| row.get::<_, String>(0)) {
                for row in rows.flatten() {
                    emails.insert(row);
                }
            }
        }
        emails.into_iter().collect()
    }

    /// Get webhook URLs for all teams a user belongs to.
    pub fn get_team_webhooks_for_user(&self, email: &str) -> Vec<String> {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return vec![],
        };
        let mut stmt = match conn.prepare(
            "SELECT DISTINCT t.webhook_url FROM team_members tm
             JOIN teams t ON tm.team_id = t.id
             WHERE tm.email = ?1 AND t.webhook_url IS NOT NULL AND t.webhook_url != ''"
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        stmt.query_map(params![email], |row| row.get::<_, String>(0))
            .map(|rows| rows.flatten().collect())
            .unwrap_or_default()
    }

    // ========================================================================
    // Named Endpoints
    // ========================================================================

    pub fn create_endpoint(
        &self,
        name: &str,
        connection_name: &str,
        database_name: &str,
        query: &str,
        description: Option<&str>,
        parameters: Option<&str>,
        created_by: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "INSERT INTO endpoints (name, connection_name, database_name, query, description, parameters, created_by)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![name, connection_name, database_name, query, description, parameters, created_by],
        )
        .map_err(|e| format!("Failed to create endpoint: {}", e))?;
        Ok(())
    }

    pub fn update_endpoint(
        &self,
        name: &str,
        connection_name: &str,
        database_name: &str,
        query: &str,
        description: Option<&str>,
        parameters: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let affected = conn.execute(
            "UPDATE endpoints SET connection_name = ?2, database_name = ?3, query = ?4,
             description = ?5, parameters = ?6, updated_at = datetime('now')
             WHERE name = ?1",
            params![name, connection_name, database_name, query, description, parameters],
        )
        .map_err(|e| format!("Failed to update endpoint: {}", e))?;
        if affected == 0 {
            return Err(format!("Endpoint '{}' not found", name));
        }
        Ok(())
    }

    pub fn delete_endpoint(&self, name: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute("DELETE FROM endpoints WHERE name = ?1", params![name])
            .map_err(|e| format!("Failed to delete endpoint: {}", e))?;
        Ok(())
    }

    pub fn get_endpoint(&self, name: &str) -> Result<Option<Endpoint>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let result = conn.query_row(
            "SELECT name, connection_name, database_name, query, description, parameters,
                    created_by, updated_at, created_at
             FROM endpoints WHERE name = ?1",
            params![name],
            |row| {
                Ok(Endpoint {
                    name: row.get(0)?,
                    connection_name: row.get(1)?,
                    database_name: row.get(2)?,
                    query: row.get(3)?,
                    description: row.get(4)?,
                    parameters: row.get(5)?,
                    created_by: row.get(6)?,
                    updated_at: row.get(7)?,
                    created_at: row.get(8)?,
                })
            },
        );
        match result {
            Ok(ep) => Ok(Some(ep)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(format!("Query error: {}", e)),
        }
    }

    pub fn list_endpoints(&self) -> Result<Vec<Endpoint>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn
            .prepare(
                "SELECT name, connection_name, database_name, query, description, parameters,
                        created_by, updated_at, created_at
                 FROM endpoints ORDER BY name",
            )
            .map_err(|e| format!("Query error: {}", e))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(Endpoint {
                    name: row.get(0)?,
                    connection_name: row.get(1)?,
                    database_name: row.get(2)?,
                    query: row.get(3)?,
                    description: row.get(4)?,
                    parameters: row.get(5)?,
                    created_by: row.get(6)?,
                    updated_at: row.get(7)?,
                    created_at: row.get(8)?,
                })
            })
            .map_err(|e| format!("Query error: {}", e))?;
        let mut endpoints = Vec::new();
        for row in rows {
            endpoints.push(row.map_err(|e| format!("Row error: {}", e))?);
        }
        Ok(endpoints)
    }

    pub fn set_endpoint_permissions(&self, endpoint_name: &str, emails: &[String]) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        conn.execute(
            "DELETE FROM endpoint_permissions WHERE endpoint_name = ?1",
            params![endpoint_name],
        )
        .map_err(|e| format!("Delete error: {}", e))?;
        for email in emails {
            conn.execute(
                "INSERT INTO endpoint_permissions (email, endpoint_name) VALUES (?1, ?2)",
                params![email, endpoint_name],
            )
            .map_err(|e| format!("Insert error: {}", e))?;
        }
        Ok(())
    }

    pub fn get_endpoint_permissions(&self, endpoint_name: &str) -> Result<Vec<String>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;
        let mut stmt = conn
            .prepare("SELECT email FROM endpoint_permissions WHERE endpoint_name = ?1")
            .map_err(|e| format!("Query error: {}", e))?;
        let names: Vec<String> = stmt
            .query_map(params![endpoint_name], |row| row.get(0))
            .map_err(|e| format!("Query error: {}", e))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(names)
    }

    /// Check endpoint access for a user. No permission rows = no access (locked by default).
    pub fn check_endpoint_access(&self, email: &str, endpoint_name: &str) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };
        // Admins can access all endpoints
        let is_admin: bool = conn
            .query_row("SELECT is_admin FROM users WHERE email = ?1", params![email], |row| row.get(0))
            .unwrap_or(false);
        if is_admin {
            return true;
        }
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM endpoint_permissions WHERE email = ?1 AND endpoint_name = ?2",
                params![email, endpoint_name],
                |row| row.get(0),
            )
            .unwrap_or(0);
        count > 0
    }


    /// Check endpoint access for a service account. No rows = no access.
    pub fn check_sa_endpoint_access(&self, account_name: &str, endpoint_name: &str) -> bool {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sa_endpoint_permissions WHERE account_name = ?1 AND endpoint_name = ?2",
                params![account_name, endpoint_name],
                |row| row.get(0),
            )
            .unwrap_or(0);
        count > 0
    }
}

// ============================================================================
// Helpers
// ============================================================================

pub(crate) fn generate_random_hex(num_bytes: usize) -> String {
    use std::io::Read;
    let mut buf = vec![0u8; num_bytes];
    let mut rng = std::fs::File::open("/dev/urandom").expect("Failed to open /dev/urandom");
    rng.read_exact(&mut buf).expect("Failed to read random bytes");
    hex::encode(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> AccessControlDb {
        AccessControlDb::new(":memory:", "test-key-12345").unwrap()
    }

    #[test]
    fn create_user_and_token() {
        let db = test_db();
        db.create_user("test@example.com", Some("Test User"), false)
            .unwrap();

        let token = db
            .generate_token("test@example.com", Some("test token"), Some(36), None)
            .unwrap();
        assert_eq!(token.len(), 64);

        let info = db.validate_token(&token).unwrap();
        assert_eq!(info.email, "test@example.com");
        assert!(info.is_active);
    }

    #[test]
    fn token_not_found() {
        let db = test_db();
        let result = db.validate_token("nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn revoke_token() {
        let db = test_db();
        db.create_user("test@example.com", None, false).unwrap();
        let token = db
            .generate_token("test@example.com", None, None, None)
            .unwrap();

        // Token works
        assert!(db.validate_token(&token).is_ok());

        // Revoke
        db.revoke_token(&token).unwrap();

        // Token no longer works
        let result = db.validate_token(&token);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("revoked"));
    }

    #[test]
    fn permissions() {
        let db = test_db();
        db.create_user("test@example.com", None, false).unwrap();

        // No permissions yet
        assert!(!db.check_permission("test@example.com", "spACEOPSTOOLS", false));

        // Grant read-only
        db.set_permissions(
            "test@example.com",
            &[PermissionEntry {
                database_name: "spACEOPSTOOLS".to_string(),
                table_pattern: None,
                can_read: Some(true),
                can_write: Some(false),
                can_update: None,
                can_delete: None,
            }],
        )
        .unwrap();

        assert!(db.check_permission("test@example.com", "spACEOPSTOOLS", false));
        assert!(!db.check_permission("test@example.com", "spACEOPSTOOLS", true));
        assert!(!db.check_permission("test@example.com", "other_db", false));
    }

    #[test]
    fn wildcard_permission() {
        let db = test_db();
        db.create_user("admin@example.com", None, true).unwrap();

        db.set_permissions(
            "admin@example.com",
            &[PermissionEntry {
                database_name: "*".to_string(),
                table_pattern: None,
                can_read: Some(true),
                can_write: Some(true),
                can_update: None,
                can_delete: None,
            }],
        )
        .unwrap();

        assert!(db.check_permission("admin@example.com", "spACEOPSTOOLS", false));
        assert!(db.check_permission("admin@example.com", "spACEOPSTOOLS", true));
        assert!(db.check_permission("admin@example.com", "any_database", true));
    }

    #[test]
    fn disabled_user_token_rejected() {
        let db = test_db();
        db.create_user("test@example.com", None, false).unwrap();
        let token = db.generate_token("test@example.com", None, None, None).unwrap();

        // Works initially
        assert!(db.validate_token(&token).is_ok());

        // Disable user
        db.update_user("test@example.com", None, None, Some(false), None, None, None, None)
            .unwrap();

        // Token now rejected
        let result = db.validate_token(&token);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("disabled"));
    }

    #[test]
    fn delete_user_cascades() {
        let db = test_db();
        db.create_user("test@example.com", None, false).unwrap();
        let token = db.generate_token("test@example.com", None, None, None).unwrap();
        db.set_permissions(
            "test@example.com",
            &[PermissionEntry {
                database_name: "db".to_string(),
                table_pattern: None,
                can_read: Some(true),
                can_write: None,
                can_update: None,
                can_delete: None,
            }],
        )
        .unwrap();

        db.delete_user("test@example.com").unwrap();

        // Token gone
        assert!(db.validate_token(&token).is_err());
        // Permissions gone
        assert!(db.get_permissions("test@example.com").unwrap().is_empty());
    }

    #[test]
    fn audit_log() {
        let db = test_db();
        db.log_access(
            Some("abc12345"),
            Some("test@example.com"),
            Some("1.2.3.4"),
            Some("spACEOPSTOOLS"),
            Some("read"),
            "allowed",
            None,
        );

        let entries = db.query_audit_log(None, None, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action.as_deref(), Some("allowed"));
    }

    #[test]
    fn table_permission_wildcard() {
        let db = test_db();
        db.create_user("test@example.com", None, false).unwrap();
        db.set_permissions(
            "test@example.com",
            &[PermissionEntry {
                database_name: "mydb".to_string(),
                table_pattern: Some("*".to_string()),
                can_read: Some(true),
                can_write: Some(false),
                can_update: None,
                can_delete: None,
            }],
        )
        .unwrap();

        assert!(db.check_table_permission("test@example.com", "mydb", "Orders", false));
        assert!(db.check_table_permission("test@example.com", "mydb", "Employees", false));
        assert!(!db.check_table_permission("test@example.com", "mydb", "Orders", true));
        assert!(!db.check_table_permission("test@example.com", "other_db", "Orders", false));
    }

    #[test]
    fn table_permission_exact() {
        let db = test_db();
        db.create_user("test@example.com", None, false).unwrap();
        db.set_permissions(
            "test@example.com",
            &[PermissionEntry {
                database_name: "mydb".to_string(),
                table_pattern: Some("Orders".to_string()),
                can_read: Some(true),
                can_write: Some(true),
                can_update: None,
                can_delete: None,
            }],
        )
        .unwrap();

        assert!(db.check_table_permission("test@example.com", "mydb", "Orders", false));
        assert!(db.check_table_permission("test@example.com", "mydb", "Orders", true));
        // Case-insensitive
        assert!(db.check_table_permission("test@example.com", "mydb", "orders", false));
        // Other tables denied
        assert!(!db.check_table_permission("test@example.com", "mydb", "Employees", false));
    }

    #[test]
    fn table_permission_prefix() {
        let db = test_db();
        db.create_user("test@example.com", None, false).unwrap();
        db.set_permissions(
            "test@example.com",
            &[PermissionEntry {
                database_name: "mydb".to_string(),
                table_pattern: Some("Order*".to_string()),
                can_read: Some(true),
                can_write: Some(false),
                can_update: None,
                can_delete: None,
            }],
        )
        .unwrap();

        assert!(db.check_table_permission("test@example.com", "mydb", "Orders", false));
        assert!(db.check_table_permission("test@example.com", "mydb", "OrderDetails", false));
        assert!(!db.check_table_permission("test@example.com", "mydb", "Employees", false));
    }

    #[test]
    fn mcp_enabled_flag() {
        let db = test_db();
        db.create_user("test@example.com", None, false).unwrap();

        // MCP enabled by default
        assert!(db.is_mcp_enabled("test@example.com"));

        // Disable MCP
        db.update_user("test@example.com", None, None, None, Some(false), None, None, None).unwrap();
        assert!(!db.is_mcp_enabled("test@example.com"));

        // Re-enable
        db.update_user("test@example.com", None, None, None, Some(true), None, None, None).unwrap();
        assert!(db.is_mcp_enabled("test@example.com"));
    }

    #[test]
    fn teams_and_delegation() {
        let db = test_db();
        // Create users
        db.create_user("lead@example.com", Some("Lead"), false).unwrap();
        db.create_user("member@example.com", Some("Member"), false).unwrap();
        db.create_user("outsider@example.com", Some("Outsider"), false).unwrap();
        db.create_user("admin@example.com", Some("Admin"), true).unwrap();

        // Create team
        let team_id = db.create_team("Engineering", None).unwrap();

        // Add members
        db.add_team_member(&team_id, "lead@example.com", "team_lead").unwrap();
        db.add_team_member(&team_id, "member@example.com", "member").unwrap();

        // List teams
        let teams = db.list_teams().unwrap();
        assert_eq!(teams.len(), 1);
        assert_eq!(teams[0].member_count, 2);

        // List members
        let members = db.list_team_members(&team_id).unwrap();
        assert_eq!(members.len(), 2);

        // can_approve: team lead can approve member
        assert!(db.can_approve("lead@example.com", "member@example.com"));
        // member cannot approve lead
        assert!(!db.can_approve("member@example.com", "lead@example.com"));
        // self-approval always true
        assert!(db.can_approve("member@example.com", "member@example.com"));
        // admin can approve anyone
        assert!(db.can_approve("admin@example.com", "member@example.com"));
        // outsider cannot approve member
        assert!(!db.can_approve("outsider@example.com", "member@example.com"));

        // get_user_teams
        let memberships = db.get_user_teams("member@example.com").unwrap();
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].team_name, "Engineering");
        assert_eq!(memberships[0].role, "member");

        // Projects
        let proj_id = db.create_project(&team_id, "Backend").unwrap();
        db.add_project_member(&proj_id, "lead@example.com", "project_lead").unwrap();
        db.add_project_member(&proj_id, "member@example.com", "member").unwrap();

        let projects = db.list_projects(&team_id).unwrap();
        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].member_count, 2);

        // Project lead can approve project member
        assert!(db.can_approve("lead@example.com", "member@example.com"));

        // Delete team cascades
        db.delete_team(&team_id).unwrap();
        assert_eq!(db.list_teams().unwrap().len(), 0);

        // After team deleted, lead can no longer approve member (no team relation)
        assert!(!db.can_approve("lead@example.com", "member@example.com"));
    }
}
