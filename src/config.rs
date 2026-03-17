use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::Path;

use crate::pii;

// ============================================================================
// Per-Connection Config Types
// ============================================================================

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DbOptions {
    pub encrypt: bool,
    #[serde(rename = "trustServerCertificate")]
    pub trust_server_certificate: bool,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            encrypt: false,
            trust_server_certificate: true,
        }
    }
}

/// MSSQL-specific connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MssqlConnectionConfig {
    pub server: String,
    #[serde(default = "default_mssql_port")]
    pub port: u16,
    #[serde(default = "default_mssql_database")]
    pub database: String,
    pub user: String,
    pub password: String,
    #[serde(default)]
    pub options: DbOptions,
}

fn default_mssql_port() -> u16 {
    1433
}

fn default_mssql_database() -> String {
    "master".to_string()
}

fn default_pg_port() -> u16 {
    5432
}

fn default_pg_database() -> String {
    "postgres".to_string()
}

/// Postgres-specific connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PostgresConnectionConfig {
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    #[serde(default = "default_pg_database")]
    pub database: String,
    pub user: String,
    pub password: String,
    #[serde(default)]
    pub sslmode: Option<String>,
}

/// DuckDB connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(not(feature = "duckdb_backend"), allow(dead_code))]
pub struct DuckDbConnectionConfig {
    pub path: String,
    #[serde(default)]
    pub read_only: Option<bool>,
}

/// MinIO/S3 connection configuration.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(not(feature = "storage"), allow(dead_code))]
pub struct MinioConnectionConfig {
    pub endpoint: String,
    #[serde(default = "default_minio_port")]
    pub port: u16,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default = "default_minio_region")]
    pub region: String,
    #[serde(default = "default_true_val")]
    pub path_style: bool,
}

fn default_minio_port() -> u16 {
    9000
}

fn default_minio_region() -> String {
    "us-east-1".to_string()
}

fn default_true_val() -> bool {
    true
}

/// Tagged union for connection configs.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ConnectionConfig {
    Mssql(MssqlConnectionConfig),
    Postgres(PostgresConnectionConfig),
    #[cfg(feature = "duckdb_backend")]
    #[serde(rename = "duckdb")]
    DuckDb(DuckDbConnectionConfig),
    #[cfg(feature = "storage")]
    Minio(MinioConnectionConfig),
}

/// A named connection entry in the config file.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NamedConnection {
    pub name: String,
    #[serde(flatten)]
    pub config: ConnectionConfig,
}

// ============================================================================
// Application Config (multi-connection)
// ============================================================================

/// Top-level application configuration supporting multiple named connections.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    pub default_connection: Option<String>,
    pub connections: Vec<NamedConnection>,
}

// ============================================================================
// Legacy DbConfig (single MSSQL connection)
// ============================================================================

/// Legacy single-connection config for backward compatibility.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DbConfig {
    pub server: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub options: DbOptions,
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            server: "localhost".to_string(),
            port: 1433,
            database: "master".to_string(),
            user: "sa".to_string(),
            password: String::new(),
            options: DbOptions::default(),
        }
    }
}

impl From<DbConfig> for MssqlConnectionConfig {
    fn from(c: DbConfig) -> Self {
        MssqlConnectionConfig {
            server: c.server,
            port: c.port,
            database: c.database,
            user: c.user,
            password: c.password,
            options: c.options,
        }
    }
}

impl From<MssqlConnectionConfig> for DbConfig {
    fn from(c: MssqlConnectionConfig) -> Self {
        DbConfig {
            server: c.server,
            port: c.port,
            database: c.database,
            user: c.user,
            password: c.password,
            options: c.options,
        }
    }
}

// ============================================================================
// Loading
// ============================================================================

/// Load the multi-connection `AppConfig`.
///
/// Detection order:
/// 1. `config.json` with `"connections"` array → new multi-connection format
/// 2. `config.json` with `"server"` key → legacy single-MSSQL format, wrapped
/// 3. No config.json → build single MSSQL from env vars (DB_SERVER, etc.)
pub fn load_app_config() -> Result<AppConfig> {
    dotenv::dotenv().ok();

    let config_path = Path::new("config.json");
    if config_path.exists() {
        let content = fs::read_to_string(config_path).context("Failed to read config.json")?;
        let raw: serde_json::Value =
            serde_json::from_str(&content).context("Failed to parse config.json")?;

        if raw.get("connections").is_some() {
            // New multi-connection format
            let config: AppConfig = serde_json::from_value(raw)
                .context("Failed to parse config.json as multi-connection config")?;
            return Ok(config);
        }

        // Legacy single-MSSQL format (has "server" key)
        let legacy: DbConfig = serde_json::from_value(raw)
            .context("Failed to parse config.json as legacy DbConfig")?;
        return Ok(AppConfig {
            default_connection: Some("default".to_string()),
            connections: vec![NamedConnection {
                name: "default".to_string(),
                config: ConnectionConfig::Mssql(legacy.into()),
            }],
        });
    }

    // No config.json — build from env vars (only if DB_SERVER is explicitly set)
    let has_db_env = env::var("DB_SERVER").is_ok();
    if !has_db_env {
        tracing::info!("No config.json and no DB_SERVER env var — starting with no database connections");
        return Ok(AppConfig {
            default_connection: None,
            connections: vec![],
        });
    }

    let mssql_cfg = MssqlConnectionConfig {
        server: env::var("DB_SERVER").unwrap_or_else(|_| "localhost".to_string()),
        port: env::var("DB_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1433),
        database: env::var("DB_DATABASE").unwrap_or_else(|_| "master".to_string()),
        user: env::var("DB_USER").unwrap_or_else(|_| "sa".to_string()),
        password: env::var("DB_PASSWORD").unwrap_or_default(),
        options: DbOptions {
            encrypt: env::var("DB_ENCRYPT")
                .ok()
                .map(|s| s.to_lowercase() == "true")
                .unwrap_or(false),
            trust_server_certificate: env::var("DB_TRUST_CERT")
                .ok()
                .map(|s| s.to_lowercase() != "false")
                .unwrap_or(true),
        },
    };

    Ok(AppConfig {
        default_connection: Some("default".to_string()),
        connections: vec![NamedConnection {
            name: "default".to_string(),
            config: ConnectionConfig::Mssql(mssql_cfg),
        }],
    })
}

/// Legacy loader — returns the first MSSQL connection as a `DbConfig`.
/// Used by code that hasn't migrated to multi-connection yet.
#[allow(dead_code)]
pub fn load_db_config() -> Result<DbConfig> {
    let app = load_app_config()?;
    for conn in &app.connections {
        if let ConnectionConfig::Mssql(ref cfg) = conn.config {
            return Ok(cfg.clone().into());
        }
    }
    // Fallback to defaults
    Ok(DbConfig::default())
}

/// Helper: get the default database name from an `AppConfig`.
#[allow(dead_code)]
pub fn default_database(config: &AppConfig) -> String {
    let default_name = config
        .default_connection
        .as_deref()
        .or_else(|| config.connections.first().map(|c| c.name.as_str()));

    if let Some(name) = default_name {
        for conn in &config.connections {
            if conn.name == name {
                return match &conn.config {
                    ConnectionConfig::Mssql(c) => c.database.clone(),
                    ConnectionConfig::Postgres(c) => c.database.clone(),
                    #[cfg(feature = "duckdb_backend")]
                    ConnectionConfig::DuckDb(c) => c.path.clone(),
                    #[cfg(feature = "storage")]
                    ConnectionConfig::Minio(_) => String::new(),
                };
            }
        }
    }

    "master".to_string()
}

// ============================================================================
// PII Helpers (unchanged)
// ============================================================================

pub fn parse_pii_mode_str(s: &str) -> Option<pii::PiiMode> {
    match s.to_ascii_lowercase().as_str() {
        "scrub" => Some(pii::PiiMode::Scrub),
        _ => None,
    }
}

pub fn env_default_pii_mode() -> Option<pii::PiiMode> {
    env::var("LANE_DEFAULT_PII_MODE")
        .ok()
        .and_then(|v| parse_pii_mode_str(&v))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_legacy_config() {
        let json = r#"{
            "server": "sql.example.com",
            "port": 1433,
            "database": "mydb",
            "user": "sa",
            "password": "secret",
            "options": { "encrypt": false, "trustServerCertificate": true }
        }"#;

        let raw: serde_json::Value = serde_json::from_str(json).unwrap();
        assert!(raw.get("connections").is_none());
        assert!(raw.get("server").is_some());

        let legacy: DbConfig = serde_json::from_value(raw).unwrap();
        let app = AppConfig {
            default_connection: Some("default".to_string()),
            connections: vec![NamedConnection {
                name: "default".to_string(),
                config: ConnectionConfig::Mssql(legacy.into()),
            }],
        };

        assert_eq!(app.connections.len(), 1);
        assert_eq!(app.connections[0].name, "default");
        match &app.connections[0].config {
            ConnectionConfig::Mssql(c) => {
                assert_eq!(c.server, "sql.example.com");
                assert_eq!(c.database, "mydb");
            }
            _ => panic!("Expected MSSQL config"),
        }
    }

    #[test]
    fn parse_multi_connection_config() {
        let json = r#"{
            "default_connection": "production",
            "connections": [
                {
                    "name": "production",
                    "type": "mssql",
                    "server": "sql.example.com",
                    "port": 1433,
                    "database": "master",
                    "user": "sa",
                    "password": "secret",
                    "options": { "encrypt": false, "trustServerCertificate": true }
                },
                {
                    "name": "analytics",
                    "type": "postgres",
                    "host": "pg.example.com",
                    "port": 5432,
                    "database": "analytics",
                    "user": "readonly",
                    "password": "secret"
                }
            ]
        }"#;

        let app: AppConfig = serde_json::from_str(json).unwrap();
        assert_eq!(app.default_connection.as_deref(), Some("production"));
        assert_eq!(app.connections.len(), 2);

        assert_eq!(app.connections[0].name, "production");
        match &app.connections[0].config {
            ConnectionConfig::Mssql(c) => assert_eq!(c.server, "sql.example.com"),
            _ => panic!("Expected MSSQL"),
        }

        assert_eq!(app.connections[1].name, "analytics");
        match &app.connections[1].config {
            ConnectionConfig::Postgres(c) => {
                assert_eq!(c.host, "pg.example.com");
                assert_eq!(c.database, "analytics");
            }
            _ => panic!("Expected Postgres"),
        }
    }

    #[test]
    fn default_database_helper() {
        let app = AppConfig {
            default_connection: Some("pg".to_string()),
            connections: vec![
                NamedConnection {
                    name: "mssql".to_string(),
                    config: ConnectionConfig::Mssql(MssqlConnectionConfig {
                        server: "localhost".into(),
                        port: 1433,
                        database: "master".into(),
                        user: "sa".into(),
                        password: "".into(),
                        options: DbOptions::default(),
                    }),
                },
                NamedConnection {
                    name: "pg".to_string(),
                    config: ConnectionConfig::Postgres(PostgresConnectionConfig {
                        host: "localhost".into(),
                        port: 5432,
                        database: "analytics".into(),
                        user: "user".into(),
                        password: "".into(),
                        sslmode: None,
                    }),
                },
            ],
        };

        assert_eq!(default_database(&app), "analytics");
    }
}
