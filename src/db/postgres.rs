//! Postgres database backend implementation using tokio-postgres + deadpool.

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use deadpool_postgres::{Pool, Runtime};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio_postgres::types::Type;
use tokio_postgres::NoTls;
use postgres_native_tls::MakeTlsConnector;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::PostgresConnectionConfig;
use crate::query::{
    BlobFormat, ColumnMeta, CountMode, QueryMetadata, QueryParams, QueryResult,
    ROW_NUMBER_ALIAS, build_pii_processor, format_binary_data,
    pagination::{create_paginated_query, get_count_query},
    validation::is_exec_query,
};

use super::{DatabaseBackend, Dialect, StreamChunk};

// ---------------------------------------------------------------------------
// Pool management
// ---------------------------------------------------------------------------

/// Postgres backend with lazy per-database pool creation.
///
/// Postgres doesn't have `USE [db]` — each connection is bound to a database.
/// For the primary database (from config), a pool is created on init.
/// For other databases on the same server, pools are lazily created.
pub struct PostgresBackend {
    config: PostgresConnectionConfig,
    pools: RwLock<HashMap<String, Pool>>,
    default_db: String,
}

impl PostgresBackend {
    const MAX_POOLS: usize = 10;
    const POOL_SIZE: usize = 10;

    pub async fn new(config: PostgresConnectionConfig) -> Result<Self> {
        let default_db = config.database.clone();
        let backend = Self {
            config,
            pools: RwLock::new(HashMap::new()),
            default_db: default_db.clone(),
        };

        // Eagerly create a pool for the default database to verify connectivity
        backend.get_pool(&default_db).await?;
        info!(
            "Postgres connection pool initialized for '{}' (max_size={})",
            default_db,
            Self::POOL_SIZE
        );

        Ok(backend)
    }

    /// Get or create a pool for the given database.
    async fn get_pool(&self, database: &str) -> Result<Pool> {
        // Fast path: pool exists
        {
            let pools = self.pools.read().await;
            if let Some(pool) = pools.get(database) {
                return Ok(pool.clone());
            }
        }

        // Slow path: create a new pool
        let mut pools = self.pools.write().await;

        // Double-check after acquiring write lock
        if let Some(pool) = pools.get(database) {
            return Ok(pool.clone());
        }

        if pools.len() >= Self::MAX_POOLS {
            anyhow::bail!(
                "Maximum pool count ({}) reached. Cannot create pool for database '{}'. \
                 Consider using separate named connections for different databases.",
                Self::MAX_POOLS,
                database
            );
        }

        let pool = self.create_pool(database)?;

        // Test connectivity
        let client = pool
            .get()
            .await
            .context(format!("Failed to connect to Postgres database '{}'", database))?;
        drop(client);

        debug!("Created Postgres pool for database '{}'", database);
        pools.insert(database.to_string(), pool.clone());

        Ok(pool)
    }

    fn create_pool(&self, database: &str) -> Result<Pool> {
        let sslmode = self.config.sslmode.as_deref().unwrap_or("prefer");

        // Build tokio_postgres::Config directly — deadpool's Config doesn't propagate channel_binding
        let mut pg_config = tokio_postgres::Config::new();
        pg_config.host(&self.config.host);
        pg_config.port(self.config.port);
        pg_config.dbname(database);
        pg_config.user(&self.config.user);
        pg_config.password(&self.config.password);

        let pool = match sslmode {
            "disable" => {
                let mgr = deadpool_postgres::Manager::new(pg_config, NoTls);
                Pool::builder(mgr)
                    .max_size(Self::POOL_SIZE)
                    .runtime(Runtime::Tokio1)
                    .build()
                    .context("Failed to create Postgres connection pool")?
            }
            "prefer" => {
                // Best-effort TLS: encrypt if possible, accept any cert
                // Disable channel binding — SCRAM fails when cert is unverified
                pg_config.channel_binding(tokio_postgres::config::ChannelBinding::Disable);
                let mut builder = native_tls::TlsConnector::builder();
                builder.danger_accept_invalid_certs(true);
                let connector = builder.build().context("Failed to build TLS connector")?;
                let tls = MakeTlsConnector::new(connector);
                let mgr = deadpool_postgres::Manager::new(pg_config, tls);
                Pool::builder(mgr)
                    .max_size(Self::POOL_SIZE)
                    .runtime(Runtime::Tokio1)
                    .build()
                    .context("Failed to create Postgres connection pool (TLS)")?
            }
            "require" | "verify-ca" | "verify-full" => {
                // TLS required, cert validated, channel binding enabled
                pg_config.channel_binding(tokio_postgres::config::ChannelBinding::Prefer);
                let connector = native_tls::TlsConnector::builder()
                    .build()
                    .context("Failed to build TLS connector")?;
                let tls = MakeTlsConnector::new(connector);
                let mgr = deadpool_postgres::Manager::new(pg_config, tls);
                Pool::builder(mgr)
                    .max_size(Self::POOL_SIZE)
                    .runtime(Runtime::Tokio1)
                    .build()
                    .context("Failed to create Postgres connection pool (TLS)")?
            }
            other => anyhow::bail!("Unsupported Postgres sslmode: '{}'. Use disable, prefer, require, verify-ca, or verify-full.", other),
        };

        Ok(pool)
    }
}

// ---------------------------------------------------------------------------
// Row conversion helpers
// ---------------------------------------------------------------------------

fn pg_row_to_json(
    row: &tokio_postgres::Row,
    pii_processor: &crate::pii::PiiProcessor,
    blob_format: &BlobFormat,
) -> HashMap<String, Value> {
    let mut map = HashMap::new();

    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        let value = pg_column_to_value(row, i, col.type_(), blob_format);
        map.insert(name, value);
    }

    crate::pii::process_json_row(pii_processor, &mut map);
    map
}

fn pg_column_to_value(
    row: &tokio_postgres::Row,
    idx: usize,
    pg_type: &Type,
    blob_format: &BlobFormat,
) -> Value {
    // Try each type; if the column is NULL, the get() returns None

    match *pg_type {
        // Booleans
        Type::BOOL => match row.get::<_, Option<bool>>(idx) {
            Some(v) => Value::Bool(v),
            None => Value::Null,
        },

        // Integers
        Type::INT2 => match row.get::<_, Option<i16>>(idx) {
            Some(v) => Value::from(v),
            None => Value::Null,
        },
        Type::INT4 | Type::OID => match row.get::<_, Option<i32>>(idx) {
            Some(v) => Value::from(v),
            None => Value::Null,
        },
        Type::INT8 => match row.get::<_, Option<i64>>(idx) {
            Some(v) => Value::from(v),
            None => Value::Null,
        },

        // Floats
        Type::FLOAT4 => match row.get::<_, Option<f32>>(idx) {
            Some(v) => serde_json::Number::from_f64(v as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            None => Value::Null,
        },
        Type::FLOAT8 => match row.get::<_, Option<f64>>(idx) {
            Some(v) => serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            None => Value::Null,
        },
        // NUMERIC/DECIMAL — postgres-types 0.2 has no native Rust mapping.
        // Decode from Postgres binary wire format directly.
        Type::NUMERIC => pg_numeric_to_value(row, idx),

        // Text types
        Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::NAME | Type::CHAR => {
            match row.get::<_, Option<String>>(idx) {
                Some(v) => Value::String(v),
                None => Value::Null,
            }
        }

        // UUID
        Type::UUID => match row.get::<_, Option<Uuid>>(idx) {
            Some(v) => Value::String(v.to_string()),
            None => Value::Null,
        },

        // Date/Time
        Type::DATE => match row.get::<_, Option<NaiveDate>>(idx) {
            Some(v) => Value::String(v.to_string()),
            None => Value::Null,
        },
        Type::TIME => match row.get::<_, Option<NaiveTime>>(idx) {
            Some(v) => Value::String(v.to_string()),
            None => Value::Null,
        },
        Type::TIMESTAMP => match row.get::<_, Option<NaiveDateTime>>(idx) {
            Some(v) => Value::String(v.to_string()),
            None => Value::Null,
        },
        Type::TIMESTAMPTZ => {
            match row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
                Some(v) => Value::String(v.to_rfc3339()),
                None => Value::Null,
            }
        }

        // JSON/JSONB
        Type::JSON | Type::JSONB => match row.get::<_, Option<Value>>(idx) {
            Some(v) => v,
            None => Value::Null,
        },

        // Binary
        Type::BYTEA => match row.get::<_, Option<Vec<u8>>>(idx) {
            Some(v) => Value::String(format_binary_data(&v, blob_format)),
            None => Value::Null,
        },

        // Arrays (common types) — serialize as JSON arrays
        Type::BOOL_ARRAY => match row.get::<_, Option<Vec<bool>>>(idx) {
            Some(v) => Value::Array(v.into_iter().map(Value::Bool).collect()),
            None => Value::Null,
        },
        Type::INT4_ARRAY => match row.get::<_, Option<Vec<i32>>>(idx) {
            Some(v) => Value::Array(v.into_iter().map(Value::from).collect()),
            None => Value::Null,
        },
        Type::INT8_ARRAY => match row.get::<_, Option<Vec<i64>>>(idx) {
            Some(v) => Value::Array(v.into_iter().map(Value::from).collect()),
            None => Value::Null,
        },
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => {
            match row.get::<_, Option<Vec<String>>>(idx) {
                Some(v) => Value::Array(v.into_iter().map(Value::String).collect()),
                None => Value::Null,
            }
        }

        // Fallback: try as string
        _ => match row.try_get::<_, Option<String>>(idx) {
            Ok(Some(v)) => Value::String(v),
            Ok(None) => Value::Null,
            Err(_) => Value::String(format!("[unsupported type: {}]", pg_type.name())),
        },
    }
}

/// Decode a Postgres NUMERIC value from the binary wire format.
///
/// Wire format: 4 x i16 header (ndigits, weight, sign, dscale) + ndigits x i16 base-10000 digits.
fn pg_numeric_to_value(row: &tokio_postgres::Row, idx: usize) -> Value {
    use tokio_postgres::types::FromSql;

    // Try to get raw bytes via the private-but-accessible path
    // We implement a tiny wrapper that accepts NUMERIC
    struct PgNumeric(String);

    impl<'a> FromSql<'a> for PgNumeric {
        fn from_sql(
            _ty: &Type,
            raw: &'a [u8],
        ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
            if raw.len() < 8 {
                return Err("numeric too short".into());
            }
            let ndigits = i16::from_be_bytes([raw[0], raw[1]]) as usize;
            let weight = i16::from_be_bytes([raw[2], raw[3]]);
            let sign = u16::from_be_bytes([raw[4], raw[5]]);
            let dscale = i16::from_be_bytes([raw[6], raw[7]]) as usize;

            if raw.len() < 8 + ndigits * 2 {
                return Err("numeric data truncated".into());
            }

            // Special values
            const NUMERIC_POS: u16 = 0x0000;
            const NUMERIC_NEG: u16 = 0x4000;
            const NUMERIC_NAN: u16 = 0xC000;

            if sign == NUMERIC_NAN {
                return Ok(PgNumeric("NaN".to_string()));
            }

            let mut digits = Vec::with_capacity(ndigits);
            for i in 0..ndigits {
                let offset = 8 + i * 2;
                digits.push(i16::from_be_bytes([raw[offset], raw[offset + 1]]));
            }

            if ndigits == 0 {
                return if dscale > 0 {
                    let mut s = "0.".to_string();
                    s.extend(std::iter::repeat('0').take(dscale));
                    Ok(PgNumeric(s))
                } else {
                    Ok(PgNumeric("0".to_string()))
                };
            }

            let mut result = String::new();
            if sign == NUMERIC_NEG {
                result.push('-');
            }

            // Integer part: digits with weight >= 0
            let int_digits = (weight + 1).max(0) as usize;
            if int_digits == 0 {
                result.push('0');
            } else {
                for i in 0..int_digits {
                    let d = if i < ndigits { digits[i] } else { 0 };
                    if i == 0 {
                        result.push_str(&d.to_string());
                    } else {
                        result.push_str(&format!("{:04}", d));
                    }
                }
            }

            // Fractional part
            if dscale > 0 {
                result.push('.');
                let mut frac_written = 0usize;
                for i in int_digits..ndigits {
                    let d = digits[i];
                    let s = format!("{:04}", d);
                    for ch in s.chars() {
                        if frac_written >= dscale {
                            break;
                        }
                        result.push(ch);
                        frac_written += 1;
                    }
                }
                // Pad with zeros if needed
                while frac_written < dscale {
                    result.push('0');
                    frac_written += 1;
                }
            }

            Ok(PgNumeric(result))
        }

        fn accepts(ty: &Type) -> bool {
            *ty == Type::NUMERIC
        }
    }

    match row.try_get::<_, Option<PgNumeric>>(idx) {
        Ok(Some(PgNumeric(s))) => Value::String(s),
        Ok(None) => Value::Null,
        Err(e) => {
            warn!("Failed to decode NUMERIC column {}: {}", idx, e);
            Value::Null
        }
    }
}

fn pg_type_name(pg_type: &Type) -> &str {
    match *pg_type {
        Type::BOOL => "Boolean",
        Type::INT2 => "SmallInt",
        Type::INT4 => "Integer",
        Type::INT8 => "BigInt",
        Type::FLOAT4 => "Real",
        Type::FLOAT8 => "Double",
        Type::NUMERIC => "Numeric",
        Type::VARCHAR | Type::BPCHAR | Type::CHAR => "VarChar",
        Type::TEXT => "Text",
        Type::NAME => "Name",
        Type::UUID => "UUID",
        Type::DATE => "Date",
        Type::TIME => "Time",
        Type::TIMESTAMP => "Timestamp",
        Type::TIMESTAMPTZ => "TimestampTz",
        Type::JSON => "JSON",
        Type::JSONB => "JSONB",
        Type::BYTEA => "Bytea",
        Type::OID => "OID",
        _ => pg_type.name(),
    }
}

// ---------------------------------------------------------------------------
// Error enrichment helpers
// ---------------------------------------------------------------------------

/// Extract table names from FROM/JOIN clauses in a SQL query.
fn extract_tables_from_query(query: &str) -> Vec<String> {
    let re = regex::Regex::new(
        r#"(?i)(?:FROM|JOIN)\s+("?[A-Za-z_][A-Za-z0-9_]*"?(?:\."?[A-Za-z_][A-Za-z0-9_]*"?)?)"#
    ).unwrap();
    let mut tables = Vec::new();
    for cap in re.captures_iter(query) {
        let raw = &cap[1];
        // Take the last segment (after schema dot) and strip quotes
        let table_name = raw
            .rsplit('.')
            .next()
            .unwrap_or(raw)
            .trim_matches('"')
            .to_string();
        if !tables.contains(&table_name) {
            tables.push(table_name);
        }
    }
    tables
}

/// Extract a value from a Postgres "column X does not exist" or "relation X does not exist" error.
fn extract_pg_error_object(message: &str) -> Option<String> {
    // Patterns: 'column "foo" does not exist', 'column e.foo does not exist',
    //           'relation "foo" does not exist'
    let re = regex::Regex::new(r#"(?:column|relation)\s+"?([^"\s]+)"?\s+does not exist"#).ok()?;
    re.captures(message).map(|c| c[1].to_string())
}

/// Enrich a Postgres "column does not exist" error with available column names.
async fn enrich_pg_column_error(
    pool: &Pool,
    database: &str,
    query: &str,
    error_msg: &str,
) -> Option<String> {
    let tables = extract_tables_from_query(query);
    if tables.is_empty() {
        return None;
    }

    let client = pool.get().await.ok()?;
    let mut parts: Vec<String> = Vec::new();

    for table in &tables {
        let rows = client
            .query(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = $1 ORDER BY ordinal_position",
                &[table],
            )
            .await
            .ok()?;

        let col_names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        if !col_names.is_empty() {
            parts.push(format!("[{}]: {}", table, col_names.join(", ")));
        }
    }

    if parts.is_empty() {
        return None;
    }

    let obj = extract_pg_error_object(error_msg).unwrap_or_default();
    Some(format!(
        "Column '{}' not found. Available columns: {}",
        obj,
        parts.join("; ")
    ))
}

/// Enrich a Postgres "relation does not exist" error with similar table names.
async fn enrich_pg_relation_error(
    pool: &Pool,
    _database: &str,
    error_msg: &str,
) -> Option<String> {
    let invalid_table = extract_pg_error_object(error_msg)?;
    let client = pool.get().await.ok()?;

    // Try fuzzy match first
    let pattern = format!("%{}%", invalid_table);
    let rows = client
        .query(
            "SELECT table_schema || '.' || table_name \
             FROM information_schema.tables \
             WHERE table_name LIKE $1 \
             AND table_schema NOT IN ('pg_catalog', 'information_schema') \
             ORDER BY table_name LIMIT 20",
            &[&pattern],
        )
        .await
        .ok()?;

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    if !names.is_empty() {
        return Some(format!(
            "Table '{}' not found. Similar tables: {}",
            invalid_table,
            names.join(", ")
        ));
    }

    // No fuzzy matches — list all tables
    let rows = client
        .query(
            "SELECT table_schema || '.' || table_name \
             FROM information_schema.tables \
             WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
             ORDER BY table_name LIMIT 50",
            &[],
        )
        .await
        .ok()?;

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    if !names.is_empty() {
        return Some(format!(
            "Table '{}' not found. Available tables: {}",
            invalid_table,
            names.join(", ")
        ));
    }

    None
}

/// Try to enrich a Postgres error with schema hints.
async fn try_enrich_pg_error(
    err: tokio_postgres::Error,
    pool: &Pool,
    database: &str,
    query: &str,
    context_msg: &str,
) -> anyhow::Error {
    // err.to_string() only returns "db error" — the actual message is in the source chain.
    // Use the full "{}" chain to get the real Postgres error text.
    let error_msg = if let Some(db_err) = err.as_db_error() {
        db_err.message().to_string()
    } else {
        format!("{}", err)
    };

    let hint = if error_msg.contains("does not exist") {
        if error_msg.contains("column") {
            enrich_pg_column_error(pool, database, query, &error_msg).await
        } else if error_msg.contains("relation") {
            enrich_pg_relation_error(pool, database, &error_msg).await
        } else {
            None
        }
    } else {
        None
    };

    if let Some(hint_text) = hint {
        return anyhow::anyhow!("{} | Hint: {}", err, hint_text)
            .context(context_msg.to_string());
    }

    anyhow::Error::new(err).context(context_msg.to_string())
}

// ---------------------------------------------------------------------------
// DatabaseBackend implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl DatabaseBackend for PostgresBackend {
    async fn execute_query(&self, params: &QueryParams) -> Result<QueryResult> {
        let start = Instant::now();
        let pool = self.get_pool(&params.database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection from pool")?;

        let pii_processor = build_pii_processor(params);

        // Enforce read-only at the database level for ReadOnly users.
        // Wraps the query in a READ ONLY transaction so even functions with
        // side effects are blocked by Postgres itself.
        if params.read_only {
            client.batch_execute("BEGIN TRANSACTION READ ONLY").await
                .context("Failed to start read-only transaction")?;
        }

        // For EXEC queries (not applicable to Postgres), just run directly
        if is_exec_query(&params.query) {
            // Postgres doesn't have EXEC, treat as direct execution
            let rows = match client.query(&params.query, &[]).await {
                Ok(rows) => rows,
                Err(e) => {
                    if params.read_only { let _ = client.batch_execute("ROLLBACK").await; }
                    return Err(try_enrich_pg_error(e, &pool, &params.database, &params.query, "Failed to execute query").await);
                }
            };

            let mut data = Vec::new();
            for row in &rows {
                data.push(pg_row_to_json(row, &pii_processor, &params.blob_format));
            }

            if params.read_only { let _ = client.batch_execute("COMMIT").await; }
            let elapsed = start.elapsed().as_millis();
            let total = data.len() as i64;

            return Ok(QueryResult {
                success: true,
                total_rows: total,
                execution_time_ms: elapsed,
                rows_per_second: if elapsed > 0 {
                    total as f64 / (elapsed as f64 / 1000.0)
                } else {
                    total as f64
                },
                data,
                result_sets: None,
                result_set_count: None,
                metadata: if params.include_metadata && !rows.is_empty() {
                    Some(build_metadata(rows[0].columns()))
                } else {
                    None
                },
            });
        }

        // Paginated path
        if params.pagination {
            let result = self
                .execute_paginated(&client, params, &pii_processor, start)
                .await;
            if params.read_only { let _ = client.batch_execute("COMMIT").await; }
            return result;
        }

        // Standard buffered path
        let rows = match client.query(&params.query, &[]).await {
            Ok(rows) => rows,
            Err(e) => {
                if params.read_only { let _ = client.batch_execute("ROLLBACK").await; }
                return Err(try_enrich_pg_error(e, &pool, &params.database, &params.query, "Failed to execute query").await);
            }
        };

        let metadata = if params.include_metadata && !rows.is_empty() {
            Some(build_metadata(rows[0].columns()))
        } else {
            None
        };

        let mut data = Vec::new();
        for row in &rows {
            data.push(pg_row_to_json(row, &pii_processor, &params.blob_format));
        }

        if params.read_only { let _ = client.batch_execute("COMMIT").await; }
        let elapsed = start.elapsed().as_millis();
        let total = data.len() as i64;

        Ok(QueryResult {
            success: true,
            total_rows: total,
            execution_time_ms: elapsed,
            rows_per_second: if elapsed > 0 {
                total as f64 / (elapsed as f64 / 1000.0)
            } else {
                total as f64
            },
            data,
            result_sets: None,
            result_set_count: None,
            metadata,
        })
    }

    async fn validate_query(&self, database: &str, query: &str) -> Result<(), String> {
        let pool = self.get_pool(database).await.map_err(|e| e.to_string())?;
        let client = pool
            .get()
            .await
            .map_err(|e| format!("Failed to get connection: {}", e))?;

        // Postgres validation: PREPARE then DEALLOCATE
        let stmt_name = format!("__bq_validate_{}", uuid::Uuid::new_v4().simple());
        let prepare_sql = format!("PREPARE {} AS {}", stmt_name, query);

        if let Err(e) = client.execute(&prepare_sql, &[]).await {
            return Err(format!("{}", e));
        }

        // Clean up the prepared statement
        let deallocate_sql = format!("DEALLOCATE {}", stmt_name);
        let _ = client.execute(&deallocate_sql, &[]).await;

        Ok(())
    }

    async fn list_databases(&self) -> Result<Vec<HashMap<String, Value>>> {
        let pool = self.get_pool(&self.default_db).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let rows = client
            .query(
                "SELECT datname as name FROM pg_database WHERE datistemplate = false ORDER BY datname",
                &[],
            )
            .await
            .context("Failed to list databases")?;

        let mut databases = Vec::new();
        for row in &rows {
            let mut map = HashMap::new();
            let name: String = row.get("name");
            map.insert("name".to_string(), Value::String(name));
            databases.push(map);
        }

        Ok(databases)
    }

    async fn list_schemas(&self, database: &str) -> Result<Vec<HashMap<String, Value>>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let rows = client
            .query(
                "SELECT schema_name FROM information_schema.schemata \
                 WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
                 ORDER BY schema_name",
                &[],
            )
            .await
            .context("Failed to list schemas")?;

        let mut schemas = Vec::new();
        for row in &rows {
            let mut map = HashMap::new();
            let name: String = row.get("schema_name");
            map.insert("schema_name".to_string(), Value::String(name));
            schemas.push(map);
        }

        Ok(schemas)
    }

    async fn list_tables(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let rows = client
            .query(
                "SELECT t.table_schema AS \"TABLE_SCHEMA\", \
                        t.table_name AS \"TABLE_NAME\", \
                        t.table_type AS \"TABLE_TYPE\", \
                        COALESCE(c.reltuples::bigint, 0) AS row_count \
                 FROM information_schema.tables t \
                 LEFT JOIN pg_class c ON c.relname = t.table_name \
                   AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = t.table_schema) \
                 WHERE t.table_schema = $1 \
                 ORDER BY t.table_name",
                &[&schema],
            )
            .await
            .context("Failed to list tables")?;

        let mut tables = Vec::new();
        for row in &rows {
            let mut map = HashMap::new();
            let table_schema: String = row.get("TABLE_SCHEMA");
            let table_name: String = row.get("TABLE_NAME");
            let table_type: String = row.get("TABLE_TYPE");
            let row_count: i64 = row.get("row_count");

            map.insert("TABLE_SCHEMA".to_string(), Value::String(table_schema));
            map.insert("TABLE_NAME".to_string(), Value::String(table_name));
            map.insert("TABLE_TYPE".to_string(), Value::String(table_type));
            map.insert("ROW_COUNT".to_string(), Value::from(row_count));
            tables.push(map);
        }

        Ok(tables)
    }

    async fn describe_table(
        &self,
        database: &str,
        table: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let rows = client
            .query(
                "SELECT c.column_name AS \"COLUMN_NAME\", \
                        c.data_type AS \"DATA_TYPE\", \
                        c.is_nullable AS \"IS_NULLABLE\", \
                        c.character_maximum_length::int AS char_max_len, \
                        c.numeric_precision::int AS num_precision, \
                        c.numeric_scale::int AS num_scale, \
                        CASE WHEN pk.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS \"IS_PRIMARY_KEY\" \
                 FROM information_schema.columns c \
                 LEFT JOIN ( \
                     SELECT kcu.column_name \
                     FROM information_schema.table_constraints tc \
                     JOIN information_schema.key_column_usage kcu \
                       ON tc.constraint_name = kcu.constraint_name \
                       AND tc.table_schema = kcu.table_schema \
                     WHERE tc.constraint_type = 'PRIMARY KEY' \
                       AND tc.table_name = $1 \
                       AND tc.table_schema = $2 \
                 ) pk ON c.column_name = pk.column_name \
                 WHERE c.table_name = $1 AND c.table_schema = $2 \
                 ORDER BY c.ordinal_position",
                &[&table, &schema],
            )
            .await
            .context("Failed to describe table")?;

        let mut columns = Vec::new();
        for row in &rows {
            let mut map = HashMap::new();
            let col_name: String = row.get("COLUMN_NAME");
            let data_type: String = row.get("DATA_TYPE");
            let is_nullable: String = row.get("IS_NULLABLE");
            let char_max: Option<i32> = row.get("char_max_len");
            let num_prec: Option<i32> = row.get("num_precision");
            let num_scale: Option<i32> = row.get("num_scale");
            let is_pk: String = row.get("IS_PRIMARY_KEY");

            map.insert("COLUMN_NAME".to_string(), Value::String(col_name));
            map.insert("DATA_TYPE".to_string(), Value::String(data_type));
            map.insert("IS_NULLABLE".to_string(), Value::String(is_nullable));
            map.insert(
                "CHARACTER_MAXIMUM_LENGTH".to_string(),
                char_max.map(|v| Value::from(v as i64)).unwrap_or(Value::Null),
            );
            map.insert(
                "NUMERIC_PRECISION".to_string(),
                num_prec.map(|v| Value::from(v as i64)).unwrap_or(Value::Null),
            );
            map.insert(
                "NUMERIC_SCALE".to_string(),
                num_scale.map(|v| Value::from(v as i64)).unwrap_or(Value::Null),
            );
            map.insert("IS_PRIMARY_KEY".to_string(), Value::String(is_pk));
            columns.push(map);
        }

        Ok(columns)
    }

    async fn get_foreign_keys(
        &self,
        database: &str,
        table: &str,
        schema: &str,
    ) -> Result<Vec<super::ForeignKeyInfo>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection for FK query")?;

        // Query both outgoing and incoming FKs using information_schema.
        // Returns one row per FK column mapping.
        let rows = client
            .query(
                "SELECT \
                    tc.constraint_name, \
                    kcu.table_schema AS from_schema, \
                    kcu.table_name AS from_table, \
                    kcu.column_name AS from_column, \
                    ccu.table_schema AS to_schema, \
                    ccu.table_name AS to_table, \
                    ccu.column_name AS to_column, \
                    kcu.ordinal_position \
                 FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage kcu \
                   ON tc.constraint_name = kcu.constraint_name \
                   AND tc.table_schema = kcu.table_schema \
                 JOIN information_schema.constraint_column_usage ccu \
                   ON tc.constraint_name = ccu.constraint_name \
                   AND tc.table_schema = ccu.table_schema \
                 WHERE tc.constraint_type = 'FOREIGN KEY' \
                   AND ((kcu.table_name = $1 AND kcu.table_schema = $2) \
                     OR (ccu.table_name = $1 AND ccu.table_schema = $2)) \
                 ORDER BY tc.constraint_name, kcu.ordinal_position",
                &[&table, &schema],
            )
            .await
            .context("Failed to query foreign keys")?;

        let mut fk_map: std::collections::BTreeMap<String, super::ForeignKeyInfo> =
            std::collections::BTreeMap::new();

        for row in &rows {
            let constraint_name: String = row.get("constraint_name");
            let from_schema: String = row.get("from_schema");
            let from_table: String = row.get("from_table");
            let from_column: String = row.get("from_column");
            let to_schema: String = row.get("to_schema");
            let to_table: String = row.get("to_table");
            let to_column: String = row.get("to_column");

            let entry = fk_map.entry(constraint_name.clone()).or_insert_with(|| {
                super::ForeignKeyInfo {
                    constraint_name,
                    from_schema,
                    from_table,
                    from_columns: Vec::new(),
                    to_schema,
                    to_table,
                    to_columns: Vec::new(),
                }
            });
            entry.from_columns.push(from_column);
            entry.to_columns.push(to_column);
        }

        Ok(fk_map.into_values().collect())
    }

    async fn list_views(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let rows = client
            .query(
                "SELECT v.viewname AS name, v.schemaname AS schema_name, 'VIEW' AS type \
                 FROM pg_views v WHERE v.schemaname = $1 \
                 UNION ALL \
                 SELECT m.matviewname AS name, m.schemaname AS schema_name, 'MATERIALIZED VIEW' AS type \
                 FROM pg_matviews m WHERE m.schemaname = $1 \
                 ORDER BY name",
                &[&schema],
            )
            .await
            .context("Failed to list views")?;

        let mut views = Vec::new();
        for row in &rows {
            let mut map = HashMap::new();
            let name: String = row.get("name");
            let schema_name: String = row.get("schema_name");
            let view_type: String = row.get("type");
            map.insert("name".to_string(), Value::String(name));
            map.insert("schema_name".to_string(), Value::String(schema_name));
            map.insert("type".to_string(), Value::String(view_type));
            views.push(map);
        }

        Ok(views)
    }

    async fn list_routines(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let rows = client
            .query(
                "SELECT p.proname AS name, n.nspname AS schema_name, \
                        CASE p.prokind WHEN 'f' THEN 'FUNCTION' WHEN 'p' THEN 'PROCEDURE' END AS routine_type \
                 FROM pg_proc p \
                 JOIN pg_namespace n ON p.pronamespace = n.oid \
                 WHERE n.nspname = $1 AND p.prokind IN ('f','p') \
                 ORDER BY p.prokind, p.proname",
                &[&schema],
            )
            .await
            .context("Failed to list routines")?;

        let mut routines = Vec::new();
        for row in &rows {
            let mut map = HashMap::new();
            let name: String = row.get("name");
            let schema_name: String = row.get("schema_name");
            let routine_type: String = row.get("routine_type");
            map.insert("name".to_string(), Value::String(name));
            map.insert("schema_name".to_string(), Value::String(schema_name));
            map.insert("routine_type".to_string(), Value::String(routine_type));
            routines.push(map);
        }

        Ok(routines)
    }

    async fn get_object_definition(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        object_type: &str,
    ) -> Result<Option<HashMap<String, Value>>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::String(name.to_string()));
        map.insert("schema_name".to_string(), Value::String(schema.to_string()));
        map.insert("type".to_string(), Value::String(object_type.to_string()));

        match object_type {
            "view" | "materialized_view" => {
                // Get view definition
                let rows = client
                    .query(
                        "SELECT c.oid, pg_get_viewdef(c.oid, true) AS definition \
                         FROM pg_class c \
                         JOIN pg_namespace n ON c.relnamespace = n.oid \
                         WHERE n.nspname = $1 AND c.relname = $2",
                        &[&schema, &name],
                    )
                    .await
                    .context("Failed to get view definition")?;

                if rows.is_empty() {
                    return Ok(None);
                }

                let def: String = rows[0].get("definition");
                let kind = if object_type == "materialized_view" {
                    "MATERIALIZED VIEW"
                } else {
                    "VIEW"
                };
                let full_def = format!(
                    "CREATE {} {}.{} AS\n{}",
                    kind, schema, name, def
                );
                map.insert("definition".to_string(), Value::String(full_def));
            }
            _ => {
                // Function or procedure
                let rows = client
                    .query(
                        "SELECT p.oid, pg_get_functiondef(p.oid) AS definition, \
                                pg_get_function_arguments(p.oid) AS arguments, \
                                pg_get_function_result(p.oid) AS return_type \
                         FROM pg_proc p \
                         JOIN pg_namespace n ON p.pronamespace = n.oid \
                         WHERE n.nspname = $1 AND p.proname = $2",
                        &[&schema, &name],
                    )
                    .await
                    .context("Failed to get routine definition")?;

                if rows.is_empty() {
                    return Ok(None);
                }

                let def: String = rows[0].get("definition");
                map.insert("definition".to_string(), Value::String(def));

                let arguments: Option<String> = rows[0].get("arguments");
                if let Some(args) = arguments {
                    map.insert("arguments".to_string(), Value::String(args));
                }
                let return_type: Option<String> = rows[0].get("return_type");
                if let Some(rt) = return_type {
                    map.insert("return_type".to_string(), Value::String(rt));
                }
            }
        }

        Ok(Some(map))
    }

    async fn list_triggers(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let rows = client
            .query(
                "SELECT t.tgname AS name, n.nspname AS schema_name, c.relname AS parent_table, \
                        NOT t.tgenabled = 'D' AS is_enabled, \
                        t.tgtype::int & 1 = 1 AS is_row_level, \
                        t.tgtype::int & 2 = 2 AS is_before, \
                        t.tgtype::int & 64 = 64 AS is_instead_of, \
                        ARRAY_TO_STRING(ARRAY( \
                            SELECT CASE bit \
                                WHEN 4 THEN 'INSERT' \
                                WHEN 8 THEN 'DELETE' \
                                WHEN 16 THEN 'UPDATE' \
                                WHEN 32 THEN 'TRUNCATE' \
                            END \
                            FROM UNNEST(ARRAY[4,8,16,32]) AS bit \
                            WHERE t.tgtype::int & bit = bit \
                        ), ', ') AS events, \
                        p.proname AS function_name \
                 FROM pg_trigger t \
                 JOIN pg_class c ON t.tgrelid = c.oid \
                 JOIN pg_namespace n ON c.relnamespace = n.oid \
                 JOIN pg_proc p ON t.tgfoid = p.oid \
                 WHERE n.nspname = $1 AND c.relname = $2 \
                   AND NOT t.tgisinternal \
                 ORDER BY t.tgname",
                &[&schema, &table],
            )
            .await
            .context("Failed to list triggers")?;

        let mut triggers = Vec::new();
        for row in &rows {
            let mut map = HashMap::new();
            let name: String = row.get("name");
            let schema_name: String = row.get("schema_name");
            let parent_table: String = row.get("parent_table");
            let is_enabled: bool = row.get("is_enabled");
            let is_before: bool = row.get("is_before");
            let is_instead_of: bool = row.get("is_instead_of");
            let events: String = row.get("events");
            let function_name: String = row.get("function_name");

            let timing = if is_instead_of {
                "INSTEAD OF"
            } else if is_before {
                "BEFORE"
            } else {
                "AFTER"
            };

            map.insert("name".to_string(), Value::String(name));
            map.insert("schema_name".to_string(), Value::String(schema_name));
            map.insert("parent_table".to_string(), Value::String(parent_table));
            map.insert("is_disabled".to_string(), Value::Bool(!is_enabled));
            map.insert("is_instead_of_trigger".to_string(), Value::Bool(is_instead_of));
            map.insert("events".to_string(), Value::String(format!("{} {}", timing, events)));
            map.insert("function_name".to_string(), Value::String(function_name));
            triggers.push(map);
        }

        Ok(triggers)
    }

    async fn get_trigger_definition(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<HashMap<String, Value>>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let rows = client
            .query(
                "SELECT t.oid, pg_get_triggerdef(t.oid, true) AS definition \
                 FROM pg_trigger t \
                 JOIN pg_class c ON t.tgrelid = c.oid \
                 JOIN pg_namespace n ON c.relnamespace = n.oid \
                 WHERE n.nspname = $1 AND t.tgname = $2 \
                   AND NOT t.tgisinternal",
                &[&schema, &name],
            )
            .await
            .context("Failed to get trigger definition")?;

        if rows.is_empty() {
            return Ok(None);
        }

        let def: String = rows[0].get("definition");
        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::String(name.to_string()));
        map.insert("schema_name".to_string(), Value::String(schema.to_string()));
        map.insert("definition".to_string(), Value::String(def));

        Ok(Some(map))
    }

    async fn get_related_objects(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        // Views that depend on this table (via pg_depend + pg_rewrite)
        let view_rows = client
            .query(
                "SELECT DISTINCT v.relname AS object_name, vn.nspname AS schema_name, \
                        'VIEW' AS object_type \
                 FROM pg_depend d \
                 JOIN pg_rewrite r ON d.objid = r.oid \
                 JOIN pg_class v ON r.ev_class = v.oid \
                 JOIN pg_namespace vn ON v.relnamespace = vn.oid \
                 JOIN pg_class t ON d.refobjid = t.oid \
                 JOIN pg_namespace tn ON t.relnamespace = tn.oid \
                 WHERE tn.nspname = $1 AND t.relname = $2 \
                   AND v.relkind IN ('v', 'm') \
                   AND v.oid != t.oid \
                 ORDER BY object_name",
                &[&schema, &table],
            )
            .await
            .context("Failed to get related views")?;

        // Functions/procedures that reference this table (heuristic via prosrc)
        let pattern = format!("{}%{}%", schema, table);
        let func_rows = client
            .query(
                "SELECT DISTINCT p.proname AS object_name, n.nspname AS schema_name, \
                        CASE p.prokind WHEN 'f' THEN 'FUNCTION' WHEN 'p' THEN 'PROCEDURE' END AS object_type \
                 FROM pg_proc p \
                 JOIN pg_namespace n ON p.pronamespace = n.oid \
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') \
                   AND p.prokind IN ('f', 'p') \
                   AND p.prosrc LIKE $1 \
                 ORDER BY object_name",
                &[&pattern],
            )
            .await
            .context("Failed to get related routines")?;

        let mut objects = Vec::new();
        for row in &view_rows {
            let mut map = HashMap::new();
            let name: String = row.get("object_name");
            let sn: String = row.get("schema_name");
            let ot: String = row.get("object_type");
            map.insert("object_name".to_string(), Value::String(name));
            map.insert("schema_name".to_string(), Value::String(sn));
            map.insert("object_type".to_string(), Value::String(ot));
            objects.push(map);
        }
        for row in &func_rows {
            let mut map = HashMap::new();
            let name: String = row.get("object_name");
            let sn: String = row.get("schema_name");
            let ot: String = row.get("object_type");
            map.insert("object_name".to_string(), Value::String(name));
            map.insert("schema_name".to_string(), Value::String(sn));
            map.insert("object_type".to_string(), Value::String(ot));
            objects.push(map);
        }

        Ok(objects)
    }

    async fn list_rls_policies(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let rows = client
            .query(
                "SELECT pol.polname AS policy_name, \
                        CASE pol.polcmd WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT' \
                          WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE' WHEN '*' THEN 'ALL' END AS command, \
                        pol.polpermissive AS is_permissive, \
                        ARRAY_TO_STRING(ARRAY( \
                            SELECT rolname FROM pg_roles WHERE oid = ANY(pol.polroles) \
                        ), ', ') AS roles, \
                        pg_get_expr(pol.polqual, pol.polrelid) AS using_expr, \
                        pg_get_expr(pol.polwithcheck, pol.polrelid) AS with_check_expr \
                 FROM pg_policy pol \
                 JOIN pg_class c ON pol.polrelid = c.oid \
                 JOIN pg_namespace n ON c.relnamespace = n.oid \
                 WHERE n.nspname = $1 AND c.relname = $2 \
                 ORDER BY pol.polname",
                &[&schema, &table],
            )
            .await
            .context("Failed to list RLS policies")?;

        let mut policies = Vec::new();
        for row in &rows {
            let mut map = HashMap::new();
            let name: String = row.get("policy_name");
            let cmd: String = row.get("command");
            let permissive: bool = row.get("is_permissive");
            let roles: String = row.get("roles");
            let using_expr: Option<String> = row.get("using_expr");
            let with_check: Option<String> = row.get("with_check_expr");
            map.insert("policy_name".to_string(), Value::String(name));
            map.insert("command".to_string(), Value::String(cmd));
            map.insert("is_permissive".to_string(), Value::Bool(permissive));
            map.insert("roles".to_string(), Value::String(roles));
            if let Some(expr) = using_expr {
                map.insert("using_expr".to_string(), Value::String(expr));
            }
            if let Some(expr) = with_check {
                map.insert("with_check_expr".to_string(), Value::String(expr));
            }
            policies.push(map);
        }

        Ok(policies)
    }

    async fn get_rls_status(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<HashMap<String, Value>>> {
        let pool = self.get_pool(database).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let row = client
            .query_opt(
                "SELECT c.relrowsecurity AS rls_enabled, c.relforcerowsecurity AS rls_forced \
                 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&schema, &table],
            )
            .await
            .context("Failed to get RLS status")?;

        match row {
            Some(r) => {
                let mut map = HashMap::new();
                let enabled: bool = r.get("rls_enabled");
                let forced: bool = r.get("rls_forced");
                map.insert("rls_enabled".to_string(), Value::Bool(enabled));
                map.insert("rls_forced".to_string(), Value::Bool(forced));
                Ok(Some(map))
            }
            None => Ok(None),
        }
    }

    async fn generate_rls_sql(
        &self,
        _database: &str,
        schema: &str,
        table: &str,
        action: &str,
        params: &HashMap<String, String>,
    ) -> Result<String> {
        let qualified = format!("\"{}\".\"{}\"\n", schema, table);
        match action {
            "enable_rls" => Ok(format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY;", qualified.trim())),
            "disable_rls" => Ok(format!("ALTER TABLE {} DISABLE ROW LEVEL SECURITY;", qualified.trim())),
            "force_rls" => Ok(format!("ALTER TABLE {} FORCE ROW LEVEL SECURITY;", qualified.trim())),
            "no_force_rls" => Ok(format!("ALTER TABLE {} NO FORCE ROW LEVEL SECURITY;", qualified.trim())),
            "create_policy" => {
                let name = params.get("policy_name").ok_or_else(|| anyhow::anyhow!("policy_name required"))?;
                let command = params.get("command").map(|s| s.as_str()).unwrap_or("ALL");
                let permissive = params.get("permissive").map(|s| s.as_str()).unwrap_or("true");
                let perm_clause = if permissive == "true" { "PERMISSIVE" } else { "RESTRICTIVE" };
                let roles = params.get("roles").map(|s| s.as_str()).unwrap_or("PUBLIC");
                let using_expr = params.get("using_expr");
                let with_check = params.get("with_check_expr");

                let mut sql = format!(
                    "CREATE POLICY \"{}\" ON {} AS {} FOR {} TO {}",
                    name, qualified.trim(), perm_clause, command, roles
                );
                if let Some(expr) = using_expr {
                    if !expr.is_empty() {
                        sql.push_str(&format!(" USING ({})", expr));
                    }
                }
                if let Some(expr) = with_check {
                    if !expr.is_empty() {
                        sql.push_str(&format!(" WITH CHECK ({})", expr));
                    }
                }
                sql.push(';');
                Ok(sql)
            }
            "drop_policy" => {
                let name = params.get("policy_name").ok_or_else(|| anyhow::anyhow!("policy_name required"))?;
                Ok(format!("DROP POLICY \"{}\" ON {};", name, qualified.trim()))
            }
            _ => anyhow::bail!("Unknown RLS action: {}", action),
        }
    }

    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    fn default_database(&self) -> &str {
        &self.default_db
    }

    async fn health_check(&self) -> Result<()> {
        let pool = self.get_pool(&self.default_db).await?;
        let client = pool.get().await.context("Health check: failed to get connection")?;
        client.query_one("SELECT 1", &[]).await.context("Health check failed")?;
        Ok(())
    }

    fn pool_stats(&self) -> Option<super::PoolStats> {
        let pools = self.pools.try_read().ok()?;
        let mut total: u32 = 0;
        let mut available: u32 = 0;
        let mut max: u32 = 0;
        for pool in pools.values() {
            let s = pool.status();
            total += s.size as u32;
            available += s.available as u32;
            max += s.max_size as u32;
        }
        Some(super::PoolStats {
            total_connections: total,
            idle_connections: available,
            active_connections: total.saturating_sub(available),
            max_size: max,
        })
    }

    async fn list_active_queries(&self) -> Result<Vec<HashMap<String, Value>>> {
        let pool = self.get_pool(&self.default_db).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let rows = client
            .query(
                "SELECT \
                    pid, \
                    state, \
                    EXTRACT(EPOCH FROM (now() - query_start))::int AS duration_seconds, \
                    wait_event_type, \
                    wait_event, \
                    datname AS database_name, \
                    usename AS username, \
                    query AS query_text \
                FROM pg_stat_activity \
                WHERE pid != pg_backend_pid() \
                  AND state IS NOT NULL \
                ORDER BY query_start NULLS LAST",
                &[],
            )
            .await
            .context("Failed to list active queries")?;

        let mut queries = Vec::new();
        for row in &rows {
            let mut map = HashMap::new();
            let pid: i32 = row.get("pid");
            map.insert("spid".to_string(), Value::from(pid as i64));
            let state: Option<String> = row.get("state");
            map.insert("status".to_string(), state.map(Value::String).unwrap_or(Value::Null));
            let duration: Option<i32> = row.get("duration_seconds");
            map.insert("duration_seconds".to_string(), duration.map(|v| Value::from(v as i64)).unwrap_or(Value::Null));
            let wait_type: Option<String> = row.get("wait_event_type");
            map.insert("wait_type".to_string(), wait_type.map(Value::String).unwrap_or(Value::Null));
            let wait_event: Option<String> = row.get("wait_event");
            map.insert("wait_event".to_string(), wait_event.map(Value::String).unwrap_or(Value::Null));
            let db_name: Option<String> = row.get("database_name");
            map.insert("database_name".to_string(), db_name.map(Value::String).unwrap_or(Value::Null));
            let username: Option<String> = row.get("username");
            map.insert("username".to_string(), username.map(Value::String).unwrap_or(Value::Null));
            let query_text: Option<String> = row.get("query_text");
            map.insert("query_text".to_string(), query_text.map(Value::String).unwrap_or(Value::Null));
            queries.push(map);
        }

        Ok(queries)
    }

    async fn kill_query(&self, process_id: i64) -> Result<()> {
        let pool = self.get_pool(&self.default_db).await?;
        let client = pool.get().await
            .context("Failed to get Postgres connection")?;

        let result: bool = client
            .query_one(
                "SELECT pg_terminate_backend($1::int)",
                &[&(process_id as i32)],
            )
            .await
            .context("Failed to terminate backend")?
            .get(0);

        if !result {
            anyhow::bail!("pg_terminate_backend returned false — process {} may not exist", process_id);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Paginated execution
// ---------------------------------------------------------------------------

impl PostgresBackend {
    async fn execute_paginated(
        &self,
        client: &deadpool_postgres::Client,
        params: &QueryParams,
        pii_processor: &crate::pii::PiiProcessor,
        start: Instant,
    ) -> Result<QueryResult> {
        // Get count
        let count_sql = get_count_query(&params.query, &params.count_mode)?;
        let total_rows: i64 = if count_sql.is_empty() {
            -1 // Window mode defers count
        } else {
            let count_rows = client.query(&count_sql, &[]).await
                .context("Failed to execute count query")?;
            if let Some(row) = count_rows.first() {
                row.get::<_, i64>(0)
            } else {
                0
            }
        };

        // Get paginated data
        let paginated_sql = create_paginated_query(
            &params.query,
            0,
            params.batch_size,
            &params.count_mode,
            params.order.as_deref(),
            params.allow_unstable_pagination,
            Dialect::Postgres,
        )?;

        let rows = client.query(&paginated_sql, &[]).await
            .context("Failed to execute paginated query")?;

        let metadata = if params.include_metadata && !rows.is_empty() {
            Some(build_metadata(rows[0].columns()))
        } else {
            None
        };

        let mut data = Vec::new();
        for row in &rows {
            let mut json_row = pg_row_to_json(row, pii_processor, &params.blob_format);
            // Remove internal row number column if present (from window mode)
            json_row.remove(ROW_NUMBER_ALIAS);
            data.push(json_row);
        }

        let actual_total = if total_rows < 0 {
            data.len() as i64
        } else {
            total_rows
        };

        let elapsed = start.elapsed().as_millis();

        Ok(QueryResult {
            success: true,
            total_rows: actual_total,
            execution_time_ms: elapsed,
            rows_per_second: if elapsed > 0 {
                actual_total as f64 / (elapsed as f64 / 1000.0)
            } else {
                actual_total as f64
            },
            data,
            result_sets: None,
            result_set_count: None,
            metadata,
        })
    }
}

fn build_metadata(columns: &[tokio_postgres::Column]) -> QueryMetadata {
    QueryMetadata {
        columns: columns
            .iter()
            .map(|col| ColumnMeta {
                name: col.name().to_string(),
                data_type: pg_type_name(col.type_()).to_string(),
            })
            .collect(),
    }
}
