//! MSSQL database backend implementation using Tiberius.
//!
//! This module extracts and refactors the Tiberius-specific code from the
//! original query CLI into a proper library module that returns structured
//! `QueryResult` values instead of printing to stdout.

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use futures::TryStreamExt;
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tiberius::{AuthMethod, Client, Config as TibConfig, QueryItem, Row};
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tracing::{debug, info, warn};
use uuid::Uuid;

use regex::Regex;

use crate::config::DbConfig;
use crate::query::{
    BlobFormat, ColumnMeta, CountMode, QueryMetadata, QueryParams, QueryResult,
    ROW_NUMBER_ALIAS, build_pii_processor, format_binary_data,
    pagination::{create_paginated_query, get_count_query},
    validation::is_exec_query,
};

use super::{DatabaseBackend, Dialect, StreamChunk};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CONNECT_RETRY_LIMIT: usize = 3;
const CONNECT_RETRY_BACKOFF_MS: u64 = 500;

// ---------------------------------------------------------------------------
// ColumnType enum — maps Tiberius wire types to our logical categories
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum ColumnType {
    // Integer types
    TinyInt,
    SmallInt,
    I32,
    BigInt,
    // Float types
    Real,
    F64,
    // Decimal types
    Decimal,
    Money,
    SmallMoney,
    // Date/Time types
    Date,
    Time,
    DateTime,
    DateTime2,
    DateTimeOffset,
    SmallDateTime,
    // String types
    String,
    Char,
    VarChar,
    Text,
    NChar,
    NVarChar,
    NText,
    // Binary types
    Binary,
    VarBinary,
    Image,
    // Other types
    UniqueIdentifier,
    Bool,
    Xml,
    Null,
    Unknown,
}

impl ColumnType {
    pub(crate) fn type_name(&self) -> &str {
        match self {
            ColumnType::TinyInt => "TinyInt",
            ColumnType::SmallInt => "SmallInt",
            ColumnType::I32 => "Int",
            ColumnType::BigInt => "BigInt",
            ColumnType::Real => "Real",
            ColumnType::F64 => "Float",
            ColumnType::Decimal => "Decimal",
            ColumnType::Money => "Money",
            ColumnType::SmallMoney => "SmallMoney",
            ColumnType::Date => "Date",
            ColumnType::Time => "Time",
            ColumnType::DateTime => "DateTime",
            ColumnType::DateTime2 => "DateTime2",
            ColumnType::DateTimeOffset => "DateTimeOffset",
            ColumnType::SmallDateTime => "SmallDateTime",
            ColumnType::String => "String",
            ColumnType::Char => "Char",
            ColumnType::VarChar => "VarChar",
            ColumnType::Text => "Text",
            ColumnType::NChar => "NChar",
            ColumnType::NVarChar => "NVarChar",
            ColumnType::NText => "NText",
            ColumnType::Binary => "Binary",
            ColumnType::VarBinary => "VarBinary",
            ColumnType::Image => "Image",
            ColumnType::UniqueIdentifier => "UniqueIdentifier",
            ColumnType::Bool => "Bit",
            ColumnType::Xml => "Xml",
            ColumnType::Null => "NULL",
            ColumnType::Unknown => "Unknown",
        }
    }
}

impl From<&tiberius::Column> for ColumnType {
    fn from(col: &tiberius::Column) -> Self {
        use tiberius::ColumnType as TdsType;

        match col.column_type() {
            // Integer types
            TdsType::Bit | TdsType::Bitn => ColumnType::Bool,
            TdsType::Int1 => ColumnType::TinyInt,
            TdsType::Int2 => ColumnType::SmallInt,
            TdsType::Int4 => ColumnType::I32,
            TdsType::Int8 => ColumnType::BigInt,
            // Float types
            TdsType::Float4 => ColumnType::Real,
            TdsType::Float8 => ColumnType::F64,
            // Decimal/Numeric types
            TdsType::Numericn | TdsType::Decimaln => ColumnType::Decimal,
            TdsType::Money | TdsType::Money4 => ColumnType::Money,
            // Date/Time types
            TdsType::Daten => ColumnType::Date,
            TdsType::Timen => ColumnType::Time,
            TdsType::Datetime | TdsType::Datetimen => ColumnType::DateTime,
            TdsType::Datetime2 => ColumnType::DateTime2,
            TdsType::Datetime4 => ColumnType::SmallDateTime,
            TdsType::DatetimeOffsetn => ColumnType::DateTimeOffset,
            // String types
            TdsType::BigChar => ColumnType::Char,
            TdsType::BigVarChar => ColumnType::VarChar,
            TdsType::Text => ColumnType::Text,
            TdsType::NChar => ColumnType::NChar,
            TdsType::NVarchar => ColumnType::NVarChar,
            TdsType::NText => ColumnType::NText,
            // Binary types
            TdsType::BigBinary => ColumnType::Binary,
            TdsType::BigVarBin => ColumnType::VarBinary,
            TdsType::Image => ColumnType::Image,
            // GUID
            TdsType::Guid => ColumnType::UniqueIdentifier,
            // XML
            TdsType::Xml => ColumnType::Xml,
            // Null
            TdsType::Null => ColumnType::Null,
            // Catch-all
            _ => ColumnType::Unknown,
        }
    }
}

// ---------------------------------------------------------------------------
// Connection pool (bb8)
// ---------------------------------------------------------------------------

/// Simple error type for the connection pool that implements std::error::Error.
#[derive(Debug)]
struct PoolError(String);

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PoolError {}

/// Convert a bb8 RunError into an anyhow::Error.
fn pool_run_err(e: bb8::RunError<PoolError>) -> anyhow::Error {
    match e {
        bb8::RunError::User(e) => anyhow::anyhow!("Pool connection error: {}", e.0),
        bb8::RunError::TimedOut => anyhow::anyhow!("Pool connection timed out"),
    }
}

fn pool_run_err_string(e: bb8::RunError<PoolError>) -> String {
    match e {
        bb8::RunError::User(e) => format!("Pool connection error: {}", e.0),
        bb8::RunError::TimedOut => "Pool connection timed out".to_string(),
    }
}

struct TiberiusConnectionManager {
    config: DbConfig,
    default_database: String,
}

impl bb8::ManageConnection for TiberiusConnectionManager {
    type Connection = TibClient;
    type Error = PoolError;

    fn connect(&self) -> impl std::future::Future<Output = Result<TibClient, PoolError>> + Send {
        let config = self.config.clone();
        let db = self.default_database.clone();
        async move {
            create_client(&config, &db).await.map_err(|e| PoolError(e.to_string()))
        }
    }

    fn is_valid(&self, conn: &mut TibClient) -> impl std::future::Future<Output = Result<(), PoolError>> + Send {
        async {
            conn.simple_query("SELECT 1")
                .await
                .map_err(|e| PoolError(e.to_string()))?
                .into_row()
                .await
                .map_err(|e| PoolError(e.to_string()))?;
            Ok(())
        }
    }

    fn has_broken(&self, _conn: &mut TibClient) -> bool {
        false
    }
}

/// Switch a pooled connection to the target database via USE statement.
async fn switch_database(client: &mut TibClient, database: &str) -> Result<()> {
    let use_stmt = format!("USE [{}]", database.replace(']', "]]"));
    client.simple_query(&use_stmt).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// MssqlBackend
// ---------------------------------------------------------------------------

/// MSSQL database backend backed by Tiberius with bb8 connection pool.
pub struct MssqlBackend {
    pool: bb8::Pool<TiberiusConnectionManager>,
    default_db: String,
}

impl MssqlBackend {
    pub async fn new(config: DbConfig) -> Result<Self> {
        let default_db = config.database.clone();
        let manager = TiberiusConnectionManager {
            default_database: config.database.clone(),
            config,
        };
        let pool = bb8::Pool::builder()
            .max_size(10)
            .min_idle(Some(1))
            .connection_timeout(Duration::from_secs(15))
            .idle_timeout(Some(Duration::from_secs(300)))
            .max_lifetime(Some(Duration::from_secs(1800)))
            .build(manager)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to build connection pool: {}", e.0))?;
        info!("Connection pool initialized (max_size=10, min_idle=1)");
        Ok(Self { pool, default_db })
    }
}

// ---------------------------------------------------------------------------
// Connection creation with retry logic
// ---------------------------------------------------------------------------

type TibClient = Client<tokio_util::compat::Compat<TcpStream>>;

async fn create_client(config: &DbConfig, database: &str) -> Result<TibClient> {
    let mut tib_config = TibConfig::new();
    tib_config.host(&config.server);
    tib_config.port(config.port);
    tib_config.database(database);
    tib_config.authentication(AuthMethod::sql_server(&config.user, &config.password));
    if config.options.encrypt || config.options.trust_server_certificate {
        tib_config.encryption(tiberius::EncryptionLevel::Required);
    } else {
        tib_config.encryption(tiberius::EncryptionLevel::Off);
    }
    if config.options.trust_server_certificate {
        tib_config.trust_cert();
    }

    let target = format!("{}:{}", config.server, config.port);
    let mut attempt = 0;
    let mut last_error: Option<anyhow::Error> = None;

    while attempt < CONNECT_RETRY_LIMIT {
        attempt += 1;

        match TcpStream::connect(&target).await {
            Ok(tcp) => {
                if let Err(err) = tcp.set_nodelay(true) {
                    if attempt == CONNECT_RETRY_LIMIT {
                        warn!("Failed to enable TCP_NODELAY: {}", err);
                    }
                }

                match Client::connect(tib_config.clone(), tcp.compat_write()).await {
                    Ok(client) => return Ok(client),
                    Err(err) => {
                        last_error = Some(
                            anyhow::anyhow!("SQL Server connection failed: {}", err),
                        );
                    }
                }
            }
            Err(err) => {
                last_error =
                    Some(anyhow::Error::new(err).context("Failed to open TCP connection"));
            }
        }

        if attempt < CONNECT_RETRY_LIMIT {
            let backoff = CONNECT_RETRY_BACKOFF_MS * 2u64.pow((attempt - 1) as u32);
            if let Some(ref err) = last_error {
                warn!(
                    "Connection attempt {}/{} failed: {}. Retrying in {} ms...",
                    attempt, CONNECT_RETRY_LIMIT, err, backoff
                );
            }
            sleep(Duration::from_millis(backoff)).await;
        }
    }

    Err(last_error.unwrap_or_else(|| {
        anyhow::anyhow!(
            "Failed to connect to SQL Server after {} attempts",
            CONNECT_RETRY_LIMIT
        )
    }))
}

// ---------------------------------------------------------------------------
// Row conversion functions
// ---------------------------------------------------------------------------

/// Convert a Tiberius row to a JSON-compatible HashMap.
pub(crate) fn row_to_json_value(
    row: &Row,
    column_names: &[String],
    column_types: &[ColumnType],
    preserve_decimal_precision: bool,
    blob_format: &BlobFormat,
) -> HashMap<String, Value> {
    let mut json_row = HashMap::new();

    for (i, col_type) in column_types.iter().enumerate() {
        let col_name = column_names
            .get(i)
            .map(|s| s.as_str())
            .unwrap_or("unknown");

        let json_value = match col_type {
            // Integer types
            ColumnType::TinyInt => row
                .get::<u8, _>(i)
                .map(|v| Value::from(v as i64))
                .unwrap_or(Value::Null),
            ColumnType::SmallInt => row
                .get::<i16, _>(i)
                .map(|v| Value::from(v as i64))
                .unwrap_or(Value::Null),
            ColumnType::I32 => row.get::<i32, _>(i).map(Value::from).unwrap_or(Value::Null),
            ColumnType::BigInt => row.get::<i64, _>(i).map(Value::from).unwrap_or(Value::Null),
            // Float types
            ColumnType::Real => row
                .get::<f32, _>(i)
                .map(|v| Value::from(v as f64))
                .unwrap_or(Value::Null),
            ColumnType::F64 => row.get::<f64, _>(i).map(Value::from).unwrap_or(Value::Null),
            // Decimal types
            ColumnType::Decimal | ColumnType::Money | ColumnType::SmallMoney => row
                .get::<Decimal, _>(i)
                .map(|v| {
                    if preserve_decimal_precision {
                        Value::String(v.to_string())
                    } else {
                        if let Ok(f) = v.to_string().parse::<f64>() {
                            Value::from(f)
                        } else {
                            Value::String(v.to_string())
                        }
                    }
                })
                .unwrap_or(Value::Null),
            // Date/Time types
            ColumnType::Date => row
                .get::<NaiveDate, _>(i)
                .map(|v| Value::String(v.format("%Y-%m-%d").to_string()))
                .unwrap_or(Value::Null),
            ColumnType::Time => row
                .get::<NaiveTime, _>(i)
                .map(|v| Value::String(v.format("%H:%M:%S%.f").to_string()))
                .unwrap_or(Value::Null),
            ColumnType::DateTime | ColumnType::DateTime2 | ColumnType::SmallDateTime => row
                .get::<NaiveDateTime, _>(i)
                .map(|v| Value::String(v.format("%Y-%m-%d %H:%M:%S").to_string()))
                .unwrap_or(Value::Null),
            ColumnType::DateTimeOffset => row
                .get::<DateTime<FixedOffset>, _>(i)
                .map(|v| Value::String(v.to_rfc3339()))
                .unwrap_or(Value::Null),
            // String types
            ColumnType::String
            | ColumnType::Char
            | ColumnType::VarChar
            | ColumnType::Text
            | ColumnType::NChar
            | ColumnType::NVarChar
            | ColumnType::NText
            | ColumnType::Xml => row
                .get::<&str, _>(i)
                .map(|v| Value::String(v.to_string()))
                .unwrap_or(Value::Null),
            // Binary types
            ColumnType::Binary | ColumnType::VarBinary | ColumnType::Image => row
                .get::<&[u8], _>(i)
                .map(|v| Value::String(format_binary_data(v, blob_format)))
                .unwrap_or(Value::Null),
            // Other types
            ColumnType::UniqueIdentifier => row
                .get::<Uuid, _>(i)
                .map(|v| Value::String(v.to_string()))
                .unwrap_or(Value::Null),
            ColumnType::Bool => row
                .get::<bool, _>(i)
                .map(Value::from)
                .unwrap_or(Value::Null),

            ColumnType::Null | ColumnType::Unknown => Value::Null,
        };

        json_row.insert(col_name.to_string(), json_value);
    }

    json_row
}

// ---------------------------------------------------------------------------
// Error enrichment helpers
// ---------------------------------------------------------------------------

/// Extract table names from FROM/JOIN clauses in a SQL query.
fn extract_tables_from_query(query: &str) -> Vec<String> {
    let re = Regex::new(
        r"(?i)(?:FROM|JOIN)\s+(\[?[A-Za-z_][A-Za-z0-9_]*\]?(?:\.\[?[A-Za-z_][A-Za-z0-9_]*\]?)?)"
    ).unwrap();
    let mut tables = Vec::new();
    for cap in re.captures_iter(query) {
        let raw = &cap[1];
        // Take the last segment (after schema dot) and strip brackets
        let table_name = raw
            .rsplit('.')
            .next()
            .unwrap_or(raw)
            .trim_matches('[')
            .trim_matches(']')
            .to_string();
        if !tables.contains(&table_name) {
            tables.push(table_name);
        }
    }
    tables
}

/// Extract a single-quoted value from an error message (e.g. 'Foo' → "Foo").
fn extract_quoted_value_local(message: &str) -> Option<String> {
    let re = Regex::new(r"'([^']+)'").ok()?;
    re.captures(message).map(|c| c[1].to_string())
}

/// Enrich error 207 (invalid column) by querying INFORMATION_SCHEMA for actual columns.
async fn enrich_error_207(
    pool: &bb8::Pool<TiberiusConnectionManager>,
    database: &str,
    query: &str,
    invalid_column: &str,
) -> Option<String> {
    let tables = extract_tables_from_query(query);
    if tables.is_empty() {
        return None;
    }

    let mut conn = pool.get().await.ok()?;
    switch_database(&mut conn, database).await.ok()?;

    let mut parts: Vec<String> = Vec::new();
    for table in &tables {
        let col_query = format!(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' ORDER BY ORDINAL_POSITION",
            table.replace('\'', "''")
        );
        if let Ok(stream) = conn.query(&col_query, &[]).await {
            if let Ok(rows) = stream.into_first_result().await {
                let col_names: Vec<String> = rows
                    .iter()
                    .filter_map(|r| r.get::<&str, _>(0).map(|s| s.to_string()))
                    .collect();
                if !col_names.is_empty() {
                    parts.push(format!("[{}]: {}", table, col_names.join(", ")));
                }
            }
        }
    }

    if parts.is_empty() {
        return None;
    }

    Some(format!(
        "Column '{}' not found. Available columns: {}",
        invalid_column,
        parts.join("; ")
    ))
}

/// Enrich error 208 (invalid table) by querying INFORMATION_SCHEMA for similar/all tables.
async fn enrich_error_208(
    pool: &bb8::Pool<TiberiusConnectionManager>,
    database: &str,
    invalid_table: &str,
) -> Option<String> {
    let mut conn = pool.get().await.ok()?;
    switch_database(&mut conn, database).await.ok()?;

    // Try fuzzy match first
    let fuzzy_query = format!(
        "SELECT TOP 20 TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%{}%' ORDER BY TABLE_NAME",
        invalid_table.replace('\'', "''")
    );
    if let Ok(stream) = conn.query(&fuzzy_query, &[]).await {
        if let Ok(rows) = stream.into_first_result().await {
            let names: Vec<String> = rows
                .iter()
                .filter_map(|r| r.get::<&str, _>(0).map(|s| s.to_string()))
                .collect();
            if !names.is_empty() {
                return Some(format!(
                    "Table '{}' not found. Similar tables: {}",
                    invalid_table,
                    names.join(", ")
                ));
            }
        }
    }

    // No fuzzy matches — list all tables
    let all_query =
        "SELECT TOP 50 TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME";
    if let Ok(stream) = conn.query(all_query, &[]).await {
        if let Ok(rows) = stream.into_first_result().await {
            let names: Vec<String> = rows
                .iter()
                .filter_map(|r| r.get::<&str, _>(0).map(|s| s.to_string()))
                .collect();
            if !names.is_empty() {
                return Some(format!(
                    "Table '{}' not found. Available tables: {}",
                    invalid_table,
                    names.join(", ")
                ));
            }
        }
    }

    None
}

/// Attempt to enrich a Tiberius error with schema hints for codes 207/208.
/// Returns the original error (possibly with appended hint) as an anyhow::Error.
async fn try_enrich_error(
    err: tiberius::error::Error,
    pool: &bb8::Pool<TiberiusConnectionManager>,
    params: &QueryParams,
    context_msg: &str,
) -> anyhow::Error {
    if let tiberius::error::Error::Server(ref token_err) = err {
        let code = token_err.code();
        let message = token_err.message();

        let hint = match code {
            207 => {
                let col = extract_quoted_value_local(message).unwrap_or_default();
                enrich_error_207(pool, &params.database, &params.query, &col).await
            }
            208 => {
                let table = extract_quoted_value_local(message).unwrap_or_default();
                enrich_error_208(pool, &params.database, &table).await
            }
            _ => None,
        };

        if let Some(hint_text) = hint {
            return anyhow::anyhow!("{} | Hint: {}", err, hint_text)
                .context(context_msg.to_string());
        }
    }

    anyhow::Error::new(err).context(context_msg.to_string())
}

// ---------------------------------------------------------------------------
// Query execution — paginated path
// ---------------------------------------------------------------------------

/// Execute a paginated query, collecting all rows across batches.
async fn execute_query_paginated(
    client: &mut TibClient,
    params: &QueryParams,
    pii_processor: &crate::pii::PiiProcessor,
    pool: &bb8::Pool<TiberiusConnectionManager>,
) -> Result<QueryResult> {
    let is_window_mode = matches!(params.count_mode, CountMode::Window);
    let start_time = Instant::now();

    // ---- determine total count (window mode defers) ----
    let mut total_count: i64 = if is_window_mode {
        -1 // sentinel: unknown until first partial batch
    } else {
        let count_query = get_count_query(&params.query, &params.count_mode)?;
        let count_stream = match client.query(&count_query, &[]).await {
            Ok(s) => s,
            Err(tib_err) => {
                return Err(try_enrich_error(tib_err, pool, params, "Failed to execute count query").await);
            }
        };
        let count_row = count_stream
            .into_row()
            .await
            .context("Failed to get count result")?
            .unwrap();

        let count: i32 = count_row.get(0).unwrap();
        let count = count as i64;
        info!("Total rows to process: {}", count);

        if count == 0 {
            info!("No rows to process");
            let elapsed = start_time.elapsed();
            return Ok(QueryResult {
                success: true,
                total_rows: 0,
                execution_time_ms: elapsed.as_millis(),
                rows_per_second: 0.0,
                data: Vec::new(),
                result_sets: None,
                result_set_count: None,
                metadata: None,
            });
        }
        count
    };

    let mut offset = 0usize;
    let mut first_batch = true;
    let mut all_column_names: Vec<String> = Vec::new();
    let mut json_results: Vec<HashMap<String, Value>> = Vec::new();
    let mut stored_columns: Vec<tiberius::Column> = Vec::new();
    let mut actual_row_count: i64 = 0;

    // Loop through paginated batches
    while (is_window_mode && total_count == -1)
        || (total_count > 0 && offset < total_count as usize)
    {
        let upper_query = params.query.to_uppercase();

        if !is_window_mode && offset > i32::MAX as usize {
            return Err(anyhow::anyhow!(
                "Pagination offset {} exceeds SQL Server LIMIT of {} rows. \
                 Consider using count_mode=window.",
                offset,
                i32::MAX
            ));
        }

        let batch_query_sql = if upper_query.contains(" TOP ") {
            params.query.clone()
        } else {
            create_paginated_query(
                &params.query,
                offset,
                params.batch_size,
                &params.count_mode,
                params.order.as_deref(),
                params.allow_unstable_pagination,
                Dialect::Mssql,
            )?
        };

        debug!(
            "Processing batch: {} to {} (total {})",
            offset + 1,
            offset + params.batch_size,
            if total_count > 0 {
                total_count.to_string()
            } else {
                "unknown".to_string()
            }
        );

        let mut result_stream = match client.query(&batch_query_sql, &[]).await {
            Ok(s) => s,
            Err(tib_err) => {
                return Err(try_enrich_error(tib_err, pool, params, "Failed to execute batch query").await);
            }
        };

        let columns = result_stream
            .columns()
            .await
            .unwrap_or(None)
            .unwrap_or(&[])
            .to_vec();

        // Locate internal row-number column for window mode
        let row_number_idx = if is_window_mode {
            columns
                .iter()
                .rposition(|col| col.name() == ROW_NUMBER_ALIAS)
        } else {
            None
        };
        let visible_columns: Vec<tiberius::Column> = if let Some(idx) = row_number_idx {
            columns[..idx].to_vec()
        } else {
            columns.clone()
        };

        let column_types: Vec<ColumnType> =
            visible_columns.iter().map(ColumnType::from).collect();
        all_column_names.clear();
        all_column_names
            .extend(visible_columns.iter().map(|col| col.name().to_string()));

        if first_batch {
            stored_columns = visible_columns.clone();
            debug!(
                "Column mapping: {}",
                all_column_names
                    .iter()
                    .enumerate()
                    .map(|(i, n)| format!(
                        "{}: {}",
                        n,
                        column_types.get(i).map(|t| t.type_name()).unwrap_or("?")
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            first_batch = false;
        }

        let mut row_count: usize = 0;
        let mut max_rn: Option<i64> = None;

        while let Some(item) = result_stream
            .try_next()
            .await
            .context("Failed to fetch row")?
        {
            if let QueryItem::Row(row) = item {
                // Track ROW_NUMBER for window-mode total detection
                if is_window_mode && total_count == -1 {
                    if let Some(rn_idx) = row_number_idx {
                        if let Ok(Some(rn_value)) = row.try_get::<i64, _>(rn_idx) {
                            max_rn = Some(max_rn.map_or(rn_value, |m| m.max(rn_value)));
                        }
                    } else {
                        max_rn = Some(actual_row_count + row_count as i64 + 1);
                    }
                }

                let mut json_row = row_to_json_value(
                    &row,
                    &all_column_names,
                    &column_types,
                    params.preserve_decimal_precision,
                    &params.blob_format,
                );
                crate::pii::process_json_row(pii_processor, &mut json_row);
                json_results.push(json_row);

                row_count += 1;
            }
        }

        actual_row_count += row_count as i64;

        // Update total_count for window mode
        if is_window_mode && total_count == -1 {
            if row_count == 0 {
                total_count = actual_row_count;
                info!("Total rows to process: {}", total_count);
            } else if row_count < params.batch_size {
                total_count = max_rn.unwrap_or(actual_row_count);
                info!("Total rows to process: {}", total_count);
            }
        }

        offset += params.batch_size;

        // TOP queries run only once
        if upper_query.contains(" TOP ") {
            break;
        }
        // Window mode: stop once we know the total and have passed it
        if is_window_mode && total_count > 0 && offset >= total_count as usize {
            break;
        }
    }

    // Ensure total_count is valid for window mode
    if is_window_mode && total_count == -1 {
        total_count = actual_row_count;
    }

    let total_elapsed = start_time.elapsed();
    let elapsed_secs = total_elapsed.as_secs_f64();
    let rows_per_second = if elapsed_secs > 0.0 {
        total_count as f64 / elapsed_secs
    } else {
        0.0
    };

    // Build metadata
    let metadata = if params.include_metadata && !stored_columns.is_empty() {
        Some(QueryMetadata {
            columns: stored_columns
                .iter()
                .map(|col| {
                    let ct = ColumnType::from(col);
                    ColumnMeta {
                        name: col.name().to_string(),
                        data_type: ct.type_name().to_string(),
                    }
                })
                .collect(),
        })
    } else {
        None
    };

    Ok(QueryResult {
        success: true,
        total_rows: total_count,
        execution_time_ms: total_elapsed.as_millis(),
        rows_per_second,
        data: json_results,
        result_sets: None,
        result_set_count: None,
        metadata,
    })
}

// ---------------------------------------------------------------------------
// Query execution — non-paginated path (EXEC, single shot, multi-result-set)
// ---------------------------------------------------------------------------

/// Execute a query without pagination, handling multiple result sets.
async fn execute_query_no_paging(
    client: &mut TibClient,
    params: &QueryParams,
    pii_processor: &crate::pii::PiiProcessor,
    pool: &bb8::Pool<TiberiusConnectionManager>,
) -> Result<QueryResult> {
    let start_time = Instant::now();

    debug!("Executing statement once (no paging)");

    let mut result_stream = match client.query(&params.query, &[]).await {
        Ok(s) => s,
        Err(tib_err) => {
            return Err(try_enrich_error(tib_err, pool, params, "Failed to execute query").await);
        }
    };

    let columns = result_stream
        .columns()
        .await
        .unwrap_or(None)
        .unwrap_or(&[])
        .to_vec();

    let mut column_types: Vec<ColumnType> = columns.iter().map(ColumnType::from).collect();
    let mut all_column_names: Vec<String> =
        columns.iter().map(|c| c.name().to_string()).collect();

    if !columns.is_empty() {
        debug!(
            "Column mapping: {}",
            all_column_names
                .iter()
                .enumerate()
                .map(|(i, n)| format!(
                    "{}: {}",
                    n,
                    column_types.get(i).map(|t| t.type_name()).unwrap_or("?")
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let stored_columns = columns.clone();
    let mut total_count: i64 = 0;
    let mut json_results: Vec<Vec<HashMap<String, Value>>> = Vec::new();
    let mut current_result_set: Vec<HashMap<String, Value>> = Vec::new();
    let mut result_set_index = 0;

    while let Some(item) = result_stream
        .try_next()
        .await
        .context("Failed to fetch row")?
    {
        match item {
            QueryItem::Metadata(meta) => {
                // New result set detected
                result_set_index += 1;
                debug!("Result set #{}", result_set_index);

                // Save previous result set
                if !current_result_set.is_empty() {
                    json_results.push(current_result_set.clone());
                    current_result_set.clear();
                }

                // Extract new schema
                let new_columns = meta.columns().to_vec();
                column_types = new_columns.iter().map(ColumnType::from).collect();
                all_column_names =
                    new_columns.iter().map(|c| c.name().to_string()).collect();
            }
            QueryItem::Row(row) => {
                let mut json_row = row_to_json_value(
                    &row,
                    &all_column_names,
                    &column_types,
                    params.preserve_decimal_precision,
                    &params.blob_format,
                );
                crate::pii::process_json_row(pii_processor, &mut json_row);
                current_result_set.push(json_row);
                total_count += 1;
            }
        }
    }

    // Push final result set
    if !current_result_set.is_empty() {
        json_results.push(current_result_set);
    }

    let total_elapsed = start_time.elapsed();
    let rows_per_second = if total_elapsed.as_secs_f64() > 0.0 {
        total_count as f64 / total_elapsed.as_secs_f64()
    } else {
        0.0
    };

    // Build metadata
    let metadata = if params.include_metadata && !stored_columns.is_empty() {
        Some(QueryMetadata {
            columns: stored_columns
                .iter()
                .map(|col| {
                    let ct = ColumnType::from(col);
                    ColumnMeta {
                        name: col.name().to_string(),
                        data_type: ct.type_name().to_string(),
                    }
                })
                .collect(),
        })
    } else {
        None
    };

    // Structure output: multi-result-set vs single
    let (data, result_sets, result_set_count) = if json_results.len() > 1 {
        let count = json_results.len();
        (Vec::new(), Some(json_results), Some(count))
    } else {
        let data = if json_results.is_empty() {
            Vec::new()
        } else {
            json_results.into_iter().next().unwrap_or_default()
        };
        (data, None, None)
    };

    Ok(QueryResult {
        success: true,
        total_rows: total_count,
        execution_time_ms: total_elapsed.as_millis(),
        rows_per_second,
        data,
        result_sets,
        result_set_count,
        metadata,
    })
}

// ---------------------------------------------------------------------------
// Internal dispatch — chooses paginated vs non-paginated path
// ---------------------------------------------------------------------------

/// Main entry point for query execution. Chooses paginated or non-paginated
/// path depending on `params.pagination` and query type.
async fn execute_query_internal(
    pool: &bb8::Pool<TiberiusConnectionManager>,
    params: &QueryParams,
) -> Result<QueryResult> {
    let mut conn = pool.get().await
        .map_err(pool_run_err)?;
    switch_database(&mut conn, &params.database).await?;

    // For read-only users, wrap query in BEGIN TRAN ... ROLLBACK so any
    // writes that slip through string validation are always rolled back.
    let params = if params.read_only {
        let mut p = params.clone();
        p.query = format!("BEGIN TRANSACTION\n{}\nROLLBACK TRANSACTION", p.query);
        p
    } else {
        params.clone()
    };

    let pii_processor = build_pii_processor(&params);

    if params.pagination && !is_exec_query(&params.query) {
        execute_query_paginated(&mut conn, &params, &pii_processor, pool).await
    } else {
        execute_query_no_paging(&mut conn, &params, &pii_processor, pool).await
    }
}

// ---------------------------------------------------------------------------
// Streaming query execution (NDJSON)
// ---------------------------------------------------------------------------

/// Helper: serialize a value as a single NDJSON line (JSON + newline).
fn ndjson_line(value: &Value) -> bytes::Bytes {
    let mut buf = serde_json::to_vec(value).unwrap_or_default();
    buf.push(b'\n');
    bytes::Bytes::from(buf)
}

/// Background task that streams query results as NDJSON chunks.
///
/// **Two-phase design:**
/// 1. Setup: acquire connection, switch DB, start query, extract columns.
///    Signals success/failure via `setup_tx`.
/// 2. Streaming: iterate rows, serialize to NDJSON, send via `tx`.
async fn stream_query_task(
    pool: &bb8::Pool<TiberiusConnectionManager>,
    params: &QueryParams,
    tx: tokio::sync::mpsc::Sender<StreamChunk>,
    setup_tx: tokio::sync::oneshot::Sender<Result<()>>,
) -> Result<()> {
    // --- Setup phase ---
    let mut conn = match pool.get().await.map_err(pool_run_err) {
        Ok(c) => c,
        Err(e) => {
            let _ = setup_tx.send(Err(anyhow::anyhow!("{:#}", e)));
            return Err(e);
        }
    };

    if let Err(e) = switch_database(&mut conn, &params.database).await {
        let _ = setup_tx.send(Err(anyhow::anyhow!("{:#}", e)));
        return Err(e);
    }

    let mut result_stream = match conn.query(&params.query, &[]).await {
        Ok(s) => s,
        Err(tib_err) => {
            let enriched = try_enrich_error(tib_err, pool, params, "Failed to execute query").await;
            let _ = setup_tx.send(Err(anyhow::anyhow!("{:#}", enriched)));
            return Err(enriched);
        }
    };

    let columns = result_stream
        .columns()
        .await
        .unwrap_or(None)
        .unwrap_or(&[])
        .to_vec();

    let mut column_types: Vec<ColumnType> = columns.iter().map(ColumnType::from).collect();
    let mut column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

    // Setup succeeded — signal the handler so it can send HTTP 200 headers
    let _ = setup_tx.send(Ok(()));

    // --- Send metadata line (if requested) ---
    if params.include_metadata && !columns.is_empty() {
        let meta = serde_json::json!({
            "metadata": {
                "columns": columns.iter().map(|col| {
                    let ct = ColumnType::from(col);
                    serde_json::json!({
                        "name": col.name(),
                        "type": ct.type_name()
                    })
                }).collect::<Vec<_>>()
            }
        });
        if tx.send(Ok(ndjson_line(&meta))).await.is_err() {
            // Client disconnected
            return Ok(());
        }
    }

    // --- Streaming phase ---
    let pii_processor = build_pii_processor(params);
    let start_time = Instant::now();
    let mut row_count: u64 = 0;

    loop {
        let item = match result_stream.try_next().await {
            Ok(Some(item)) => item,
            Ok(None) => break, // stream finished
            Err(e) => {
                // Mid-stream error — send error object and stop
                let err_line = serde_json::json!({
                    "error": {
                        "message": format!("{}", e),
                        "partial": true,
                        "rows_before_error": row_count
                    }
                });
                let _ = tx.send(Ok(ndjson_line(&err_line))).await;
                return Err(anyhow::Error::new(e).context("Error during row streaming"));
            }
        };

        match item {
            QueryItem::Metadata(meta) => {
                // New result set — update column schema
                let new_columns = meta.columns().to_vec();
                column_types = new_columns.iter().map(ColumnType::from).collect();
                column_names = new_columns.iter().map(|c| c.name().to_string()).collect();
            }
            QueryItem::Row(row) => {
                let mut json_row = row_to_json_value(
                    &row,
                    &column_names,
                    &column_types,
                    params.preserve_decimal_precision,
                    &params.blob_format,
                );
                crate::pii::process_json_row(&pii_processor, &mut json_row);
                row_count += 1;

                let line = ndjson_line(&Value::Object(
                    json_row.into_iter().collect::<serde_json::Map<String, Value>>(),
                ));
                if tx.send(Ok(line)).await.is_err() {
                    // Client disconnected — exit cleanly
                    info!("NDJSON client disconnected after {} rows", row_count);
                    return Ok(());
                }
            }
        }
    }

    // --- Summary line ---
    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let rows_per_second = if elapsed_secs > 0.0 {
        row_count as f64 / elapsed_secs
    } else {
        0.0
    };

    let summary = serde_json::json!({
        "summary": {
            "total_rows": row_count,
            "execution_time_ms": elapsed.as_millis() as u64,
            "rows_per_second": (rows_per_second * 10.0).round() / 10.0
        }
    });
    let _ = tx.send(Ok(ndjson_line(&summary))).await;

    info!(
        "NDJSON stream complete: {} rows in {:.1}ms ({:.1} rows/s)",
        row_count,
        elapsed.as_secs_f64() * 1000.0,
        rows_per_second
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// DatabaseBackend trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl DatabaseBackend for MssqlBackend {
    async fn execute_query(&self, params: &QueryParams) -> Result<QueryResult> {
        execute_query_internal(&self.pool, params).await
    }

    async fn execute_query_streaming(
        &self,
        params: &QueryParams,
        tx: tokio::sync::mpsc::Sender<StreamChunk>,
    ) -> Result<()> {
        let pool = self.pool.clone();
        let params = params.clone();
        let (setup_tx, setup_rx) = tokio::sync::oneshot::channel::<Result<()>>();

        tokio::spawn(async move {
            let result = stream_query_task(&pool, &params, tx, setup_tx).await;
            if let Err(e) = result {
                warn!("Streaming task ended with error: {:#}", e);
            }
        });

        // Wait for setup phase to complete — if it fails, we return the error
        // before the HTTP response headers are sent
        setup_rx
            .await
            .map_err(|_| anyhow::anyhow!("Streaming task terminated before setup completed"))?
    }

    async fn validate_query(&self, database: &str, query: &str) -> Result<(), String> {
        let mut conn = self.pool.get().await
            .map_err(pool_run_err_string)?;
        switch_database(&mut conn, database).await
            .map_err(|e| e.to_string())?;

        let validation_sql = format!("SET NOEXEC ON; {}\nSET NOEXEC OFF;", query);
        let mut result_stream = conn
            .simple_query(validation_sql)
            .await
            .map_err(|e| format!("Failed to execute validation query: {}", e))?;

        while let Some(_item) = result_stream
            .try_next()
            .await
            .map_err(|e| format!("Failed to read validation result: {}", e))?
        {
            // consume all results to surface server errors
        }

        Ok(())
    }

    async fn list_databases(&self) -> Result<Vec<HashMap<String, Value>>> {
        let mut conn = self.pool.get().await
            .map_err(pool_run_err)?;

        let mut result_stream = conn
            .query("SELECT name FROM sys.databases ORDER BY name", &[])
            .await
            .context("Failed to list databases")?;

        let mut databases = Vec::new();
        while let Some(item) = result_stream
            .try_next()
            .await
            .context("Failed to read database list")?
        {
            if let QueryItem::Row(row) = item {
                let mut map = HashMap::new();
                if let Some(name) = row.get::<&str, _>(0) {
                    map.insert("name".to_string(), Value::String(name.to_string()));
                }
                databases.push(map);
            }
        }

        Ok(databases)
    }

    async fn list_schemas(&self, database: &str) -> Result<Vec<HashMap<String, Value>>> {
        let mut conn = self.pool.get().await
            .map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        let mut result_stream = conn
            .query(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME",
                &[],
            )
            .await
            .context("Failed to list schemas")?;

        let mut schemas = Vec::new();
        while let Some(item) = result_stream
            .try_next()
            .await
            .context("Failed to read schema list")?
        {
            if let QueryItem::Row(row) = item {
                let mut map = HashMap::new();
                if let Some(v) = row.get::<&str, _>(0) {
                    map.insert("schema_name".to_string(), Value::String(v.to_string()));
                }
                schemas.push(map);
            }
        }

        Ok(schemas)
    }

    async fn list_tables(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut conn = self.pool.get().await
            .map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        let query = format!(
            "SELECT t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_TYPE, \
                    COALESCE(p.rows, 0) as ROW_COUNT \
             FROM INFORMATION_SCHEMA.TABLES t \
             LEFT JOIN ( \
                 SELECT o.name, SUM(p.rows) as rows \
                 FROM sys.partitions p \
                 JOIN sys.objects o ON p.object_id = o.object_id \
                 WHERE p.index_id < 2 \
                 GROUP BY o.name \
             ) p ON p.name = t.TABLE_NAME \
             WHERE t.TABLE_SCHEMA = '{}' \
             ORDER BY t.TABLE_NAME",
            schema.replace('\'', "''")
        );

        let mut result_stream = conn
            .query(&query, &[])
            .await
            .context("Failed to list tables")?;

        let mut tables = Vec::new();
        while let Some(item) = result_stream
            .try_next()
            .await
            .context("Failed to read table list")?
        {
            if let QueryItem::Row(row) = item {
                let mut map = HashMap::new();
                if let Some(v) = row.get::<&str, _>(0) {
                    map.insert("TABLE_SCHEMA".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(1) {
                    map.insert("TABLE_NAME".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(2) {
                    map.insert("TABLE_TYPE".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<i64, _>(3) {
                    map.insert("ROW_COUNT".to_string(), Value::from(v));
                }
                tables.push(map);
            }
        }

        Ok(tables)
    }

    async fn describe_table(
        &self,
        database: &str,
        table: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut conn = self.pool.get().await
            .map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        let query = format!(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.CHARACTER_MAXIMUM_LENGTH, \
                    c.NUMERIC_PRECISION, c.NUMERIC_SCALE, \
                    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END as IS_PRIMARY_KEY \
             FROM INFORMATION_SCHEMA.COLUMNS c \
             LEFT JOIN ( \
                 SELECT ku.TABLE_NAME, ku.COLUMN_NAME \
                 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
                 JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME \
                 WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
             ) pk ON c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME \
             WHERE c.TABLE_NAME = '{}' AND c.TABLE_SCHEMA = '{}' \
             ORDER BY c.ORDINAL_POSITION",
            table.replace('\'', "''"),
            schema.replace('\'', "''")
        );

        let mut result_stream = conn
            .query(&query, &[])
            .await
            .context("Failed to describe table")?;

        let mut columns_out = Vec::new();
        while let Some(item) = result_stream
            .try_next()
            .await
            .context("Failed to read column description")?
        {
            if let QueryItem::Row(row) = item {
                let mut map = HashMap::new();
                if let Some(v) = row.get::<&str, _>(0) {
                    map.insert("COLUMN_NAME".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(1) {
                    map.insert("DATA_TYPE".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(2) {
                    map.insert("IS_NULLABLE".to_string(), Value::String(v.to_string()));
                }
                // CHARACTER_MAXIMUM_LENGTH can be NULL or an int
                if let Some(v) = row.get::<i32, _>(3) {
                    map.insert(
                        "CHARACTER_MAXIMUM_LENGTH".to_string(),
                        Value::from(v as i64),
                    );
                } else {
                    map.insert("CHARACTER_MAXIMUM_LENGTH".to_string(), Value::Null);
                }
                // NUMERIC_PRECISION
                if let Some(v) = row.get::<u8, _>(4) {
                    map.insert("NUMERIC_PRECISION".to_string(), Value::from(v as i64));
                } else {
                    map.insert("NUMERIC_PRECISION".to_string(), Value::Null);
                }
                // NUMERIC_SCALE
                if let Some(v) = row.get::<i32, _>(5) {
                    map.insert("NUMERIC_SCALE".to_string(), Value::from(v as i64));
                } else {
                    map.insert("NUMERIC_SCALE".to_string(), Value::Null);
                }
                if let Some(v) = row.get::<&str, _>(6) {
                    map.insert(
                        "IS_PRIMARY_KEY".to_string(),
                        Value::String(v.to_string()),
                    );
                }
                columns_out.push(map);
            }
        }

        Ok(columns_out)
    }

    async fn get_foreign_keys(
        &self,
        database: &str,
        table: &str,
        schema: &str,
    ) -> Result<Vec<super::ForeignKeyInfo>> {
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        // Query returns both outgoing and incoming FKs for the given table.
        // Uses sys catalog views for proper column-level FK mappings.
        let query = format!(
            "SELECT \
                fk.name AS constraint_name, \
                fs.name AS from_schema, \
                ft.name AS from_table, \
                fc.name AS from_column, \
                ts.name AS to_schema, \
                tt.name AS to_table, \
                tc.name AS to_column, \
                fkc.constraint_column_id AS col_ordinal \
             FROM sys.foreign_keys fk \
             JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id \
             JOIN sys.tables ft ON fkc.parent_object_id = ft.object_id \
             JOIN sys.schemas fs ON ft.schema_id = fs.schema_id \
             JOIN sys.columns fc ON fkc.parent_object_id = fc.object_id AND fkc.parent_column_id = fc.column_id \
             JOIN sys.tables tt ON fkc.referenced_object_id = tt.object_id \
             JOIN sys.schemas ts ON tt.schema_id = ts.schema_id \
             JOIN sys.columns tc ON fkc.referenced_object_id = tc.object_id AND fkc.referenced_column_id = tc.column_id \
             WHERE (ft.name = '{table}' AND fs.name = '{schema}') \
                OR (tt.name = '{table}' AND ts.name = '{schema}') \
             ORDER BY fk.name, fkc.constraint_column_id",
            table = table.replace('\'', "''"),
            schema = schema.replace('\'', "''")
        );

        let mut result_stream = conn
            .query(&query, &[])
            .await
            .context("Failed to query foreign keys")?;

        let mut fk_map: std::collections::BTreeMap<String, super::ForeignKeyInfo> =
            std::collections::BTreeMap::new();

        while let Some(item) = result_stream.try_next().await.context("Failed to read FK row")? {
            if let QueryItem::Row(row) = item {
                let constraint_name: &str = row.get::<&str, _>(0).unwrap_or_default();
                let from_schema: &str = row.get::<&str, _>(1).unwrap_or_default();
                let from_table: &str = row.get::<&str, _>(2).unwrap_or_default();
                let from_column: &str = row.get::<&str, _>(3).unwrap_or_default();
                let to_schema: &str = row.get::<&str, _>(4).unwrap_or_default();
                let to_table: &str = row.get::<&str, _>(5).unwrap_or_default();
                let to_column: &str = row.get::<&str, _>(6).unwrap_or_default();

                let entry = fk_map.entry(constraint_name.to_string()).or_insert_with(|| {
                    super::ForeignKeyInfo {
                        constraint_name: constraint_name.to_string(),
                        from_schema: from_schema.to_string(),
                        from_table: from_table.to_string(),
                        from_columns: Vec::new(),
                        to_schema: to_schema.to_string(),
                        to_table: to_table.to_string(),
                        to_columns: Vec::new(),
                    }
                });
                entry.from_columns.push(from_column.to_string());
                entry.to_columns.push(to_column.to_string());
            }
        }

        Ok(fk_map.into_values().collect())
    }

    async fn list_views(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        let query = format!(
            "SELECT v.name, s.name AS schema_name, \
                    CONVERT(varchar, v.create_date, 120) AS create_date, \
                    CONVERT(varchar, v.modify_date, 120) AS modify_date \
             FROM sys.views v \
             JOIN sys.schemas s ON v.schema_id = s.schema_id \
             WHERE s.name = '{}' \
             ORDER BY v.name",
            schema.replace('\'', "''")
        );

        let mut result_stream = conn
            .query(&query, &[])
            .await
            .context("Failed to list views")?;

        let mut views = Vec::new();
        while let Some(item) = result_stream.try_next().await.context("Failed to read view row")? {
            if let QueryItem::Row(row) = item {
                let mut map = HashMap::new();
                if let Some(v) = row.get::<&str, _>(0) {
                    map.insert("name".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(1) {
                    map.insert("schema_name".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(2) {
                    map.insert("create_date".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(3) {
                    map.insert("modify_date".to_string(), Value::String(v.to_string()));
                }
                map.insert("type".to_string(), Value::String("VIEW".to_string()));
                views.push(map);
            }
        }

        Ok(views)
    }

    async fn list_routines(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        let query = format!(
            "SELECT o.name, s.name AS schema_name, \
                    CONVERT(varchar, o.create_date, 120) AS create_date, \
                    CONVERT(varchar, o.modify_date, 120) AS modify_date, \
                    CASE o.type \
                        WHEN 'P' THEN 'PROCEDURE' \
                        WHEN 'FN' THEN 'SCALAR_FUNCTION' \
                        WHEN 'IF' THEN 'INLINE_TABLE_FUNCTION' \
                        WHEN 'TF' THEN 'TABLE_FUNCTION' \
                    END AS routine_type \
             FROM sys.objects o \
             JOIN sys.schemas s ON o.schema_id = s.schema_id \
             WHERE o.type IN ('P','FN','IF','TF') AND s.name = '{}' \
             ORDER BY o.type, o.name",
            schema.replace('\'', "''")
        );

        let mut result_stream = conn
            .query(&query, &[])
            .await
            .context("Failed to list routines")?;

        let mut routines = Vec::new();
        while let Some(item) = result_stream.try_next().await.context("Failed to read routine row")? {
            if let QueryItem::Row(row) = item {
                let mut map = HashMap::new();
                if let Some(v) = row.get::<&str, _>(0) {
                    map.insert("name".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(1) {
                    map.insert("schema_name".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(2) {
                    map.insert("create_date".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(3) {
                    map.insert("modify_date".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(4) {
                    map.insert("routine_type".to_string(), Value::String(v.to_string()));
                }
                routines.push(map);
            }
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
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        let qualified = format!("[{}].[{}]", schema.replace('\'', "''"), name.replace('\'', "''"));

        // Get definition
        let def_query = format!(
            "SELECT OBJECT_DEFINITION(OBJECT_ID('{}')) AS definition",
            qualified.replace('\'', "''")
        );

        let definition: Option<String> = {
            let mut result_stream = conn
                .query(&def_query, &[])
                .await
                .context("Failed to get object definition")?;

            let mut def = None;
            while let Some(item) = result_stream.try_next().await.context("Failed to read definition")? {
                if let QueryItem::Row(row) = item {
                    def = row.get::<&str, _>(0).map(|s| s.to_string());
                }
            }
            def
        };

        let definition = match definition {
            Some(d) => d,
            None => return Ok(None), // encrypted or not found
        };

        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::String(name.to_string()));
        map.insert("schema_name".to_string(), Value::String(schema.to_string()));
        map.insert("type".to_string(), Value::String(object_type.to_string()));
        map.insert("definition".to_string(), Value::String(definition));

        // For procedures/functions, get parameters
        if object_type != "view" {
            let params_query = format!(
                "SELECT p.name, TYPE_NAME(p.user_type_id) AS type_name, \
                        p.max_length, p.is_output \
                 FROM sys.parameters p \
                 WHERE p.object_id = OBJECT_ID('{}') \
                 ORDER BY p.parameter_id",
                qualified.replace('\'', "''")
            );

            let mut param_stream = conn
                .query(&params_query, &[])
                .await
                .context("Failed to get parameters")?;

            let mut params = Vec::new();
            while let Some(item) = param_stream.try_next().await.context("Failed to read param row")? {
                if let QueryItem::Row(row) = item {
                    let mut p = HashMap::new();
                    if let Some(v) = row.get::<&str, _>(0) {
                        p.insert("param_name".to_string(), Value::String(v.to_string()));
                    }
                    if let Some(v) = row.get::<&str, _>(1) {
                        p.insert("type_name".to_string(), Value::String(v.to_string()));
                    }
                    if let Some(v) = row.get::<i16, _>(2) {
                        p.insert("max_length".to_string(), Value::from(v as i64));
                    }
                    if let Some(v) = row.get::<bool, _>(3) {
                        p.insert("is_output".to_string(), Value::Bool(v));
                    }
                    params.push(Value::Object(p.into_iter().collect()));
                }
            }

            if !params.is_empty() {
                map.insert("parameters".to_string(), Value::Array(params));
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
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        let query = format!(
            "SELECT t.name, s.name AS schema_name, OBJECT_NAME(t.parent_id) AS parent_table, \
                    t.is_disabled, t.is_instead_of_trigger, \
                    CONVERT(varchar, t.create_date, 120) AS create_date, \
                    CONVERT(varchar, t.modify_date, 120) AS modify_date, \
                    STUFF((SELECT ', ' + te.type_desc FROM sys.trigger_events te \
                           WHERE te.object_id = t.object_id FOR XML PATH('')), 1, 2, '') AS events \
             FROM sys.triggers t \
             JOIN sys.objects o ON t.parent_id = o.object_id \
             JOIN sys.schemas s ON o.schema_id = s.schema_id \
             WHERE s.name = '{}' AND OBJECT_NAME(t.parent_id) = '{}' \
             ORDER BY t.name",
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );

        let mut result_stream = conn
            .query(&query, &[])
            .await
            .context("Failed to list triggers")?;

        let mut triggers = Vec::new();
        while let Some(item) = result_stream.try_next().await.context("Failed to read trigger row")? {
            if let QueryItem::Row(row) = item {
                let mut map = HashMap::new();
                if let Some(v) = row.get::<&str, _>(0) {
                    map.insert("name".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(1) {
                    map.insert("schema_name".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(2) {
                    map.insert("parent_table".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<bool, _>(3) {
                    map.insert("is_disabled".to_string(), Value::Bool(v));
                }
                if let Some(v) = row.get::<bool, _>(4) {
                    map.insert("is_instead_of_trigger".to_string(), Value::Bool(v));
                }
                if let Some(v) = row.get::<&str, _>(5) {
                    map.insert("create_date".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(6) {
                    map.insert("modify_date".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(7) {
                    map.insert("events".to_string(), Value::String(v.to_string()));
                }
                triggers.push(map);
            }
        }

        Ok(triggers)
    }

    async fn get_trigger_definition(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<HashMap<String, Value>>> {
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        let qualified = format!("[{}].[{}]", schema.replace('\'', "''"), name.replace('\'', "''"));
        let def_query = format!(
            "SELECT OBJECT_DEFINITION(OBJECT_ID('{}')) AS definition",
            qualified.replace('\'', "''")
        );

        let definition: Option<String> = {
            let mut result_stream = conn
                .query(&def_query, &[])
                .await
                .context("Failed to get trigger definition")?;

            let mut def = None;
            while let Some(item) = result_stream.try_next().await.context("Failed to read definition")? {
                if let QueryItem::Row(row) = item {
                    def = row.get::<&str, _>(0).map(|s| s.to_string());
                }
            }
            def
        };

        let definition = match definition {
            Some(d) => d,
            None => return Ok(None),
        };

        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::String(name.to_string()));
        map.insert("schema_name".to_string(), Value::String(schema.to_string()));
        map.insert("definition".to_string(), Value::String(definition));

        Ok(Some(map))
    }

    async fn get_related_objects(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        let query = format!(
            "SELECT DISTINCT OBJECT_NAME(d.referencing_id) AS object_name, s.name AS schema_name, \
                    CASE o.type WHEN 'V' THEN 'VIEW' WHEN 'P' THEN 'PROCEDURE' \
                      WHEN 'FN' THEN 'SCALAR_FUNCTION' WHEN 'IF' THEN 'INLINE_TABLE_FUNCTION' \
                      WHEN 'TF' THEN 'TABLE_FUNCTION' END AS object_type, \
                    CONVERT(varchar, o.modify_date, 120) AS modify_date \
             FROM sys.sql_expression_dependencies d \
             JOIN sys.objects o ON d.referencing_id = o.object_id \
             JOIN sys.schemas s ON o.schema_id = s.schema_id \
             WHERE d.referenced_schema_name = '{}' AND d.referenced_entity_name = '{}' \
               AND o.type IN ('V','P','FN','IF','TF') \
             ORDER BY object_type, object_name",
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );

        let mut result_stream = conn
            .query(&query, &[])
            .await
            .context("Failed to get related objects")?;

        let mut objects = Vec::new();
        while let Some(item) = result_stream.try_next().await.context("Failed to read related object row")? {
            if let QueryItem::Row(row) = item {
                let mut map = HashMap::new();
                if let Some(v) = row.get::<&str, _>(0) {
                    map.insert("object_name".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(1) {
                    map.insert("schema_name".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(2) {
                    map.insert("object_type".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(3) {
                    map.insert("modify_date".to_string(), Value::String(v.to_string()));
                }
                objects.push(map);
            }
        }

        Ok(objects)
    }

    async fn list_rls_policies(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        let query = format!(
            "SELECT sp.name AS policy_name, sp.is_enabled, \
                    CONVERT(varchar, sp.create_date, 120) AS create_date, \
                    CONVERT(varchar, sp.modify_date, 120) AS modify_date, \
                    pred.predicate_type_desc AS predicate_type, \
                    pred.predicate_definition, \
                    pred.operation_desc AS operation \
             FROM sys.security_policies sp \
             JOIN sys.security_predicates pred ON sp.object_id = pred.object_id \
             JOIN sys.objects o ON pred.target_object_id = o.object_id \
             JOIN sys.schemas s ON o.schema_id = s.schema_id \
             WHERE s.name = '{}' AND o.name = '{}' \
             ORDER BY sp.name, pred.predicate_type_desc",
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );

        let mut result_stream = conn
            .query(&query, &[])
            .await
            .context("Failed to list RLS policies")?;

        let mut policies = Vec::new();
        while let Some(item) = result_stream.try_next().await.context("Failed to read RLS policy row")? {
            if let QueryItem::Row(row) = item {
                let mut map = HashMap::new();
                if let Some(v) = row.get::<&str, _>(0) {
                    map.insert("policy_name".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<bool, _>(1) {
                    map.insert("is_enabled".to_string(), Value::Bool(v));
                }
                if let Some(v) = row.get::<&str, _>(2) {
                    map.insert("create_date".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(3) {
                    map.insert("modify_date".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(4) {
                    map.insert("predicate_type".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(5) {
                    map.insert("predicate_definition".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(6) {
                    map.insert("operation".to_string(), Value::String(v.to_string()));
                }
                policies.push(map);
            }
        }

        Ok(policies)
    }

    async fn get_rls_status(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<HashMap<String, Value>>> {
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        switch_database(&mut conn, database).await?;

        let query = format!(
            "SELECT COUNT(*) AS policy_count, \
                    SUM(CASE WHEN sp.is_enabled = 1 THEN 1 ELSE 0 END) AS enabled_count \
             FROM sys.security_policies sp \
             JOIN sys.security_predicates pred ON sp.object_id = pred.object_id \
             JOIN sys.objects o ON pred.target_object_id = o.object_id \
             JOIN sys.schemas s ON o.schema_id = s.schema_id \
             WHERE s.name = '{}' AND o.name = '{}'",
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );

        let mut result_stream = conn
            .query(&query, &[])
            .await
            .context("Failed to get RLS status")?;

        while let Some(item) = result_stream.try_next().await.context("Failed to read RLS status row")? {
            if let QueryItem::Row(row) = item {
                let policy_count: i32 = row.get::<i32, _>(0).unwrap_or(0);
                let enabled_count: i32 = row.get::<i32, _>(1).unwrap_or(0);
                let mut map = HashMap::new();
                map.insert("rls_enabled".to_string(), Value::Bool(policy_count > 0));
                map.insert("policy_count".to_string(), Value::Number(policy_count.into()));
                map.insert("enabled_count".to_string(), Value::Number(enabled_count.into()));
                return Ok(Some(map));
            }
        }

        Ok(None)
    }

    async fn generate_rls_sql(
        &self,
        _database: &str,
        schema: &str,
        table: &str,
        action: &str,
        params: &HashMap<String, String>,
    ) -> Result<String> {
        match action {
            "enable_policy" => {
                let name = params.get("policy_name").ok_or_else(|| anyhow::anyhow!("policy_name required"))?;
                Ok(format!("ALTER SECURITY POLICY [{}].[{}] WITH (STATE = ON);", schema, name))
            }
            "disable_policy" => {
                let name = params.get("policy_name").ok_or_else(|| anyhow::anyhow!("policy_name required"))?;
                Ok(format!("ALTER SECURITY POLICY [{}].[{}] WITH (STATE = OFF);", schema, name))
            }
            "drop_policy" => {
                let name = params.get("policy_name").ok_or_else(|| anyhow::anyhow!("policy_name required"))?;
                Ok(format!("DROP SECURITY POLICY [{}].[{}];", schema, name))
            }
            "create_policy" => {
                let name = params.get("policy_name").ok_or_else(|| anyhow::anyhow!("policy_name required"))?;
                let predicate_type = params.get("predicate_type").map(|s| s.as_str()).unwrap_or("FILTER");
                let predicate_fn = params.get("predicate_function").ok_or_else(|| anyhow::anyhow!("predicate_function required"))?;
                let predicate_args = params.get("predicate_args").map(|s| s.as_str()).unwrap_or("");

                let predicate_keyword = match predicate_type {
                    "BLOCK" => "BLOCK",
                    _ => "FILTER",
                };

                Ok(format!(
                    "CREATE SECURITY POLICY [{}].[{}]\n  ADD {} PREDICATE {}({}) ON [{}].[{}]\n  WITH (STATE = ON);",
                    schema, name, predicate_keyword, predicate_fn, predicate_args, schema, table
                ))
            }
            _ => anyhow::bail!("Unknown RLS action: {}", action),
        }
    }

    fn dialect(&self) -> Dialect {
        Dialect::Mssql
    }

    fn default_database(&self) -> &str {
        &self.default_db
    }

    async fn health_check(&self) -> Result<()> {
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        conn.query("SELECT 1", &[]).await.context("Health check failed")?;
        Ok(())
    }

    fn pool_stats(&self) -> Option<super::PoolStats> {
        let state = self.pool.state();
        Some(super::PoolStats {
            total_connections: state.connections,
            idle_connections: state.idle_connections,
            active_connections: state.connections - state.idle_connections,
            max_size: 10,
        })
    }

    async fn list_active_queries(&self) -> Result<Vec<HashMap<String, Value>>> {
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        let query = "\
            SELECT \
                r.session_id AS spid, \
                r.status, \
                r.command, \
                DATEDIFF(SECOND, r.start_time, GETDATE()) AS duration_seconds, \
                r.wait_type, \
                r.wait_time, \
                r.blocking_session_id, \
                DB_NAME(r.database_id) AS database_name, \
                t.text AS query_text \
            FROM sys.dm_exec_requests r \
            CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t \
            WHERE r.session_id > 50 \
              AND r.session_id != @@SPID \
            ORDER BY r.start_time";

        let mut result_stream = conn
            .query(query, &[])
            .await
            .context("Failed to list active queries")?;

        let mut queries = Vec::new();
        while let Some(item) = result_stream
            .try_next()
            .await
            .context("Failed to read active queries")?
        {
            if let QueryItem::Row(row) = item {
                let mut map = HashMap::new();
                if let Some(v) = row.get::<i16, _>(0) {
                    map.insert("spid".to_string(), Value::from(v as i64));
                }
                if let Some(v) = row.get::<&str, _>(1) {
                    map.insert("status".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(2) {
                    map.insert("command".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<i32, _>(3) {
                    map.insert("duration_seconds".to_string(), Value::from(v as i64));
                }
                if let Some(v) = row.get::<&str, _>(4) {
                    map.insert("wait_type".to_string(), Value::String(v.to_string()));
                } else {
                    map.insert("wait_type".to_string(), Value::Null);
                }
                if let Some(v) = row.get::<i32, _>(5) {
                    map.insert("wait_time".to_string(), Value::from(v as i64));
                }
                if let Some(v) = row.get::<i16, _>(6) {
                    map.insert("blocking_session_id".to_string(), Value::from(v as i64));
                }
                if let Some(v) = row.get::<&str, _>(7) {
                    map.insert("database_name".to_string(), Value::String(v.to_string()));
                }
                if let Some(v) = row.get::<&str, _>(8) {
                    map.insert("query_text".to_string(), Value::String(v.to_string()));
                }
                queries.push(map);
            }
        }

        Ok(queries)
    }

    async fn kill_query(&self, process_id: i64) -> Result<()> {
        let mut conn = self.pool.get().await.map_err(pool_run_err)?;
        let kill_sql = format!("KILL {}", process_id);
        let mut result_stream = conn
            .simple_query(&kill_sql)
            .await
            .context("Failed to kill query")?;
        // Consume the stream
        while let Some(_item) = result_stream
            .try_next()
            .await
            .context("Failed to read kill result")?
        {}
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_tables_simple() {
        assert_eq!(extract_tables_from_query("SELECT * FROM Users"), vec!["Users"]);
    }

    #[test]
    fn test_extract_tables_bracketed() {
        assert_eq!(extract_tables_from_query("SELECT * FROM [Users]"), vec!["Users"]);
    }

    #[test]
    fn test_extract_tables_schema_qualified() {
        assert_eq!(extract_tables_from_query("SELECT * FROM dbo.Users"), vec!["Users"]);
    }

    #[test]
    fn test_extract_tables_join() {
        let tables = extract_tables_from_query(
            "SELECT u.Name FROM Users u JOIN Orders o ON u.Id = o.UserId"
        );
        assert_eq!(tables, vec!["Users", "Orders"]);
    }

    #[test]
    fn test_extract_tables_cte() {
        let tables = extract_tables_from_query(
            "WITH cte AS (SELECT * FROM Users) SELECT * FROM cte"
        );
        assert!(tables.contains(&"Users".to_string()));
        assert!(tables.contains(&"cte".to_string()));
    }

    #[test]
    fn test_extract_tables_left_join() {
        let tables = extract_tables_from_query(
            "SELECT * FROM Employees e LEFT JOIN Departments d ON e.DeptId = d.Id"
        );
        assert_eq!(tables, vec!["Employees", "Departments"]);
    }

    #[test]
    fn test_extract_quoted_value_found() {
        assert_eq!(
            extract_quoted_value_local("Invalid column name 'Foo'."),
            Some("Foo".to_string())
        );
    }

    #[test]
    fn test_extract_quoted_value_not_found() {
        assert_eq!(extract_quoted_value_local("No quotes here"), None);
    }

    #[test]
    fn test_extract_quoted_value_table() {
        assert_eq!(
            extract_quoted_value_local("Invalid object name 'Userz'."),
            Some("Userz".to_string())
        );
    }

    #[test]
    fn test_column_type_name_mapping() {
        assert_eq!(ColumnType::BigInt.type_name(), "BigInt");
        assert_eq!(ColumnType::I32.type_name(), "Int");
        assert_eq!(ColumnType::SmallInt.type_name(), "SmallInt");
        assert_eq!(ColumnType::TinyInt.type_name(), "TinyInt");
        assert_eq!(ColumnType::Bool.type_name(), "Bit");
        assert_eq!(ColumnType::Decimal.type_name(), "Decimal");
        assert_eq!(ColumnType::Money.type_name(), "Money");
        assert_eq!(ColumnType::SmallMoney.type_name(), "SmallMoney");
        assert_eq!(ColumnType::F64.type_name(), "Float");
        assert_eq!(ColumnType::Real.type_name(), "Real");
        assert_eq!(ColumnType::Date.type_name(), "Date");
        assert_eq!(ColumnType::Time.type_name(), "Time");
        assert_eq!(ColumnType::DateTime.type_name(), "DateTime");
        assert_eq!(ColumnType::DateTime2.type_name(), "DateTime2");
        assert_eq!(ColumnType::DateTimeOffset.type_name(), "DateTimeOffset");
        assert_eq!(ColumnType::SmallDateTime.type_name(), "SmallDateTime");
        assert_eq!(ColumnType::Char.type_name(), "Char");
        assert_eq!(ColumnType::VarChar.type_name(), "VarChar");
        assert_eq!(ColumnType::Text.type_name(), "Text");
        assert_eq!(ColumnType::NChar.type_name(), "NChar");
        assert_eq!(ColumnType::NVarChar.type_name(), "NVarChar");
        assert_eq!(ColumnType::NText.type_name(), "NText");
        assert_eq!(ColumnType::Binary.type_name(), "Binary");
        assert_eq!(ColumnType::VarBinary.type_name(), "VarBinary");
        assert_eq!(ColumnType::Image.type_name(), "Image");
        assert_eq!(ColumnType::UniqueIdentifier.type_name(), "UniqueIdentifier");
        assert_eq!(ColumnType::Xml.type_name(), "Xml");
        assert_eq!(ColumnType::Null.type_name(), "NULL");
        assert_eq!(ColumnType::Unknown.type_name(), "Unknown");
    }

}
