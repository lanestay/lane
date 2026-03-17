//! DuckDB database backend implementation.
//!
//! DuckDB's Rust crate is synchronous, so all operations are wrapped in
//! `tokio::task::spawn_blocking` to avoid blocking the async runtime.
//!
//! We use the Arrow API (`query_arrow`) instead of the Row API (`query`)
//! because duckdb-rs v1.1.1 has a bug where `row.get_ref()` panics with
//! "called `Option::unwrap()` on a `None` value" in `column_type()`.

use anyhow::{Context, Result};
use async_trait::async_trait;
use duckdb::arrow::array::*;
use duckdb::arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tracing::info;

use regex::Regex;

use crate::config::DuckDbConnectionConfig;
use crate::query::{
    BlobFormat, ColumnMeta, QueryMetadata, QueryParams, QueryResult, ROW_NUMBER_ALIAS,
    build_pii_processor, format_binary_data,
    pagination::{create_paginated_query, get_count_query},
};

use super::{DatabaseBackend, Dialect};

/// DuckDB backend — single connection behind a Mutex.
pub struct DuckDbBackend {
    conn: Arc<Mutex<duckdb::Connection>>,
    path: String,
}

impl DuckDbBackend {
    pub async fn new(config: DuckDbConnectionConfig) -> Result<Self> {
        let path = config.path.clone();
        let backend = tokio::task::spawn_blocking(move || -> Result<DuckDbBackend> {
            let conn = if config.path == ":memory:" {
                duckdb::Connection::open_in_memory()
                    .context("Failed to open in-memory DuckDB")?
            } else if config.read_only == Some(true) {
                let db_config = duckdb::Config::default()
                    .access_mode(duckdb::AccessMode::ReadOnly)
                    .map_err(|e| anyhow::anyhow!("DuckDB config error: {}", e))?;
                duckdb::Connection::open_with_flags(&config.path, db_config)
                    .context(format!(
                        "Failed to open DuckDB at '{}' (read-only)",
                        config.path
                    ))?
            } else {
                duckdb::Connection::open(&config.path)
                    .context(format!("Failed to open DuckDB at '{}'", config.path))?
            };

            // Verify connectivity
            conn.execute_batch("SELECT 1")
                .context("DuckDB connectivity check failed")?;

            Ok(DuckDbBackend {
                conn: Arc::new(Mutex::new(conn)),
                path: config.path,
            })
        })
        .await??;

        info!("DuckDB connection initialized for '{}'", path);
        Ok(backend)
    }
}

// ---------------------------------------------------------------------------
// Arrow → JSON conversion
// ---------------------------------------------------------------------------

/// Convert an Arrow DataType to a human-readable SQL type string.
fn arrow_type_to_sql(dt: &ArrowDataType) -> String {
    match dt {
        ArrowDataType::Null => "NULL".into(),
        ArrowDataType::Boolean => "BOOLEAN".into(),
        ArrowDataType::Int8 => "TINYINT".into(),
        ArrowDataType::Int16 => "SMALLINT".into(),
        ArrowDataType::Int32 => "INTEGER".into(),
        ArrowDataType::Int64 => "BIGINT".into(),
        ArrowDataType::UInt8 => "UTINYINT".into(),
        ArrowDataType::UInt16 => "USMALLINT".into(),
        ArrowDataType::UInt32 => "UINTEGER".into(),
        ArrowDataType::UInt64 => "UBIGINT".into(),
        ArrowDataType::Float16 | ArrowDataType::Float32 => "FLOAT".into(),
        ArrowDataType::Float64 => "DOUBLE".into(),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => "VARCHAR".into(),
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::FixedSizeBinary(_) => {
            "BLOB".into()
        }
        ArrowDataType::Date32 | ArrowDataType::Date64 => "DATE".into(),
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => "TIME".into(),
        ArrowDataType::Timestamp(_, _) => "TIMESTAMP".into(),
        ArrowDataType::Decimal128(p, s) => format!("DECIMAL({},{})", p, s),
        ArrowDataType::List(_) | ArrowDataType::LargeList(_) => "LIST".into(),
        ArrowDataType::Struct(_) => "STRUCT".into(),
        ArrowDataType::Map(_, _) => "MAP".into(),
        _ => format!("{}", dt),
    }
}

/// Extract a JSON value from an Arrow array at the given row index.
fn arrow_value_at(
    array: &dyn Array,
    row: usize,
    blob_format: &BlobFormat,
) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(a.value(row))
        }
        ArrowDataType::Int8 => {
            let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Value::from(a.value(row) as i64)
        }
        ArrowDataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Value::from(a.value(row) as i64)
        }
        ArrowDataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::from(a.value(row) as i64)
        }
        ArrowDataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::from(a.value(row))
        }
        ArrowDataType::UInt8 => {
            let a = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Value::from(a.value(row) as u64)
        }
        ArrowDataType::UInt16 => {
            let a = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Value::from(a.value(row) as u64)
        }
        ArrowDataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Value::from(a.value(row) as u64)
        }
        ArrowDataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::from(a.value(row))
        }
        ArrowDataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            serde_json::Number::from_f64(a.value(row) as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        ArrowDataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::Number::from_f64(a.value(row))
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        ArrowDataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(a.value(row).to_string())
        }
        ArrowDataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(a.value(row).to_string())
        }
        ArrowDataType::Binary => {
            let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Value::String(format_binary_data(a.value(row), blob_format))
        }
        ArrowDataType::LargeBinary => {
            let a = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Value::String(format_binary_data(a.value(row), blob_format))
        }
        ArrowDataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = a.value(row);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163);
            match date {
                Some(d) => Value::String(d.to_string()),
                None => Value::String(format!("[date32: {}]", days)),
            }
        }
        ArrowDataType::Date64 => {
            let a = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let ms = a.value(row);
            match chrono::DateTime::from_timestamp_millis(ms) {
                Some(dt) => Value::String(dt.format("%Y-%m-%d").to_string()),
                None => Value::String(format!("[date64: {}]", ms)),
            }
        }
        ArrowDataType::Timestamp(unit, _) => {
            let micros = match unit {
                ArrowTimeUnit::Second => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    a.value(row) * 1_000_000
                }
                ArrowTimeUnit::Millisecond => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    a.value(row) * 1_000
                }
                ArrowTimeUnit::Microsecond => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    a.value(row)
                }
                ArrowTimeUnit::Nanosecond => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    a.value(row) / 1_000
                }
            };
            let secs = micros / 1_000_000;
            let nsecs = ((micros % 1_000_000).unsigned_abs() * 1_000) as u32;
            match chrono::DateTime::from_timestamp(secs, nsecs) {
                Some(dt) => Value::String(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
                None => Value::String(format!("[timestamp: {}]", micros)),
            }
        }
        ArrowDataType::Decimal128(_, scale) => {
            let a = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            let raw = a.value(row);
            let scale = *scale as u32;
            if scale == 0 {
                Value::from(raw as i64)
            } else {
                let divisor = 10_f64.powi(scale as i32);
                serde_json::Number::from_f64(raw as f64 / divisor)
                    .map(Value::Number)
                    .unwrap_or(Value::String(format!("{}", raw)))
            }
        }
        // Fallback: use Arrow's Display formatting
        _ => {
            let formatted = duckdb::arrow::util::display::array_value_to_string(array, row)
                .unwrap_or_else(|_| "?".to_string());
            Value::String(formatted)
        }
    }
}

/// Execute a query using the Arrow API and convert results to our format.
fn execute_duckdb_query(
    conn: &duckdb::Connection,
    sql: &str,
    blob_format: &BlobFormat,
    pii_processor: Option<&crate::pii::PiiProcessor>,
) -> Result<(Vec<HashMap<String, Value>>, Vec<ColumnMeta>)> {
    let mut stmt = conn
        .prepare(sql)
        .context("Failed to prepare DuckDB query")?;

    let arrow_iter = stmt
        .query_arrow([])
        .context("Failed to execute DuckDB query")?;

    // Get schema from the arrow iterator
    let schema = arrow_iter.get_schema();
    let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let columns_meta: Vec<ColumnMeta> = schema
        .fields()
        .iter()
        .map(|f| ColumnMeta {
            name: f.name().clone(),
            data_type: arrow_type_to_sql(f.data_type()),
        })
        .collect();

    let mut data = Vec::new();
    for batch in arrow_iter {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            let mut map = HashMap::new();
            for col_idx in 0..num_cols {
                let array = batch.column(col_idx);
                let json_val = arrow_value_at(array.as_ref(), row_idx, blob_format);
                map.insert(column_names[col_idx].clone(), json_val);
            }
            if let Some(proc) = pii_processor {
                crate::pii::process_json_row(proc, &mut map);
            }
            data.push(map);
        }
    }

    Ok((data, columns_meta))
}

/// Execute a simple metadata query (no PII processing needed).
fn execute_metadata_query(
    conn: &duckdb::Connection,
    sql: &str,
) -> Result<Vec<HashMap<String, Value>>> {
    let (data, _) = execute_duckdb_query(conn, sql, &BlobFormat::Length, None)?;
    Ok(data)
}

/// Execute a count query, returning the count as i64.
fn execute_count_query(conn: &duckdb::Connection, sql: &str) -> Result<i64> {
    let mut stmt = conn.prepare(sql).context("Failed to prepare count query")?;
    let batches: Vec<RecordBatch> = stmt
        .query_arrow([])
        .context("Failed to execute count query")?
        .collect();

    if let Some(batch) = batches.first() {
        if batch.num_rows() > 0 && batch.num_columns() > 0 {
            let col = batch.column(0);
            if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                return Ok(a.value(0));
            }
            // Try other integer types
            if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                return Ok(a.value(0) as i64);
            }
        }
    }
    Ok(0)
}

// ---------------------------------------------------------------------------
// Error enrichment helpers (sync — DuckDB runs inside spawn_blocking)
// ---------------------------------------------------------------------------

/// Extract table names from FROM/JOIN clauses in a SQL query.
fn extract_tables_from_query(query: &str) -> Vec<String> {
    let re = Regex::new(
        r#"(?i)(?:FROM|JOIN)\s+("?[A-Za-z_][A-Za-z0-9_]*"?(?:\."?[A-Za-z_][A-Za-z0-9_]*"?)?)"#,
    )
    .unwrap();
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

/// Extract object name from a DuckDB "does not exist" / "not found" error.
/// Returns `(is_column, object_name)`.
fn extract_duckdb_error_object(message: &str) -> Option<(bool, String)> {
    // Pattern 1: "Table with name X does not exist" (catalog errors)
    let table_re =
        Regex::new(r#"(?i)Table with name (\S+) does not exist"#).ok()?;
    if let Some(caps) = table_re.captures(message) {
        let name = caps[1].trim_matches('"').trim_matches('[').trim_matches(']').to_string();
        return Some((false, name));
    }

    // Pattern 2: 'Referenced column "X" not found in FROM clause' (binder errors)
    let col_re =
        Regex::new(r#"(?i)Referenced column "([^"]+)" not found"#).ok()?;
    if let Some(caps) = col_re.captures(message) {
        return Some((true, caps[1].to_string()));
    }

    // Pattern 3: "Column with name X does not exist" (older DuckDB versions)
    let col_re2 =
        Regex::new(r#"(?i)Column with name (\S+) does not exist"#).ok()?;
    if let Some(caps) = col_re2.captures(message) {
        let name = caps[1].trim_matches('"').trim_matches('[').trim_matches(']').to_string();
        return Some((true, name));
    }

    None
}

/// Enrich a DuckDB "column not found" error with available column names.
fn enrich_duckdb_column_error(
    conn: &duckdb::Connection,
    query: &str,
    bad_column: &str,
) -> Option<String> {
    let tables = extract_tables_from_query(query);
    if tables.is_empty() {
        return None;
    }

    let mut parts: Vec<String> = Vec::new();
    for table in &tables {
        let safe_table = table.replace('\'', "''");
        let col_sql = format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = '{}' ORDER BY ordinal_position",
            safe_table
        );
        if let Ok(rows) = execute_metadata_query(conn, &col_sql) {
            let col_names: Vec<String> = rows
                .iter()
                .filter_map(|r| r.get("column_name").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .collect();
            if !col_names.is_empty() {
                parts.push(format!("[{}]: {}", table, col_names.join(", ")));
            }
        }
    }

    if parts.is_empty() {
        return None;
    }

    Some(format!(
        "Column '{}' not found. Available columns: {}",
        bad_column,
        parts.join("; ")
    ))
}

/// Enrich a DuckDB "table not found" error with available table names.
fn enrich_duckdb_table_error(conn: &duckdb::Connection, bad_table: &str) -> Option<String> {
    let sql = "SELECT table_name FROM information_schema.tables \
               WHERE table_schema = 'main' ORDER BY table_name LIMIT 50";
    let rows = execute_metadata_query(conn, sql).ok()?;
    let names: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get("table_name").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    if names.is_empty() {
        return None;
    }

    Some(format!(
        "Table '{}' not found. Available tables: {}",
        bad_table,
        names.join(", ")
    ))
}

/// Try to enrich a DuckDB error with schema hints for "does not exist" errors.
fn try_enrich_duckdb_error(
    err: anyhow::Error,
    conn: &duckdb::Connection,
    query: &str,
) -> anyhow::Error {
    let error_msg = format!("{:#}", err);

    if !error_msg.contains("does not exist") && !error_msg.contains("not found") {
        return err;
    }

    let hint = match extract_duckdb_error_object(&error_msg) {
        Some((true, ref col)) => enrich_duckdb_column_error(conn, query, col),
        Some((false, ref table)) => enrich_duckdb_table_error(conn, table),
        None => None,
    };

    match hint {
        Some(hint_text) => anyhow::anyhow!("{} | Hint: {}", err, hint_text),
        None => err,
    }
}

// ---------------------------------------------------------------------------
// Public convenience methods for workspace usage
// ---------------------------------------------------------------------------

impl DuckDbBackend {
    /// Execute arbitrary SQL (DDL/DML). Returns rows affected (if applicable).
    pub async fn execute_sql(&self, sql: &str) -> Result<usize> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || -> Result<usize> {
            let conn = conn.lock().map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            conn.execute(&sql, []).context("DuckDB execute failed")
        })
        .await
        .context("DuckDB task panicked")?
    }

    /// Execute a query and return rows as Vec<HashMap<String, Value>>.
    pub async fn query_rows(&self, sql: &str) -> Result<Vec<HashMap<String, Value>>> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<HashMap<String, Value>>> {
            let conn = conn.lock().map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            execute_metadata_query(&conn, &sql)
        })
        .await
        .context("DuckDB task panicked")?
    }

    /// Execute a count query, returning a single i64.
    pub async fn query_count(&self, sql: &str) -> Result<i64> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || -> Result<i64> {
            let conn = conn.lock().map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            execute_count_query(&conn, &sql)
        })
        .await
        .context("DuckDB task panicked")?
    }
}

// ---------------------------------------------------------------------------
// DatabaseBackend implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl DatabaseBackend for DuckDbBackend {
    async fn execute_query(&self, params: &QueryParams) -> Result<QueryResult> {
        let start = Instant::now();
        let pii_processor = build_pii_processor(params);
        let blob_format = params.blob_format.clone();
        let include_metadata = params.include_metadata;
        let conn = Arc::clone(&self.conn);

        // For paginated queries, use the pagination helpers
        if params.pagination {
            let count_sql = get_count_query(&params.query, &params.count_mode)?;
            let paginated_sql = create_paginated_query(
                &params.query,
                0,
                params.batch_size,
                &params.count_mode,
                params.order.as_deref(),
                params.allow_unstable_pagination,
                Dialect::DuckDb,
            )?;

            let (mut data, columns_meta, total_rows) = tokio::task::spawn_blocking(
                move || -> Result<(Vec<HashMap<String, Value>>, Vec<ColumnMeta>, i64)> {
                    let conn = conn
                        .lock()
                        .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

                    let total_rows: i64 = if count_sql.is_empty() {
                        -1
                    } else {
                        execute_count_query(&conn, &count_sql)?
                    };

                    let (data, columns_meta) = execute_duckdb_query(
                        &conn,
                        &paginated_sql,
                        &blob_format,
                        Some(&pii_processor),
                    )
                    .map_err(|e| try_enrich_duckdb_error(e, &conn, &paginated_sql))?;

                    Ok((data, columns_meta, total_rows))
                },
            )
            .await
            .context("DuckDB task panicked")??;

            for row in &mut data {
                row.remove(ROW_NUMBER_ALIAS);
            }

            let actual_total = if total_rows < 0 {
                data.len() as i64
            } else {
                total_rows
            };

            let elapsed = start.elapsed().as_millis();

            return Ok(QueryResult {
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
                metadata: if include_metadata && !columns_meta.is_empty() {
                    Some(QueryMetadata {
                        columns: columns_meta,
                    })
                } else {
                    None
                },
            });
        }

        // Standard (non-paginated) path
        let query = params.query.clone();

        let (data, columns_meta) = tokio::task::spawn_blocking(move || -> Result<_> {
            let conn = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            execute_duckdb_query(&conn, &query, &blob_format, Some(&pii_processor))
                .map_err(|e| try_enrich_duckdb_error(e, &conn, &query))
        })
        .await
        .context("DuckDB task panicked")??;

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
            metadata: if include_metadata && !columns_meta.is_empty() {
                Some(QueryMetadata {
                    columns: columns_meta,
                })
            } else {
                None
            },
        })
    }

    async fn validate_query(&self, _database: &str, query: &str) -> Result<(), String> {
        let query = query.to_string();
        let conn = Arc::clone(&self.conn);

        tokio::task::spawn_blocking(move || -> Result<(), String> {
            let conn = conn
                .lock()
                .map_err(|e| format!("Lock poisoned: {}", e))?;
            let explain_sql = format!("EXPLAIN {}", query);
            conn.execute_batch(&explain_sql)
                .map_err(|e| format!("{}", e))?;
            Ok(())
        })
        .await
        .map_err(|e| format!("DuckDB task panicked: {}", e))?
    }

    async fn list_databases(&self) -> Result<Vec<HashMap<String, Value>>> {
        let display_name = if self.path == ":memory:" {
            ":memory:".to_string()
        } else {
            self.path.clone()
        };

        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::String(display_name));
        Ok(vec![map])
    }

    async fn list_schemas(&self, _database: &str) -> Result<Vec<HashMap<String, Value>>> {
        let conn = Arc::clone(&self.conn);

        tokio::task::spawn_blocking(move || -> Result<Vec<HashMap<String, Value>>> {
            let conn = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            execute_metadata_query(
                &conn,
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
            )
        })
        .await
        .context("DuckDB task panicked")?
    }

    async fn list_tables(
        &self,
        _database: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let schema = schema.to_string();
        let conn = Arc::clone(&self.conn);

        tokio::task::spawn_blocking(move || -> Result<Vec<HashMap<String, Value>>> {
            let conn = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            // Use string interpolation since Arrow API doesn't support parameter binding easily
            let safe_schema = schema.replace('\'', "''");
            let sql = format!(
                "SELECT table_schema AS \"TABLE_SCHEMA\", \
                        table_name AS \"TABLE_NAME\", \
                        table_type AS \"TABLE_TYPE\" \
                 FROM information_schema.tables \
                 WHERE table_schema = '{}' \
                 ORDER BY table_name",
                safe_schema
            );

            execute_metadata_query(&conn, &sql)
        })
        .await
        .context("DuckDB task panicked")?
    }

    async fn describe_table(
        &self,
        _database: &str,
        table: &str,
        schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let table = table.to_string();
        let schema = schema.to_string();
        let conn = Arc::clone(&self.conn);

        tokio::task::spawn_blocking(move || -> Result<Vec<HashMap<String, Value>>> {
            let conn = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            let safe_schema = schema.replace('\'', "''");
            let safe_table = table.replace('\'', "''");
            let sql = format!(
                "SELECT column_name AS \"COLUMN_NAME\", \
                        data_type AS \"DATA_TYPE\", \
                        is_nullable AS \"IS_NULLABLE\", \
                        character_maximum_length AS \"CHARACTER_MAXIMUM_LENGTH\", \
                        numeric_precision AS \"NUMERIC_PRECISION\", \
                        numeric_scale AS \"NUMERIC_SCALE\" \
                 FROM information_schema.columns \
                 WHERE table_schema = '{}' AND table_name = '{}' \
                 ORDER BY ordinal_position",
                safe_schema, safe_table
            );

            execute_metadata_query(&conn, &sql)
        })
        .await
        .context("DuckDB task panicked")?
    }

    fn dialect(&self) -> Dialect {
        Dialect::DuckDb
    }

    fn default_database(&self) -> &str {
        &self.path
    }

    async fn health_check(&self) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            conn.execute_batch("SELECT 1")
                .context("DuckDB health check failed")?;
            Ok(())
        })
        .await
        .context("DuckDB task panicked")?
    }

    fn pool_stats(&self) -> Option<super::PoolStats> {
        None
    }
}
