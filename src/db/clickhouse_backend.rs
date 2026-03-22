//! ClickHouse database backend using the HTTP interface (port 8123) via reqwest.
//!
//! The official `clickhouse` crate requires compile-time row schemas via derive macros.
//! Lane works with arbitrary tables/columns at runtime, so we hit the HTTP API directly
//! using `FORMAT JSONEachRow` — one JSON object per line, which we parse into HashMaps.
//! `reqwest` is already a Lane dependency (used for webhooks).

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, info};

use crate::config::ClickHouseConnectionConfig;
use crate::query::{
    ColumnMeta, CountMode, QueryMetadata, QueryParams, QueryResult,
    ROW_NUMBER_ALIAS, build_pii_processor,
    pagination::{create_paginated_query, get_count_query},
    validation::is_exec_query,
};

use super::{DatabaseBackend, Dialect, StreamChunk};

// ---------------------------------------------------------------------------
// Read-only validation
// ---------------------------------------------------------------------------

/// ClickHouse has no `BEGIN TRANSACTION READ ONLY`. We validate query text instead.
fn is_read_only_query(query: &str) -> bool {
    let mut s = query.trim();
    // Skip leading comments
    loop {
        s = s.trim_start();
        if s.starts_with("--") {
            s = s.find('\n').map(|i| &s[i + 1..]).unwrap_or("");
        } else if s.starts_with("/*") {
            s = s.find("*/").map(|i| &s[i + 2..]).unwrap_or("");
        } else {
            break;
        }
    }
    let first_word = s
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_start_matches('(')
        .to_ascii_uppercase();
    matches!(
        first_word.as_str(),
        "SELECT" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "EXISTS" | "WITH" | "USE"
    )
}

// ---------------------------------------------------------------------------
// Backend struct
// ---------------------------------------------------------------------------

/// ClickHouse backend using the HTTP interface.
///
/// Sends queries to ClickHouse's HTTP endpoint with `FORMAT JSONEachRow`
/// and parses the newline-delimited JSON response into `HashMap<String, Value>`.
pub struct ClickHouseBackend {
    http: reqwest::Client,
    base_url: String,
    default_db: String,
    user: String,
    password: String,
}

impl ClickHouseBackend {
    pub async fn new(config: ClickHouseConnectionConfig) -> Result<Self> {
        let scheme = if config.secure.unwrap_or(false) {
            "https"
        } else {
            "http"
        };
        let base_url = format!("{}://{}:{}", scheme, config.host, config.port);

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .context("Failed to build HTTP client for ClickHouse")?;

        let backend = Self {
            http,
            base_url,
            default_db: config.database.clone(),
            user: config.user.clone(),
            password: config.password.clone(),
        };

        // Verify connectivity
        backend
            .health_check()
            .await
            .context("Failed to connect to ClickHouse. Check host, port, and credentials.")?;

        info!(
            "ClickHouse connection initialized ({}:{}), database '{}'",
            config.host, config.port, config.database
        );

        Ok(backend)
    }

    /// Execute a query and return rows as JSON maps.
    async fn query_rows(
        &self,
        database: &str,
        sql: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let trimmed = sql.trim().trim_end_matches(';');
        let body = format!("{} FORMAT JSONEachRow", trimmed);

        let resp = self
            .http
            .post(&self.base_url)
            .query(&[("database", database)])
            .basic_auth(&self.user, Some(&self.password))
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body)
            .send()
            .await
            .context("ClickHouse HTTP request failed")?;

        let status = resp.status();
        let text = resp.text().await.context("Failed to read ClickHouse response")?;

        if !status.is_success() {
            return Err(anyhow::anyhow!("{}", text.trim()));
        }

        // Parse newline-delimited JSON
        let mut rows = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let row: HashMap<String, Value> =
                serde_json::from_str(line).context("Failed to parse ClickHouse JSON row")?;
            rows.push(row);
        }

        Ok(rows)
    }

    /// Execute a query that returns no rows (DDL, mutations, etc.)
    async fn execute_ddl(&self, database: &str, sql: &str) -> Result<()> {
        let trimmed = sql.trim().trim_end_matches(';');

        let resp = self
            .http
            .post(&self.base_url)
            .query(&[("database", database)])
            .basic_auth(&self.user, Some(&self.password))
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(trimmed.to_string())
            .send()
            .await
            .context("ClickHouse HTTP request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("{}", text.trim()));
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DatabaseBackend implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl DatabaseBackend for ClickHouseBackend {
    async fn execute_query(&self, params: &QueryParams) -> Result<QueryResult> {
        let start = Instant::now();

        // Read-only enforcement
        if params.read_only && !is_read_only_query(&params.query) {
            anyhow::bail!(
                "Read-only mode: only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are allowed"
            );
        }

        let pii_processor = build_pii_processor(params);

        // For EXEC queries, just run directly
        if is_exec_query(&params.query) {
            let data = self.query_rows(&params.database, &params.query).await?;
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
                metadata: None,
            });
        }

        // Paginated path
        if params.pagination {
            let count_query = get_count_query(&params.query, &params.count_mode)?;
            let total_rows = if !count_query.is_empty() {
                let count_rows = self.query_rows(&params.database, &count_query).await?;
                count_rows
                    .first()
                    .and_then(|r| r.get("total"))
                    .and_then(|v| {
                        v.as_i64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    })
                    .unwrap_or(0)
            } else {
                0
            };

            let paginated_sql = create_paginated_query(
                &params.query,
                0,
                params.batch_size,
                &params.count_mode,
                params.order.as_deref(),
                params.allow_unstable_pagination,
                Dialect::ClickHouse,
            )?;

            let mut data = self.query_rows(&params.database, &paginated_sql).await?;

            for row in &mut data {
                crate::pii::process_json_row(&pii_processor, row);
            }

            // Strip ROW_NUMBER alias if present (window mode)
            if matches!(params.count_mode, CountMode::Window) {
                for row in &mut data {
                    row.remove(ROW_NUMBER_ALIAS);
                }
            }

            let elapsed = start.elapsed().as_millis();
            let effective_total = if total_rows > 0 {
                total_rows
            } else {
                data.len() as i64
            };

            return Ok(QueryResult {
                success: true,
                total_rows: effective_total,
                execution_time_ms: elapsed,
                rows_per_second: if elapsed > 0 {
                    effective_total as f64 / (elapsed as f64 / 1000.0)
                } else {
                    effective_total as f64
                },
                data,
                result_sets: None,
                result_set_count: None,
                metadata: None,
            });
        }

        // Standard buffered path
        let mut data = self.query_rows(&params.database, &params.query).await?;

        // Build metadata from first row
        let metadata = if params.include_metadata && !data.is_empty() {
            let columns: Vec<ColumnMeta> = data[0]
                .keys()
                .map(|k| ColumnMeta {
                    name: k.clone(),
                    data_type: "String".to_string(),
                })
                .collect();
            Some(QueryMetadata { columns })
        } else {
            None
        };

        for row in &mut data {
            crate::pii::process_json_row(&pii_processor, row);
        }

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
        // ClickHouse: use EXPLAIN SYNTAX to validate without executing
        let explain_sql = format!("EXPLAIN SYNTAX {}", query.trim().trim_end_matches(';'));
        self.execute_ddl(database, &explain_sql)
            .await
            .map_err(|e| format!("{}", e))
    }

    async fn list_databases(&self) -> Result<Vec<HashMap<String, Value>>> {
        self.query_rows(
            &self.default_db,
            "SELECT name FROM system.databases ORDER BY name",
        )
        .await
        .context("Failed to list databases")
    }

    async fn list_schemas(&self, _database: &str) -> Result<Vec<HashMap<String, Value>>> {
        // ClickHouse has no schema layer — return a single "default" entry
        let mut map = HashMap::new();
        map.insert(
            "schema_name".to_string(),
            Value::String("default".to_string()),
        );
        Ok(vec![map])
    }

    async fn list_tables(
        &self,
        database: &str,
        _schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let sql = format!(
            "SELECT \
                 'default' AS \"TABLE_SCHEMA\", \
                 name AS \"TABLE_NAME\", \
                 engine AS \"TABLE_TYPE\", \
                 total_rows AS \"ROW_COUNT\", \
                 total_bytes \
             FROM system.tables \
             WHERE database = '{}' \
               AND name NOT LIKE '.%' \
             ORDER BY name",
            database.replace('\'', "\\'")
        );
        self.query_rows(&self.default_db, &sql)
            .await
            .context("Failed to list tables")
    }

    async fn describe_table(
        &self,
        database: &str,
        table: &str,
        _schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let sql = format!(
            "SELECT \
                 name AS \"COLUMN_NAME\", \
                 type AS \"DATA_TYPE\", \
                 if(position(type, 'Nullable') > 0, 'YES', 'NO') AS \"IS_NULLABLE\", \
                 'NO' AS \"IS_PRIMARY_KEY\", \
                 NULL AS \"CHARACTER_MAXIMUM_LENGTH\", \
                 NULL AS \"NUMERIC_PRECISION\", \
                 NULL AS \"NUMERIC_SCALE\" \
             FROM system.columns \
             WHERE database = '{}' AND table = '{}' \
             ORDER BY position",
            database.replace('\'', "\\'"),
            table.replace('\'', "\\'")
        );
        self.query_rows(&self.default_db, &sql)
            .await
            .context("Failed to describe table")
    }

    async fn execute_query_streaming(
        &self,
        params: &QueryParams,
        tx: tokio::sync::mpsc::Sender<StreamChunk>,
    ) -> Result<()> {
        if params.read_only && !is_read_only_query(&params.query) {
            anyhow::bail!(
                "Read-only mode: only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are allowed"
            );
        }

        let pii_processor = build_pii_processor(params);
        let trimmed = params.query.trim().trim_end_matches(';');
        let body = format!("{} FORMAT JSONEachRow", trimmed);

        let resp = self
            .http
            .post(&self.base_url)
            .query(&[("database", params.database.as_str())])
            .basic_auth(&self.user, Some(&self.password))
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body)
            .send()
            .await
            .context("ClickHouse streaming request failed")?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("{}", text.trim());
        }

        // Stream the response line by line
        use futures::StreamExt;
        let mut stream = resp.bytes_stream();
        let mut buffer = String::new();
        let mut count: usize = 0;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("Error reading ClickHouse stream")?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            // Process complete lines
            while let Some(newline_pos) = buffer.find('\n') {
                let line = buffer[..newline_pos].trim().to_string();
                buffer = buffer[newline_pos + 1..].to_string();

                if line.is_empty() {
                    continue;
                }

                match serde_json::from_str::<HashMap<String, Value>>(&line) {
                    Ok(mut row) => {
                        crate::pii::process_json_row(&pii_processor, &mut row);
                        let json_line = serde_json::to_string(&row).unwrap_or_default();
                        let out = format!("{}\n", json_line);
                        if tx.send(Ok(bytes::Bytes::from(out))).await.is_err() {
                            debug!("Stream receiver dropped after {} rows", count);
                            return Ok(());
                        }
                        count += 1;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("JSON parse error: {}", e),
                            )))
                            .await;
                        return Ok(());
                    }
                }
            }
        }

        // Process any remaining data in buffer
        let line = buffer.trim();
        if !line.is_empty() {
            if let Ok(mut row) = serde_json::from_str::<HashMap<String, Value>>(line) {
                crate::pii::process_json_row(&pii_processor, &mut row);
                let json_line = serde_json::to_string(&row).unwrap_or_default();
                let out = format!("{}\n", json_line);
                let _ = tx.send(Ok(bytes::Bytes::from(out))).await;
                count += 1;
            }
        }

        debug!("ClickHouse stream completed: {} rows", count);
        Ok(())
    }

    fn dialect(&self) -> Dialect {
        Dialect::ClickHouse
    }

    fn default_database(&self) -> &str {
        &self.default_db
    }

    async fn health_check(&self) -> Result<()> {
        self.execute_ddl(&self.default_db, "SELECT 1").await
    }

    async fn list_active_queries(&self) -> Result<Vec<HashMap<String, Value>>> {
        self.query_rows(
            &self.default_db,
            "SELECT \
                 query_id, \
                 user, \
                 query, \
                 elapsed, \
                 read_rows, \
                 memory_usage \
             FROM system.processes \
             ORDER BY elapsed DESC",
        )
        .await
    }

    async fn kill_query(&self, process_id: i64) -> Result<()> {
        let sql = format!("KILL QUERY WHERE query_id = '{}'", process_id);
        self.execute_ddl(&self.default_db, &sql).await
    }

    async fn list_views(
        &self,
        database: &str,
        _schema: &str,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let sql = format!(
            "SELECT name, engine AS type, 'default' AS schema_name \
             FROM system.tables \
             WHERE database = '{}' AND engine LIKE '%View%' \
             ORDER BY name",
            database.replace('\'', "\\'")
        );
        self.query_rows(&self.default_db, &sql).await
    }
}
