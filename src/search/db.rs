use rusqlite::{params, Connection};
use serde::Serialize;
use std::sync::Mutex;

// ============================================================================
// Result types
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct SchemaSearchResult {
    pub connection: String,
    pub database: String,
    pub schema: String,
    pub object_name: String,
    pub object_type: String,
    pub columns: String,
    pub snippet: String,
    pub rank: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuerySearchResult {
    pub email: String,
    pub connection: String,
    pub database: String,
    pub sql_text: String,
    pub snippet: String,
    pub rank: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EndpointSearchResult {
    pub name: String,
    pub connection: String,
    pub database: String,
    pub description: String,
    pub query: String,
    pub snippet: String,
    pub rank: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct UnifiedSearchResult {
    pub schema: Vec<SchemaSearchResult>,
    pub queries: Vec<QuerySearchResult>,
    pub endpoints: Vec<EndpointSearchResult>,
}

// ============================================================================
// SearchDb
// ============================================================================

pub struct SearchDb {
    conn: Mutex<Connection>,
}

impl SearchDb {
    pub fn new(path: &str, key: &str) -> Result<Self, String> {
        let conn =
            Connection::open(path).map_err(|e| format!("Failed to open search DB: {}", e))?;

        conn.pragma_update(None, "key", key)
            .map_err(|e| format!("Failed to set SQLCipher key: {}", e))?;

        conn.execute_batch("SELECT count(*) FROM sqlite_master;")
            .map_err(|e| format!("SQLCipher key verification failed (wrong key?): {}", e))?;

        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| format!("Failed to set WAL mode: {}", e))?;

        // Create FTS5 virtual tables
        conn.execute_batch(
            "
            CREATE VIRTUAL TABLE IF NOT EXISTS schema_fts USING fts5(
                connection,
                database,
                schema,
                object_name,
                object_type,
                columns,
                tokenize='porter unicode61'
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS query_fts USING fts5(
                email,
                connection,
                database,
                sql_text,
                tokenize='porter unicode61'
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS endpoint_fts USING fts5(
                name,
                connection,
                database,
                description,
                query,
                tokenize='porter unicode61'
            );
            ",
        )
        .map_err(|e| format!("Failed to create FTS5 tables: {}", e))?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    // ========================================================================
    // Schema indexing
    // ========================================================================

    pub fn index_schema(
        &self,
        connection: &str,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
        columns: &[String],
    ) {
        let conn = self.conn.lock().unwrap();
        let cols_text = columns.join(" ");
        let _ = conn.execute(
            "INSERT INTO schema_fts (connection, database, schema, object_name, object_type, columns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![connection, database, schema, object_name, object_type, cols_text],
        );
    }

    pub fn clear_connection_schema(&self, connection: &str) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "DELETE FROM schema_fts WHERE connection = ?1",
            params![connection],
        );
    }

    // ========================================================================
    // Query indexing
    // ========================================================================

    pub fn index_query(&self, email: &str, connection: &str, database: &str, sql: &str) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "INSERT INTO query_fts (email, connection, database, sql_text) VALUES (?1, ?2, ?3, ?4)",
            params![email, connection, database, sql],
        );
    }

    // ========================================================================
    // Endpoint indexing
    // ========================================================================

    pub fn index_endpoint(
        &self,
        name: &str,
        connection: &str,
        database: &str,
        description: &str,
        query: &str,
    ) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "INSERT INTO endpoint_fts (name, connection, database, description, query)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![name, connection, database, description, query],
        );
    }

    pub fn remove_endpoint(&self, name: &str) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "DELETE FROM endpoint_fts WHERE name = ?1",
            params![name],
        );
    }

    pub fn clear_endpoints(&self) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute("DELETE FROM endpoint_fts", []);
    }

    pub fn clear_queries(&self) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute("DELETE FROM query_fts", []);
    }

    // ========================================================================
    // Search
    // ========================================================================

    /// Escape FTS5 special characters in user queries.
    fn escape_fts_query(query: &str) -> String {
        // Wrap each token in double quotes to treat as literal
        query
            .split_whitespace()
            .map(|token| {
                // Strip any existing quotes and special chars, then wrap
                let clean: String = token.chars().filter(|c| c.is_alphanumeric() || *c == '_').collect();
                if clean.is_empty() {
                    String::new()
                } else {
                    format!("\"{}\"", clean)
                }
            })
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub fn search_schema(&self, query: &str, limit: usize) -> Vec<SchemaSearchResult> {
        let conn = self.conn.lock().unwrap();
        let fts_query = Self::escape_fts_query(query);
        if fts_query.is_empty() {
            return Vec::new();
        }

        let mut stmt = match conn.prepare(
            "SELECT connection, database, schema, object_name, object_type, columns,
                    snippet(schema_fts, 3, '<b>', '</b>', '...', 32) as snip,
                    rank
             FROM schema_fts
             WHERE schema_fts MATCH ?1
             ORDER BY rank
             LIMIT ?2",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let rows = match stmt.query_map(params![fts_query, limit as i64], |row| {
            Ok(SchemaSearchResult {
                connection: row.get(0)?,
                database: row.get(1)?,
                schema: row.get(2)?,
                object_name: row.get(3)?,
                object_type: row.get(4)?,
                columns: row.get(5)?,
                snippet: row.get(6)?,
                rank: row.get(7)?,
            })
        }) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        rows.filter_map(|r| r.ok()).collect()
    }

    pub fn search_queries(
        &self,
        query: &str,
        email: Option<&str>,
        limit: usize,
    ) -> Vec<QuerySearchResult> {
        let conn = self.conn.lock().unwrap();
        let fts_query = Self::escape_fts_query(query);
        if fts_query.is_empty() {
            return Vec::new();
        }

        // If email filter, combine with FTS match
        let full_query = if let Some(email) = email {
            format!("email:\"{}\" {}", email.replace('"', ""), fts_query)
        } else {
            fts_query
        };

        let mut stmt = match conn.prepare(
            "SELECT email, connection, database, sql_text,
                    snippet(query_fts, 3, '<b>', '</b>', '...', 64) as snip,
                    rank
             FROM query_fts
             WHERE query_fts MATCH ?1
             ORDER BY rank
             LIMIT ?2",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let rows = match stmt.query_map(params![full_query, limit as i64], |row| {
            Ok(QuerySearchResult {
                email: row.get(0)?,
                connection: row.get(1)?,
                database: row.get(2)?,
                sql_text: row.get(3)?,
                snippet: row.get(4)?,
                rank: row.get(5)?,
            })
        }) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        rows.filter_map(|r| r.ok()).collect()
    }

    pub fn search_endpoints(&self, query: &str, limit: usize) -> Vec<EndpointSearchResult> {
        let conn = self.conn.lock().unwrap();
        let fts_query = Self::escape_fts_query(query);
        if fts_query.is_empty() {
            return Vec::new();
        }

        let mut stmt = match conn.prepare(
            "SELECT name, connection, database, description, query,
                    snippet(endpoint_fts, 3, '<b>', '</b>', '...', 32) as snip,
                    rank
             FROM endpoint_fts
             WHERE endpoint_fts MATCH ?1
             ORDER BY rank
             LIMIT ?2",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let rows = match stmt.query_map(params![fts_query, limit as i64], |row| {
            Ok(EndpointSearchResult {
                name: row.get(0)?,
                connection: row.get(1)?,
                database: row.get(2)?,
                description: row.get(3)?,
                query: row.get(4)?,
                snippet: row.get(5)?,
                rank: row.get(6)?,
            })
        }) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        rows.filter_map(|r| r.ok()).collect()
    }

    pub fn search_all(&self, query: &str, limit: usize) -> UnifiedSearchResult {
        UnifiedSearchResult {
            schema: self.search_schema(query, limit),
            queries: self.search_queries(query, None, limit),
            endpoints: self.search_endpoints(query, limit),
        }
    }

    // ========================================================================
    // Stats
    // ========================================================================

    pub fn stats(&self) -> Result<(i64, i64, i64), String> {
        let conn = self.conn.lock().map_err(|e| format!("Lock error: {}", e))?;

        let schema_count: i64 = conn
            .query_row("SELECT count(*) FROM schema_fts", [], |row| row.get(0))
            .unwrap_or(0);
        let query_count: i64 = conn
            .query_row("SELECT count(*) FROM query_fts", [], |row| row.get(0))
            .unwrap_or(0);
        let endpoint_count: i64 = conn
            .query_row("SELECT count(*) FROM endpoint_fts", [], |row| row.get(0))
            .unwrap_or(0);

        Ok((schema_count, query_count, endpoint_count))
    }
}
