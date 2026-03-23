use crate::db::Dialect;

pub fn is_exec_query(query: &str) -> bool {
    let trimmed = query.trim_start();
    let upper = trimmed.to_uppercase();
    upper.starts_with("EXEC ")
        || upper.starts_with("EXECUTE ")
        || upper == "EXEC"
        || upper == "EXECUTE"
}

pub fn is_select_like(query: &str) -> bool {
    matches!(
        leading_keyword(query).as_deref(),
        Some("SELECT") | Some("WITH")
    )
}

/// Strict read-only check — blocks CTE-prefixed DML, SELECT INTO, and multi-statement batches.
pub fn is_read_only_safe(query: &str) -> bool {
    let trimmed = query.trim();
    let upper = trimmed.to_uppercase();

    // Block multi-statement batches: strip first statement keyword check is not enough
    // Look for semicolons outside of string literals at depth 0 followed by non-whitespace
    if contains_multiple_statements(trimmed) {
        return false;
    }

    // Block SELECT ... INTO (creates a table in MSSQL)
    if upper.starts_with("SELECT") && contains_into_clause(&upper) {
        return false;
    }

    // Block WITH ... INSERT/UPDATE/DELETE (CTE-prefixed DML)
    if upper.starts_with("WITH") && cte_contains_write(&upper) {
        return false;
    }

    upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("EXEC SP_HELP")
        || upper.starts_with("EXEC SP_COLUMNS")
        || upper.starts_with("EXEC SP_TABLES")
        || upper.starts_with("EXEC SP_DATABASES")
        || upper.starts_with("EXEC SP_SPACEUSED")
}

/// Check if a SELECT query contains an INTO clause at top-level (SELECT ... INTO new_table).
fn contains_into_clause(upper_query: &str) -> bool {
    let mut depth = 0i32;
    let bytes = upper_query.as_bytes();
    let into = b"INTO";
    for i in 0..bytes.len().saturating_sub(3) {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'I' if depth == 0
                && &bytes[i..i + 4] == into
                && (i == 0 || bytes[i - 1].is_ascii_whitespace())
                && (i + 4 >= bytes.len() || bytes[i + 4].is_ascii_whitespace()) =>
            {
                return true;
            }
            _ => {}
        }
    }
    false
}

/// Check if a WITH (CTE) query body contains a write statement.
fn cte_contains_write(upper_query: &str) -> bool {
    let bytes = upper_query.as_bytes();
    let mut depth = 0i32;
    let mut i = 4; // skip "WITH"
    let mut last_close = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    last_close = i;
                }
            }
            _ => {}
        }
        i += 1;
    }
    if last_close > 0 && last_close + 1 < bytes.len() {
        let tail = upper_query[last_close + 1..].trim_start();
        let write_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "MERGE"];
        return write_keywords.iter().any(|kw| tail.starts_with(kw));
    }
    false
}

/// Check if query contains multiple statements separated by semicolons (outside string literals).
fn contains_multiple_statements(query: &str) -> bool {
    let bytes = query.as_bytes();
    let mut in_single_quote = false;
    let mut found_semi = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                // Handle escaped quotes ('')
                if in_single_quote && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single_quote = !in_single_quote;
            }
            b';' if !in_single_quote => {
                found_semi = true;
            }
            c if found_semi && !in_single_quote && !c.is_ascii_whitespace() => {
                return true;
            }
            _ => {}
        }
        i += 1;
    }
    false
}

pub fn is_ddl_query(query: &str) -> bool {
    matches!(
        leading_keyword(query).as_deref(),
        Some("CREATE") | Some("ALTER") | Some("DROP") | Some("TRUNCATE")
    )
}

pub fn leading_keyword(mut sql: &str) -> Option<String> {
    loop {
        sql = sql.trim_start_matches(|c: char| c.is_whitespace() || c == '(' || c == ';');
        if sql.is_empty() {
            return None;
        }

        // Skip block comments
        if sql.starts_with("/*") {
            if let Some(end) = sql.find("*/") {
                sql = &sql[end + 2..];
                continue;
            }
            return None;
        }

        // Skip single-line comments
        if sql.starts_with("--") {
            if let Some(end) = sql.find('\n') {
                sql = &sql[end + 1..];
                continue;
            }
            return None;
        }

        return sql
            .split_whitespace()
            .next()
            .map(|kw| kw.trim_start_matches('(').to_ascii_uppercase());
    }
}

/// Apply a row limit using the specified dialect.
pub fn apply_row_limit_dialect(query: &str, limit: usize, dialect: Dialect) -> String {
    if !is_select_like(query) {
        return query.to_string();
    }
    match dialect {
        Dialect::Mssql => format!(
            "SELECT TOP {} * FROM (\n{}\n) AS ai_subquery",
            limit, query
        ),
        Dialect::Postgres | Dialect::DuckDb | Dialect::ClickHouse => format!(
            "SELECT * FROM (\n{}\n) AS ai_subquery LIMIT {}",
            query, limit
        ),
    }
}

/// Wrap query in EXEC sp_executesql for DDL execution (MSSQL only)
pub fn wrap_exec_sql(query: &str) -> String {
    let escaped = query.replace('\'', "''");
    format!("EXEC sp_executesql N'{}'", escaped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mssql_row_limit() {
        let sql = apply_row_limit_dialect("SELECT * FROM users", 100, Dialect::Mssql);
        assert!(sql.contains("SELECT TOP 100"));
    }

    #[test]
    fn postgres_row_limit() {
        let sql = apply_row_limit_dialect("SELECT * FROM users", 100, Dialect::Postgres);
        assert!(sql.contains("LIMIT 100"));
        assert!(!sql.contains("TOP"));
    }

    #[test]
    fn non_select_unchanged() {
        let sql = apply_row_limit_dialect("INSERT INTO users VALUES (1)", 100, Dialect::Mssql);
        assert_eq!(sql, "INSERT INTO users VALUES (1)");
    }

    #[test]
    fn read_only_blocks_cte_write() {
        assert!(!is_read_only_safe("WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte"));
        assert!(is_read_only_safe("WITH cte AS (SELECT 1) SELECT * FROM cte"));
    }

    #[test]
    fn read_only_blocks_select_into() {
        assert!(!is_read_only_safe("SELECT * INTO new_table FROM users"));
        assert!(is_read_only_safe("SELECT * FROM users"));
    }

    #[test]
    fn read_only_blocks_multi_statement() {
        assert!(!is_read_only_safe("SELECT 1; DELETE FROM users"));
        assert!(is_read_only_safe("SELECT 1"));
        // Trailing semicolon with no following statement is fine
        assert!(is_read_only_safe("SELECT 1;"));
    }

    #[test]
    fn read_only_blocks_writes() {
        assert!(!is_read_only_safe("INSERT INTO t VALUES (1)"));
        assert!(!is_read_only_safe("UPDATE t SET x = 1"));
        assert!(!is_read_only_safe("DELETE FROM t"));
        assert!(!is_read_only_safe("DROP TABLE t"));
        assert!(!is_read_only_safe("EXEC my_proc"));
    }

    #[test]
    fn read_only_allows_safe_exec() {
        assert!(is_read_only_safe("EXEC SP_HELP"));
        assert!(is_read_only_safe("EXEC SP_COLUMNS"));
        assert!(is_read_only_safe("EXEC SP_TABLES"));
    }

    #[test]
    fn multi_statement_ignores_semicolons_in_strings() {
        assert!(is_read_only_safe("SELECT * FROM t WHERE x = 'a;b'"));
    }
}
