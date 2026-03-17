use crate::db::Dialect;
use crate::import::sql_gen::escape_identifier;
use serde_json::{Map, Value};

use super::types::{FilterOp, RestFilter, RestQuery, SortDir};

/// Escape a string value for SQL (single quotes doubled).
fn escape_value(val: &str) -> String {
    val.replace('\'', "''")
}

/// Check if a string looks numeric (integer or decimal).
fn is_numeric(val: &str) -> bool {
    // Allow leading minus, digits, optional dot + digits
    val.parse::<f64>().is_ok() && !val.is_empty()
}

/// Format a SQL literal value — numeric values unquoted, strings quoted.
fn sql_literal(val: &str) -> String {
    if is_numeric(val) {
        val.to_string()
    } else {
        format!("'{}'", escape_value(val))
    }
}

/// Build a SELECT query from a RestQuery.
pub fn build_select(
    table: &str,
    schema: &str,
    query: &RestQuery,
    dialect: Dialect,
) -> String {
    let schema_id = escape_identifier(schema, dialect);
    let table_id = escape_identifier(table, dialect);

    // SELECT columns
    let select_clause = match &query.select {
        Some(cols) => cols
            .iter()
            .map(|c| escape_identifier(c, dialect))
            .collect::<Vec<_>>()
            .join(", "),
        None => "*".to_string(),
    };

    let mut sql = format!(
        "SELECT {} FROM {}.{}",
        select_clause, schema_id, table_id
    );

    // WHERE
    let where_clause = build_where_clause(&query.filters, dialect);
    if !where_clause.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clause);
    }

    // ORDER BY
    let order_clause = build_order_clause(&query.order, dialect);
    if !order_clause.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_clause);
    }

    // PAGINATION
    match dialect {
        Dialect::Mssql => {
            if query.limit.is_some() || query.offset.is_some() {
                // MSSQL requires ORDER BY for OFFSET/FETCH
                if order_clause.is_empty() {
                    sql.push_str(" ORDER BY (SELECT NULL)");
                }
                let offset = query.offset.unwrap_or(0);
                let limit = query.limit.unwrap_or(1000);
                sql.push_str(&format!(
                    " OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
                    offset, limit
                ));
            }
        }
        Dialect::Postgres | Dialect::DuckDb => {
            if let Some(limit) = query.limit {
                sql.push_str(&format!(" LIMIT {}", limit));
            }
            if let Some(offset) = query.offset {
                sql.push_str(&format!(" OFFSET {}", offset));
            }
        }
    }

    sql
}

/// Build a SELECT query for a single row by primary key.
pub fn build_select_by_pk(
    table: &str,
    schema: &str,
    pk_col: &str,
    pk_val: &str,
    dialect: Dialect,
) -> String {
    let schema_id = escape_identifier(schema, dialect);
    let table_id = escape_identifier(table, dialect);
    let pk_id = escape_identifier(pk_col, dialect);

    format!(
        "SELECT * FROM {}.{} WHERE {} = {}",
        schema_id,
        table_id,
        pk_id,
        sql_literal(pk_val)
    )
}

/// Build an INSERT statement. Returns the SQL and, for Postgres, uses RETURNING *.
pub fn build_insert(
    table: &str,
    schema: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    dialect: Dialect,
) -> String {
    let schema_id = escape_identifier(schema, dialect);
    let table_id = escape_identifier(table, dialect);

    let col_list = columns
        .iter()
        .map(|c| escape_identifier(c, dialect))
        .collect::<Vec<_>>()
        .join(", ");

    let value_rows: Vec<String> = rows
        .iter()
        .map(|row| {
            let vals: Vec<String> = row
                .iter()
                .map(|v| value_to_sql_literal(v, dialect))
                .collect();
            format!("({})", vals.join(", "))
        })
        .collect();

    let mut sql = format!(
        "INSERT INTO {}.{} ({}) VALUES {}",
        schema_id,
        table_id,
        col_list,
        value_rows.join(", ")
    );

    // Postgres/DuckDb: RETURNING *
    match dialect {
        Dialect::Postgres | Dialect::DuckDb => {
            sql.push_str(" RETURNING *");
        }
        Dialect::Mssql => {
            // MSSQL: use OUTPUT inserted.* (insert before VALUES)
            // Rewrite: INSERT INTO ... OUTPUT inserted.* VALUES ...
            let insert_prefix = format!("INSERT INTO {}.{} ({})", schema_id, table_id, col_list);
            let values_part = format!("VALUES {}", value_rows.join(", "));
            sql = format!("{} OUTPUT inserted.* {}", insert_prefix, values_part);
        }
    }

    sql
}

/// Build an UPDATE statement.
pub fn build_update(
    table: &str,
    schema: &str,
    pk_col: &str,
    pk_val: &str,
    updates: &Map<String, Value>,
    dialect: Dialect,
) -> String {
    let schema_id = escape_identifier(schema, dialect);
    let table_id = escape_identifier(table, dialect);
    let pk_id = escape_identifier(pk_col, dialect);

    let set_clauses: Vec<String> = updates
        .iter()
        .map(|(col, val)| {
            format!(
                "{} = {}",
                escape_identifier(col, dialect),
                value_to_sql_literal(val, dialect)
            )
        })
        .collect();

    match dialect {
        Dialect::Postgres | Dialect::DuckDb => {
            format!(
                "UPDATE {}.{} SET {} WHERE {} = {} RETURNING *",
                schema_id,
                table_id,
                set_clauses.join(", "),
                pk_id,
                sql_literal(pk_val)
            )
        }
        Dialect::Mssql => {
            // MSSQL: UPDATE ... OUTPUT inserted.* WHERE ...
            format!(
                "UPDATE {}.{} SET {} OUTPUT inserted.* WHERE {} = {}",
                schema_id,
                table_id,
                set_clauses.join(", "),
                pk_id,
                sql_literal(pk_val)
            )
        }
    }
}

/// Build a DELETE statement.
pub fn build_delete(
    table: &str,
    schema: &str,
    pk_col: &str,
    pk_val: &str,
    dialect: Dialect,
) -> String {
    let schema_id = escape_identifier(schema, dialect);
    let table_id = escape_identifier(table, dialect);
    let pk_id = escape_identifier(pk_col, dialect);

    format!(
        "DELETE FROM {}.{} WHERE {} = {}",
        schema_id,
        table_id,
        pk_id,
        sql_literal(pk_val)
    )
}

/// Build a COUNT query with the same filters as a SELECT.
pub fn build_count(
    table: &str,
    schema: &str,
    filters: &[RestFilter],
    dialect: Dialect,
) -> String {
    let schema_id = escape_identifier(schema, dialect);
    let table_id = escape_identifier(table, dialect);

    let mut sql = format!("SELECT COUNT(*) AS total FROM {}.{}", schema_id, table_id);

    let where_clause = build_where_clause(filters, dialect);
    if !where_clause.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clause);
    }

    sql
}

// ============================================================================
// Internal helpers
// ============================================================================

/// Build a WHERE clause from a list of filters.
fn build_where_clause(filters: &[RestFilter], dialect: Dialect) -> String {
    if filters.is_empty() {
        return String::new();
    }

    let conditions: Vec<String> = filters
        .iter()
        .map(|f| filter_to_condition(f, dialect))
        .collect();

    conditions.join(" AND ")
}

/// Convert a single RestFilter to a SQL condition.
fn filter_to_condition(filter: &RestFilter, dialect: Dialect) -> String {
    let col = escape_identifier(&filter.column, dialect);

    match &filter.operator {
        FilterOp::Eq => format!("{} = {}", col, sql_literal(&filter.value)),
        FilterOp::Neq => format!("{} != {}", col, sql_literal(&filter.value)),
        FilterOp::Gt => format!("{} > {}", col, sql_literal(&filter.value)),
        FilterOp::Gte => format!("{} >= {}", col, sql_literal(&filter.value)),
        FilterOp::Lt => format!("{} < {}", col, sql_literal(&filter.value)),
        FilterOp::Lte => format!("{} <= {}", col, sql_literal(&filter.value)),
        FilterOp::Like => format!("{} LIKE {}", col, sql_literal(&filter.value)),
        FilterOp::Ilike => {
            match dialect {
                // Postgres has native ILIKE
                Dialect::Postgres | Dialect::DuckDb => {
                    format!("{} ILIKE {}", col, sql_literal(&filter.value))
                }
                // MSSQL is case-insensitive by default collation, so just LIKE
                Dialect::Mssql => format!("{} LIKE {}", col, sql_literal(&filter.value)),
            }
        }
        FilterOp::In => {
            // Value format: (a,b,c)
            let inner = filter
                .value
                .trim_start_matches('(')
                .trim_end_matches(')');
            let items: Vec<String> = inner
                .split(',')
                .map(|s| sql_literal(s.trim()))
                .collect();
            format!("{} IN ({})", col, items.join(", "))
        }
        FilterOp::Is => {
            let v = filter.value.to_lowercase();
            if v == "null" {
                format!("{} IS NULL", col)
            } else if v == "not.null" {
                format!("{} IS NOT NULL", col)
            } else {
                format!("{} IS NULL", col) // fallback
            }
        }
    }
}

/// Build an ORDER BY clause.
fn build_order_clause(order: &Option<Vec<(String, SortDir)>>, dialect: Dialect) -> String {
    match order {
        None => String::new(),
        Some(orders) => orders
            .iter()
            .map(|(col, dir)| {
                let dir_str = match dir {
                    SortDir::Asc => "ASC",
                    SortDir::Desc => "DESC",
                };
                format!("{} {}", escape_identifier(col, dialect), dir_str)
            })
            .collect::<Vec<_>>()
            .join(", "),
    }
}

/// Convert a serde_json::Value to a SQL literal, dialect-aware for booleans.
fn value_to_sql_literal(val: &Value, dialect: Dialect) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => match dialect {
            Dialect::Postgres | Dialect::DuckDb => {
                if *b { "TRUE".to_string() } else { "FALSE".to_string() }
            }
            Dialect::Mssql => {
                if *b { "1".to_string() } else { "0".to_string() }
            }
        },
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", escape_value(s)),
        _ => format!("'{}'", escape_value(&val.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rest::types::RestFilter;

    #[test]
    fn test_build_select_basic() {
        let query = RestQuery {
            select: Some(vec!["id".into(), "name".into()]),
            filters: vec![RestFilter {
                column: "age".into(),
                operator: FilterOp::Gt,
                value: "21".into(),
            }],
            order: Some(vec![("name".into(), SortDir::Asc)]),
            limit: Some(10),
            offset: Some(0),
        };
        let sql = build_select("users", "public", &query, Dialect::Postgres);
        assert!(sql.contains("SELECT \"id\", \"name\""));
        assert!(sql.contains("FROM \"public\".\"users\""));
        assert!(sql.contains("WHERE \"age\" > 21"));
        assert!(sql.contains("ORDER BY \"name\" ASC"));
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 0"));
    }

    #[test]
    fn test_build_select_mssql_pagination() {
        let query = RestQuery {
            limit: Some(50),
            offset: Some(100),
            ..Default::default()
        };
        let sql = build_select("Users", "dbo", &query, Dialect::Mssql);
        assert!(sql.contains("OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY"));
    }

    #[test]
    fn test_build_insert_postgres() {
        let cols = vec!["name".to_string(), "age".to_string()];
        let rows = vec![vec![Value::String("Alice".into()), Value::Number(30.into())]];
        let sql = build_insert("users", "public", &cols, &rows, Dialect::Postgres);
        assert!(sql.contains("INSERT INTO \"public\".\"users\""));
        assert!(sql.contains("('Alice', 30)"));
        assert!(sql.contains("RETURNING *"));
    }

    #[test]
    fn test_build_insert_mssql() {
        let cols = vec!["name".to_string()];
        let rows = vec![vec![Value::String("Bob".into())]];
        let sql = build_insert("Users", "dbo", &cols, &rows, Dialect::Mssql);
        assert!(sql.contains("OUTPUT inserted.*"));
    }

    #[test]
    fn test_build_update() {
        let mut updates = Map::new();
        updates.insert("age".to_string(), Value::Number(31.into()));
        let sql = build_update("users", "public", "id", "42", &updates, Dialect::Postgres);
        assert!(sql.contains("SET \"age\" = 31"));
        assert!(sql.contains("WHERE \"id\" = 42"));
        assert!(sql.contains("RETURNING *"));
    }

    #[test]
    fn test_build_delete() {
        let sql = build_delete("users", "public", "id", "42", Dialect::Postgres);
        assert!(sql.contains("DELETE FROM \"public\".\"users\""));
        assert!(sql.contains("WHERE \"id\" = 42"));
    }

    #[test]
    fn test_build_count() {
        let filters = vec![RestFilter {
            column: "active".into(),
            operator: FilterOp::Eq,
            value: "true".into(),
        }];
        let sql = build_count("users", "public", &filters, Dialect::Postgres);
        assert!(sql.contains("SELECT COUNT(*) AS total"));
        assert!(sql.contains("WHERE \"active\" = 'true'"));
    }

    #[test]
    fn test_in_filter() {
        let filter = RestFilter {
            column: "status".into(),
            operator: FilterOp::In,
            value: "(active,pending)".into(),
        };
        let cond = filter_to_condition(&filter, Dialect::Postgres);
        assert!(cond.contains("IN ('active', 'pending')"));
    }

    #[test]
    fn test_is_null() {
        let filter = RestFilter {
            column: "deleted_at".into(),
            operator: FilterOp::Is,
            value: "null".into(),
        };
        let cond = filter_to_condition(&filter, Dialect::Postgres);
        assert_eq!(cond, "\"deleted_at\" IS NULL");
    }

    #[test]
    fn test_is_not_null() {
        let filter = RestFilter {
            column: "email".into(),
            operator: FilterOp::Is,
            value: "not.null".into(),
        };
        let cond = filter_to_condition(&filter, Dialect::Postgres);
        assert_eq!(cond, "\"email\" IS NOT NULL");
    }

    #[test]
    fn test_sql_injection_escape() {
        let filter = RestFilter {
            column: "name".into(),
            operator: FilterOp::Eq,
            value: "O'Brien".into(),
        };
        let cond = filter_to_condition(&filter, Dialect::Postgres);
        assert!(cond.contains("'O''Brien'"));
    }
}
