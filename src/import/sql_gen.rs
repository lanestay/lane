use crate::db::Dialect;

use super::InferredColumn;

/// Escape a SQL identifier for the given dialect.
pub fn escape_identifier(name: &str, dialect: Dialect) -> String {
    match dialect {
        Dialect::Mssql => format!("[{}]", name.replace(']', "]]")),
        Dialect::Postgres | Dialect::DuckDb | Dialect::ClickHouse => format!("\"{}\"", name.replace('"', "\"\"")),
    }
}

/// Generate a CREATE TABLE statement.
pub fn generate_create_table(
    columns: &[InferredColumn],
    schema: &str,
    table: &str,
    dialect: Dialect,
) -> String {
    let schema_id = escape_identifier(schema, dialect);
    let table_id = escape_identifier(table, dialect);

    let col_defs: Vec<String> = columns
        .iter()
        .map(|col| {
            let name = escape_identifier(&col.name, dialect);
            format!("    {} {} NULL", name, col.sql_type)
        })
        .collect();

    format!(
        "CREATE TABLE {}.{} (\n{}\n)",
        schema_id,
        table_id,
        col_defs.join(",\n")
    )
}

/// Generate a multi-row INSERT statement for a batch of rows.
pub fn generate_insert_batch(
    columns: &[InferredColumn],
    rows: &[Vec<Option<String>>],
    schema: &str,
    table: &str,
    dialect: Dialect,
) -> String {
    let schema_id = escape_identifier(schema, dialect);
    let table_id = escape_identifier(table, dialect);

    let col_names: Vec<String> = columns
        .iter()
        .map(|c| escape_identifier(&c.name, dialect))
        .collect();

    let mut sql = format!(
        "INSERT INTO {}.{} ({}) VALUES\n",
        schema_id,
        table_id,
        col_names.join(", ")
    );

    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push('(');
        for (j, col) in columns.iter().enumerate() {
            if j > 0 {
                sql.push_str(", ");
            }
            match row.get(j).and_then(|v| v.as_deref()) {
                None => sql.push_str("NULL"),
                Some(val) => {
                    sql.push_str(&escape_value(val, &col.sql_type, dialect));
                }
            }
        }
        sql.push(')');
    }

    sql
}

/// Escape a value for SQL insertion based on type and dialect.
fn escape_value(val: &str, sql_type: &str, dialect: Dialect) -> String {
    let sql_type_upper = sql_type.to_uppercase();

    // Boolean
    if sql_type_upper == "BIT" {
        return match val.to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" | "t" | "y" => "1".to_string(),
            _ => "0".to_string(),
        };
    }
    if sql_type_upper == "BOOLEAN" {
        return match val.to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" | "t" | "y" => "TRUE".to_string(),
            _ => "FALSE".to_string(),
        };
    }

    // Numeric types — no quoting
    if sql_type_upper == "BIGINT" || sql_type_upper == "FLOAT" || sql_type_upper == "DOUBLE PRECISION" {
        // Validate it's actually numeric; if not, fall through to string
        if val.parse::<f64>().is_ok() {
            return val.to_string();
        }
        // If it can't parse, treat as NULL to avoid SQL errors
        return "NULL".to_string();
    }

    // String / date types — quoted
    let escaped = val.replace('\'', "''");
    match dialect {
        Dialect::Mssql => format!("N'{}'", escaped),
        Dialect::Postgres | Dialect::DuckDb | Dialect::ClickHouse => format!("'{}'", escaped),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::import::InferredType;

    fn test_columns() -> Vec<InferredColumn> {
        vec![
            InferredColumn {
                name: "id".to_string(),
                inferred_type: InferredType::Integer,
                sql_type: "BIGINT".to_string(),
                nullable: false,
            },
            InferredColumn {
                name: "name".to_string(),
                inferred_type: InferredType::Text,
                sql_type: "NVARCHAR(MAX)".to_string(),
                nullable: true,
            },
            InferredColumn {
                name: "active".to_string(),
                inferred_type: InferredType::Boolean,
                sql_type: "BIT".to_string(),
                nullable: false,
            },
        ]
    }

    #[test]
    fn test_escape_identifier_mssql() {
        assert_eq!(escape_identifier("my_table", Dialect::Mssql), "[my_table]");
        assert_eq!(
            escape_identifier("has]bracket", Dialect::Mssql),
            "[has]]bracket]"
        );
    }

    #[test]
    fn test_escape_identifier_postgres() {
        assert_eq!(
            escape_identifier("my_table", Dialect::Postgres),
            "\"my_table\""
        );
        assert_eq!(
            escape_identifier("has\"quote", Dialect::Postgres),
            "\"has\"\"quote\""
        );
    }

    #[test]
    fn test_create_table_mssql() {
        let cols = test_columns();
        let sql = generate_create_table(&cols, "dbo", "test_table", Dialect::Mssql);
        assert!(sql.contains("CREATE TABLE [dbo].[test_table]"));
        assert!(sql.contains("[id] BIGINT NULL"));
        assert!(sql.contains("[name] NVARCHAR(MAX) NULL"));
        assert!(sql.contains("[active] BIT NULL"));
    }

    #[test]
    fn test_create_table_postgres() {
        let cols = vec![
            InferredColumn {
                name: "id".to_string(),
                inferred_type: InferredType::Integer,
                sql_type: "BIGINT".to_string(),
                nullable: false,
            },
            InferredColumn {
                name: "value".to_string(),
                inferred_type: InferredType::Float,
                sql_type: "DOUBLE PRECISION".to_string(),
                nullable: true,
            },
        ];
        let sql = generate_create_table(&cols, "public", "test_table", Dialect::Postgres);
        assert!(sql.contains("CREATE TABLE \"public\".\"test_table\""));
        assert!(sql.contains("\"id\" BIGINT NULL"));
        assert!(sql.contains("\"value\" DOUBLE PRECISION NULL"));
    }

    #[test]
    fn test_insert_batch_mssql() {
        let cols = test_columns();
        let rows = vec![
            vec![
                Some("1".to_string()),
                Some("Alice".to_string()),
                Some("true".to_string()),
            ],
            vec![
                Some("2".to_string()),
                None,
                Some("false".to_string()),
            ],
        ];
        let sql = generate_insert_batch(&cols, &rows, "dbo", "test_table", Dialect::Mssql);
        assert!(sql.contains("INSERT INTO [dbo].[test_table]"));
        assert!(sql.contains("(1, N'Alice', 1)"));
        assert!(sql.contains("(2, NULL, 0)"));
    }

    #[test]
    fn test_insert_batch_postgres() {
        let cols = vec![
            InferredColumn {
                name: "id".to_string(),
                inferred_type: InferredType::Integer,
                sql_type: "BIGINT".to_string(),
                nullable: false,
            },
            InferredColumn {
                name: "name".to_string(),
                inferred_type: InferredType::Text,
                sql_type: "TEXT".to_string(),
                nullable: true,
            },
            InferredColumn {
                name: "active".to_string(),
                inferred_type: InferredType::Boolean,
                sql_type: "BOOLEAN".to_string(),
                nullable: false,
            },
        ];
        let rows = vec![vec![
            Some("1".to_string()),
            Some("O'Brien".to_string()),
            Some("true".to_string()),
        ]];
        let sql = generate_insert_batch(&cols, &rows, "public", "data", Dialect::Postgres);
        assert!(sql.contains("INSERT INTO \"public\".\"data\""));
        assert!(sql.contains("(1, 'O''Brien', TRUE)"));
    }

    #[test]
    fn test_escape_sql_injection() {
        // Ensure single quotes are doubled
        let escaped = escape_value("Robert'; DROP TABLE students;--", "NVARCHAR(MAX)", Dialect::Mssql);
        assert_eq!(escaped, "N'Robert''; DROP TABLE students;--'");
    }

    #[test]
    fn test_non_numeric_in_numeric_column() {
        // If a value can't parse as number, it should become NULL
        let escaped = escape_value("not_a_number", "BIGINT", Dialect::Mssql);
        assert_eq!(escaped, "NULL");
    }
}
