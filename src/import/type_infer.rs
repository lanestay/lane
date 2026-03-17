use crate::db::Dialect;

use super::{InferredColumn, InferredType, ParsedFile};

/// Infer column types from parsed file data.
/// Scans up to 1000 rows per column. A type is accepted if ≥95% of non-empty cells match.
pub fn infer_columns(file: &ParsedFile, dialect: Dialect) -> Vec<InferredColumn> {
    file.headers
        .iter()
        .enumerate()
        .map(|(col_idx, name)| {
            let values: Vec<&str> = file
                .rows
                .iter()
                .take(1000)
                .filter_map(|row| row.get(col_idx).and_then(|v| v.as_deref()))
                .collect();

            let has_nulls = file
                .rows
                .iter()
                .take(1000)
                .any(|row| row.get(col_idx).is_none_or(|v| v.is_none()));

            let inferred_type = infer_type(&values);
            let sql_type = sql_type_for(&inferred_type, dialect);

            InferredColumn {
                name: name.clone(),
                inferred_type,
                sql_type,
                nullable: has_nulls,
            }
        })
        .collect()
}

/// Infer the best type for a set of non-empty string values.
/// Tries types from most specific to least: Boolean → Integer → Float → Date → DateTime → Text.
fn infer_type(values: &[&str]) -> InferredType {
    if values.is_empty() {
        return InferredType::Text;
    }

    let threshold = 0.95;
    let total = values.len() as f64;

    // Boolean
    let bool_count = values
        .iter()
        .filter(|v| is_boolean(v))
        .count();
    if bool_count as f64 / total >= threshold {
        return InferredType::Boolean;
    }

    // Integer
    let int_count = values
        .iter()
        .filter(|v| is_integer(v))
        .count();
    if int_count as f64 / total >= threshold {
        return InferredType::Integer;
    }

    // Float
    let float_count = values
        .iter()
        .filter(|v| is_float(v))
        .count();
    if float_count as f64 / total >= threshold {
        return InferredType::Float;
    }

    // Date (YYYY-MM-DD only)
    let date_count = values
        .iter()
        .filter(|v| is_date(v))
        .count();
    if date_count as f64 / total >= threshold {
        return InferredType::Date;
    }

    // DateTime
    let datetime_count = values
        .iter()
        .filter(|v| is_datetime(v))
        .count();
    if datetime_count as f64 / total >= threshold {
        return InferredType::DateTime;
    }

    InferredType::Text
}

fn is_boolean(s: &str) -> bool {
    matches!(
        s.to_ascii_lowercase().as_str(),
        "true" | "false" | "yes" | "no" | "1" | "0" | "t" | "f" | "y" | "n"
    )
}

fn is_integer(s: &str) -> bool {
    s.parse::<i64>().is_ok()
}

fn is_float(s: &str) -> bool {
    s.parse::<f64>().is_ok()
}

fn is_date(s: &str) -> bool {
    // YYYY-MM-DD
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
        // MM/DD/YYYY
        || chrono::NaiveDate::parse_from_str(s, "%m/%d/%Y").is_ok()
}

fn is_datetime(s: &str) -> bool {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").is_ok()
        || chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").is_ok()
        || chrono::NaiveDateTime::parse_from_str(s, "%m/%d/%Y %H:%M:%S").is_ok()
}

/// Map an InferredType to the SQL type for a given dialect.
pub fn sql_type_for(t: &InferredType, dialect: Dialect) -> String {
    match (t, dialect) {
        (InferredType::Boolean, Dialect::Mssql) => "BIT".to_string(),
        (InferredType::Boolean, Dialect::Postgres | Dialect::DuckDb) => "BOOLEAN".to_string(),
        (InferredType::Integer, Dialect::Mssql) => "BIGINT".to_string(),
        (InferredType::Integer, Dialect::Postgres | Dialect::DuckDb) => "BIGINT".to_string(),
        (InferredType::Float, Dialect::Mssql) => "FLOAT".to_string(),
        (InferredType::Float, Dialect::Postgres | Dialect::DuckDb) => "DOUBLE PRECISION".to_string(),
        (InferredType::Date, Dialect::Mssql) => "DATE".to_string(),
        (InferredType::Date, Dialect::Postgres | Dialect::DuckDb) => "DATE".to_string(),
        (InferredType::DateTime, Dialect::Mssql) => "DATETIME2".to_string(),
        (InferredType::DateTime, Dialect::Postgres | Dialect::DuckDb) => "TIMESTAMP".to_string(),
        (InferredType::Text, Dialect::Mssql) => "NVARCHAR(MAX)".to_string(),
        (InferredType::Text, Dialect::Postgres | Dialect::DuckDb) => "TEXT".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_integers() {
        let values = vec!["1", "2", "3", "100", "-5"];
        assert_eq!(infer_type(&values), InferredType::Integer);
    }

    #[test]
    fn test_infer_floats() {
        let values = vec!["1.5", "2.3", "3.14", "100.0", "-5.5"];
        assert_eq!(infer_type(&values), InferredType::Float);
    }

    #[test]
    fn test_infer_booleans() {
        let values = vec!["true", "false", "true", "false"];
        assert_eq!(infer_type(&values), InferredType::Boolean);
    }

    #[test]
    fn test_infer_dates() {
        let values = vec!["2024-01-01", "2024-06-15", "2023-12-31"];
        assert_eq!(infer_type(&values), InferredType::Date);
    }

    #[test]
    fn test_infer_datetime() {
        let values = vec![
            "2024-01-01 12:00:00",
            "2024-06-15 08:30:00",
            "2023-12-31 23:59:59",
        ];
        assert_eq!(infer_type(&values), InferredType::DateTime);
    }

    #[test]
    fn test_infer_text() {
        let values = vec!["hello", "world", "foo bar"];
        assert_eq!(infer_type(&values), InferredType::Text);
    }

    #[test]
    fn test_mixed_mostly_int() {
        // 95% threshold: 19 ints + 1 text out of 20 = 95%, should pass
        let mut values: Vec<&str> = (0..19).map(|_| "42").collect();
        values.push("not_a_number");
        assert_eq!(infer_type(&values), InferredType::Integer);
    }

    #[test]
    fn test_sql_types_mssql() {
        assert_eq!(sql_type_for(&InferredType::Boolean, Dialect::Mssql), "BIT");
        assert_eq!(
            sql_type_for(&InferredType::Text, Dialect::Mssql),
            "NVARCHAR(MAX)"
        );
        assert_eq!(
            sql_type_for(&InferredType::DateTime, Dialect::Mssql),
            "DATETIME2"
        );
    }

    #[test]
    fn test_sql_types_postgres() {
        assert_eq!(
            sql_type_for(&InferredType::Boolean, Dialect::Postgres),
            "BOOLEAN"
        );
        assert_eq!(
            sql_type_for(&InferredType::Text, Dialect::Postgres),
            "TEXT"
        );
        assert_eq!(
            sql_type_for(&InferredType::Float, Dialect::Postgres),
            "DOUBLE PRECISION"
        );
    }

    #[test]
    fn test_infer_columns() {
        let file = ParsedFile {
            headers: vec!["id".to_string(), "name".to_string(), "active".to_string()],
            rows: vec![
                vec![
                    Some("1".to_string()),
                    Some("Alice".to_string()),
                    Some("true".to_string()),
                ],
                vec![
                    Some("2".to_string()),
                    Some("Bob".to_string()),
                    Some("false".to_string()),
                ],
                vec![
                    Some("3".to_string()),
                    None,
                    Some("true".to_string()),
                ],
            ],
            total_rows: 3,
        };

        let columns = infer_columns(&file, Dialect::Mssql);
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].inferred_type, InferredType::Integer);
        assert_eq!(columns[0].sql_type, "BIGINT");
        assert!(!columns[0].nullable);
        assert_eq!(columns[1].inferred_type, InferredType::Text);
        assert!(columns[1].nullable); // has a None
        assert_eq!(columns[2].inferred_type, InferredType::Boolean);
        assert_eq!(columns[2].sql_type, "BIT");
    }
}
