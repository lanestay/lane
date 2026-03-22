use anyhow::Result;

use super::{CountMode, ROW_NUMBER_ALIAS};
use crate::db::Dialect;

const ORDER_BY_KEYWORD: &str = "ORDER BY";

pub fn get_count_query(query: &str, mode: &CountMode) -> Result<String> {
    let upper_query = query.to_uppercase();

    // If query has TOP, extract the number and return it directly
    if let Some(top_pos) = upper_query.find(" TOP ") {
        let after_top = &query[top_pos + 5..];
        if let Some(space_pos) = after_top.find(' ') {
            let top_num = &after_top[..space_pos].trim();
            if let Ok(num) = top_num.parse::<i32>() {
                return Ok(format!("SELECT {} as total", num));
            }
        }
    }

    Ok(match mode {
        CountMode::Window => {
            // Window mode doesn't need a separate count query
            String::new()
        }
        CountMode::Subquery => {
            let stripped = strip_order_by_clause(query);
            format!("SELECT COUNT(*) as total FROM ({}) AS __subquery", stripped)
        }
        CountMode::Exact => {
            if let Some(from_pos) = upper_query.find(" FROM ") {
                let after_from = &query[from_pos..];
                if let Some(order_pos) = find_top_level_order_by(after_from) {
                    format!("SELECT COUNT(*) as total{}", &after_from[..order_pos])
                } else {
                    format!("SELECT COUNT(*) as total{}", after_from)
                }
            } else {
                return Err(anyhow::anyhow!(
                    "Invalid query: no FROM clause found. Use --count-mode=window or --count-mode=subquery for complex queries."
                ));
            }
        }
    })
}

pub fn find_top_level_order_by(query: &str) -> Option<usize> {
    let upper = query.to_uppercase();
    let bytes = query.as_bytes();
    let upper_bytes = upper.as_bytes();

    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            if in_string {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                } else {
                    in_string = false;
                    i += 1;
                    continue;
                }
            } else {
                in_string = true;
                i += 1;
                continue;
            }
        }

        if in_string {
            i += 1;
            continue;
        }

        match b {
            b'(' => depth += 1,
            b')' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            _ => {}
        }

        if depth == 0
            && upper_bytes[i..].starts_with(ORDER_BY_KEYWORD.as_bytes())
            && (i == 0
                || (!upper_bytes[i - 1].is_ascii_alphanumeric() && upper_bytes[i - 1] != b'_'))
        {
            return Some(i);
        }

        i += 1;
    }

    None
}

pub fn has_order_by(query: &str) -> bool {
    find_top_level_order_by(query).is_some()
}

pub fn extract_order_by_clause(query: &str) -> Option<String> {
    find_top_level_order_by(query).map(|idx| {
        let start = idx + ORDER_BY_KEYWORD.len();
        query[start..].trim().to_string()
    })
}

pub fn strip_order_by_clause(query: &str) -> String {
    if let Some(order_pos) = find_top_level_order_by(query) {
        query[..order_pos].trim_end().to_string()
    } else {
        query.to_string()
    }
}

/// Create a paginated query. The `dialect` parameter controls OFFSET/FETCH vs LIMIT/OFFSET syntax.
pub fn create_paginated_query(
    query: &str,
    offset: usize,
    batch_size: usize,
    mode: &CountMode,
    order_by: Option<&str>,
    allow_unstable: bool,
    dialect: Dialect,
) -> Result<String> {
    let order_clause = if let Some(order) = order_by {
        order.to_string()
    } else if let Some(existing_order) = extract_order_by_clause(query) {
        existing_order
    } else if allow_unstable {
        match dialect {
            Dialect::Mssql => "(SELECT NULL)".to_string(),
            Dialect::Postgres | Dialect::DuckDb | Dialect::ClickHouse => "1".to_string(),
        }
    } else {
        return Err(anyhow::anyhow!(
            "Pagination requires an ORDER BY clause for deterministic results.\n\
             Options:\n\
             1. Add ORDER BY to your query\n\
             2. Use --order 'column_name' to specify ordering\n\
             3. Use --allow-unstable-pagination to allow non-deterministic results (not recommended)"
        ));
    };

    let stripped_query = strip_order_by_clause(query);

    let result = match mode {
        CountMode::Window => {
            // Window mode works the same for both dialects (ROW_NUMBER is standard SQL)
            format!(
                "WITH __OriginalQuery AS ({query}) \
                 SELECT * FROM ( \
                   SELECT *, ROW_NUMBER() OVER (ORDER BY {order}) as {alias} \
                   FROM __OriginalQuery \
                 ) __t WHERE {alias} > {offset} AND {alias} <= {limit}",
                query = stripped_query,
                order = order_clause,
                alias = ROW_NUMBER_ALIAS,
                offset = offset,
                limit = offset + batch_size
            )
        }
        CountMode::Subquery => match dialect {
            Dialect::Mssql => format!(
                "SELECT * FROM ({}) AS __subquery \
                 ORDER BY {} \
                 OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
                stripped_query, order_clause, offset, batch_size
            ),
            Dialect::Postgres | Dialect::DuckDb | Dialect::ClickHouse => format!(
                "SELECT * FROM ({}) AS __subquery \
                 ORDER BY {} \
                 LIMIT {} OFFSET {}",
                stripped_query, order_clause, batch_size, offset
            ),
        },
        CountMode::Exact => match dialect {
            Dialect::Mssql => {
                if has_order_by(query) {
                    format!(
                        "{} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
                        query, offset, batch_size
                    )
                } else if !order_clause.is_empty() {
                    format!(
                        "{} ORDER BY {} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
                        query, order_clause, offset, batch_size
                    )
                } else {
                    return Err(anyhow::anyhow!("ORDER BY required for pagination"));
                }
            }
            Dialect::Postgres | Dialect::DuckDb | Dialect::ClickHouse => {
                if has_order_by(query) {
                    format!("{} LIMIT {} OFFSET {}", query, batch_size, offset)
                } else if !order_clause.is_empty() {
                    format!(
                        "{} ORDER BY {} LIMIT {} OFFSET {}",
                        query, order_clause, batch_size, offset
                    )
                } else {
                    return Err(anyhow::anyhow!("ORDER BY required for pagination"));
                }
            }
        },
    };

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mssql_subquery_pagination() {
        let sql = create_paginated_query(
            "SELECT * FROM users",
            10,
            50,
            &CountMode::Subquery,
            Some("id"),
            false,
            Dialect::Mssql,
        )
        .unwrap();
        assert!(sql.contains("OFFSET 10 ROWS FETCH NEXT 50 ROWS ONLY"));
    }

    #[test]
    fn postgres_subquery_pagination() {
        let sql = create_paginated_query(
            "SELECT * FROM users",
            10,
            50,
            &CountMode::Subquery,
            Some("id"),
            false,
            Dialect::Postgres,
        )
        .unwrap();
        assert!(sql.contains("LIMIT 50 OFFSET 10"));
        assert!(!sql.contains("FETCH NEXT"));
    }

    #[test]
    fn postgres_exact_pagination() {
        let sql = create_paginated_query(
            "SELECT * FROM users",
            0,
            100,
            &CountMode::Exact,
            Some("id"),
            false,
            Dialect::Postgres,
        )
        .unwrap();
        assert!(sql.contains("LIMIT 100 OFFSET 0"));
    }

    #[test]
    fn window_mode_same_for_both_dialects() {
        let mssql = create_paginated_query(
            "SELECT * FROM users",
            0,
            50,
            &CountMode::Window,
            Some("id"),
            false,
            Dialect::Mssql,
        )
        .unwrap();
        let pg = create_paginated_query(
            "SELECT * FROM users",
            0,
            50,
            &CountMode::Window,
            Some("id"),
            false,
            Dialect::Postgres,
        )
        .unwrap();
        // Both use ROW_NUMBER, so should be the same
        assert_eq!(mssql, pg);
    }
}
