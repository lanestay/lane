use std::collections::HashMap;

use super::types::{FilterOp, RestFilter, RestQuery, SortDir};

/// Reserved query parameter names that are not column filters.
const RESERVED_PARAMS: &[&str] = &["select", "order", "limit", "offset"];

/// Parse a raw query parameter map into a structured RestQuery.
pub fn parse_rest_query(params: &HashMap<String, String>) -> Result<RestQuery, String> {
    let mut query = RestQuery::default();

    // Parse select columns
    if let Some(select) = params.get("select") {
        let cols: Vec<String> = select
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !cols.is_empty() {
            query.select = Some(cols);
        }
    }

    // Parse order
    if let Some(order) = params.get("order") {
        let mut orders = Vec::new();
        for part in order.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (col, dir) = if let Some(dot) = part.rfind('.') {
                let col = &part[..dot];
                let dir_str = &part[dot + 1..];
                let dir = match dir_str.to_lowercase().as_str() {
                    "desc" => SortDir::Desc,
                    _ => SortDir::Asc,
                };
                (col.to_string(), dir)
            } else {
                (part.to_string(), SortDir::Asc)
            };
            orders.push((col, dir));
        }
        if !orders.is_empty() {
            query.order = Some(orders);
        }
    }

    // Parse limit
    if let Some(limit) = params.get("limit") {
        query.limit = Some(
            limit
                .parse::<usize>()
                .map_err(|_| format!("Invalid limit: {}", limit))?,
        );
    }

    // Parse offset
    if let Some(offset) = params.get("offset") {
        query.offset = Some(
            offset
                .parse::<usize>()
                .map_err(|_| format!("Invalid offset: {}", offset))?,
        );
    }

    // Parse column filters
    for (key, value) in params {
        if RESERVED_PARAMS.contains(&key.as_str()) {
            continue;
        }
        let filter = parse_filter(key, value)?;
        query.filters.push(filter);
    }

    Ok(query)
}

/// Parse a single filter value like `eq.hello` or `in.(a,b,c)`.
fn parse_filter(column: &str, value: &str) -> Result<RestFilter, String> {
    // Find the operator prefix
    let (op, val) = if let Some(rest) = value.strip_prefix("eq.") {
        (FilterOp::Eq, rest)
    } else if let Some(rest) = value.strip_prefix("neq.") {
        (FilterOp::Neq, rest)
    } else if let Some(rest) = value.strip_prefix("gt.") {
        (FilterOp::Gt, rest)
    } else if let Some(rest) = value.strip_prefix("gte.") {
        (FilterOp::Gte, rest)
    } else if let Some(rest) = value.strip_prefix("lt.") {
        (FilterOp::Lt, rest)
    } else if let Some(rest) = value.strip_prefix("lte.") {
        (FilterOp::Lte, rest)
    } else if let Some(rest) = value.strip_prefix("like.") {
        (FilterOp::Like, rest)
    } else if let Some(rest) = value.strip_prefix("ilike.") {
        (FilterOp::Ilike, rest)
    } else if let Some(rest) = value.strip_prefix("in.") {
        (FilterOp::In, rest)
    } else if let Some(rest) = value.strip_prefix("is.") {
        (FilterOp::Is, rest)
    } else {
        // Default to equality if no operator prefix
        (FilterOp::Eq, value)
    };

    Ok(RestFilter {
        column: column.to_string(),
        operator: op,
        value: val.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_filters() {
        let mut params = HashMap::new();
        params.insert("name".to_string(), "eq.Alice".to_string());
        params.insert("age".to_string(), "gt.21".to_string());

        let query = parse_rest_query(&params).unwrap();
        assert_eq!(query.filters.len(), 2);
    }

    #[test]
    fn test_parse_select_and_order() {
        let mut params = HashMap::new();
        params.insert("select".to_string(), "id,name,age".to_string());
        params.insert("order".to_string(), "name.asc,age.desc".to_string());
        params.insert("limit".to_string(), "10".to_string());
        params.insert("offset".to_string(), "20".to_string());

        let query = parse_rest_query(&params).unwrap();
        assert_eq!(query.select, Some(vec!["id".into(), "name".into(), "age".into()]));
        assert_eq!(query.order, Some(vec![
            ("name".into(), SortDir::Asc),
            ("age".into(), SortDir::Desc),
        ]));
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.offset, Some(20));
    }

    #[test]
    fn test_parse_in_filter() {
        let mut params = HashMap::new();
        params.insert("status".to_string(), "in.(active,pending,review)".to_string());

        let query = parse_rest_query(&params).unwrap();
        assert_eq!(query.filters.len(), 1);
        assert_eq!(query.filters[0].operator, FilterOp::In);
        assert_eq!(query.filters[0].value, "(active,pending,review)");
    }

    #[test]
    fn test_parse_is_null() {
        let mut params = HashMap::new();
        params.insert("deleted_at".to_string(), "is.null".to_string());

        let query = parse_rest_query(&params).unwrap();
        assert_eq!(query.filters[0].operator, FilterOp::Is);
        assert_eq!(query.filters[0].value, "null");
    }

    #[test]
    fn test_default_eq_without_prefix() {
        let mut params = HashMap::new();
        params.insert("id".to_string(), "42".to_string());

        let query = parse_rest_query(&params).unwrap();
        assert_eq!(query.filters[0].operator, FilterOp::Eq);
        assert_eq!(query.filters[0].value, "42");
    }
}
