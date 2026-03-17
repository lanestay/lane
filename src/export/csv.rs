use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

use crate::query::QueryResult;

/// Convert a QueryResult into a CSV file as bytes.
pub fn query_result_to_csv(result: &QueryResult) -> Result<Vec<u8>> {
    let mut wtr = csv::Writer::from_writer(Vec::new());

    // Use first result set if multiple, otherwise use data
    let rows: &Vec<HashMap<String, Value>> = if let Some(ref sets) = result.result_sets {
        sets.first().unwrap_or(&result.data)
    } else {
        &result.data
    };

    // Determine column order
    let columns: Vec<String> = if let Some(ref meta) = result.metadata {
        meta.columns.iter().map(|c| c.name.clone()).collect()
    } else if let Some(first_row) = rows.first() {
        let mut keys: Vec<String> = first_row.keys().cloned().collect();
        keys.sort();
        keys
    } else {
        vec![]
    };

    // Header row
    wtr.write_record(&columns)?;

    // Data rows
    for row in rows {
        let record: Vec<String> = columns
            .iter()
            .map(|col| match row.get(col) {
                None | Some(Value::Null) => String::new(),
                Some(Value::String(s)) => s.clone(),
                Some(Value::Number(n)) => n.to_string(),
                Some(Value::Bool(b)) => b.to_string(),
                Some(other) => other.to_string(),
            })
            .collect();
        wtr.write_record(&record)?;
    }

    wtr.flush()?;
    Ok(wtr.into_inner()?)
}
