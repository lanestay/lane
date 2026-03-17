use anyhow::Result;
use rust_xlsxwriter::{Format, Workbook};
use serde_json::Value;
use std::collections::HashMap;

use crate::query::QueryResult;

/// Convert a QueryResult into an xlsx file as bytes.
pub fn query_result_to_xlsx(result: &QueryResult) -> Result<Vec<u8>> {
    let mut workbook = Workbook::new();
    let bold = Format::new().set_bold();

    // Determine sheets: multiple result sets or single data set
    let sheets: Vec<(&str, &Vec<HashMap<String, Value>>)> =
        if let Some(ref sets) = result.result_sets {
            sets.iter()
                .enumerate()
                .map(|(i, s)| {
                    // Leak a short-lived string for sheet names — bounded by result_set count
                    let name: &str = Box::leak(format!("Results {}", i + 1).into_boxed_str());
                    (name, s)
                })
                .collect()
        } else {
            vec![("Results", &result.data)]
        };

    for (sheet_name, rows) in &sheets {
        let worksheet = workbook.add_worksheet();
        worksheet.set_name(*sheet_name)?;

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
        for (col, name) in columns.iter().enumerate() {
            worksheet.write_string_with_format(0, col as u16, name, &bold)?;
        }

        // Data rows
        for (row_idx, row) in rows.iter().enumerate() {
            let row_num = (row_idx + 1) as u32;
            for (col_idx, col_name) in columns.iter().enumerate() {
                let col_num = col_idx as u16;
                if let Some(value) = row.get(col_name) {
                    match value {
                        Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                worksheet.write_number(row_num, col_num, f)?;
                            }
                        }
                        Value::String(s) => {
                            // Try parsing as number for decimal strings
                            if let Ok(f) = s.parse::<f64>() {
                                worksheet.write_number(row_num, col_num, f)?;
                            } else {
                                worksheet.write_string(row_num, col_num, s)?;
                            }
                        }
                        Value::Bool(b) => {
                            worksheet.write_boolean(row_num, col_num, *b)?;
                        }
                        Value::Null => {} // leave cell empty
                        _ => {
                            // Arrays/objects — serialize as JSON string
                            worksheet.write_string(row_num, col_num, &value.to_string())?;
                        }
                    }
                }
            }
        }

        worksheet.autofit();
    }

    let buf = workbook.save_to_buffer()?;
    Ok(buf)
}
