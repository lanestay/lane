use anyhow::{Context, Result};
use chrono::Timelike;
use std::io::Cursor;

use super::ParsedFile;

/// Parse a file by dispatching on its extension.
pub fn parse_file(bytes: &[u8], filename: &str) -> Result<ParsedFile> {
    let ext = filename
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "csv" | "tsv" => parse_csv(bytes, true),
        "xlsx" | "xls" | "xlsb" | "ods" => parse_excel(bytes),
        _ => anyhow::bail!(
            "Unsupported file type '.{}'. Supported: csv, tsv, xlsx, xls, xlsb, ods",
            ext
        ),
    }
}

/// Parse CSV bytes into a ParsedFile.
pub fn parse_csv(bytes: &[u8], has_header: bool) -> Result<ParsedFile> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_header)
        .flexible(true)
        .from_reader(Cursor::new(bytes));

    let mut first_row: Option<csv::StringRecord> = None;
    let headers: Vec<String> = if has_header {
        reader
            .headers()
            .context("Failed to read CSV headers")?
            .iter()
            .map(|h| h.to_string())
            .collect()
    } else {
        // Auto-generate column names from first record width (peek at first row)
        match reader.records().next() {
            Some(Ok(record)) => {
                let hdrs = (0..record.len())
                    .map(|i| format!("column_{}", i + 1))
                    .collect();
                first_row = Some(record);
                hdrs
            }
            _ => {
                return Ok(ParsedFile {
                    headers: vec![],
                    rows: vec![],
                    total_rows: 0,
                });
            }
        }
    };

    let num_cols = headers.len();
    let mut rows = Vec::new();

    let record_to_row = |record: &csv::StringRecord, num_cols: usize| -> Vec<Option<String>> {
        (0..num_cols)
            .map(|i| {
                record.get(i).and_then(|v| {
                    let trimmed = v.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
            })
            .collect()
    };

    // Include the first row if it was consumed during header detection
    if let Some(ref record) = first_row {
        rows.push(record_to_row(record, num_cols));
    }

    for result in reader.records() {
        let record = result.context("Failed to read CSV row")?;
        rows.push(record_to_row(&record, num_cols));
    }

    let total_rows = rows.len();
    Ok(ParsedFile {
        headers,
        rows,
        total_rows,
    })
}

/// Parse an Excel file (xlsx/xls/xlsb/ods) into a ParsedFile using calamine.
pub fn parse_excel(bytes: &[u8]) -> Result<ParsedFile> {
    use calamine::{open_workbook_auto_from_rs, Data, Reader};

    let cursor = Cursor::new(bytes);
    let mut workbook =
        open_workbook_auto_from_rs(cursor).context("Failed to open Excel workbook")?;

    let sheet_name = workbook
        .sheet_names()
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Workbook has no sheets"))?;

    let range = workbook
        .worksheet_range(&sheet_name)
        .context("Failed to read worksheet")?;

    let mut row_iter = range.rows();

    // First row = headers
    let header_row = row_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("Worksheet is empty"))?;

    let headers: Vec<String> = header_row
        .iter()
        .enumerate()
        .map(|(i, cell)| {
            let val = cell_to_string(cell);
            if val.is_empty() {
                format!("column_{}", i + 1)
            } else {
                val
            }
        })
        .collect();

    let num_cols = headers.len();
    let mut rows = Vec::new();

    for row in row_iter {
        let parsed_row: Vec<Option<String>> = (0..num_cols)
            .map(|i| {
                row.get(i).and_then(|cell| {
                    if matches!(cell, Data::Empty) {
                        None
                    } else {
                        let s = cell_to_string(cell);
                        if s.is_empty() {
                            None
                        } else {
                            Some(s)
                        }
                    }
                })
            })
            .collect();

        // Skip fully empty rows
        if parsed_row.iter().all(|c| c.is_none()) {
            continue;
        }

        rows.push(parsed_row);
    }

    let total_rows = rows.len();
    Ok(ParsedFile {
        headers,
        rows,
        total_rows,
    })
}

/// Convert a calamine Data cell to a string.
fn cell_to_string(cell: &calamine::Data) -> String {
    use calamine::Data;
    match cell {
        Data::Empty => String::new(),
        Data::String(s) => s.clone(),
        Data::Int(i) => i.to_string(),
        Data::Float(f) => {
            // Avoid trailing .0 for whole numbers
            if *f == (*f as i64) as f64 && f.is_finite() {
                format!("{}", *f as i64)
            } else {
                f.to_string()
            }
        }
        Data::Bool(b) => b.to_string(),
        Data::DateTime(dt) => {
            // calamine DateTime is days since 1899-12-30
            // Try to format as date or datetime
            if let Some(naive) = dt.as_datetime() {
                if naive.hour() == 0 && naive.minute() == 0 && naive.second() == 0 {
                    naive.format("%Y-%m-%d").to_string()
                } else {
                    naive.format("%Y-%m-%d %H:%M:%S").to_string()
                }
            } else {
                format!("{}", dt)
            }
        }
        Data::DateTimeIso(s) => s.clone(),
        Data::DurationIso(s) => s.clone(),
        Data::Error(e) => format!("#ERR:{:?}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_csv_basic() {
        let csv = b"name,age,active\nAlice,30,true\nBob,,false\n";
        let result = parse_csv(csv, true).unwrap();
        assert_eq!(result.headers, vec!["name", "age", "active"]);
        assert_eq!(result.total_rows, 2);
        assert_eq!(result.rows[0][0], Some("Alice".to_string()));
        assert_eq!(result.rows[1][1], None); // empty cell
    }

    #[test]
    fn test_parse_csv_no_header() {
        let csv = b"Alice,30\nBob,25\n";
        let result = parse_csv(csv, false).unwrap();
        assert_eq!(result.headers, vec!["column_1", "column_2"]);
        assert_eq!(result.total_rows, 2);
    }

    #[test]
    fn test_unsupported_extension() {
        let result = parse_file(b"data", "file.json");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported file type"));
    }
}
