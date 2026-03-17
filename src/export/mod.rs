// Export format helpers

pub mod csv;

#[cfg(feature = "xlsx")]
pub mod xlsx;

/// Supported export formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ExportFormat {
    Csv,
    Json,
    #[cfg(feature = "xlsx")]
    Xlsx,
    Parquet,
}

impl ExportFormat {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            #[cfg(feature = "xlsx")]
            Self::Xlsx => "xlsx",
            Self::Parquet => "parquet",
        }
    }

    #[allow(dead_code)]
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Csv => "text/csv",
            Self::Json => "application/json",
            #[cfg(feature = "xlsx")]
            Self::Xlsx => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            Self::Parquet => "application/octet-stream",
        }
    }
}

/// Infer export format from a file extension on the key, or validate an explicit format string.
/// If `explicit` is provided, it takes priority. Otherwise, the extension of `key` is used.
#[allow(dead_code)]
pub fn infer_export_format(key: &str, explicit: Option<&str>) -> Result<ExportFormat, String> {
    let raw = explicit.unwrap_or_else(|| {
        key.rsplit('.').next().unwrap_or("")
    });

    match raw.to_ascii_lowercase().as_str() {
        "csv" => Ok(ExportFormat::Csv),
        "json" => Ok(ExportFormat::Json),
        #[cfg(feature = "xlsx")]
        "xlsx" => Ok(ExportFormat::Xlsx),
        "parquet" => Ok(ExportFormat::Parquet),
        other => Err(format!(
            "Unsupported export format '{}'. Supported: csv, json, {}parquet",
            other,
            if cfg!(feature = "xlsx") { "xlsx, " } else { "" }
        )),
    }
}
