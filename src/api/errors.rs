use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Serialize;
use serde_json::Value;

use crate::db::Dialect;

// ============================================================================
// Structured Error Response
// ============================================================================

/// Top-level error grouping.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// SQL syntax or semantic check failed before execution.
    Sql,
    /// Query ran but the engine returned an error.
    Runtime,
    /// Network / auth / connectivity issue.
    Transport,
    /// Bad HTTP request (missing params, wrong content-type, etc.).
    Client,
}

/// Wire format returned to callers on failure.
#[derive(Debug, Clone, Serialize)]
pub struct ApiError {
    pub success: bool,
    pub error: ApiErrorDetails,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiErrorDetails {
    pub category: ErrorCategory,
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dialect: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_state: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

/// Human-readable label for a SQL dialect
pub fn dialect_label(dialect: Dialect) -> String {
    match dialect {
        Dialect::Mssql => "T-SQL (Microsoft SQL Server)".to_string(),
        Dialect::Postgres => "PostgreSQL".to_string(),
        Dialect::DuckDb => "DuckDB SQL".to_string(),
        Dialect::ClickHouse => "ClickHouse SQL".to_string(),
    }
}

impl ApiError {
    pub fn to_response(&self, status: StatusCode) -> Response {
        (status, Json(self.clone())).into_response()
    }
}

// ============================================================================
// SQL error code classification
// ============================================================================

/// Classify a SQL Server error code into a semantic code + optional fix hint.
pub fn map_sql_error(sql_code: i32, message: &str) -> (String, Option<String>) {
    // Table-driven: (sql_codes, semantic_code, suggestion_template)
    static RULES: &[(&[i32], &str, &str)] = &[
        (&[208],       "TABLE_NOT_FOUND",    "Object does not exist. Check spelling or run: SELECT name FROM sys.tables WHERE name LIKE '%{}%'"),
        (&[207],       "COLUMN_NOT_FOUND",   "Column '{}' not recognized. List columns: SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'your_table'"),
        (&[2627, 2601],"UNIQUE_VIOLATION",   "Row conflicts with a unique/primary key constraint. Use UPDATE or check for duplicates first."),
        (&[547],       "FK_CHECK_VIOLATION",  "Foreign key or check constraint failed. Ensure referenced rows exist and values satisfy constraints."),
        (&[515],       "NOT_NULL_VIOLATION",  "A required column received NULL. Supply values for all non-nullable columns."),
        (&[102, 156],  "SYNTAX",             "SQL syntax error near the indicated position. Look for missing keywords or unmatched parentheses."),
        (&[229],       "ACCESS_DENIED",      "Insufficient privileges on this object. Contact your DBA."),
        (&[245, 8114], "TYPE_MISMATCH",      "Implicit type conversion failed. Verify that values match the target column types."),
        (&[8152],      "VALUE_TOO_LONG",     "String data would be truncated. Shorten the value or widen the column."),
        (&[1205],      "DEADLOCK_VICTIM",    "Transaction chosen as deadlock victim. Retry the operation."),
        (&[-2],        "QUERY_TIMEOUT",      "Execution timed out. Add indexes, reduce result set with TOP/LIMIT, or split into smaller queries."),
        (&[53, 40],    "UNREACHABLE",        "Cannot reach the database server. Verify host, port, and network path."),
        (&[18456],     "AUTH_FAILED",        "Authentication failed. Check username and password."),
    ];

    for &(codes, semantic, tpl) in RULES {
        if codes.contains(&sql_code) {
            let suggestion = if tpl.contains("{}") {
                let val = extract_quoted_value(message).unwrap_or_default();
                tpl.replace("{}", &val)
            } else {
                tpl.to_string()
            };
            return (semantic.to_string(), Some(suggestion));
        }
    }

    (format!("SQL_{}", sql_code), None)
}

/// Pull the first single-quoted value from an error string.
pub fn extract_quoted_value(message: &str) -> Option<String> {
    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"'([^']+)'").unwrap());
    RE.captures(message).map(|c| c[1].to_string())
}

/// Extract (code, state, message) from a Tiberius error string.
pub fn parse_sql_error(error_str: &str) -> Option<(i32, i32, String)> {
    static CODE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"code:\s*(\d+)").unwrap());
    static STATE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"state:\s*(\d+)").unwrap());
    static MSG_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"Token error:\s*'(.+)'\s+on\s+server").unwrap());

    let code: i32 = CODE_RE.captures(error_str)?.get(1)?.as_str().parse().ok()?;
    let state: i32 = STATE_RE
        .captures(error_str)
        .and_then(|c| c.get(1)?.as_str().parse().ok())
        .unwrap_or(0);
    let message = MSG_RE
        .captures(error_str)
        .map(|c| c[1].to_string())
        .unwrap_or_else(|| error_str.to_string());

    Some((code, state, message))
}

// ============================================================================
// Error constructors
// ============================================================================

/// Structured error for SQL validation failures.
pub fn validation_error(raw_message: &str, dialect: Dialect) -> ApiError {
    let dialect = dialect_label(dialect);

    if let Some((sql_code, sql_state, message)) = parse_sql_error(raw_message) {
        let (code, suggestion) = map_sql_error(sql_code, &message);
        ApiError {
            success: false,
            error: ApiErrorDetails {
                category: ErrorCategory::Sql,
                code,
                message,
                dialect: Some(dialect),
                sql_code: Some(sql_code),
                sql_state: Some(sql_state),
                suggestion,
            },
        }
    } else {
        let suggestion = if raw_message.contains("timeout") {
            Some("Validation timed out. The query may be too complex or the server is slow.".to_string())
        } else {
            Some(format!("Check SQL syntax. This API expects valid {} queries.", dialect))
        };

        ApiError {
            success: false,
            error: ApiErrorDetails {
                category: ErrorCategory::Sql,
                code: "VALIDATION_FAILED".to_string(),
                message: raw_message.to_string(),
                dialect: Some(dialect),
                sql_code: None,
                sql_state: None,
                suggestion,
            },
        }
    }
}

/// Structured error for query execution failures.
pub fn execution_error(raw_error: &str, dialect: Dialect) -> ApiError {
    let dialect = dialect_label(dialect);

    // Try to parse nested JSON from CLI-style output
    if let Some(start) = raw_error.find("output: {") {
        let json_start = start + 8;
        if let Ok(cli_output) = serde_json::from_str::<Value>(&raw_error[json_start..]) {
            if let Some(error_obj) = cli_output.get("error") {
                let details = error_obj
                    .get("details")
                    .and_then(|d| d.as_str())
                    .unwrap_or("");

                if let Some((sql_code, sql_state, message)) = parse_sql_error(details) {
                    let (code, suggestion) = map_sql_error(sql_code, &message);
                    return ApiError {
                        success: false,
                        error: ApiErrorDetails {
                            category: ErrorCategory::Runtime,
                            code,
                            message,
                            dialect: Some(dialect),
                            sql_code: Some(sql_code),
                            sql_state: Some(sql_state),
                            suggestion,
                        },
                    };
                }

                let message = error_obj
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Query execution failed");
                return ApiError {
                    success: false,
                    error: ApiErrorDetails {
                        category: ErrorCategory::Runtime,
                        code: error_obj
                            .get("code")
                            .and_then(|c| c.as_str())
                            .unwrap_or("EXEC_FAILED")
                            .to_string(),
                        message: message.to_string(),
                        dialect: Some(dialect),
                        sql_code: None,
                        sql_state: None,
                        suggestion: Some(
                            "Check the query syntax and ensure all referenced objects exist."
                                .to_string(),
                        ),
                    },
                };
            }
        }
    }

    // Try to parse SQL error directly
    if let Some((sql_code, sql_state, message)) = parse_sql_error(raw_error) {
        let (code, mut suggestion) = map_sql_error(sql_code, &message);

        // Check for enrichment hint appended by try_enrich_error()
        if matches!(sql_code, 207 | 208) {
            if let Some(hint_pos) = raw_error.find(" | Hint: ") {
                let hint_text = &raw_error[hint_pos + 9..];
                let hint_clean = hint_text.split("\n\nContext").next().unwrap_or(hint_text);
                suggestion = Some(hint_clean.to_string());
            }
        }

        return ApiError {
            success: false,
            error: ApiErrorDetails {
                category: ErrorCategory::Runtime,
                code,
                message,
                dialect: Some(dialect),
                sql_code: Some(sql_code),
                sql_state: Some(sql_state),
                suggestion,
            },
        };
    }

    // Keyword-based fallback
    let (code, category, suggestion) = if raw_error.contains("connection")
        || raw_error.contains("connect")
        || raw_error.contains("TCP")
    {
        (
            "UNREACHABLE".to_string(),
            ErrorCategory::Transport,
            Some("Check database connectivity and credentials.".to_string()),
        )
    } else if raw_error.contains("timeout") {
        (
            "QUERY_TIMEOUT".to_string(),
            ErrorCategory::Runtime,
            Some("Query timed out. Consider optimizing or adding indexes.".to_string()),
        )
    } else {
        (
            "EXEC_FAILED".to_string(),
            ErrorCategory::Runtime,
            Some("Check query and ensure all referenced objects exist.".to_string()),
        )
    };

    ApiError {
        success: false,
        error: ApiErrorDetails {
            category,
            code,
            message: raw_error.to_string(),
            dialect: Some(dialect),
            sql_code: None,
            sql_state: None,
            suggestion,
        },
    }
}

/// Structured error for bad HTTP requests (not SQL-related).
pub fn request_error(code: &str, message: &str, suggestion: Option<&str>) -> ApiError {
    ApiError {
        success: false,
        error: ApiErrorDetails {
            category: ErrorCategory::Client,
            code: code.to_string(),
            message: message.to_string(),
            dialect: None,
            sql_code: None,
            sql_state: None,
            suggestion: suggestion.map(|s| s.to_string()),
        },
    }
}

// ============================================================================
// Convenience helpers
// ============================================================================

pub fn bad_request(message: impl Into<String>, suggestion: Option<String>) -> Response {
    request_error(
        "INVALID_REQUEST",
        &message.into(),
        suggestion.as_deref(),
    )
    .to_response(StatusCode::BAD_REQUEST)
}

pub fn unsupported_media_type() -> Response {
    request_error(
        "UNSUPPORTED_MEDIA_TYPE",
        "Only application/json and multipart/form-data are supported",
        Some("Set Content-Type header to 'application/json' or 'multipart/form-data'"),
    )
    .to_response(StatusCode::UNSUPPORTED_MEDIA_TYPE)
}

pub fn payload_too_large(limit_mb: usize) -> Response {
    request_error(
        "PAYLOAD_TOO_LARGE",
        &format!("Payload exceeds limit of {} MB", limit_mb),
        Some("Reduce request size or increase bodyLimitMb parameter"),
    )
    .to_response(StatusCode::PAYLOAD_TOO_LARGE)
}
