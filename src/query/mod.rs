pub mod pagination;
pub mod validation;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::pii;

pub const DEFAULT_BATCH_SIZE: usize = 50_000;
pub const ROW_NUMBER_ALIAS: &str = "__internal_rn";

#[derive(Debug, Clone, PartialEq)]
pub enum CountMode {
    Window,   // Use ROW_NUMBER() OVER() window function
    Subquery, // Wrap query in subquery for COUNT
    Exact,    // Use exact COUNT(*) query (legacy)
}

impl CountMode {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "window" => Ok(CountMode::Window),
            "subquery" => Ok(CountMode::Subquery),
            "exact" => Ok(CountMode::Exact),
            _ => Err(anyhow::anyhow!(
                "Invalid count mode: {}. Use 'window', 'subquery', or 'exact'",
                s
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BlobFormat {
    Length, // Show only byte length: [BINARY n bytes]
    Base64, // Base64 encode the binary data
    Hex,    // Hex encode the binary data
}

impl BlobFormat {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "length" => Ok(BlobFormat::Length),
            "base64" => Ok(BlobFormat::Base64),
            "hex" => Ok(BlobFormat::Hex),
            _ => Err(anyhow::anyhow!(
                "Invalid blob format: {}. Use 'length', 'base64', or 'hex'",
                s
            )),
        }
    }
}

/// Core query parameters used across REST API and MCP
#[derive(Debug, Clone)]
pub struct QueryParams {
    pub database: String,
    pub query: String,
    pub batch_size: usize,
    pub pagination: bool,
    pub count_mode: CountMode,
    pub order: Option<String>,
    pub allow_unstable_pagination: bool,
    pub preserve_decimal_precision: bool,
    pub blob_format: BlobFormat,
    pub include_metadata: bool,
    #[allow(dead_code)]
    pub max_memory_bytes: Option<usize>,
    pub pii_mode: Option<String>,
    pub pii_column_hints: Option<Vec<String>>,
    pub pii_column_excludes: Option<Vec<String>>,
    pub pii_processor_override: Option<pii::PiiProcessor>,
    pub json_stream: bool,
    /// When true, Postgres queries run inside a READ ONLY transaction.
    pub read_only: bool,
}

impl Default for QueryParams {
    fn default() -> Self {
        Self {
            database: "master".to_string(),
            query: String::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            pagination: false,
            count_mode: CountMode::Window,
            order: None,
            allow_unstable_pagination: false,
            preserve_decimal_precision: true,
            blob_format: BlobFormat::Length,
            include_metadata: false,
            max_memory_bytes: None,
            pii_mode: None,
            pii_column_hints: None,
            pii_column_excludes: None,
            pii_processor_override: None,
            json_stream: false,
            read_only: false,
        }
    }
}

/// Query result returned by the execution engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub success: bool,
    pub total_rows: i64,
    pub execution_time_ms: u128,
    pub rows_per_second: f64,
    pub data: Vec<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_sets: Option<Vec<Vec<HashMap<String, Value>>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_set_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<QueryMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetadata {
    pub columns: Vec<ColumnMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

/// Build a PII processor from query params and environment
pub fn build_pii_processor(params: &QueryParams) -> pii::PiiProcessor {
    // If an enriched override is set, use it directly
    if let Some(ref processor) = params.pii_processor_override {
        return processor.clone();
    }
    let pii_mode = match params.pii_mode.as_deref() {
        Some("scrub") => pii::PiiMode::Scrub,
        Some("none") => pii::PiiMode::None,
        _ => {
            // Check env defaults
            crate::config::env_default_pii_mode().unwrap_or(pii::PiiMode::None)
        }
    };

    pii::PiiProcessor::new(
        pii_mode,
        params.pii_column_hints.clone().unwrap_or_default(),
        params.pii_column_excludes.clone().unwrap_or_default(),
    )
}

/// Context for PII resolution — carries token/user/access info through the resolution chain.
pub struct PiiContext {
    pub token_pii_mode: Option<String>,
    pub email: Option<String>,
    pub is_full_access: bool,
}

/// Parse a PII mode string into a PiiMode enum.
fn parse_pii_mode(s: &str) -> Option<pii::PiiMode> {
    match s {
        "scrub" => Some(pii::PiiMode::Scrub),
        "none" => Some(pii::PiiMode::None),
        _ => None,
    }
}

/// Build a PII processor enriched with admin-managed rules, column tags, and per-token/user/connection settings.
///
/// Resolution chain (first non-null wins):
/// 1. Token-level pii_mode (admin sets per token)
/// 2. User-level pii_mode (admin sets default for user)
/// 3. Connection-level pii_override_{connection} (existing system_config key)
/// 4. System default → None
///
/// FullAccess (master API key) always returns None (no PII processing).
/// Request-level pii_mode param only applies if no admin-set mode exists (levels 1-3).
pub fn build_enriched_pii_processor(
    params: &QueryParams,
    access_db: Option<&crate::auth::access_control::AccessControlDb>,
    connection_name: Option<&str>,
    pii_ctx: &PiiContext,
) -> Option<pii::PiiProcessor> {
    // FullAccess (master API key) — always skip PII
    if pii_ctx.is_full_access {
        return None;
    }

    let access_db = access_db?;

    // Resolve admin-set mode: token → user → connection override
    let admin_mode: Option<pii::PiiMode> = pii_ctx
        .token_pii_mode
        .as_deref()
        .and_then(parse_pii_mode)
        .or_else(|| {
            pii_ctx
                .email
                .as_deref()
                .and_then(|email| access_db.get_user_pii_mode(email).ok().flatten())
                .as_deref()
                .and_then(parse_pii_mode)
        })
        .or_else(|| {
            connection_name
                .and_then(|cn| {
                    access_db
                        .get_config(&format!("pii_override_{}", cn))
                        .ok()
                        .flatten()
                })
                .as_deref()
                .and_then(parse_pii_mode)
        });

    // If admin mode is set, use it (request param cannot downgrade)
    // If admin mode is None, use request param
    let pii_mode = if let Some(mode) = admin_mode {
        mode
    } else {
        // Request-level param
        let request_mode = params.pii_mode.as_deref().and_then(parse_pii_mode);
        request_mode.unwrap_or(pii::PiiMode::None)
    };

    // Load enabled custom rules
    let custom_rules: Vec<pii::CustomRule> = access_db
        .get_enabled_pii_rules()
        .unwrap_or_default()
        .into_iter()
        .filter(|r| !r.is_builtin) // built-ins already handled by detect_entities
        .filter_map(|r| {
            regex::Regex::new(&r.regex_pattern).ok().map(|regex| {
                pii::CustomRule {
                    name: r.name,
                    regex,
                    replacement_text: r.replacement_text,
                }
            })
        })
        .collect();

    // Check which built-in detectors are disabled
    // (we'll handle this via the existing detect_entities flow - built-in toggles
    // are just informational for now; the built-in regexes always run but we
    // skip disabled ones by not including them as custom rules)

    // Load column tags and merge into column_hints
    let mut column_hints = params.pii_column_hints.clone().unwrap_or_default();
    if let Some(conn) = connection_name {
        if let Ok(tags) = access_db.get_pii_columns_for_query(conn, &params.database) {
            for tag in tags {
                let col_lower = tag.column_name.to_ascii_lowercase();
                if !column_hints.iter().any(|h| h.to_ascii_lowercase() == col_lower) {
                    column_hints.push(tag.column_name);
                }
            }
        }
    }

    let processor = pii::PiiProcessor::new(
        pii_mode,
        column_hints,
        params.pii_column_excludes.clone().unwrap_or_default(),
    )
    .with_custom_rules(custom_rules);

    Some(processor)
}

pub fn format_binary_data(data: &[u8], format: &BlobFormat) -> String {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
    match format {
        BlobFormat::Length => format!("[BINARY {} bytes]", data.len()),
        BlobFormat::Base64 => BASE64.encode(data),
        BlobFormat::Hex => hex::encode(data),
    }
}
