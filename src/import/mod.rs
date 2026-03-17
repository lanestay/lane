pub mod parser;
pub mod sql_gen;
pub mod type_infer;

use serde::{Deserialize, Serialize};

/// Inferred column type from file data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum InferredType {
    Boolean,
    Integer,
    Float,
    Date,
    DateTime,
    Text,
}

impl std::fmt::Display for InferredType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InferredType::Boolean => write!(f, "Boolean"),
            InferredType::Integer => write!(f, "Integer"),
            InferredType::Float => write!(f, "Float"),
            InferredType::Date => write!(f, "Date"),
            InferredType::DateTime => write!(f, "DateTime"),
            InferredType::Text => write!(f, "Text"),
        }
    }
}

/// A column with inferred type and dialect-specific SQL type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredColumn {
    pub name: String,
    pub inferred_type: InferredType,
    pub sql_type: String,
    pub nullable: bool,
}

/// Parsed file data: headers + rows of optional string values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedFile {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub total_rows: usize,
}
