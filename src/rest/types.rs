/// A single filter condition parsed from a query parameter like `?col=eq.value`.
#[derive(Debug, Clone)]
pub struct RestFilter {
    pub column: String,
    pub operator: FilterOp,
    pub value: String,
}

/// Supported PostgREST-style filter operators.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    Ilike,
    In,
    Is,
}

/// Sort direction for ORDER BY.
#[derive(Debug, Clone, PartialEq)]
pub enum SortDir {
    Asc,
    Desc,
}

/// Parsed query parameters for a REST list request.
#[derive(Debug, Clone, Default)]
pub struct RestQuery {
    pub select: Option<Vec<String>>,
    pub filters: Vec<RestFilter>,
    pub order: Option<Vec<(String, SortDir)>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

