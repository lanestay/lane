// SQL generation for row-level editing, filtering, and sorting.
// Pure logic — no React. Handles MSSQL vs Postgres dialect differences.

import type { ColumnInfo } from "./api";

export type Dialect = "mssql" | "postgres" | "duckdb";

// ---------------------------------------------------------------------------
// Filter & sort types
// ---------------------------------------------------------------------------

export type FilterOperator =
  | "contains"
  | "equals"
  | "not_equals"
  | "starts_with"
  | "ends_with"
  | "greater_than"
  | "less_than"
  | "greater_or_equal"
  | "less_or_equal"
  | "is_null"
  | "is_not_null";

export interface ColumnFilter {
  column: string;
  operator: FilterOperator;
  value: string;
  dataType: string;
}

export type SortDirection = "ASC" | "DESC";
export interface SortSpec {
  column: string;
  direction: SortDirection;
}

/** Quote an identifier: MSSQL uses [brackets], Postgres/DuckDB use "double quotes". */
export function quoteIdentifier(name: string, dialect: Dialect): string {
  if (dialect === "postgres" || dialect === "duckdb") {
    return `"${name.replace(/"/g, '""')}"`;
  }
  return `[${name.replace(/\]/g, "]]")}]`;
}

/** Format a literal value for SQL. NULL → NULL, numbers unquoted, strings single-quoted with escaping. */
export function formatLiteral(value: unknown, dataType?: string): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "number") return String(value);
  if (typeof value === "boolean") return value ? "1" : "0";
  // If the data type is numeric and the string looks like a number, don't quote
  if (dataType && isNumericType(dataType) && typeof value === "string" && value !== "" && !isNaN(Number(value))) {
    return value;
  }
  // String — escape single quotes
  const escaped = String(value).replace(/'/g, "''");
  return `'${escaped}'`;
}

/** Check whether a data type is numeric (for input hints). */
export function isNumericType(dataType: string): boolean {
  const t = dataType.toLowerCase();
  return /^(tiny|small|big)?int/.test(t)
    || /^(numeric|decimal|float|real|double|money|smallmoney|serial|bigserial)/.test(t);
}

/** Get columns marked as primary key. */
export function getPkColumns(columns: ColumnInfo[]): ColumnInfo[] {
  return columns.filter((c) => c.IS_PRIMARY_KEY === "YES");
}

/** Check whether the table has a primary key (editing requires PK). */
export function hasPrimaryKey(columns: ColumnInfo[]): boolean {
  return getPkColumns(columns).length > 0;
}

export interface CellChange {
  original: unknown;
  current: unknown;
}

/**
 * Generate an UPDATE statement for a single row.
 *
 * @param schema  - Schema name (e.g. "dbo")
 * @param table   - Table name
 * @param columns - Full column metadata (used for PK identification and data types)
 * @param rowData - Original row data (used for WHERE clause PK values)
 * @param changes - Map of columnName → { original, current } for changed cells
 * @param dialect - "mssql" or "postgres"
 */
export function generateUpdate(
  schema: string,
  table: string,
  columns: ColumnInfo[],
  rowData: Record<string, unknown>,
  changes: Map<string, CellChange>,
  dialect: Dialect,
): string {
  const pkCols = getPkColumns(columns);
  if (pkCols.length === 0) throw new Error("Cannot generate UPDATE: table has no primary key");
  if (changes.size === 0) throw new Error("Cannot generate UPDATE: no changes");

  const colMap = new Map(columns.map((c) => [c.COLUMN_NAME, c]));

  const tableName = `${quoteIdentifier(schema, dialect)}.${quoteIdentifier(table, dialect)}`;

  // SET clause
  const setClauses: string[] = [];
  for (const [colName, change] of changes) {
    const col = colMap.get(colName);
    const dataType = col?.DATA_TYPE;
    setClauses.push(`${quoteIdentifier(colName, dialect)} = ${formatLiteral(change.current, dataType)}`);
  }

  // WHERE clause — PK columns with original values
  const whereClauses: string[] = [];
  for (const pk of pkCols) {
    const val = rowData[pk.COLUMN_NAME];
    if (val === null || val === undefined) {
      whereClauses.push(`${quoteIdentifier(pk.COLUMN_NAME, dialect)} IS NULL`);
    } else {
      whereClauses.push(`${quoteIdentifier(pk.COLUMN_NAME, dialect)} = ${formatLiteral(val, pk.DATA_TYPE)}`);
    }
  }

  return `UPDATE ${tableName} SET ${setClauses.join(", ")} WHERE ${whereClauses.join(" AND ")}`;
}

// ---------------------------------------------------------------------------
// Filter & sort SQL generation
// ---------------------------------------------------------------------------

/** Escape special LIKE pattern characters: %, _, [, \ */
export function escapeLikeValue(value: string): string {
  return value
    .replace(/\\/g, "\\\\")
    .replace(/%/g, "\\%")
    .replace(/_/g, "\\_")
    .replace(/\[/g, "\\[");
}

const STRING_TYPES = new Set([
  "char", "varchar", "nchar", "nvarchar", "text", "ntext",
  "character varying", "character", "bpchar", "citext",
]);

function isStringType(dataType: string): boolean {
  return STRING_TYPES.has(dataType.toLowerCase());
}

/** Return the operators applicable to a given data type. */
export function getOperatorsForType(dataType: string): FilterOperator[] {
  if (isStringType(dataType)) {
    return ["contains", "equals", "not_equals", "starts_with", "ends_with", "is_null", "is_not_null"];
  }
  if (isNumericType(dataType)) {
    return ["equals", "not_equals", "greater_than", "less_than", "greater_or_equal", "less_or_equal", "is_null", "is_not_null"];
  }
  // All other types (date, bit, etc.) — all operators
  return [
    "contains", "equals", "not_equals", "starts_with", "ends_with",
    "greater_than", "less_than", "greater_or_equal", "less_or_equal",
    "is_null", "is_not_null",
  ];
}

/** Build a single column filter expression. */
function buildColumnFilterExpr(f: ColumnFilter, dialect: Dialect): string {
  const col = quoteIdentifier(f.column, dialect);

  switch (f.operator) {
    case "is_null":
      return `${col} IS NULL`;
    case "is_not_null":
      return `${col} IS NOT NULL`;
    case "equals":
      return `${col} = ${formatLiteral(f.value, f.dataType)}`;
    case "not_equals":
      return `${col} <> ${formatLiteral(f.value, f.dataType)}`;
    case "greater_than":
      return `${col} > ${formatLiteral(f.value, f.dataType)}`;
    case "less_than":
      return `${col} < ${formatLiteral(f.value, f.dataType)}`;
    case "greater_or_equal":
      return `${col} >= ${formatLiteral(f.value, f.dataType)}`;
    case "less_or_equal":
      return `${col} <= ${formatLiteral(f.value, f.dataType)}`;
    case "contains": {
      const pat = escapeLikeValue(f.value);
      if (dialect === "postgres" || dialect === "duckdb") return `${col}::TEXT ILIKE '%${pat}%'`;
      return `${col} LIKE '%${pat}%'`;
    }
    case "starts_with": {
      const pat = escapeLikeValue(f.value);
      if (dialect === "postgres" || dialect === "duckdb") return `${col}::TEXT ILIKE '${pat}%'`;
      return `${col} LIKE '${pat}%'`;
    }
    case "ends_with": {
      const pat = escapeLikeValue(f.value);
      if (dialect === "postgres" || dialect === "duckdb") return `${col}::TEXT ILIKE '%${pat}'`;
      return `${col} LIKE '%${pat}'`;
    }
  }
}

/**
 * Build a WHERE clause string (without the "WHERE" keyword) from global
 * search text and per-column filters. Returns empty string if no filters.
 */
export function generateWhereClause(
  globalSearch: string,
  columnFilters: ColumnFilter[],
  dialect: Dialect,
  displayColumns: string[],
): string {
  const parts: string[] = [];

  // Global search — OR across all display columns
  const term = globalSearch.trim();
  if (term) {
    const escaped = escapeLikeValue(term);
    const colExprs = displayColumns.map((col) => {
      const qcol = quoteIdentifier(col, dialect);
      if (dialect === "postgres" || dialect === "duckdb") {
        return `${qcol}::TEXT ILIKE '%${escaped}%'`;
      }
      return `CAST(${qcol} AS VARCHAR(MAX)) LIKE '%${escaped}%'`;
    });
    if (colExprs.length > 0) {
      parts.push(`(${colExprs.join(" OR ")})`);
    }
  }

  // Column filters — AND
  for (const f of columnFilters) {
    parts.push(buildColumnFilterExpr(f, dialect));
  }

  return parts.join(" AND ");
}

/**
 * Build a full SELECT preview query with optional WHERE and ORDER BY.
 */
export function buildPreviewQuery(
  schema: string,
  table: string,
  dialect: Dialect,
  globalSearch: string,
  columnFilters: ColumnFilter[],
  sort: SortSpec | null,
  displayColumns: string[],
  limit = 100,
): string {
  const tableName = `${quoteIdentifier(schema, dialect)}.${quoteIdentifier(table, dialect)}`;
  const whereClause = generateWhereClause(globalSearch, columnFilters, dialect, displayColumns);
  const orderBy = sort
    ? ` ORDER BY ${quoteIdentifier(sort.column, dialect)} ${sort.direction}`
    : "";

  if (dialect === "postgres" || dialect === "duckdb") {
    return `SELECT * FROM ${tableName}${whereClause ? ` WHERE ${whereClause}` : ""}${orderBy} LIMIT ${limit}`;
  }
  return `SELECT TOP ${limit} * FROM ${tableName}${whereClause ? ` WHERE ${whereClause}` : ""}${orderBy}`;
}
