// FK and index query logic for schema exploration.
// Pure logic — no React. Dialect-specific SQL against system catalogs.

import { executeQuery } from "./api";
import type { QueryResult } from "./api";
import type { Dialect } from "./sql-gen";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ForeignKeyInfo {
  FK_NAME: string;
  PARENT_TABLE: string;
  PARENT_SCHEMA: string;
  PARENT_COLUMN: string;
  REFERENCED_TABLE: string;
  REFERENCED_SCHEMA: string;
  REFERENCED_COLUMN: string;
}

export interface IndexInfo {
  INDEX_NAME: string;
  TABLE_NAME: string;
  TABLE_SCHEMA: string;
  COLUMN_NAME: string;
  IS_UNIQUE: boolean;
}

// ---------------------------------------------------------------------------
// Dialect-specific SQL
// ---------------------------------------------------------------------------

function fkSqlMssql(): string {
  return `SELECT
  fk.name AS FK_NAME,
  tp.name AS PARENT_TABLE,
  SCHEMA_NAME(tp.schema_id) AS PARENT_SCHEMA,
  cp.name AS PARENT_COLUMN,
  tr.name AS REFERENCED_TABLE,
  SCHEMA_NAME(tr.schema_id) AS REFERENCED_SCHEMA,
  cr.name AS REFERENCED_COLUMN
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
ORDER BY fk.name, fkc.constraint_column_id`;
}

function fkSqlPostgres(): string {
  return `SELECT
  tc.constraint_name AS "FK_NAME",
  tc.table_name AS "PARENT_TABLE",
  tc.table_schema AS "PARENT_SCHEMA",
  kcu.column_name AS "PARENT_COLUMN",
  ccu.table_name AS "REFERENCED_TABLE",
  ccu.table_schema AS "REFERENCED_SCHEMA",
  ccu.column_name AS "REFERENCED_COLUMN"
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
  ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.constraint_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.constraint_name, kcu.ordinal_position`;
}

function indexSqlMssql(): string {
  return `SELECT
  i.name AS INDEX_NAME,
  t.name AS TABLE_NAME,
  SCHEMA_NAME(t.schema_id) AS TABLE_SCHEMA,
  c.name AS COLUMN_NAME,
  i.is_unique AS IS_UNIQUE
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.tables t ON i.object_id = t.object_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.name IS NOT NULL
ORDER BY t.name, i.name, ic.key_ordinal`;
}

function indexSqlPostgres(): string {
  return `SELECT
  i.relname AS "INDEX_NAME",
  t.relname AS "TABLE_NAME",
  n.nspname AS "TABLE_SCHEMA",
  a.attname AS "COLUMN_NAME",
  ix.indisunique AS "IS_UNIQUE"
FROM pg_index ix
JOIN pg_class i ON ix.indexrelid = i.oid
JOIN pg_class t ON ix.indrelid = t.oid
JOIN pg_namespace n ON t.relnamespace = n.oid
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND t.relkind = 'r'
ORDER BY t.relname, i.relname`;
}

// ---------------------------------------------------------------------------
// Data fetching
// ---------------------------------------------------------------------------

function normalizeRow(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(row)) {
    out[key.toUpperCase()] = value;
  }
  return out;
}

export async function fetchForeignKeys(
  database: string,
  dialect: Dialect,
  connection?: string,
): Promise<ForeignKeyInfo[]> {
  const sql = dialect === "postgres" || dialect === "duckdb" ? fkSqlPostgres() : fkSqlMssql();
  const result: QueryResult = await executeQuery(sql, database, connection);
  return result.data.map((row) => {
    const r = normalizeRow(row);
    return {
      FK_NAME: String(r.FK_NAME ?? ""),
      PARENT_TABLE: String(r.PARENT_TABLE ?? ""),
      PARENT_SCHEMA: String(r.PARENT_SCHEMA ?? ""),
      PARENT_COLUMN: String(r.PARENT_COLUMN ?? ""),
      REFERENCED_TABLE: String(r.REFERENCED_TABLE ?? ""),
      REFERENCED_SCHEMA: String(r.REFERENCED_SCHEMA ?? ""),
      REFERENCED_COLUMN: String(r.REFERENCED_COLUMN ?? ""),
    };
  });
}

export async function fetchIndexes(
  database: string,
  dialect: Dialect,
  connection?: string,
): Promise<IndexInfo[]> {
  const sql = dialect === "postgres" || dialect === "duckdb" ? indexSqlPostgres() : indexSqlMssql();
  const result: QueryResult = await executeQuery(sql, database, connection);
  return result.data.map((row) => {
    const r = normalizeRow(row);
    return {
      INDEX_NAME: String(r.INDEX_NAME ?? ""),
      TABLE_NAME: String(r.TABLE_NAME ?? ""),
      TABLE_SCHEMA: String(r.TABLE_SCHEMA ?? ""),
      COLUMN_NAME: String(r.COLUMN_NAME ?? ""),
      IS_UNIQUE: r.IS_UNIQUE === true || r.IS_UNIQUE === 1 || r.IS_UNIQUE === "true",
    };
  });
}

// ---------------------------------------------------------------------------
// Filtering helpers
// ---------------------------------------------------------------------------

/** Get FKs where the given table is the parent (outgoing FKs). */
export function getForeignKeysForTable(
  allFks: ForeignKeyInfo[],
  schema: string,
  table: string,
): ForeignKeyInfo[] {
  return allFks.filter(
    (fk) => fk.PARENT_SCHEMA === schema && fk.PARENT_TABLE === table,
  );
}

/** Get FKs where the given table is referenced (incoming FKs). */
export function getReferencingForeignKeys(
  allFks: ForeignKeyInfo[],
  schema: string,
  table: string,
): ForeignKeyInfo[] {
  return allFks.filter(
    (fk) => fk.REFERENCED_SCHEMA === schema && fk.REFERENCED_TABLE === table,
  );
}

// ---------------------------------------------------------------------------
// ERD generation
// ---------------------------------------------------------------------------

function sanitizeEntityName(schema: string, table: string): string {
  const needsPrefix = schema !== "dbo" && schema !== "public";
  const raw = needsPrefix ? `${schema}_${table}` : table;
  return raw.replace(/[^a-zA-Z0-9_]/g, "_");
}

export function generateERDSyntax(
  tables: { schema: string; name: string }[],
  foreignKeys: ForeignKeyInfo[],
): string {
  const lines: string[] = ["erDiagram"];

  // Entities
  for (const t of tables) {
    const entity = sanitizeEntityName(t.schema, t.name);
    lines.push(`  ${entity} {`);
    lines.push(`  }`);
  }

  // Relationships — deduplicate by FK_NAME (composite FKs produce multiple rows)
  const seen = new Set<string>();
  for (const fk of foreignKeys) {
    if (seen.has(fk.FK_NAME)) continue;
    seen.add(fk.FK_NAME);

    const parent = sanitizeEntityName(fk.PARENT_SCHEMA, fk.PARENT_TABLE);
    const ref = sanitizeEntityName(fk.REFERENCED_SCHEMA, fk.REFERENCED_TABLE);
    lines.push(`  ${parent} }o--|| ${ref} : "${fk.FK_NAME}"`);
  }

  return lines.join("\n");
}
