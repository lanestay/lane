export interface SqlTemplate {
  name: string;
  description: string;
  dialects: ("mssql" | "postgres" | "duckdb")[];
  sql: string;
}

export const templates: SqlTemplate[] = [
  {
    name: "Top 100 Rows",
    description: "Select top 100 rows from a table",
    dialects: ["mssql"],
    sql: "SELECT TOP 100 *\nFROM [table_name]\nORDER BY 1;",
  },
  {
    name: "Top 100 Rows",
    description: "Select first 100 rows from a table",
    dialects: ["postgres", "duckdb"],
    sql: "SELECT *\nFROM table_name\nORDER BY 1\nLIMIT 100;",
  },
  {
    name: "Table Sizes",
    description: "Show table row counts and disk usage",
    dialects: ["mssql"],
    sql: `SELECT
    s.name AS SchemaName,
    t.name AS TableName,
    p.rows AS RowCount,
    SUM(a.total_pages) * 8 / 1024 AS TotalSizeMB
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.indexes i ON t.object_id = i.object_id
JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE i.index_id <= 1
GROUP BY s.name, t.name, p.rows
ORDER BY p.rows DESC;`,
  },
  {
    name: "Table Sizes",
    description: "Show table row counts and disk usage",
    dialects: ["postgres"],
    sql: `SELECT
    schemaname,
    relname AS table_name,
    n_live_tup AS row_count,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;`,
  },
  {
    name: "Active Queries",
    description: "Show currently running queries",
    dialects: ["mssql"],
    sql: `SELECT
    r.session_id,
    r.status,
    r.command,
    r.wait_type,
    r.wait_time,
    r.cpu_time,
    r.total_elapsed_time,
    SUBSTRING(t.text, r.statement_start_offset/2 + 1,
        (CASE WHEN r.statement_end_offset = -1
            THEN LEN(CONVERT(nvarchar(max), t.text)) * 2
            ELSE r.statement_end_offset END - r.statement_start_offset) / 2 + 1
    ) AS current_statement
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE r.session_id <> @@SPID
ORDER BY r.total_elapsed_time DESC;`,
  },
  {
    name: "Active Queries",
    description: "Show currently running queries",
    dialects: ["postgres"],
    sql: `SELECT
    pid,
    state,
    query_start,
    now() - query_start AS duration,
    query
FROM pg_stat_activity
WHERE state = 'active'
  AND pid <> pg_backend_pid()
ORDER BY query_start;`,
  },
  {
    name: "Blocking Queries",
    description: "Show blocked and blocking sessions",
    dialects: ["mssql"],
    sql: `SELECT
    blocked.session_id AS blocked_session,
    blocked.wait_type,
    blocked.wait_time AS wait_ms,
    blocker.session_id AS blocker_session,
    blocker_text.text AS blocker_sql,
    blocked_text.text AS blocked_sql
FROM sys.dm_exec_requests blocked
JOIN sys.dm_exec_sessions blocker ON blocked.blocking_session_id = blocker.session_id
CROSS APPLY sys.dm_exec_sql_text(blocked.sql_handle) blocked_text
OUTER APPLY sys.dm_exec_sql_text(blocker.most_recent_sql_handle) blocker_text
WHERE blocked.blocking_session_id > 0;`,
  },
  {
    name: "Index Usage",
    description: "Show index usage statistics",
    dialects: ["mssql"],
    sql: `SELECT
    OBJECT_NAME(s.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates,
    s.last_user_seek,
    s.last_user_scan
FROM sys.dm_db_index_usage_stats s
JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
ORDER BY s.user_seeks + s.user_scans + s.user_lookups DESC;`,
  },
  {
    name: "Database Sizes",
    description: "Show size of each database",
    dialects: ["postgres"],
    sql: `SELECT
    datname AS database_name,
    pg_size_pretty(pg_database_size(datname)) AS size
FROM pg_database
WHERE datistemplate = false
ORDER BY pg_database_size(datname) DESC;`,
  },
];

export function getTemplatesForDialect(dialect?: string): SqlTemplate[] {
  if (!dialect) return templates;
  return templates.filter((t) => t.dialects.includes(dialect as "mssql" | "postgres" | "duckdb"));
}
