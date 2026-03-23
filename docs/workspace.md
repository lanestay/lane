# Workspace

The workspace is a per-session DuckDB database for importing, transforming, and exporting data.

Requires the `duckdb_backend` feature flag.

## Features

- **Import** — import query results or storage objects into workspace tables
- **Query** — run SQL against workspace tables (joins, aggregations, etc.)
- **Export** — export workspace tables as CSV, JSON, or Excel
- **Storage download** — pull objects from [[storage]] directly into workspace
- **Import from storage** — download a file from storage and load it as a workspace table (via REST or UI)
- **Export to storage** — query the workspace and upload results to storage as CSV, JSON, or Parquet

## Import Flow

1. **Preview**: `POST /api/lane/workspace/import/preview` — parse a file (CSV, JSON, Parquet) and return schema + sample rows
2. **Execute**: `POST /api/lane/workspace/import/execute` — create a workspace table from the file
3. The imported table is available for SQL queries against the workspace connection

## REST Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/workspace/tables` | GET | List workspace tables |
| `/api/lane/workspace/import-query` | POST | Import query results from any connection |
| `/api/lane/workspace/query` | POST | Execute SQL against workspace |
| `/api/lane/workspace/import/preview` | POST | Preview import (schema + sample) |
| `/api/lane/workspace/import/execute` | POST | Execute import |
| `/api/lane/workspace/export/{table}` | GET | Export table (format query param) |
| `/api/lane/workspace/tables/{table}` | DELETE | Drop workspace table |

## Cross-Connection Import

Import query results from any database connection (MSSQL, Postgres, ClickHouse) into a workspace table:

```
POST /api/lane/workspace/import-query
{
  "connection": "clickhouse",
  "database": "default",
  "query": "SELECT * FROM trips LIMIT 10000",
  "table_name": "trips",
  "if_exists": "replace"
}
```

- `connection` — name of the source connection (optional, uses default)
- `database` — database on the source connection (optional, uses connection default)
- `query` — must be a SELECT/read-only query
- `table_name` — name for the workspace table
- `if_exists` — `"replace"` to overwrite, or omit to fail if table exists

**Auth requirements:** authenticated + supervised SQL mode + connection-level access to the source.

Once imported, tables from different connections can be joined in the workspace via `/workspace/query`.

## MCP Tools

[[mcp]] provides workspace tools:

| Tool | Description |
|------|-------------|
| `workspace_query` | Run SQL against the workspace |
| `workspace_list_tables` | List workspace tables |
| `workspace_import` | Import data into workspace |
| `storage_download_to_workspace` | Download storage object → workspace table |
| `workspace_export_to_storage` | Export workspace query results to storage |

## Storage Integration

Data flows bidirectionally between storage and workspace:

**Into workspace:**
- `storage_download_to_workspace` MCP tool
- `POST /api/lane/storage/import-to-workspace` REST endpoint
- "Import" button on the [[ui#Storage Page]]
- Supported formats: CSV, TSV, Parquet, JSON, JSONL, NDJSON, XLSX, XLS

**From workspace to storage:**
- `workspace_export_to_storage` MCP tool
- `POST /api/lane/storage/workspace-export` REST endpoint
- Supported formats: CSV, JSON, Parquet (Parquet uses DuckDB native `COPY`)

See [[storage]] for full details.

## UI

The [[ui#Workspace Page]] and [[ui#Import Page]] provide the visual interface for workspace operations.

## Related

- [[storage]] — Object storage system
- [[mcp]] — MCP workspace and storage tools
- [[query-engine]] — Main query execution system
