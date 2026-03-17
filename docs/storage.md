# Storage

Lane supports MinIO/S3-compatible object storage for browsing, uploading, downloading, and previewing files. Storage integrates with the query engine and [[workspace]] for seamless data flow between databases, workspace, and object storage.

Requires the `storage` feature flag. Features that interact with the workspace also require `duckdb_backend`.

## Configuration

Add a MinIO/S3 connection in your config file. See [[connections#MinIO / S3]].

## Features

- **Browse** — list buckets and objects with folder-style navigation
- **Upload** — upload files to any bucket (with [[permissions#Storage Permissions|permission]])
- **Download** — download individual objects
- **Preview** — in-browser preview for CSV, JSON, Parquet, text, and images
- **Metadata** — view object size, content type, last modified, and ETag
- **Bucket management** — create and delete buckets
- **Export query results** — run a SQL query and save results directly to storage as CSV, JSON, or XLSX
- **Import to workspace** — download a file from storage and load it into the DuckDB [[workspace]]
- **Workspace export** — query the workspace and upload results to storage as CSV, JSON, or Parquet

## REST Endpoints

| Endpoint | Method | Permission | Description |
|----------|--------|------------|-------------|
| `/api/lane/storage/connections` | GET | Auth | List storage connections (filtered by [[permissions]]) |
| `/api/lane/storage/buckets` | GET | Connection | List buckets |
| `/api/lane/storage/buckets` | POST | Write | Create bucket |
| `/api/lane/storage/buckets/{name}` | DELETE | Delete | Delete bucket |
| `/api/lane/storage/objects` | GET | Read | List objects (supports `prefix` query param) |
| `/api/lane/storage/upload` | POST | Write | Upload object (multipart) |
| `/api/lane/storage/download` | GET | Read | Download object |
| `/api/lane/storage/objects` | DELETE | Delete | Delete object |
| `/api/lane/storage/metadata` | GET | Read | Object metadata |
| `/api/lane/storage/preview` | POST | Read | Preview in workspace |
| `/api/lane/storage/export-query` | POST | Write | Export query results to storage |
| `/api/lane/storage/import-to-workspace` | POST | Read | Import file to workspace |
| `/api/lane/storage/workspace-export` | POST | Write | Export workspace query to storage |

All write operations are audit-logged. See [[permissions#Audit Logging]].

### Export Query Results to Storage

`POST /api/lane/storage/export-query`

Executes a SQL query against a database connection and uploads the results to storage.

```json
{
  "connection": "production",
  "database": "mydb",
  "query": "SELECT * FROM users WHERE active = 1",
  "storage_connection": "minio",
  "bucket": "exports",
  "key": "reports/active_users.csv",
  "format": "csv"
}
```

**Response**: `{ "success": true, "key": "...", "size": 12345, "row_count": 100, "format": "csv" }`

**Formats**: `csv`, `json`, `xlsx`. Auto-detected from key extension if `format` is omitted.

### Import from Storage to Workspace

`POST /api/lane/storage/import-to-workspace`

Downloads a file from storage and loads it into a DuckDB [[workspace]] table. Requires `duckdb_backend` feature.

```json
{
  "connection": "minio",
  "bucket": "data",
  "key": "datasets/sales.parquet",
  "table_name": "sales"
}
```

**Response**: `{ "success": true, "table_name": "sales", "row_count": 5000, "source": { "connection": "minio", "bucket": "data", "key": "datasets/sales.parquet" } }`

Supported file types: CSV, TSV, Parquet, JSON, JSONL, NDJSON, XLSX, XLS. Table name is auto-generated from the filename if omitted.

### Workspace Export to Storage

`POST /api/lane/storage/workspace-export`

Queries the DuckDB workspace and uploads results to storage. Requires `duckdb_backend` feature.

```json
{
  "query": "SELECT * FROM sales WHERE region = 'US'",
  "storage_connection": "minio",
  "bucket": "exports",
  "key": "reports/us_sales.parquet",
  "format": "parquet"
}
```

**Response**: `{ "success": true, "key": "...", "size": 45678, "row_count": 2500, "format": "parquet" }`

**Formats**: `csv`, `json`, `parquet`. Parquet uses DuckDB's native `COPY` for efficient export. Format is auto-detected from key extension if omitted.

## Storage Column Links

Query results can contain references to storage objects. When a column value matches the pattern `s3://{connection}/{bucket}/{key}`, the UI renders it as a clickable link to the storage browser.

## MCP Tools

[[mcp]] provides storage tools:

| Tool | Permission | Approval |
|------|-----------|----------|
| `storage_list_buckets` | Connection | — |
| `storage_list_objects` | Read | — |
| `storage_upload` | Write | Yes (Supervised/Confirmed) |
| `storage_download_to_workspace` | Read | — |
| `storage_get_url` | Read | — |
| `storage_export_query` | Write | Yes (Supervised/Confirmed) |
| `workspace_export_to_storage` | Write | Yes (Supervised/Confirmed) |

## UI

The [[ui#Storage Page]] provides a full file browser with folder navigation, drag-and-drop upload, inline preview, and import-to-workspace. The **Save to Storage** dialog lets you export query results or workspace data directly to storage from the results toolbar.

## Related

- [[connections#MinIO / S3]] — Connection configuration
- [[permissions#Storage Permissions]] — Bucket-level access control
- [[workspace]] — Download storage objects into the DuckDB workspace
