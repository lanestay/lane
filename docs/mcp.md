# MCP Server

Lane includes an MCP (Model Context Protocol) server for AI tool integration, served over HTTP.

Server name: `lane`

## Endpoint

Two authentication methods are supported:

**Header auth (recommended)** — token in `x-lane-key` header, keeps credentials out of server access logs:

```
/mcp
```

```json
{
  "mcpServers": {
    "lane": {
      "type": "http",
      "url": "http://localhost:3401/mcp",
      "headers": {
        "x-lane-key": "YOUR_TOKEN"
      }
    }
  }
}
```

**Path auth** — token in the URL path (legacy, token visible in access logs):

```
/mcp/token/{token}
```

Both endpoints are available when auth is configured. Each connection is authenticated by the token, which maps to a user or service account. All [[permissions]] apply.

## Tools Reference

### Query Tools

#### `query`
Execute a SQL query against a named connection.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql` | string | Yes | SQL to execute |
| `connection` | string | Yes | Connection name |
| `database` | string | No | Target database |
| `page` | number | No | Page number |
| `page_size` | number | No | Results per page |

**Permissions**: SqlMode read required. Write queries need DML permission + [[approvals]] for Supervised/Confirmed.

#### `validate_query`
Validate SQL without executing.

| Parameter | Type | Required |
|-----------|------|----------|
| `sql` | string | Yes |
| `connection` | string | Yes |
| `database` | string | No |

#### `explain_query`
Get the execution plan for a query.

| Parameter | Type | Required |
|-----------|------|----------|
| `sql` | string | Yes |
| `connection` | string | Yes |
| `database` | string | No |

### Schema Tools

#### `list_connections`
List all available database connections (filtered by [[permissions]]).

#### `list_databases`
List databases for a connection.

| Parameter | Type | Required |
|-----------|------|----------|
| `connection` | string | Yes |

#### `list_tables`
List tables in a database.

| Parameter | Type | Required |
|-----------|------|----------|
| `connection` | string | Yes |
| `database` | string | Yes |
| `schema` | string | No |

#### `describe_table`
Get column definitions for a table.

| Parameter | Type | Required |
|-----------|------|----------|
| `connection` | string | Yes |
| `database` | string | Yes |
| `table` | string | Yes |
| `schema` | string | No |

#### `list_views`
List views in a database.

| Parameter | Type | Required |
|-----------|------|----------|
| `connection` | string | Yes |
| `database` | string | Yes |

#### `list_procedures`
List stored procedures.

| Parameter | Type | Required |
|-----------|------|----------|
| `connection` | string | Yes |
| `database` | string | Yes |

#### `get_procedure_definition`
Get stored procedure source code.

| Parameter | Type | Required |
|-----------|------|----------|
| `connection` | string | Yes |
| `database` | string | Yes |
| `procedure` | string | Yes |
| `schema` | string | No |

### Write Tools (Approval Required for Supervised/Confirmed)

#### `bulk_update`
Execute an UPDATE statement.

| Parameter | Type | Required |
|-----------|------|----------|
| `sql` | string | Yes |
| `connection` | string | Yes |
| `database` | string | No |

**Permissions**: SqlMode DML required. Triggers [[approvals]] for Supervised/Confirmed.

#### `bulk_insert`
Execute an INSERT statement.

Same parameters as `bulk_update`. Same approval requirements.

#### `run_migration`
Execute DDL (CREATE, ALTER, DROP).

| Parameter | Type | Required |
|-----------|------|----------|
| `sql` | string | Yes |
| `connection` | string | Yes |
| `database` | string | No |

**Permissions**: SqlMode DDL required (Full mode only). Triggers [[approvals]] for Supervised/Confirmed.

### Search Tools

See [[search]] for details on the FTS5 search system.

#### `search_schema`
Search database schema (tables, views, columns) across all connections.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | Yes | Search query (e.g. table name, column name) |
| `limit` | number | No | Max results (default: 10, max: 50) |

#### `search_queries`
Search query history by keyword.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | Yes | Search query (e.g. SQL keyword, table name) |
| `limit` | number | No | Max results (default: 10, max: 50) |

#### `search_endpoints`
Search named data endpoints by keyword.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | Yes | Search query (e.g. endpoint name, description) |
| `limit` | number | No | Max results (default: 10, max: 50) |

### History Tools

#### `query_history`
Get recent query history for the current user.

| Parameter | Type | Required |
|-----------|------|----------|
| `limit` | number | No |
| `offset` | number | No |

### Storage Tools

Requires `storage` feature flag.

#### `storage_list_buckets`
List buckets for a storage connection.

| Parameter | Type | Required |
|-----------|------|----------|
| `connection` | string | Yes |

**Permissions**: Connection access required.

#### `storage_list_objects`
List objects in a bucket.

| Parameter | Type | Required |
|-----------|------|----------|
| `connection` | string | Yes |
| `bucket` | string | Yes |
| `prefix` | string | No |

**Permissions**: Bucket read required.

#### `storage_upload`
Upload content to a storage bucket.

| Parameter | Type | Required |
|-----------|------|----------|
| `connection` | string | Yes |
| `bucket` | string | Yes |
| `key` | string | Yes |
| `content` | string | Yes |
| `content_type` | string | No |

**Permissions**: Bucket write required. Triggers [[approvals]] for Supervised/Confirmed. Audit logged.

#### `storage_download_to_workspace`
Download a storage object into a [[workspace]] table.

| Parameter | Type | Required |
|-----------|------|----------|
| `connection` | string | Yes |
| `bucket` | string | Yes |
| `key` | string | Yes |
| `table_name` | string | No |

**Permissions**: Bucket read required.

#### `storage_get_url`
Get a presigned URL for a storage object.

| Parameter | Type | Required |
|-----------|------|----------|
| `connection` | string | Yes |
| `bucket` | string | Yes |
| `key` | string | Yes |

**Permissions**: Bucket read required.

#### `storage_export_query`
Execute a SQL query and upload results to a storage bucket.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `connection` | string | No | Database connection (default if omitted) |
| `database` | string | No | Target database |
| `query` | string | Yes | SQL query to execute |
| `storage_connection` | string | No | Storage connection (first available if omitted) |
| `bucket` | string | Yes | Target bucket |
| `key` | string | Yes | Object key (e.g. `exports/results.csv`) |
| `format` | string | No | `csv`, `json`, or `xlsx` (auto-detected from key) |

**Permissions**: DB read + bucket write required. Triggers [[approvals]] for Supervised/Confirmed. Audit logged.

#### `workspace_export_to_storage`
Export workspace query results to a storage bucket.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | Yes | SQL query against workspace |
| `storage_connection` | string | No | Storage connection (first available if omitted) |
| `bucket` | string | Yes | Target bucket |
| `key` | string | Yes | Object key (e.g. `exports/data.parquet`) |
| `format` | string | No | `csv`, `json`, or `parquet` (auto-detected from key) |

**Permissions**: Bucket write required. Triggers [[approvals]] for Supervised/Confirmed. Parquet uses DuckDB native `COPY` for efficiency.

### Workspace Tools

#### `workspace_query`
Execute SQL against the DuckDB [[workspace]].

| Parameter | Type | Required |
|-----------|------|----------|
| `sql` | string | Yes |

#### `workspace_list_tables`
List tables in the workspace.

#### `workspace_import`
Import data into a workspace table.

| Parameter | Type | Required |
|-----------|------|----------|
| `data` | string | Yes |
| `format` | string | Yes |
| `table_name` | string | Yes |

## Permission Matrix

| Tool | SqlMode | Connection | Database/Table | Bucket | Approval |
|------|---------|------------|---------------|--------|----------|
| `query` (read) | Read | Yes | Yes | — | — |
| `query` (write) | DML | Yes | Yes | — | Supervised/Confirmed |
| `bulk_update` | DML | Yes | Yes | — | Supervised/Confirmed |
| `bulk_insert` | DML | Yes | Yes | — | Supervised/Confirmed |
| `run_migration` | DDL | Yes | Yes | — | Supervised/Confirmed |
| `list_connections` | Read | Filtered | — | — | — |
| `list_databases` | Read | Yes | — | — | — |
| `list_tables` | Read | Yes | Yes | — | — |
| `describe_table` | Read | Yes | Yes | — | — |
| `storage_list_buckets` | — | Yes | — | — | — |
| `storage_list_objects` | — | Yes | — | Read | — |
| `storage_upload` | — | Yes | — | Write | Supervised/Confirmed |
| `storage_download_to_workspace` | — | Yes | — | Read | — |
| `storage_get_url` | — | Yes | — | Read | — |
| `storage_export_query` | Read | Yes | Yes | Write | Supervised/Confirmed |
| `workspace_export_to_storage` | Supervised | — | — | Write | Supervised/Confirmed |
| `search_schema` | Read | — | — | — | — |
| `search_queries` | Read | — | — | — | — |
| `search_endpoints` | Read | — | — | — | — |

## Related

- [[permissions]] — Full permission model
- [[approvals]] — Approval flow for write tools
- [[storage]] — Storage system details
- [[workspace]] — Workspace system details
