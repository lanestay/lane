# Full-Text Search

Lane includes built-in full-text search powered by SQLite FTS5. Search across database schema (tables, columns), query history, and named endpoints from a single query.

## How It Works

A separate encrypted SQLite database (`data/search.db`) stores FTS5 virtual tables. It uses the same SQLCipher encryption key as the auth database (`LANE_CIPHER_KEY`). The index contains schema metadata, query history text, and endpoint definitions.

**Tokenizer**: Porter stemming + unicode61 — handles plurals, case-insensitive matching, and works with SQL keywords.

## What Gets Indexed

| Type | Source | Indexed Fields |
|------|--------|---------------|
| **Schema** | All connected databases | connection, database, schema, table/view name, column names |
| **Queries** | Query execution history | email, connection, database, SQL text |
| **Endpoints** | Named data endpoints | name, connection, database, description, query SQL |

## Indexing

### Automatic (Startup)

On server startup, a background indexer crawls all database connections:
- Enumerates databases, schemas, tables, and views
- Fetches column names via `describe_table`
- Indexes existing query history and endpoints from the auth database

Server logs will show: `Search indexer: complete — N schema objects, N queries, N endpoints`

### Incremental

- **Queries** are indexed automatically after each successful execution
- **Endpoints** are indexed on create/update and removed on delete

### Manual Re-index

Admins can trigger a full re-index via the API:

```
POST /api/lane/admin/search/reindex
```

## REST API

All search routes require authentication. See [[api]] for the full reference.

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/lane/search?q=...&limit=20` | GET | User | Unified search (all types) |
| `/api/lane/search/schema?q=...` | GET | User | Schema objects only |
| `/api/lane/search/queries?q=...&email=...` | GET | User | Query history only |
| `/api/lane/search/endpoints?q=...` | GET | User | Endpoints only |
| `/api/lane/admin/search/reindex` | POST | Admin | Trigger full re-index |
| `/api/lane/admin/search/stats` | GET | Admin | Index row counts |

**Parameters**:
- `q` (required) — search query string
- `limit` (optional) — max results per type (default: 20, max: 100)
- `email` (optional, queries only) — filter by user email

**Unified search response**:
```json
{
  "schema": [
    {
      "connection": "production",
      "database": "mydb",
      "schema": "dbo",
      "object_name": "Employees",
      "object_type": "table",
      "columns": "EmployeeID FirstName LastName Department",
      "rank": -1.5
    }
  ],
  "queries": [
    {
      "email": "user@example.com",
      "connection": "production",
      "database": "mydb",
      "sql_text": "SELECT * FROM Employees WHERE Department = 'Engineering'",
      "rank": -2.1
    }
  ],
  "endpoints": [
    {
      "name": "active-users",
      "connection": "production",
      "database": "mydb",
      "description": "List active users",
      "query": "SELECT * FROM Users WHERE active = 1",
      "rank": -1.8
    }
  ]
}
```

## MCP Tools

Three search tools are available in the MCP server. See [[mcp]] for the full reference.

| Tool | Description |
|------|-------------|
| `search_schema` | Find tables, views, and columns by keyword |
| `search_queries` | Find past queries by keyword |
| `search_endpoints` | Find named endpoints by keyword |

All accept `query` (required) and `limit` (optional, default: 10, max: 50).

These complement existing schema tools — an LLM can `search_schema` to find the right tables, then `describe_table` for details, then `query` to fetch data.

## UI — Cmd+K Search

Press **Cmd+K** (Mac) or **Ctrl+K** (Windows/Linux) anywhere in the UI to open the search dialog.

- Type to search across tables, columns, queries, and endpoints
- Results grouped by type: **Schema**, **Query History**, **Endpoints**
- Click a schema result to navigate with the connection/database pre-selected
- Click a query result to load the SQL into the editor
- Click an endpoint result to go to the admin page
- Debounced input (300ms) for responsive results

## Related

- [[api]] — REST API reference
- [[mcp]] — MCP tools reference
- [[endpoints]] — Named data endpoints
