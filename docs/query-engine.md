# Query Engine

The core of Lane — executes SQL queries against named database connections with validation, pagination, and access control.

## Execution Flow

1. Receive query via [[api]] or [[mcp]]
2. Resolve connection from `ConnectionRegistry`
3. Check [[permissions]] (SqlMode, connection, database/table)
4. Validate SQL (dialect-aware)
5. If write + Supervised/Confirmed → queue for [[approvals]]
6. Execute query with row limits and pagination
7. Apply [[pii]] redaction to results
8. Log to query history and access log
9. Return results

## SQL Validation

Dialect-aware validation before execution:

| Backend | Method | Notes |
|---------|--------|-------|
| MSSQL | `SET NOEXEC ON` | Errors 207/208 skip validation, fall through for enrichment |
| PostgreSQL | `PREPARE` / `DEALLOCATE` | Standard prepared statement validation |

## Pagination

Dialect-aware pagination applied automatically:

| Backend | Method |
|---------|--------|
| MSSQL | `OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY` |
| PostgreSQL | `LIMIT {limit} OFFSET {offset}` |

## Row Limits

Configurable max row limits enforced per query:

| Backend | Method |
|---------|--------|
| MSSQL | `SELECT TOP N` injection |
| PostgreSQL | `LIMIT N` injection |

## Error Handling

SQL errors are parsed and enriched with structured error responses:

- `parse_sql_error()` — extracts error codes and messages
- `map_sql_error()` — maps to user-friendly error categories
- MSSQL errors include line numbers and severity levels

## Query History

All queries are logged to `query_history` with:
- Email, connection, database
- SQL text
- Execution time (ms), row count
- Success/error status
- Favorite flag (user can toggle)

## REST Endpoint

```
POST /api/lane/query
Body: {
  "sql": "SELECT * FROM users",
  "connection": "production",
  "database": "mydb",
  "page": 1,
  "page_size": 100
}
```

See [[api]] for full endpoint documentation.

## Realtime Monitoring

Active queries can be monitored via the [[realtime]] system with SSE streaming.

## Related

- [[connections]] — Database backends and connection config
- [[permissions]] — Access control for queries
- [[approvals]] — Write query approval flow
- [[pii]] — Result redaction
- [[mcp]] — MCP tool query interface
