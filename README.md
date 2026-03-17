# lane

Self-contained database platform — query engine, REST API, MCP server, and React admin console — shipped as a single Rust binary. One process, no external dependencies, deploys anywhere.

## What it does

- **Query any database from one place** — connect SQL Server, PostgreSQL, and DuckDB by name. Run queries through the REST API, MCP server, or web UI.
- **Turn queries into APIs** — save SQL as named endpoints with `{{parameters}}`. Consumers call by name, no SQL needed.
- **Cross-database analysis** — export query results to S3/MinIO, import into the DuckDB workspace, join data across databases, export results back to storage as CSV, JSON, or Parquet.
- **Give AI agents safe database access** — 33 MCP tools with read/write separation, permission tiers, and approval workflows.
- **Control access** — four SQL permission tiers, per-table CRUD, per-connection restrictions, service accounts, PII detection/redaction, and teams.
- **Stream changes** — SSE on any table, no polling, no external broker.

## Quick Start

### Standalone

```bash
cargo build --release --features postgres,duckdb_backend,storage,webui
./target/release/lane
```

### Docker (standalone)

```bash
docker compose up
```

### Docker (full stack with MSSQL, Postgres, MinIO)

```bash
cp .env.example .env    # defaults work out of the box
docker compose -f docker-compose.full.yml up
```

The server starts on **http://localhost:3401**. Visit `/setup` to create your admin account.

## Connections

Add database and storage connections through the admin API or web UI:

```bash
# Add a SQL Server connection
curl -X POST http://localhost:3401/api/lane/admin/connections \
  -H "Content-Type: application/json" \
  -H "x-lane-key: YOUR_API_KEY" \
  -d '{
    "name": "production",
    "type": "mssql",
    "host": "sql.example.com",
    "port": 1433,
    "database": "master",
    "username": "sa",
    "password": "secret"
  }'

# Add a PostgreSQL connection
curl -X POST http://localhost:3401/api/lane/admin/connections \
  -H "Content-Type: application/json" \
  -H "x-lane-key: YOUR_API_KEY" \
  -d '{
    "name": "analytics",
    "type": "postgres",
    "host": "pg.example.com",
    "port": 5432,
    "database": "postgres",
    "username": "postgres",
    "password": "secret"
  }'

# Add MinIO/S3 storage
curl -X POST http://localhost:3401/api/lane/admin/connections \
  -H "Content-Type: application/json" \
  -H "x-lane-key: YOUR_API_KEY" \
  -d '{
    "name": "storage",
    "type": "minio",
    "host": "minio.example.com",
    "port": 9000,
    "database": "",
    "username": "minioadmin",
    "password": "minioadmin"
  }'
```

## Query

```bash
curl http://localhost:3401/api/lane \
  -H "Content-Type: application/json" \
  -H "x-lane-key: YOUR_API_KEY" \
  -d '{
    "query": "SELECT * FROM users ORDER BY created_at DESC",
    "connection": "production",
    "database": "mydb",
    "page": 1,
    "page_size": 100
  }'
```

## Storage Integration

Export query results to S3, import files into the DuckDB workspace, and export workspace analysis back to storage:

```bash
# Export SQL query results to MinIO as CSV
curl -X POST http://localhost:3401/api/lane/storage/export-query \
  -H "Content-Type: application/json" \
  -H "x-lane-key: YOUR_API_KEY" \
  -d '{
    "connection": "production",
    "database": "mydb",
    "query": "SELECT * FROM orders",
    "storage_connection": "storage",
    "bucket": "exports",
    "key": "reports/orders.csv"
  }'

# Import that file into the DuckDB workspace
curl -X POST http://localhost:3401/api/lane/storage/import-to-workspace \
  -H "Content-Type: application/json" \
  -H "x-lane-key: YOUR_API_KEY" \
  -d '{
    "connection": "storage",
    "bucket": "exports",
    "key": "reports/orders.csv",
    "table_name": "orders"
  }'

# Query the workspace and export to Parquet
curl -X POST http://localhost:3401/api/lane/storage/workspace-export \
  -H "Content-Type: application/json" \
  -H "x-lane-key: YOUR_API_KEY" \
  -d '{
    "query": "SELECT * FROM orders WHERE total > 1000",
    "storage_connection": "storage",
    "bucket": "exports",
    "key": "reports/large-orders.parquet"
  }'
```

## MCP Server

The MCP server exposes 33 tools for AI agent integration. Connect via streamable HTTP with header-based auth (keeps tokens out of server logs):

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

Agents can query databases, explore schemas, search across connections, export results to storage, import into the workspace, join data across databases, and export analysis — all through the same permission model as human users.

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `mssql` | yes | SQL Server backend (Tiberius) |
| `mcp` | yes | MCP server (HTTP) |
| `xlsx` | yes | Excel export |
| `semantic` | yes | Semantic search |
| `postgres` | no | PostgreSQL backend (tokio-postgres) |
| `duckdb_backend` | no | DuckDB workspace for cross-database analysis |
| `storage` | no | MinIO/S3 object storage |
| `webui` | no | Embedded React admin UI |

```bash
# Full build
cargo build --release --features postgres,duckdb_backend,storage,webui
```

## Authentication

Requests authenticate via `x-lane-key` header or session cookie. The system API key is auto-generated on first startup.

Multiple auth methods are supported — email+password, passwordless email code login (when SMTP is configured), Tailscale identity, and OIDC (Google, Microsoft, GitHub). Set `LANE_AUTH` to enable providers. All users must be pre-created by an admin.

Users and service accounts are managed through the admin API or web UI. Each has a `sql_mode` (None, Read Only, Supervised, Confirmed, Full), per-database/table CRUD permissions, per-connection access, and optional storage permissions.

## Documentation

Full documentation is in the [`docs/`](docs/index.md) directory:

- [Setup](docs/setup.md) — installation and first-run
- [Connections](docs/connections.md) — database and storage configuration
- [Query Engine](docs/query-engine.md) — SQL execution, validation, pagination
- [Endpoints](docs/endpoints.md) — named, parameterized data APIs
- [Storage](docs/storage.md) — S3/MinIO integration and data flow
- [Workspace](docs/workspace.md) — DuckDB cross-database analysis
- [MCP](docs/mcp.md) — 33 MCP tools reference
- [API](docs/api.md) — REST API reference
- [Auth](docs/auth.md) — authentication methods
- [Permissions](docs/permissions.md) — access control model
- [PII](docs/pii.md) — PII detection and redaction
- [UI](docs/ui.md) — web UI pages and features

## Contributing

lane is a personal project. Pull requests from trusted collaborators are welcome — unsolicited PRs will generally be closed.

If you find a bug or security issue, please open an issue. You're welcome to fork and customize lane for your own use — the AGPL-3.0 license requires that any modifications distributed or made available over a network be released under the same license.

## License

lane is licensed under the [GNU Affero General Public License v3.0](LICENSE).

This software is provided as-is, without warranty of any kind. See the license for details.
