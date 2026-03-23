# Connections

Lane supports multiple named database and storage connections, configured via a JSON config file.

## Config File Format

```json
{
  "connections": [
    {
      "name": "production",
      "connection_type": "mssql",
      "host": "sql.example.com",
      "port": 1433,
      "user": "reader",
      "password": "secret",
      "database": "mydb",
      "instance": "SQLEXPRESS"
    },
    {
      "name": "analytics",
      "connection_type": "postgres",
      "host": "pg.example.com",
      "port": 5432,
      "user": "analyst",
      "password": "secret",
      "database": "analytics"
    },
    {
      "name": "olap",
      "connection_type": "clickhouse",
      "host": "clickhouse.example.com",
      "port": 8123,
      "database": "default"
    },
    {
      "name": "local-files",
      "connection_type": "minio",
      "endpoint": "http://localhost:9000",
      "access_key": "minioadmin",
      "secret_key": "minioadmin",
      "region": "us-east-1"
    }
  ]
}
```

## Connection Types

### MSSQL

- Backend: Tiberius with bb8 connection pool (max 10)
- Database switching via `USE [db]` per query
- Validation: `SET NOEXEC ON` (errors 207/208 skip validation and fall through for enrichment)
- Pagination: `OFFSET/FETCH`
- Row limits: `SELECT TOP N`

| Field | Required | Description |
|-------|----------|-------------|
| `host` | Yes | Server hostname |
| `port` | No | Port (default: 1433) |
| `user` | Yes | Username |
| `password` | Yes | Password |
| `database` | No | Default database |
| `instance` | No | Named instance |

### PostgreSQL

- Backend: tokio-postgres with deadpool
- Lazy per-database pool creation (max 10 pools per connection)
- No `USE` command — separate pool per database
- Validation: `PREPARE` / `DEALLOCATE`
- Pagination: `LIMIT/OFFSET`

| Field | Required | Description |
|-------|----------|-------------|
| `host` | Yes | Server hostname |
| `port` | No | Port (default: 5432) |
| `user` | Yes | Username |
| `password` | Yes | Password |
| `database` | Yes | Database name |

### ClickHouse

- Backend: HTTP API with reqwest (uses `FORMAT JSON` for typed results)
- OLAP-oriented — best for large-scale analytics, time-series, and aggregation queries
- Database switching via `?database=` query parameter
- Pagination: `LIMIT/OFFSET`
- Row limits: `LIMIT N`

| Field | Required | Description |
|-------|----------|-------------|
| `host` | Yes | Server hostname |
| `port` | No | HTTP port (default: 8123) |
| `user` | No | Username (default: default) |
| `password` | No | Password (default: empty) |
| `database` | No | Default database (default: default) |

```json
{
  "name": "clickhouse",
  "connection_type": "clickhouse",
  "host": "clickhouse.example.com",
  "port": 8123,
  "user": "default",
  "password": "",
  "database": "default"
}
```

### DuckDB

- Backend: in-process DuckDB
- Feature flag: `duckdb_backend`

| Field | Required | Description |
|-------|----------|-------------|
| `path` | Yes | Path to `.duckdb` file |

### MinIO / S3

- Backend: rust-s3
- Feature flag: `storage`
- See [[storage]] for usage details

| Field | Required | Description |
|-------|----------|-------------|
| `endpoint` | Yes | S3-compatible endpoint URL |
| `access_key` | Yes | Access key |
| `secret_key` | Yes | Secret key |
| `region` | No | Region (default: us-east-1) |

## Legacy Config

Single-connection format (auto-detected):

```json
{
  "host": "sql.example.com",
  "port": 1433,
  "user": "reader",
  "password": "secret",
  "database": "mydb"
}
```

Also supports environment variables — see [[setup#Environment Variables]].

## ConnectionRegistry

Internally, connections are managed by `ConnectionRegistry`:

- `resolve(name)` returns `Arc<dyn DatabaseBackend>` for database connections
- `StorageRegistry` manages MinIO/S3 connections separately
- Connection names are used throughout the API, MCP tools, and [[permissions]] system
