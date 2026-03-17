# Setup

## Installation

Build from source with Cargo:

```bash
# Default features (mssql, mcp, xlsx)
cargo build --release

# All features
cargo build --release --features "mssql,postgres,mcp,xlsx,storage,duckdb_backend,webui"
```

The binary is called `lane`.

## Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `mssql` | Yes | MSSQL (Tiberius) backend |
| `postgres` | No | PostgreSQL (tokio-postgres + deadpool) backend |
| `duckdb_backend` | No | DuckDB backend |
| `storage` | No | MinIO/S3 object storage |
| `mcp` | Yes | MCP stdio server |
| `xlsx` | Yes | Excel export |
| `webui` | No | Embedded React UI (rust-embed) |

## Running

```bash
lane
```

The MCP server is available over HTTP at `/mcp/token/{token}` when auth is configured. See [[mcp]].

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PORT` | HTTP port (default: 3401) |
| `HOST` | Bind address (default: 127.0.0.1, use 0.0.0.0 for network access) |
| `LANE_AUTH` | Auth providers, comma-separated (default: `email`). Options: `email`, `tailscale`, `google`, `microsoft`, `github` |
| `LANE_AUTH_DB` | Path to SQLCipher auth database |
| `LANE_BASE_URL` | Public URL for OIDC callbacks (required when using google/microsoft/github) |
| `LANE_CONFIG` | Path to config file |
| `LANE_GOOGLE_CLIENT_ID` | Google OAuth client ID |
| `LANE_GOOGLE_CLIENT_SECRET` | Google OAuth client secret |
| `LANE_MICROSOFT_CLIENT_ID` | Microsoft OAuth client ID |
| `LANE_MICROSOFT_CLIENT_SECRET` | Microsoft OAuth client secret |
| `LANE_GITHUB_CLIENT_ID` | GitHub OAuth client ID |
| `LANE_GITHUB_CLIENT_SECRET` | GitHub OAuth client secret |
| `LANE_SMTP_HOST` | SMTP server — enables email code login when set |
| `LANE_SMTP_PORT` | SMTP port (default: 587) |
| `LANE_SMTP_TLS` | `none`, `starttls` (default), or `tls` |
| `LANE_SMTP_FROM` | From address (default: `noreply@lane.local`) |
| `LANE_SMTP_USERNAME` | SMTP username (optional) |
| `LANE_SMTP_PASSWORD` | SMTP password (optional) |
| `LANE_LOG_LEVEL` | Log level (trace/debug/info/warn/error) |

Legacy single-connection env vars (when no config file):

| Variable | Description |
|----------|-------------|
| `DB_HOST` | Database host |
| `DB_PORT` | Database port |
| `DB_USER` | Database user |
| `DB_PASSWORD` | Database password |
| `DB_NAME` | Database name |
| `DB_INSTANCE` | MSSQL named instance |

## Configuration File

See [[connections]] for the config file format.

## First-Run Setup

On first startup:

1. An API key is auto-generated and stored in the encrypted auth database
2. The auth database schema is initialized (SQLCipher-encrypted)
3. Navigate to the UI and complete admin setup at `/setup`
4. Provide admin email, password (min 8 chars), and optional display name
5. The setup endpoint returns the system API key — **save it securely and do not share it**

The system API key is for bootstrap and emergency access only. For day-to-day use, log in with your admin account and use session auth, user tokens, or service accounts. See [[auth]].

See [[auth]] for details on the authentication system.
