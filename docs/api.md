# REST API Reference

All endpoints are prefixed with `/api/lane/` unless noted. Authentication via `x-lane-key` header (API key / token) or session cookie. See [[auth]].

## Query Execution

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/query` | POST | Execute SQL query |
| `/api/lane/validate` | POST | Validate SQL without executing |
| `/api/lane/explain` | POST | Get query execution plan |

**Query body**:
```json
{
  "sql": "SELECT * FROM users",
  "connection": "production",
  "database": "mydb",
  "page": 1,
  "page_size": 100
}
```

See [[query-engine]] for execution details.

## Schema / Metadata

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/connections` | GET | List database connections |
| `/api/lane/databases?connection={name}` | GET | List databases |
| `/api/lane/tables?connection={name}&database={db}` | GET | List tables |
| `/api/lane/columns?connection={name}&database={db}&table={tbl}` | GET | List columns |
| `/api/lane/views?connection={name}&database={db}` | GET | List views |
| `/api/lane/procedures?connection={name}&database={db}` | GET | List procedures |
| `/api/lane/procedure-definition` | GET | Get procedure source |

## Query History

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/history` | GET | List query history (own) |
| `/api/lane/history/{id}/favorite` | POST | Toggle favorite |
| `/api/lane/history/{id}` | DELETE | Delete entry |

## Search

Full-text search across schema, queries, and endpoints. See [[search]].

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/lane/search?q=...&limit=20` | GET | User | Unified search (all types) |
| `/api/lane/search/schema?q=...` | GET | User | Schema objects only |
| `/api/lane/search/queries?q=...&email=...` | GET | User | Query history only |
| `/api/lane/search/endpoints?q=...` | GET | User | Endpoints only |
| `/api/lane/admin/search/reindex` | POST | Admin | Trigger full re-index |
| `/api/lane/admin/search/stats` | GET | Admin | Index row counts |

## Named Data Endpoints

See [[endpoints]] for full documentation.

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/data/endpoints` | GET | User | List accessible endpoints |
| `/api/data/endpoints/{name}?param=value` | GET | User | Execute endpoint |
| `/api/lane/admin/endpoints` | GET | Admin | List all endpoints |
| `/api/lane/admin/endpoints` | POST | Admin | Create endpoint |
| `/api/lane/admin/endpoints/{name}` | PUT | Admin | Update endpoint |
| `/api/lane/admin/endpoints/{name}` | DELETE | Admin | Delete endpoint |
| `/api/lane/admin/endpoints/{name}/permissions` | GET | Admin | Get permissions |
| `/api/lane/admin/endpoints/{name}/permissions` | PUT | Admin | Set permissions |

## Realtime & Monitoring

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/realtime/events` | GET (SSE) | Live query events |
| `/api/lane/monitor/active` | GET | Active queries |
| `/api/lane/monitor/stats` | GET | Query statistics |
| `/api/lane/health` | GET | Connection health |

See [[realtime]].

## Approvals

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/approvals` | GET | List pending |
| `/api/lane/approvals/{id}` | GET | Get details (includes SQL) |
| `/api/lane/approvals/{id}/approve` | POST | Approve |
| `/api/lane/approvals/{id}/reject` | POST | Reject (`{ "reason": "..." }`) |
| `/api/lane/approvals/events` | GET (SSE) | Approval event stream |

See [[approvals]].

## Storage

Requires `storage` feature. See [[storage]].

| Endpoint | Method | Permission | Description |
|----------|--------|------------|-------------|
| `/api/lane/storage/connections` | GET | Auth | List storage connections |
| `/api/lane/storage/buckets` | GET | Connection | List buckets |
| `/api/lane/storage/buckets` | POST | Write | Create bucket |
| `/api/lane/storage/buckets/{name}` | DELETE | Delete | Delete bucket |
| `/api/lane/storage/objects` | GET | Read | List objects |
| `/api/lane/storage/upload` | POST | Write | Upload (multipart) |
| `/api/lane/storage/download` | GET | Read | Download |
| `/api/lane/storage/objects` | DELETE | Delete | Delete object |
| `/api/lane/storage/metadata` | GET | Read | Object metadata |
| `/api/lane/storage/preview` | POST | Read | Preview in workspace |
| `/api/lane/storage/export-query` | POST | Write | Export query results to storage |
| `/api/lane/storage/import-to-workspace` | POST | Read | Import file to workspace |
| `/api/lane/storage/workspace-export` | POST | Write | Export workspace query to storage |

## Workspace

Requires `duckdb_backend` feature. See [[workspace]].

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/workspace/tables` | GET | List workspace tables |
| `/api/lane/workspace/query` | POST | Execute workspace SQL |
| `/api/lane/workspace/import/preview` | POST | Preview import |
| `/api/lane/workspace/import/execute` | POST | Execute import |
| `/api/lane/workspace/export/{table}` | GET | Export table |
| `/api/lane/workspace/tables/{table}` | DELETE | Drop table |

## Authentication

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/auth/status` | GET | None | Check setup/auth state |
| `/api/auth/setup` | POST | None | First-run admin setup |
| `/api/auth/login` | POST | None | Login (email + password → session) |
| `/api/auth/logout` | POST | Session | Destroy session |
| `/api/auth/password` | POST | Session/Token | Change own password |

See [[auth]].

## Self-Service

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/self/tokens` | GET | List own tokens |
| `/api/lane/self/tokens` | POST | Generate token |
| `/api/lane/self/tokens/{prefix}/revoke` | POST | Revoke own token |
| `/api/lane/self/me` | GET | Get own profile |
| `/api/lane/self/permissions` | GET | Get own permissions |

## Admin — Users

All admin endpoints require `is_admin`. See [[permissions]].

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/admin/users` | GET | List users |
| `/api/lane/admin/users` | POST | Create user |
| `/api/lane/admin/users/{email}` | GET | Get user |
| `/api/lane/admin/users/{email}` | PUT | Update user |
| `/api/lane/admin/users/{email}` | DELETE | Delete user |
| `/api/lane/admin/set-password` | POST | Set user password |

## Admin — Tokens

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/admin/tokens` | GET | List tokens (filter by email) |
| `/api/lane/admin/tokens/generate` | POST | Generate token for user |
| `/api/lane/admin/tokens/revoke` | POST | Revoke token |
| `/api/lane/admin/token-policy` | GET | Get token policy |
| `/api/lane/admin/token-policy` | POST | Update token policy |

## Admin — Permissions

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/admin/permissions` | GET | Get user database permissions |
| `/api/lane/admin/permissions` | POST | Set user database permissions |
| `/api/lane/admin/connection-permissions` | GET | Get user connection permissions |
| `/api/lane/admin/connection-permissions` | POST | Set user connection permissions |
| `/api/lane/admin/storage-permissions` | GET | Get user storage permissions |
| `/api/lane/admin/storage-permissions` | POST | Set user storage permissions |

## Admin — Service Accounts

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/admin/service-accounts` | GET | List service accounts |
| `/api/lane/admin/service-accounts` | POST | Create service account |
| `/api/lane/admin/service-accounts/{name}` | PUT | Update service account |
| `/api/lane/admin/service-accounts/{name}` | DELETE | Delete service account |
| `/api/lane/admin/service-accounts/{name}/rotate-key` | POST | Rotate API key |
| `/api/lane/admin/sa-permissions` | GET/POST | SA database permissions |
| `/api/lane/admin/sa-connection-permissions` | GET/POST | SA connection permissions |
| `/api/lane/admin/sa-storage-permissions` | GET/POST | SA storage permissions |

## Admin — PII

See [[pii]].

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/admin/pii/rules` | GET | List PII rules |
| `/api/lane/admin/pii/rules` | POST | Create rule |
| `/api/lane/admin/pii/rules/{id}` | PUT | Update rule |
| `/api/lane/admin/pii/rules/{id}` | DELETE | Delete rule |
| `/api/lane/admin/pii/rules/test` | POST | Test rule |
| `/api/lane/admin/pii/columns` | GET | List tagged columns |
| `/api/lane/admin/pii/columns` | POST | Tag column |
| `/api/lane/admin/pii/columns/{id}` | DELETE | Remove tag |
| `/api/lane/admin/pii/columns/discover` | POST | Auto-discover PII columns |
| `/api/lane/admin/pii/settings` | GET/PUT | Get/set PII settings |

## Admin — Teams & Projects

See [[teams]].

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/admin/teams` | GET/POST | List/create teams |
| `/api/lane/admin/teams/{id}` | PUT/DELETE | Update/delete team |
| `/api/lane/admin/teams/{id}/members` | GET/POST | Team members |
| `/api/lane/admin/teams/{team_id}/members/{email}` | PUT/DELETE | Update/remove member |
| `/api/lane/admin/teams/{id}/projects` | GET/POST | Team projects |
| `/api/lane/admin/projects/{id}` | PUT/DELETE | Update/delete project |
| `/api/lane/admin/projects/{id}/members` | GET/POST | Project members |
| `/api/lane/admin/projects/{project_id}/members/{email}` | PUT/DELETE | Update/remove member |

## Admin — Audit

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/admin/audit-log` | GET | Query access log |
| `/api/lane/admin/query-history` | GET | All users' query history |

## REST Data API

Direct table data access (no SQL required):

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/data/{connection}/{database}/{table}` | GET | List rows (with pagination) |
| `/api/lane/data/{connection}/{database}/{table}` | POST | Insert row |
| `/api/lane/data/{connection}/{database}/{table}/{id}` | GET | Get row by ID |
| `/api/lane/data/{connection}/{database}/{table}/{id}` | PUT | Update row |
| `/api/lane/data/{connection}/{database}/{table}/{id}` | DELETE | Delete row |

## Connection Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/connections` | GET | List all connections |
| `/api/lane/connections/test` | POST | Test a connection config |
