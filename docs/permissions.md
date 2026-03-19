# Permissions

Lane has a layered permission model: user-level SQL mode → connection access → database/table access → storage bucket access.

## SqlMode Tiers

Every user and service account has a `sql_mode` that controls what SQL operations they can perform.

| Mode | Read | DML (INSERT/UPDATE/DELETE) | DDL (CREATE/ALTER/DROP) | Notes |
|------|------|---------------------------|------------------------|-------|
| **None** | No | No | No | Account disabled |
| **ReadOnly** | Yes | No | No | Data analysts |
| **Supervised** | Yes | Yes (requires [[approvals]]) | No | Cannot self-approve, needs admin/lead |
| **Confirmed** | Yes | Yes (requires [[approvals]]) | No | Can self-approve |
| **Full** | Yes | Yes | Yes | No approval needed |

## ReadOnly Enforcement

Lane enforces ReadOnly mode through multiple layers:

1. **Query parsing** — blocks INSERT, UPDATE, DELETE, DROP, EXEC, SELECT INTO, CTE-prefixed DML, and multi-statement batches before execution
2. **Postgres** — queries run inside a `READ ONLY` transaction, so even functions with write side effects are blocked by the database itself
3. **MSSQL** — queries are wrapped in `BEGIN TRANSACTION ... ROLLBACK TRANSACTION`, so any writes that slip through string validation are always rolled back

These layers cover the vast majority of cases. However, for maximum protection, we recommend creating a dedicated read-only database connection:

- **MSSQL**: Create a SQL Server login with only the `db_datareader` role, then add it as a named connection (e.g. `mssql-readonly`). Assign ReadOnly users to this connection via connection permissions.
- **Postgres**: Create a database role with only `SELECT` grants, then add it as a named connection (e.g. `postgres-readonly`). Assign ReadOnly users to this connection.

With a read-only connection, the database itself refuses writes regardless of what SQL is submitted. Combined with Lane's query parsing and transaction enforcement, this provides defense in depth that is virtually impossible to circumvent.

## Connection Permissions

Controls which named connections a user can access. Stored in `connection_permissions` table.

- **No rows** for a user = unrestricted (can access all connections)
- **Any rows** = restricted to listed connections only

Applies to both database and storage connections.

Admin endpoints:
- `GET /api/lane/admin/connection-permissions?email={email}`
- `POST /api/lane/admin/connection-permissions`

## Database & Table Permissions

Fine-grained control over which databases and tables a user can read/write.

| Field | Description |
|-------|-------------|
| `database_name` | Target database |
| `table_pattern` | Glob pattern: `*` (all), `orders*` (prefix), `specific_table` (exact) |
| `can_read` | SELECT access |
| `can_write` | INSERT access |
| `can_update` | UPDATE access |
| `can_delete` | DELETE access |

**Fail-closed**: Users need explicit permission rows to access databases and tables. No rows = no access. Admins grant access by adding rows with the appropriate `can_read`, `can_write`, `can_update`, `can_delete` flags. Use `database_name = '*'` and `table_pattern = '*'` to grant access to everything.

Pattern matching:
- `*` — matches all tables
- `prefix*` — prefix match (case-insensitive)
- `exact_name` — exact match (case-insensitive)

Admin endpoints:
- `GET /api/lane/admin/permissions?email={email}`
- `POST /api/lane/admin/permissions`

## Storage Permissions

Controls access to MinIO/S3 buckets per connection. See [[storage]] for the storage system.

| Field | Description |
|-------|-------------|
| `connection_name` | Storage connection |
| `bucket_pattern` | Glob: `*` (all), `data-*` (prefix), `specific-bucket` (exact) |
| `can_read` | List/download objects |
| `can_write` | Upload objects, create buckets |
| `can_delete` | Delete objects, delete buckets |

Same fail-closed semantics and pattern matching as table permissions.

Admin endpoints:
- `GET /api/lane/admin/storage-permissions?email={email}`
- `POST /api/lane/admin/storage-permissions`

## Service Account Permissions

Service accounts are designed for machines (POS systems, ETL pipelines, webhooks) and have stricter enforcement than user accounts:

- **Raw SQL is read-only** — service accounts can run SELECT queries via `/api/lane/query` but all writes are blocked, regardless of sql_mode. This ensures table-level permissions cannot be bypassed.
- **Writes must use the REST API** — `POST/PUT/DELETE /api/data/{connection}/{database}/{table}` enforces `can_read`, `can_write`, `can_update`, `can_delete` per table.
- **Named data endpoints** — for complex read queries (JOINs, aggregations), admins can create named endpoints that service accounts call by name.

Service accounts have their own parallel permission tables:

- `sa_permissions` — database/table access (same structure as user permissions)
- `sa_connection_permissions` — connection whitelist
- `sa_storage_permissions` — storage bucket access

Admin endpoints:
- `GET/POST /api/lane/admin/service-account-permissions`
- `GET/POST /api/lane/admin/service-account-connections`
- `GET/POST /api/lane/admin/sa-storage-permissions`

## Raw SQL vs REST API Enforcement

**Raw SQL** (SQL editor, MCP tools, `/api/lane/query`) — for **users**, access is controlled at the **database level** by SqlMode. For **service accounts**, raw SQL is always read-only.

| SqlMode | User raw SQL behavior | Service account raw SQL behavior |
|---------|----------------------|--------------------------------|
| **Full** | Unrestricted on permitted databases | Read-only |
| **Confirmed** | DML/DDL requires approval (can self-approve) | Read-only |
| **Supervised** | All writes require admin approval | Read-only |
| **ReadOnly** | SELECT only — all writes blocked | Read-only |
| **None** | Blocked | Blocked |

**REST API** (`/api/data/...`) — access is controlled at the **table level** for both users and service accounts. The `can_read`, `can_write`, `can_update`, `can_delete` flags on each permission row are enforced per table and operation.

**Recommendation**: If a user needs per-table write restrictions, set them to **ReadOnly** sql_mode for raw SQL and grant granular table permissions for the REST API. They can run complex queries (JOINs, aggregations) via raw SQL and perform writes through the REST API where table-level permissions are fully enforced.

## Permission Check Order

For a database query:

1. **Auth** — identify user/SA ([[auth]])
2. **SqlMode** — can they read/write at all?
3. **Connection** — can they use this connection?
4. **Database/Table** — can they access this database and table?
5. **Approval** — if DML and Supervised/Confirmed, queue for [[approvals]]

For a storage operation:

1. **Auth** — identify user/SA
2. **Connection** — can they use this storage connection?
3. **Bucket** — can they read/write/delete in this bucket?
4. **Approval** — if MCP upload and Supervised/Confirmed, queue for [[approvals]]

## Audit Logging

All write operations (database and storage) are logged to the `access_log` table with:
- Token prefix, email, source IP
- Database/connection name
- Query type and action
- Details (SQL text, bucket/key, etc.)

Query history is tracked separately in `query_history` with execution time, row count, and success/error status.

## Related

- [[auth]] — How users are identified
- [[approvals]] — Write approval workflows
- [[teams]] — Approval delegation via team/project roles
