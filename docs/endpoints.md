# Named Data Endpoints

Named endpoints let admins save SQL queries as reusable, parameterized API endpoints. Consumers call them by name without writing SQL — useful for dashboards, integrations, and service accounts.

## Concepts

- **Read-only**: Endpoints only execute SELECT/WITH queries
- **Parameterized**: Use `{{param_name}}` placeholders for dynamic values
- **Permission-controlled**: Per-endpoint access lists (email whitelist)
- **SQL injection protected**: Parameter values are validated and escaped

## Creating Endpoints (Admin)

Admins create endpoints via the API or admin UI.

```
POST /api/lane/admin/endpoints
```

```json
{
  "name": "active-users",
  "connection_name": "production",
  "database_name": "mydb",
  "query": "SELECT * FROM Users WHERE department = '{{department}}' AND active = 1",
  "description": "List active users by department",
  "parameters": "[{\"name\": \"department\", \"type\": \"string\", \"default\": \"Engineering\"}]"
}
```

### Parameter Definitions

The `parameters` field is a JSON array of parameter definitions:

```json
[
  {
    "name": "department",
    "type": "string",
    "default": "Engineering"
  },
  {
    "name": "limit",
    "type": "number"
  }
]
```

- `name` — matches `{{name}}` in the query
- `type` — hint for consumers (not enforced server-side)
- `default` — used when the parameter is not provided by the caller

### SQL Injection Protection

Parameter values are validated before substitution:
- Semicolons, comment markers (`--`, `/* */`) are rejected
- SQL keywords (`DROP`, `DELETE`, `INSERT`, etc.) are rejected
- Single quotes are escaped (`'` → `''`)

## Consuming Endpoints

### List Available Endpoints

```
GET /api/data/endpoints
```

Returns endpoints the authenticated user has access to (filtered by permissions).

### Execute an Endpoint

```
GET /api/data/endpoints/{name}?department=Sales&limit=100
```

Parameters are passed as query string values. Missing required parameters (no default) return an error.

### Response Format

Default JSON response:
```json
{
  "columns": [...],
  "rows": [...],
  "limit": 10000
}
```

### Row Limits

| Format | Default Limit | Max | Unlimited |
|--------|--------------|-----|-----------|
| JSON | 10,000 | 10,000 | No |
| NDJSON | 100,000 | — | Yes |

- `?limit=500` — set a specific row limit
- `?format=ndjson` — streaming NDJSON format (higher limits)
- `?format=ndjson&limit=unlimited` — no row cap (NDJSON only)

JSON format is capped at 10,000 rows. For larger datasets, use `format=ndjson`.

## Permissions

By default, all authenticated users can access all endpoints. Admins can restrict access per endpoint.

### Set Endpoint Permissions

```
PUT /api/lane/admin/endpoints/{name}/permissions
```

```json
{
  "emails": ["user@example.com", "analyst@example.com"]
}
```

When an email whitelist is set, only listed users can execute the endpoint. Admins always have access.

Service accounts can also be granted endpoint access via service account permissions.

### Connection Access

Endpoint execution also requires the user to have access to the endpoint's underlying database connection (via [[permissions|connection permissions]]).

## Admin API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/admin/endpoints` | GET | List all endpoints |
| `/api/lane/admin/endpoints` | POST | Create endpoint |
| `/api/lane/admin/endpoints/{name}` | PUT | Update endpoint |
| `/api/lane/admin/endpoints/{name}` | DELETE | Delete endpoint |
| `/api/lane/admin/endpoints/{name}/permissions` | GET | Get permissions |
| `/api/lane/admin/endpoints/{name}/permissions` | PUT | Set permissions |

## Consumer API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/data/endpoints` | GET | List accessible endpoints |
| `/api/data/endpoints/{name}` | GET | Execute endpoint |

## Search

Endpoints are indexed in the [[search|full-text search]] system. Search by name, description, or query text using `Cmd+K` in the UI or the search API.

## Related

- [[search]] — Full-text search (includes endpoint search)
- [[permissions]] — Permission model
- [[api]] — REST API reference
