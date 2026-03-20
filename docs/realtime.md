# Realtime Monitoring

Lane provides live monitoring of database activity via Server-Sent Events (SSE) and outbound webhooks.

## Features

- **Table watching** — enable realtime on specific tables to stream write events (INSERT, UPDATE, DELETE)
- **SSE streaming** — subscribe to a live event stream for any watched table
- **Webhooks** — configure URLs that Lane POSTs to when writes happen on watched tables
- **Auto-expiry** — realtime tables with no SSE subscribers auto-expire after 1 hour
- **Zero-cost when idle** — no overhead when nobody is watching

## Admin API — Table Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/lane/admin/realtime/enable` | Enable realtime on a table |
| POST | `/api/lane/admin/realtime/disable` | Disable realtime on a table |
| GET | `/api/lane/admin/realtime/tables` | List all realtime-enabled tables |

Request body for enable/disable:
```json
{
  "connection": "postgres",
  "database": "postgres",
  "table": "transactions"
}
```

## SSE Subscribe

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/lane/realtime/subscribe?connection=X&database=Y&table=Z` | SSE event stream |

Requires authentication. EventSource doesn't support custom headers, so pass `&token=SESSION_TOKEN` as a query param.

Events are sent as `event: change` with JSON data:
```json
{
  "id": "uuid",
  "connection": "postgres",
  "database": "postgres",
  "table": "transactions",
  "query_type": "INSERT",
  "row_count": 1,
  "user": "pos-system",
  "timestamp": "2026-03-19T08:05:39.167Z",
  "data": { ... }
}
```

## Webhooks

Webhooks let external systems react to database changes. When a write happens on a watched table, Lane POSTs the event to all matching webhook URLs. Webhooks are **outbound only** — Lane sends data, it does not receive.

### Admin API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/lane/admin/realtime/webhooks` | Create a webhook |
| GET | `/api/lane/admin/realtime/webhooks` | List all webhooks |
| GET | `/api/lane/admin/realtime/webhooks?connection=X&database=Y&table=Z` | Filter by any combination |
| PUT | `/api/lane/admin/realtime/webhooks/{id}` | Update a webhook |
| DELETE | `/api/lane/admin/realtime/webhooks/{id}` | Delete a webhook |

All webhook endpoints require admin authentication.

### Create / Update

```json
{
  "connection": "postgres",
  "database": "postgres",
  "table": "transactions",
  "url": "https://your-app.example.com/hook",
  "events": ["INSERT", "UPDATE", "DELETE"],
  "secret": "optional-hmac-key"
}
```

- **url** — the external endpoint Lane will POST to
- **events** — which write types to fire on (defaults to all three)
- **secret** — optional HMAC-SHA256 signing key for payload verification

### Webhook Payload

Lane POSTs JSON to the configured URL:

```json
{
  "event": "INSERT",
  "connection": "postgres",
  "database": "postgres",
  "table": "transactions",
  "row_count": 1,
  "user": "pos-system",
  "timestamp": "2026-03-19T08:05:39.167Z",
  "data": {
    "id": 204600,
    "location_id": 2,
    "total": "14.00",
    "payment_method": "card",
    "created_at": "2026-03-19 08:05:39"
  }
}
```

### Headers

| Header | Value |
|--------|-------|
| `Content-Type` | `application/json` |
| `X-Lane-Event` | `INSERT`, `UPDATE`, or `DELETE` |
| `X-Lane-Signature` | `sha256=...` (only if secret is configured) |

### HMAC Verification

If a secret is configured, Lane signs the request body with HMAC-SHA256 and sends the signature in the `X-Lane-Signature` header. The receiving system should:

1. Read the raw request body
2. Compute `HMAC-SHA256(secret, body)`
3. Compare with the value after `sha256=` in the header

### Performance

- **In-memory cache** — webhook config is cached in a HashMap, no DB query per event
- **Fire-and-forget** — HTTP calls are async spawned tasks, never block the write path
- **10-second timeout** — per webhook, errors logged but don't affect the write
- **Cache invalidation** — admin create/update/delete operations mark the cache dirty; next event reloads

### Scoping

Each webhook is tied to a specific `(connection, database, table)` tuple. You can create multiple webhooks per table (different URLs), and the same URL can be registered on different tables. The unique constraint is `(connection, database, table, url)` — no duplicate registrations.

### Enable / Disable

Webhooks have an `is_enabled` toggle. Disabling a webhook stops it from firing without deleting the configuration. Toggle via PUT or the UI switch.

## UI

The Realtime page (`/realtime`) provides:

- **Table management** — enable/disable realtime monitoring on tables
- **Live watch** — open an SSE stream drawer to see events in real time
- **Webhook management** — create, list, enable/disable, reveal secrets, and delete webhooks

## Related

- [[query-engine]] — Query execution system
- [[approvals]] — Approval event streaming
- [[endpoints]] — Named endpoints (saved queries)
