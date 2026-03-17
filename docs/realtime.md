# Realtime Monitoring

Lane provides live monitoring of database activity via Server-Sent Events (SSE).

## Features

- **Active queries** — see currently running queries across all connections
- **Query events** — real-time stream of query start/complete/error events
- **Connection health** — monitor connection pool status

## SSE Endpoints

| Endpoint | Description |
|----------|-------------|
| `/api/lane/realtime/events` | Stream of query lifecycle events |
| `/api/lane/approvals/events` | Stream of [[approvals]] events |

## REST Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/monitor/active` | GET | List currently active queries |
| `/api/lane/monitor/stats` | GET | Query execution statistics |
| `/api/lane/health` | GET | Connection health check |

## UI

The [[ui#Realtime Page]] shows live query activity, and the [[ui#Monitor Page]] shows aggregated statistics.

## Related

- [[query-engine]] — Query execution system
- [[approvals]] — Approval event streaming
