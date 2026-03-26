# Graph

The graph is a lightweight metadata layer that maps relationships between tables across database connections. It enables discovery of join paths — both for humans via the UI and for MCP agents planning cross-source workspace queries.

Data is stored in a standalone SQLite database (`graph.db`) in the data directory, encrypted with SQLCipher using the same cipher key as `auth.db` and `search.db`. Re-seeding clears all existing `join_key` edges and repopulates from current FK metadata. Manually created edges (cross-connection links, custom types) are preserved.

## Concepts

- **Node** — A table identified by `(connection, database, schema, table)`. Nodes are auto-created when edges reference them.
- **Edge** — A relationship between two nodes. Has a type (`join_key`, `derives_from`, `references`, or custom) and optional column mappings.
- **Traversal** — BFS from a starting node to discover all reachable tables and the join path to each.

## Getting Started

### 1. Seed from Foreign Keys

The fastest way to populate the graph is to auto-discover FK relationships from your databases:

**UI:** Admin &rarr; Graph tab &rarr; select a connection (or "All") &rarr; click **Seed**

**API:**
```
POST /api/lane/admin/graph/seed
{"connection_name": "postgres"}
```

This scans every table in every database on the connection, finds FK constraints, and creates `join_key` edges. Re-seeding is idempotent — duplicates are ignored.

### 2. Add Cross-Connection Edges

FKs only exist within a single database. To link tables across connections (e.g., MSSQL users to Postgres analytics), create edges manually:

**UI:** Admin &rarr; Graph tab &rarr; **+ Add Edge** &rarr; pick source and target tables via dropdowns

**API:**
```
POST /api/lane/admin/graph/edges
{
  "source_connection": "mssql",
  "source_database": "ACEDW",
  "source_schema": "dbo",
  "source_table": "Users",
  "target_connection": "postgres",
  "target_database": "analytics",
  "target_schema": "public",
  "target_table": "user_events",
  "edge_type": "join_key",
  "source_columns": "user_id",
  "target_columns": "customer_id"
}
```

Nodes are auto-created if they don't exist.

### 3. Explore

Traverse the graph from any table to see what's reachable:

**UI:** Explore &rarr; Graph &rarr; pick a table &rarr; click **Explore**

**API:**
```
POST /api/lane/graph/traverse
{
  "connection_name": "mssql",
  "database_name": "testdb",
  "schema_name": "dbo",
  "table_name": "order_item_products",
  "max_depth": 3
}
```

Returns all reachable tables grouped by depth, with the full join path (columns and edge types) at each hop.

## REST Endpoints

### Admin (requires admin)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/admin/graph/nodes` | GET | List nodes. Optional `?connection=` filter |
| `/api/lane/admin/graph/nodes` | POST | Create/upsert a node |
| `/api/lane/admin/graph/nodes/{id}` | DELETE | Delete node (cascades edges) |
| `/api/lane/admin/graph/edges` | GET | List edges (expanded with node info). Optional `?edge_type=` filter |
| `/api/lane/admin/graph/edges` | POST | Create an edge (by node ID or identity) |
| `/api/lane/admin/graph/edges/{id}` | DELETE | Delete an edge |
| `/api/lane/admin/graph/seed` | POST | Auto-seed edges from FK metadata |

### User (any authenticated user)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/lane/graph/traverse` | POST | BFS traversal from a starting node |

## MCP Tools

| Tool | Description |
|------|-------------|
| `graph_traverse` | Traverse the graph from a starting table. Returns reachable nodes with join paths. |
| `graph_list_edges` | List graph edges with optional edge_type and connection filters. |

### Example: MCP Agent Workflow

An MCP agent can use the graph to plan a cross-source workspace query:

1. Call `graph_traverse` from the table of interest
2. Identify which related tables contain the needed data
3. Use `workspace_import_query` to pull each table into the workspace
4. Join the imported tables in a workspace query

## Edge Types

| Type | Meaning |
|------|---------|
| `join_key` | Tables can be joined on the specified columns (FK or manual) |
| `derives_from` | Target table was derived from source (lineage) |
| `references` | Soft reference (no FK constraint, but semantically related) |
| *(custom)* | Any string — the field is free-form |

## UI

- **Admin &rarr; Graph tab** — Seed from FKs, manage edges, create cross-connection relationships
- **Explore &rarr; Graph** — Traversal explorer for any authenticated user
