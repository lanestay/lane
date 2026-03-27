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
| `/api/lane/graph/plan` | POST | Generate a complete execution plan for combining multiple tables |

## MCP Tools

| Tool | Description |
|------|-------------|
| `graph_traverse` | Traverse the graph from a starting table. Returns reachable nodes with join paths. |
| `graph_list_edges` | List graph edges with optional edge_type and connection filters. |
| `graph_create_edge` | Create an edge between two tables. Nodes are auto-created. Enables agent-driven schema inference. |
| `graph_plan` | Generate an execution plan for combining multiple tables. Returns import queries + JOIN SQL. |

### graph_plan

Given a set of tables (across any connections), `graph_plan` finds the shortest join path through the graph — including intermediate tables — describes each table's columns, and returns:

- **`imports[]`** — one per table, with connection, dialect-correct SELECT query, and workspace table name
- **`join_query`** — ready-to-run DuckDB JOIN using the workspace table names
- **`path_description`** — human-readable join chain

```
POST /api/lane/graph/plan
{
  "tables": [
    {"connection": "postgres", "database": "postgres", "schema": "public", "table": "support_tickets"},
    {"connection": "mssql", "database": "testdb", "schema": "dbo", "table": "products"}
  ],
  "row_limit": 1000
}
```

The plan does not execute anything. The agent (or user) runs each import step with `workspace_import_query`, then runs the `join_query` with `workspace_query`.

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
