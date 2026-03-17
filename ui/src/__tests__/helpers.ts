import type { ConnectionInfo, QueryResult } from "@/lib/api";

// ---------------------------------------------------------------------------
// Fetch mock utilities
// ---------------------------------------------------------------------------

type FetchImpl = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

/**
 * Install a mock `fetch` that delegates to `handler`.
 * Returns a spy so tests can assert on calls.
 */
export function mockFetch(handler: FetchImpl) {
  const spy = vi.fn(handler);
  globalThis.fetch = spy as unknown as typeof fetch;
  return spy;
}

/** Build a successful JSON Response. */
export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// ---------------------------------------------------------------------------
// Fixture data
// ---------------------------------------------------------------------------

export const CONNECTIONS: ConnectionInfo[] = [
  { name: "dev-mssql", is_default: true, type: "mssql", default_database: "master", status: "connected", status_message: null },
  { name: "dev-pg", is_default: false, type: "postgres", default_database: "postgres", status: "connected", status_message: null },
];

export const DATABASES = [{ name: "master" }, { name: "tempdb" }];

export const TABLES = [
  { TABLE_NAME: "users", TABLE_SCHEMA: "dbo", ROW_COUNT: 42 },
  { TABLE_NAME: "orders", TABLE_SCHEMA: "dbo", ROW_COUNT: 100 },
];

export const COLUMNS = [
  { COLUMN_NAME: "id", DATA_TYPE: "int", IS_NULLABLE: "NO", IS_PRIMARY_KEY: "YES" },
  { COLUMN_NAME: "name", DATA_TYPE: "varchar", IS_NULLABLE: "YES", IS_PRIMARY_KEY: "NO" },
];

export const QUERY_RESULT: QueryResult = {
  success: true,
  total_rows: 2,
  execution_time_ms: 15,
  rows_per_second: 133,
  data: [
    { id: 1, name: "Alice" },
    { id: 2, name: "Bob" },
  ],
  metadata: {
    columns: [
      { name: "id", type: "int" },
      { name: "name", type: "varchar" },
    ],
  },
};

export const USERS = [
  {
    email: "admin@test.com",
    display_name: "Admin",
    is_admin: true,
    is_enabled: true,
    created_at: "2025-01-01T00:00:00Z",
    updated_at: "2025-01-01T00:00:00Z",
    permissions: [],
  },
  {
    email: "user@test.com",
    display_name: "User",
    is_admin: false,
    is_enabled: true,
    created_at: "2025-01-01T00:00:00Z",
    updated_at: "2025-01-01T00:00:00Z",
    permissions: [],
  },
];

export const TOKENS = [
  {
    token_prefix: "bq_abc1",
    email: "admin@test.com",
    label: "dev",
    expires_at: null,
    is_active: true,
    created_at: "2025-01-01T00:00:00Z",
  },
];

export const FOREIGN_KEYS_RESULT: QueryResult = {
  success: true,
  total_rows: 1,
  execution_time_ms: 5,
  rows_per_second: 200,
  data: [
    {
      FK_NAME: "FK_orders_users",
      PARENT_TABLE: "orders",
      PARENT_SCHEMA: "dbo",
      PARENT_COLUMN: "user_id",
      REFERENCED_TABLE: "users",
      REFERENCED_SCHEMA: "dbo",
      REFERENCED_COLUMN: "id",
    },
  ],
  metadata: { columns: [] },
};

export const INDEXES_RESULT: QueryResult = {
  success: true,
  total_rows: 1,
  execution_time_ms: 5,
  rows_per_second: 200,
  data: [
    {
      INDEX_NAME: "PK_users",
      TABLE_NAME: "users",
      TABLE_SCHEMA: "dbo",
      COLUMN_NAME: "id",
      IS_UNIQUE: true,
    },
  ],
  metadata: { columns: [] },
};

export const AUDIT_ENTRIES = [
  {
    id: 1,
    token_prefix: "bq_abc1",
    email: "admin@test.com",
    source_ip: "127.0.0.1",
    database_name: "master",
    query_type: "SELECT",
    action: "allowed",
    details: "SELECT 1",
    created_at: "2025-01-01T00:00:00Z",
  },
];
