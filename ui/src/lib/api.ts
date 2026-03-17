// API client for lane REST endpoints

export interface ConnectionInfo {
  name: string;
  is_default: boolean;
  type: string;
  default_database: string;
  status: string;
  status_message: string | null;
}

export interface DatabaseInfo {
  name: string;
}

export interface TableInfo {
  TABLE_NAME: string;
  TABLE_SCHEMA: string;
  ROW_COUNT: number;
  TABLE_TYPE?: string;
}

export interface ColumnInfo {
  COLUMN_NAME: string;
  DATA_TYPE: string;
  IS_NULLABLE: string;
  IS_PRIMARY_KEY: string;
  COLUMN_DEFAULT?: string | null;
  CHARACTER_MAXIMUM_LENGTH?: number | null;
  NUMERIC_PRECISION?: number | null;
}

export interface QueryResult {
  success: boolean;
  total_rows: number;
  execution_time_ms: number;
  rows_per_second: number;
  data: Record<string, unknown>[];
  metadata?: {
    columns: { name: string; type: string }[];
  };
}

export interface ApiError {
  error: {
    category?: string;
    code?: string;
    message: string;
    suggestion?: string;
    dialect?: string;
  };
}

// Admin types

export interface UserInfo {
  email: string;
  display_name: string | null;
  is_admin: boolean;
  is_enabled: boolean;
  mcp_enabled: boolean;
  pii_mode: string | null;
  sql_mode: "none" | "read_only" | "supervised" | "confirmed" | "full";
  max_pending_approvals: number | null;
  created_at: string;
  updated_at: string;
  permissions: Permission[];
  connection_permissions: string[] | null;
}

export interface Permission {
  id: number;
  email: string;
  database_name: string;
  table_pattern: string;
  can_read: boolean;
  can_write: boolean;
  can_update: boolean;
  can_delete: boolean;
}

export interface TokenRecord {
  token_prefix: string;
  email: string;
  label: string | null;
  expires_at: string | null;
  is_active: boolean;
  created_at: string;
  pii_mode: string | null;
}

export interface AuditEntry {
  id: number;
  token_prefix: string | null;
  email: string | null;
  source_ip: string | null;
  database_name: string | null;
  query_type: string | null;
  action: string | null;
  details: string | null;
  created_at: string;
}

// Endpoint types

export interface EndpointParam {
  name: string;
  type?: string;
  default?: string;
}

export interface EndpointInfo {
  name: string;
  connection_name: string;
  database_name: string;
  query: string;
  description: string | null;
  parameters: string | null;
  created_by: string | null;
  updated_at: string;
  created_at: string;
}

// ============================================================================
// Auth helpers — session-based
// ============================================================================

export function getSessionToken(): string {
  return localStorage.getItem("session_token") ?? "";
}

function headers(): Record<string, string> {
  const h: Record<string, string> = { "Content-Type": "application/json" };
  const token = getSessionToken();
  if (token) {
    h["Authorization"] = `Bearer ${token}`;
  }
  return h;
}

const base = "";  // same origin

// ============================================================================
// Auth API
// ============================================================================

export interface AuthStatus {
  needs_setup: boolean;
  authenticated: boolean;
  user: { email: string; is_admin: boolean } | null;
  tailscale_auth?: boolean;
  auth_providers?: string[];
  smtp_configured?: boolean;
}

export async function checkAuthStatus(): Promise<AuthStatus> {
  const res = await fetch(`${base}/api/auth/status`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export interface SetupResult {
  success: boolean;
  email: string;
  api_key: string;
  message: string;
}

export async function performSetup(data: {
  email: string;
  display_name?: string;
  password: string;
  phone?: string;
}): Promise<SetupResult> {
  const res = await fetch(`${base}/api/auth/setup`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export interface LoginResult {
  success: boolean;
  session_token: string;
  email: string;
  is_admin: boolean;
}

export async function loginWithPassword(email: string, password: string): Promise<LoginResult> {
  const res = await fetch(`${base}/api/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function loginWithTailscale(): Promise<LoginResult> {
  const res = await fetch(`${base}/api/auth/tailscale`, {
    method: "POST",
    headers: headers(),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function logoutSession(): Promise<void> {
  await fetch(`${base}/api/auth/logout`, {
    method: "POST",
    headers: headers(),
  });
}

export async function changePassword(currentPassword: string, newPassword: string): Promise<void> {
  const res = await fetch(`${base}/api/auth/password`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ current_password: currentPassword, new_password: newPassword }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
}

export async function sendEmailCode(email: string): Promise<void> {
  const res = await fetch(`${base}/api/auth/email-code/send`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
}

export async function verifyEmailCode(email: string, code: string): Promise<LoginResult> {
  const res = await fetch(`${base}/api/auth/email-code/verify`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, code }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

// ============================================================================
// Data API
// ============================================================================

export async function listConnections(): Promise<ConnectionInfo[]> {
  const res = await fetch(`${base}/api/lane/connections`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function listDatabases(connection?: string): Promise<DatabaseInfo[]> {
  const params = connection ? `?connection=${connection}` : "";
  const res = await fetch(`${base}/api/lane/databases${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function listSchemas(database: string, connection?: string): Promise<{ schema_name: string }[]> {
  const params = new URLSearchParams({ database });
  if (connection) params.set("connection", connection);
  const res = await fetch(`${base}/api/lane/schemas?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function listTables(database: string, connection?: string, schema?: string): Promise<TableInfo[]> {
  const params = new URLSearchParams({ database });
  if (connection) params.set("connection", connection);
  if (schema) params.set("schema", schema);
  const res = await fetch(`${base}/api/lane/tables?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function describeTable(database: string, table: string, connection?: string, schema?: string): Promise<ColumnInfo[]> {
  const params = new URLSearchParams({ database, table });
  if (connection) params.set("connection", connection);
  if (schema) params.set("schema", schema);
  const res = await fetch(`${base}/api/lane/describe?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

// Database objects (views, procedures, functions)

export interface ViewInfo {
  name: string;
  schema_name: string;
  type: string;
  create_date?: string;
  modify_date?: string;
}

export interface RoutineInfo {
  name: string;
  schema_name: string;
  routine_type: string;
  create_date?: string;
  modify_date?: string;
}

export interface ObjectDefinition {
  name: string;
  schema_name: string;
  type: string;
  definition: string;
  parameters?: { param_name: string; type_name: string; max_length?: number; is_output?: boolean }[];
  arguments?: string;
  return_type?: string;
}

export async function listViews(database: string, connection?: string, schema?: string): Promise<ViewInfo[]> {
  const params = new URLSearchParams({ database });
  if (connection) params.set("connection", connection);
  if (schema) params.set("schema", schema);
  const res = await fetch(`${base}/api/lane/views?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function listRoutines(database: string, connection?: string, schema?: string): Promise<RoutineInfo[]> {
  const params = new URLSearchParams({ database });
  if (connection) params.set("connection", connection);
  if (schema) params.set("schema", schema);
  const res = await fetch(`${base}/api/lane/routines?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function getObjectDefinition(
  database: string,
  name: string,
  objectType: string,
  connection?: string,
  schema?: string,
): Promise<ObjectDefinition> {
  const params = new URLSearchParams({ database, name, object_type: objectType });
  if (connection) params.set("connection", connection);
  if (schema) params.set("schema", schema);
  const res = await fetch(`${base}/api/lane/object-definition?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export interface TriggerInfo {
  name: string;
  schema_name: string;
  parent_table: string;
  is_disabled: boolean;
  is_instead_of_trigger: boolean;
  events: string;
  create_date?: string;
  modify_date?: string;
  function_name?: string;
}

export interface RelatedObject {
  object_name: string;
  schema_name: string;
  object_type: string;
  modify_date?: string;
}

export async function listTriggers(
  database: string,
  table: string,
  connection?: string,
  schema?: string,
): Promise<TriggerInfo[]> {
  const params = new URLSearchParams({ database, table });
  if (connection) params.set("connection", connection);
  if (schema) params.set("schema", schema);
  const res = await fetch(`${base}/api/lane/triggers?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function getTriggerDefinition(
  database: string,
  name: string,
  connection?: string,
  schema?: string,
): Promise<ObjectDefinition> {
  const params = new URLSearchParams({ database, name });
  if (connection) params.set("connection", connection);
  if (schema) params.set("schema", schema);
  const res = await fetch(`${base}/api/lane/trigger-definition?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function getRelatedObjects(
  database: string,
  table: string,
  connection?: string,
  schema?: string,
): Promise<RelatedObject[]> {
  const params = new URLSearchParams({ database, table });
  if (connection) params.set("connection", connection);
  if (schema) params.set("schema", schema);
  const res = await fetch(`${base}/api/lane/related-objects?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

// RLS (Row-Level Security)

export interface RlsPolicyInfo {
  policy_name: string;
  command?: string;
  is_permissive?: boolean;
  roles?: string;
  using_expr?: string;
  with_check_expr?: string;
  predicate_type?: string;
  predicate_definition?: string;
  operation?: string;
  is_enabled?: boolean;
  create_date?: string;
  modify_date?: string;
}

export interface RlsStatus {
  rls_enabled: boolean;
  rls_forced?: boolean;
  policy_count?: number;
  enabled_count?: number;
}

export interface GenerateRlsSqlRequest {
  policy_name?: string;
  command?: string;
  permissive?: string;
  roles?: string;
  using_expr?: string;
  with_check_expr?: string;
  predicate_type?: string;
  predicate_function?: string;
  predicate_args?: string;
}

export async function listRlsPolicies(
  database: string,
  table: string,
  connection?: string,
  schema?: string,
): Promise<RlsPolicyInfo[]> {
  const params = new URLSearchParams({ database, table });
  if (connection) params.set("connection", connection);
  if (schema) params.set("schema", schema);
  const res = await fetch(`${base}/api/lane/rls-policies?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function getRlsStatus(
  database: string,
  table: string,
  connection?: string,
  schema?: string,
): Promise<RlsStatus | null> {
  const params = new URLSearchParams({ database, table });
  if (connection) params.set("connection", connection);
  if (schema) params.set("schema", schema);
  const res = await fetch(`${base}/api/lane/rls-status?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function generateRlsSql(
  database: string,
  table: string,
  action: string,
  body: GenerateRlsSqlRequest,
  connection?: string,
  schema?: string,
): Promise<{ sql: string }> {
  const params = new URLSearchParams({ database, table, action });
  if (connection) params.set("connection", connection);
  if (schema) params.set("schema", schema);
  const res = await fetch(`${base}/api/lane/rls-generate?${params}`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    try {
      const err: ApiError = JSON.parse(text);
      throw new Error(err.error.message);
    } catch (e) {
      if (e instanceof SyntaxError) throw new Error(`HTTP ${res.status}: ${text}`);
      throw e;
    }
  }
  return res.json();
}

export interface ExportResponse {
  success: boolean;
  total_rows: number;
  execution_time_ms: number;
  download_url: string;
}

export async function exportQuery(
  query: string,
  database: string,
  format: "csv" | "xlsx",
  connection?: string,
): Promise<ExportResponse> {
  const body: Record<string, unknown> = {
    query,
    database,
    outputFormat: format,
  };
  if (connection) body.connection = connection;

  const res = await fetch(`${base}/api/lane`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text();
    try {
      const err: ApiError = JSON.parse(text);
      throw new Error(err.error.message);
    } catch (e) {
      if (e instanceof SyntaxError) throw new Error(`HTTP ${res.status}: ${text}`);
      throw e;
    }
  }

  return res.json();
}

export async function executeQuery(query: string, database: string, connection?: string): Promise<QueryResult> {
  const body: Record<string, unknown> = {
    query,
    database,
    includeMetadata: true,
  };
  if (connection) body.connection = connection;

  const res = await fetch(`${base}/api/lane`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify(body),
  });

  const text = await res.text();

  if (!res.ok) {
    try {
      const err: ApiError = JSON.parse(text);
      let msg = err.error.message;
      if (err.error.suggestion) msg += `\n\nSuggestion: ${err.error.suggestion}`;
      throw new Error(msg);
    } catch (e) {
      if (e instanceof SyntaxError) {
        throw new Error(`HTTP ${res.status}: ${text}`);
      }
      throw e;
    }
  }

  return JSON.parse(text);
}

// ============================================================================
// Admin API
// ============================================================================

async function adminFetch(path: string, init?: RequestInit): Promise<Response> {
  const res = await fetch(`${base}/api/lane/admin/${path}`, {
    ...init,
    headers: { ...headers(), ...init?.headers },
  });
  if (res.status === 503) throw new Error("Access control is not enabled on this server.");
  if (!res.ok) {
    const body = await res.text();
    try {
      const err = JSON.parse(body);
      throw new Error(err.error || `HTTP ${res.status}`);
    } catch (e) {
      if (e instanceof Error && !e.message.startsWith("HTTP")) throw e;
      throw new Error(`HTTP ${res.status}: ${body}`);
    }
  }
  return res;
}

export async function listUsers(): Promise<UserInfo[]> {
  const res = await adminFetch("users");
  const data = await res.json();
  return data.users ?? [];
}

export async function createUser(email: string, display_name?: string, is_admin?: boolean): Promise<void> {
  await adminFetch("users", {
    method: "POST",
    body: JSON.stringify({ email, display_name, is_admin }),
  });
}

export async function updateUser(email: string, updates: { display_name?: string; is_admin?: boolean; is_enabled?: boolean; mcp_enabled?: boolean; pii_mode?: string; sql_mode?: string; max_pending_approvals?: number }): Promise<void> {
  await adminFetch(`users/${encodeURIComponent(email)}`, {
    method: "PUT",
    body: JSON.stringify(updates),
  });
}

export async function deleteUser(email: string): Promise<void> {
  await adminFetch(`users/${encodeURIComponent(email)}`, { method: "DELETE" });
}

export async function purgeUserSessions(email: string): Promise<void> {
  await adminFetch(`users/${encodeURIComponent(email)}/sessions`, { method: "DELETE" });
}

export async function listTokens(email?: string): Promise<TokenRecord[]> {
  const params = email ? `?email=${encodeURIComponent(email)}` : "";
  const res = await adminFetch(`tokens${params}`);
  const data = await res.json();
  return data.tokens ?? [];
}

export async function generateToken(email: string, label?: string, expires_hours?: number, pii_mode?: string | null): Promise<{ token: string }> {
  const body: Record<string, unknown> = { email, label, expires_hours };
  if (pii_mode) body.pii_mode = pii_mode;
  const res = await adminFetch("tokens/generate", {
    method: "POST",
    body: JSON.stringify(body),
  });
  return res.json();
}

export async function revokeToken(token: string): Promise<void> {
  await adminFetch("tokens/revoke", {
    method: "POST",
    body: JSON.stringify({ token }),
  });
}

export async function setPermissions(email: string, permissions: { database_name: string; table_pattern?: string; can_read?: boolean; can_write?: boolean; can_update?: boolean; can_delete?: boolean }[]): Promise<void> {
  await adminFetch("permissions", {
    method: "POST",
    body: JSON.stringify({ email, permissions }),
  });
}

export async function getAuditLog(filters?: { email?: string; action?: string; limit?: number }): Promise<AuditEntry[]> {
  const params = new URLSearchParams();
  if (filters?.email) params.set("email", filters.email);
  if (filters?.action) params.set("action", filters.action);
  if (filters?.limit) params.set("limit", String(filters.limit));
  const qs = params.toString();
  const res = await adminFetch(`audit${qs ? `?${qs}` : ""}`);
  const data = await res.json();
  return data.entries ?? [];
}

// Inventory types
export interface InventoryTable {
  schema: string;
  name: string;
}

export interface InventoryDatabase {
  name: string;
  tables: InventoryTable[];
}

export interface InventoryConnection {
  name: string;
  type: string;
  databases: InventoryDatabase[];
}

export async function getInventory(): Promise<InventoryConnection[]> {
  const res = await adminFetch("inventory");
  const data = await res.json();
  return data.connections ?? [];
}

export async function adminSetPassword(email: string, password: string): Promise<void> {
  await adminFetch(`users/${encodeURIComponent(email)}/password`, {
    method: "POST",
    body: JSON.stringify({ password }),
  });
}

// API Key Rotation

export async function rotateApiKey(): Promise<{ api_key: string }> {
  const res = await adminFetch("settings/rotate-api-key", { method: "POST" });
  return res.json();
}

// Connection Permissions

export async function getConnectionPermissions(email: string): Promise<string[] | null> {
  const res = await adminFetch(`connection-permissions?email=${encodeURIComponent(email)}`);
  const data = await res.json();
  return data.connections ?? null;
}

export async function setConnectionPermissions(email: string, connections: string[]): Promise<void> {
  await adminFetch("connection-permissions", {
    method: "POST",
    body: JSON.stringify({ email, connections }),
  });
}

// Storage Permissions

export interface StoragePermission {
  id?: number;
  identity?: string;
  connection_name: string;
  bucket_pattern: string;
  can_read: boolean;
  can_write: boolean;
  can_delete: boolean;
}

export async function getStoragePermissions(email: string): Promise<StoragePermission[]> {
  const res = await adminFetch(`storage-permissions?email=${encodeURIComponent(email)}`);
  const data = await res.json();
  return data.permissions ?? [];
}

export async function setStoragePermissions(email: string, permissions: StoragePermission[]): Promise<void> {
  await adminFetch("storage-permissions", {
    method: "POST",
    body: JSON.stringify({ email, permissions }),
  });
}

export async function getSaStoragePermissions(name: string): Promise<StoragePermission[]> {
  const res = await adminFetch(`sa-storage-permissions?name=${encodeURIComponent(name)}`);
  const data = await res.json();
  return data.permissions ?? [];
}

export async function setSaStoragePermissions(name: string, permissions: StoragePermission[]): Promise<void> {
  await adminFetch("sa-storage-permissions", {
    method: "POST",
    body: JSON.stringify({ name, permissions }),
  });
}

// ============================================================================
// Service Accounts
// ============================================================================

export interface ServiceAccountInfo {
  name: string;
  description: string | null;
  api_key_prefix: string;
  sql_mode: "none" | "read_only" | "supervised" | "confirmed" | "full";
  is_enabled: boolean;
  created_at: string;
  updated_at: string;
  permissions: Permission[];
  connection_permissions: string[] | null;
}

export async function listServiceAccounts(): Promise<ServiceAccountInfo[]> {
  const res = await adminFetch("service-accounts");
  const data = await res.json();
  return data.service_accounts ?? [];
}

export async function createServiceAccount(name: string, description?: string, sql_mode?: string): Promise<{ name: string; api_key: string }> {
  const res = await adminFetch("service-accounts", {
    method: "POST",
    body: JSON.stringify({ name, description, sql_mode }),
  });
  return res.json();
}

export async function updateServiceAccount(name: string, updates: { description?: string; sql_mode?: string; is_enabled?: boolean }): Promise<void> {
  await adminFetch(`service-accounts/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify(updates),
  });
}

export async function deleteServiceAccount(name: string): Promise<void> {
  await adminFetch(`service-accounts/${encodeURIComponent(name)}`, { method: "DELETE" });
}

export async function rotateServiceAccountKey(name: string): Promise<{ api_key: string }> {
  const res = await adminFetch(`service-accounts/${encodeURIComponent(name)}/rotate-key`, { method: "POST" });
  return res.json();
}

export async function getServiceAccountPermissions(name: string): Promise<Permission[]> {
  const res = await adminFetch(`service-account-permissions?name=${encodeURIComponent(name)}`);
  const data = await res.json();
  return data.permissions ?? [];
}

export async function setServiceAccountPermissions(name: string, permissions: Array<{ database_name: string; table_pattern?: string; can_read?: boolean; can_write?: boolean; can_update?: boolean; can_delete?: boolean }>): Promise<void> {
  await adminFetch("service-account-permissions", {
    method: "POST",
    body: JSON.stringify({ name, permissions }),
  });
}

export async function getServiceAccountConnections(name: string): Promise<string[] | null> {
  const res = await adminFetch(`service-account-connections?name=${encodeURIComponent(name)}`);
  const data = await res.json();
  return data.connections ?? null;
}

export async function setServiceAccountConnections(name: string, connections: string[]): Promise<void> {
  await adminFetch("service-account-connections", {
    method: "POST",
    body: JSON.stringify({ name, connections }),
  });
}

// ============================================================================
// Self-service Token API (session auth)
// ============================================================================

export async function selfListTokens(): Promise<TokenRecord[]> {
  const res = await fetch(`${base}/api/lane/tokens`, { headers: headers() });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  const data = await res.json();
  return data.tokens ?? [];
}

export async function selfGenerateToken(label?: string, expires_hours?: number): Promise<{ token: string; expires_hours: number }> {
  const body: Record<string, unknown> = {};
  if (label) body.label = label;
  if (expires_hours !== undefined) body.expires_hours = expires_hours;
  const res = await fetch(`${base}/api/lane/tokens`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(data.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function selfRevokeToken(prefix: string): Promise<void> {
  const res = await fetch(`${base}/api/lane/tokens/${encodeURIComponent(prefix)}`, {
    method: "DELETE",
    headers: headers(),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
}

// ============================================================================
// Token Policy API (admin)
// ============================================================================

export interface TokenPolicy {
  max_lifespan_hours: number;
  default_lifespan_hours: number;
}

export async function getTokenPolicy(): Promise<TokenPolicy> {
  const res = await adminFetch("settings/token-policy");
  return res.json();
}

export async function setTokenPolicy(policy: TokenPolicy): Promise<void> {
  await adminFetch("settings/token-policy", {
    method: "PUT",
    body: JSON.stringify(policy),
  });
}

// ============================================================================
// Query History API
// ============================================================================

export interface QueryHistoryEntry {
  id: number;
  email: string;
  connection_name: string | null;
  database_name: string | null;
  sql_text: string;
  execution_time_ms: number | null;
  row_count: number | null;
  is_success: boolean;
  error_message: string | null;
  is_favorite: boolean;
  created_at: string;
}

export async function listHistory(params?: {
  limit?: number;
  offset?: number;
  search?: string;
  favorites_only?: boolean;
}): Promise<QueryHistoryEntry[]> {
  const qs = new URLSearchParams();
  if (params?.limit) qs.set("limit", String(params.limit));
  if (params?.offset) qs.set("offset", String(params.offset));
  if (params?.search) qs.set("search", params.search);
  if (params?.favorites_only) qs.set("favorites_only", "true");
  const q = qs.toString();
  const res = await fetch(`${base}/api/lane/history${q ? `?${q}` : ""}`, { headers: headers() });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return data.entries ?? [];
}

export async function toggleHistoryFavorite(id: number, is_favorite: boolean): Promise<void> {
  const res = await fetch(`${base}/api/lane/history/${id}/favorite`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ is_favorite }),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
}

export async function deleteHistoryEntry(id: number): Promise<void> {
  const res = await fetch(`${base}/api/lane/history/${id}`, {
    method: "DELETE",
    headers: headers(),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
}

// ============================================================================
// Connection Management API
// ============================================================================

export interface AdminConnectionInfo {
  name: string;
  type: string;
  host: string;
  port: number;
  database: string;
  is_default: boolean;
  is_enabled: boolean;
  status: string;
  status_message: string | null;
}

export interface CreateConnectionData {
  name: string;
  type: string;
  host: string;
  port?: number;
  database: string;
  username: string;
  password: string;
  options_json?: string;
  sslmode?: string;
  is_default?: boolean;
}

export interface UpdateConnectionData {
  type?: string;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
  options_json?: string;
  sslmode?: string;
  is_default?: boolean;
  is_enabled?: boolean;
}

export interface TestConnectionResult {
  success: boolean;
  message: string;
}

export async function listAdminConnections(): Promise<AdminConnectionInfo[]> {
  const res = await adminFetch("connections");
  const data = await res.json();
  return data.connections ?? [];
}

export async function createConnection(data: CreateConnectionData): Promise<AdminConnectionInfo> {
  const res = await adminFetch("connections", {
    method: "POST",
    body: JSON.stringify(data),
  });
  return res.json();
}

export async function updateConnection(name: string, data: UpdateConnectionData): Promise<AdminConnectionInfo> {
  const res = await adminFetch(`connections/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify(data),
  });
  return res.json();
}

export async function deleteConnection(name: string): Promise<void> {
  await adminFetch(`connections/${encodeURIComponent(name)}`, { method: "DELETE" });
}

export async function testConnection(name: string): Promise<TestConnectionResult> {
  const res = await adminFetch(`connections/${encodeURIComponent(name)}/test`, {
    method: "POST",
  });
  return res.json();
}

export async function testConnectionInline(data: {
  type: string;
  host: string;
  port?: number;
  database: string;
  username: string;
  password: string;
  options_json?: string;
  sslmode?: string;
}): Promise<TestConnectionResult> {
  const res = await adminFetch("connections/test", {
    method: "POST",
    body: JSON.stringify(data),
  });
  return res.json();
}

// ============================================================================
// PII Management API
// ============================================================================

export interface PiiRule {
  id: number;
  name: string;
  description: string | null;
  regex_pattern: string;
  replacement_text: string;
  entity_kind: string;
  is_builtin: boolean;
  is_enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface PiiColumn {
  id: number;
  connection_name: string;
  database_name: string;
  schema_name: string;
  table_name: string;
  column_name: string;
  pii_type: string;
  custom_replacement: string | null;
  created_at: string;
}

export interface PiiSettings {
  global_enabled: boolean;
  default_mode: string;
  connection_overrides: Record<string, string>;
}

export interface PiiTestResult {
  matches: { start: number; end: number; text: string }[];
  scrubbed_text: string;
}

export interface PiiDiscoveryResult {
  column_name: string;
  detected_types: string[];
  match_count: number;
  sample_matches: string[];
}

export async function listPiiRules(): Promise<PiiRule[]> {
  const res = await adminFetch("pii/rules");
  const data = await res.json();
  return data.rules ?? [];
}

export async function createPiiRule(rule: {
  name: string;
  description?: string;
  regex_pattern: string;
  replacement_text: string;
  entity_kind: string;
}): Promise<{ id: number }> {
  const res = await adminFetch("pii/rules", {
    method: "POST",
    body: JSON.stringify(rule),
  });
  return res.json();
}

export async function updatePiiRule(
  id: number,
  updates: {
    name?: string;
    description?: string;
    regex_pattern?: string;
    replacement_text?: string;
    entity_kind?: string;
    is_enabled?: boolean;
  }
): Promise<void> {
  await adminFetch(`pii/rules/${id}`, {
    method: "PUT",
    body: JSON.stringify(updates),
  });
}

export async function deletePiiRule(id: number): Promise<void> {
  await adminFetch(`pii/rules/${id}`, { method: "DELETE" });
}

export async function testPiiRule(data: {
  regex_pattern: string;
  replacement_text: string;
  sample_text: string;
}): Promise<PiiTestResult> {
  const res = await adminFetch("pii/rules/test", {
    method: "POST",
    body: JSON.stringify(data),
  });
  return res.json();
}

export async function listPiiColumns(filters?: {
  connection?: string;
  database?: string;
}): Promise<PiiColumn[]> {
  const params = new URLSearchParams();
  if (filters?.connection) params.set("connection", filters.connection);
  if (filters?.database) params.set("database", filters.database);
  const qs = params.toString();
  const res = await adminFetch(`pii/columns${qs ? `?${qs}` : ""}`);
  const data = await res.json();
  return data.columns ?? [];
}

export async function setPiiColumn(data: {
  connection_name: string;
  database_name: string;
  schema_name?: string;
  table_name: string;
  column_name: string;
  pii_type?: string;
  custom_replacement?: string;
}): Promise<void> {
  await adminFetch("pii/columns", {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export async function removePiiColumn(id: number): Promise<void> {
  await adminFetch(`pii/columns/${id}`, { method: "DELETE" });
}

export async function discoverPiiColumns(data: {
  connection: string;
  database: string;
  schema?: string;
  table: string;
  sample_rows?: number;
}): Promise<PiiDiscoveryResult[]> {
  const res = await adminFetch("pii/columns/discover", {
    method: "POST",
    body: JSON.stringify(data),
  });
  const result = await res.json();
  return result.suggestions ?? [];
}

export async function getPiiSettings(): Promise<PiiSettings> {
  const res = await adminFetch("pii/settings");
  return res.json();
}

export async function setPiiSettings(settings: {
  global_enabled?: boolean;
  default_mode?: string;
  connection_overrides?: Record<string, string>;
}): Promise<void> {
  await adminFetch("pii/settings", {
    method: "PUT",
    body: JSON.stringify(settings),
  });
}

// ============================================================================
// Realtime API
// ============================================================================

export interface RealtimeTableEntry {
  connection_name: string;
  database_name: string;
  table_name: string;
  enabled_by: string | null;
  created_at: string;
}

export interface RealtimeEvent {
  id: string;
  connection: string;
  database: string;
  table: string;
  query_type: string;
  row_count: number | null;
  user: string | null;
  timestamp: string;
}

export async function listRealtimeTables(): Promise<RealtimeTableEntry[]> {
  const res = await adminFetch("realtime/tables");
  return res.json();
}

export async function enableRealtime(connection: string, database: string, table: string): Promise<void> {
  await adminFetch("realtime/enable", {
    method: "POST",
    body: JSON.stringify({ connection, database, table }),
  });
}

export async function disableRealtime(connection: string, database: string, table: string): Promise<void> {
  await adminFetch("realtime/disable", {
    method: "POST",
    body: JSON.stringify({ connection, database, table }),
  });
}

export function subscribeRealtime(
  connection: string,
  database: string,
  table: string,
  onEvent: (event: RealtimeEvent) => void,
  onError?: (error: Event) => void,
): EventSource {
  const params = new URLSearchParams({ connection, database, table });
  const token = getSessionToken();
  // EventSource doesn't support custom headers, so pass token as query param
  if (token) params.set("token", token);
  const es = new EventSource(`${base}/api/lane/realtime/subscribe?${params}`);
  es.addEventListener("change", (e) => {
    try {
      const data: RealtimeEvent = JSON.parse(e.data);
      onEvent(data);
    } catch { /* ignore parse errors */ }
  });
  if (onError) es.onerror = onError;
  return es;
}

// ============================================================================
// Monitor API
// ============================================================================

export interface ActiveQuery {
  spid: number;
  status: string | null;
  command?: string;
  duration_seconds: number | null;
  wait_type: string | null;
  wait_time?: number;
  wait_event?: string | null;
  blocking_session_id?: number;
  database_name: string | null;
  username?: string | null;
  query_text: string | null;
}

export async function listActiveQueries(connection?: string): Promise<ActiveQuery[]> {
  const params = connection ? `?connection=${encodeURIComponent(connection)}` : "";
  const res = await fetch(`${base}/api/lane/monitor/queries${params}`, { headers: headers() });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  const data = await res.json();
  return data.queries ?? [];
}

export async function killQuery(processId: number, connection?: string): Promise<void> {
  const body: Record<string, unknown> = { process_id: processId };
  if (connection) body.connection = connection;
  const res = await fetch(`${base}/api/lane/monitor/kill`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(data.error || `HTTP ${res.status}`);
  }
}

// ============================================================================
// Connection Health API
// ============================================================================

export interface PoolStats {
  total_connections: number;
  idle_connections: number;
  active_connections: number;
  max_size: number;
}

export interface HealthHistoryEntry {
  status: string;
  error_message: string | null;
  checked_at: string;
}

export interface ConnectionHealth {
  name: string;
  dialect: string;
  status: string;
  status_message: string | null;
  pool: PoolStats | null;
  history: HealthHistoryEntry[];
}

export async function getConnectionsHealth(): Promise<ConnectionHealth[]> {
  const res = await fetch(`${base}/api/lane/connections/health`, { headers: headers() });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  const data = await res.json();
  return data.connections ?? [];
}

// ============================================================================
// Import API
// ============================================================================

export interface ImportPreviewColumn {
  name: string;
  inferred_type: string;
  sql_type: string;
  nullable: boolean;
}

export interface ImportPreviewResult {
  preview_id: string;
  file_name: string;
  total_rows: number;
  columns: ImportPreviewColumn[];
  preview_rows: (string | null)[][];
}

export interface ImportExecuteResult {
  success: boolean;
  rows_imported: number;
  batches: number;
  table_created: boolean;
  execution_time_ms: number;
}

export async function previewImport(formData: FormData): Promise<ImportPreviewResult> {
  const h: Record<string, string> = {};
  const token = getSessionToken();
  if (token) h["Authorization"] = `Bearer ${token}`;
  // Do NOT set Content-Type — browser sets multipart boundary automatically

  const res = await fetch(`${base}/api/lane/import/preview`, {
    method: "POST",
    headers: h,
    body: formData,
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: { message: `HTTP ${res.status}` } }));
    throw new Error(body.error?.message || body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

// ============================================================================
// Workspace API (DuckDB)
// ============================================================================

export interface WorkspaceTable {
  table_name: string;
  original_filename: string;
  uploaded_at: string;
  row_count: number;
  column_count: number;
}

export async function workspaceListTables(): Promise<WorkspaceTable[]> {
  const res = await fetch(`${base}/api/lane/workspace/tables`, { headers: headers() });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  const data = await res.json();
  return data.tables ?? [];
}

export async function workspaceUpload(formData: FormData): Promise<{ table_name: string; row_count: number; column_count: number; columns: string[] }> {
  const h: Record<string, string> = {};
  const token = getSessionToken();
  if (token) h["Authorization"] = `Bearer ${token}`;

  const res = await fetch(`${base}/api/lane/workspace/upload`, {
    method: "POST",
    headers: h,
    body: formData,
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: { message: `HTTP ${res.status}` } }));
    throw new Error(body.error?.message || body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function workspaceDeleteTable(name: string): Promise<void> {
  const res = await fetch(`${base}/api/lane/workspace/tables/${encodeURIComponent(name)}`, {
    method: "DELETE",
    headers: headers(),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
}

export async function workspaceClear(): Promise<void> {
  const res = await fetch(`${base}/api/lane/workspace/clear`, {
    method: "POST",
    headers: headers(),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
}

export async function workspaceQuery(query: string): Promise<QueryResult> {
  const res = await fetch(`${base}/api/lane/workspace/query`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ query }),
  });

  const text = await res.text();
  if (!res.ok) {
    try {
      const err: ApiError = JSON.parse(text);
      let msg = err.error.message;
      if (err.error.suggestion) msg += `\n\nSuggestion: ${err.error.suggestion}`;
      throw new Error(msg);
    } catch (e) {
      if (e instanceof SyntaxError) throw new Error(`HTTP ${res.status}: ${text}`);
      throw e;
    }
  }
  return JSON.parse(text);
}

export async function executeImport(body: {
  preview_id: string;
  connection?: string;
  database: string;
  schema?: string;
  table_name: string;
  if_exists?: string;
  columns?: { name: string; sql_type?: string; include?: boolean }[];
}): Promise<ImportExecuteResult> {
  const res = await fetch(`${base}/api/lane/import/execute`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const data = await res.json().catch(() => ({ error: { message: `HTTP ${res.status}` } }));
    throw new Error(data.error?.message || data.error || `HTTP ${res.status}`);
  }
  return res.json();
}

// ============================================================================
// Storage API (MinIO/S3)
// ============================================================================

export interface BucketInfo {
  name: string;
}

export interface ObjectInfo {
  key: string;
  size: number;
  last_modified: string | null;
  is_prefix: boolean;
}

export interface ObjectMeta {
  key: string;
  size: number;
  content_type: string | null;
  last_modified: string | null;
  etag: string | null;
}

export async function storageListConnections(): Promise<string[]> {
  const res = await fetch(`${base}/api/lane/storage/connections`, { headers: headers() });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  const data = await res.json();
  return data.connections ?? [];
}

export async function storageListBuckets(connection: string): Promise<BucketInfo[]> {
  const res = await fetch(`${base}/api/lane/storage/buckets?connection=${encodeURIComponent(connection)}`, { headers: headers() });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  const data = await res.json();
  return data.buckets ?? [];
}

export async function storageCreateBucket(connection: string, name: string): Promise<void> {
  const res = await fetch(`${base}/api/lane/storage/buckets`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ connection, name }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
}

export async function storageDeleteBucket(connection: string, name: string): Promise<void> {
  const res = await fetch(`${base}/api/lane/storage/buckets/${encodeURIComponent(name)}?connection=${encodeURIComponent(connection)}`, {
    method: "DELETE",
    headers: headers(),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
}

export async function storageListObjects(connection: string, bucket: string, prefix?: string): Promise<ObjectInfo[]> {
  let url = `${base}/api/lane/storage/objects?connection=${encodeURIComponent(connection)}&bucket=${encodeURIComponent(bucket)}`;
  if (prefix) url += `&prefix=${encodeURIComponent(prefix)}`;
  const res = await fetch(url, { headers: headers() });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  const data = await res.json();
  return data.objects ?? [];
}

export async function storageUploadObject(connection: string, bucket: string, file: File, key?: string): Promise<{ key: string; size: number }> {
  const formData = new FormData();
  formData.append("connection", connection);
  formData.append("bucket", bucket);
  if (key) formData.append("key", key);
  formData.append("file", file);

  const h: Record<string, string> = {};
  const token = getSessionToken();
  if (token) h["Authorization"] = `Bearer ${token}`;

  const res = await fetch(`${base}/api/lane/storage/upload`, {
    method: "POST",
    headers: h,
    body: formData,
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function storageDownloadObject(connection: string, bucket: string, key: string): Promise<Blob> {
  const url = `${base}/api/lane/storage/download?connection=${encodeURIComponent(connection)}&bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`;
  const res = await fetch(url, { headers: headers() });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.blob();
}

export async function storageDeleteObject(connection: string, bucket: string, key: string): Promise<void> {
  const url = `${base}/api/lane/storage/objects?connection=${encodeURIComponent(connection)}&bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`;
  const res = await fetch(url, {
    method: "DELETE",
    headers: headers(),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
}

export async function storageObjectMetadata(connection: string, bucket: string, key: string): Promise<ObjectMeta> {
  const url = `${base}/api/lane/storage/metadata?connection=${encodeURIComponent(connection)}&bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`;
  const res = await fetch(url, { headers: headers() });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function storagePreview(connection: string, bucket: string, key: string): Promise<{ table_name: string; row_count: number }> {
  const res = await fetch(`${base}/api/lane/storage/preview`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ connection, bucket, key }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

// ============================================================================
// Storage Integration (export query, import to workspace, workspace export)
// ============================================================================

export async function storageExportQuery(params: {
  connection?: string;
  database?: string;
  query: string;
  storage_connection: string;
  bucket: string;
  key: string;
  format?: string;
}): Promise<{ success: boolean; key: string; size: number; row_count: number; format: string }> {
  const res = await fetch(`${base}/api/lane/storage/export-query`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify(params),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function storageImportToWorkspace(params: {
  connection: string;
  bucket: string;
  key: string;
  table_name?: string;
}): Promise<{ success: boolean; table_name: string; row_count: number; source: { connection: string; bucket: string; key: string } }> {
  const res = await fetch(`${base}/api/lane/storage/import-to-workspace`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify(params),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function workspaceExportToStorage(params: {
  query: string;
  storage_connection: string;
  bucket: string;
  key: string;
  format?: string;
}): Promise<{ success: boolean; key: string; size: number; row_count: number; format: string }> {
  const res = await fetch(`${base}/api/lane/storage/workspace-export`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify(params),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

// ============================================================================
// Storage Column Links
// ============================================================================

export interface StorageColumnLink {
  id: number;
  connection_name: string;
  database_name: string;
  schema_name: string | null;
  table_name: string;
  column_name: string;
  storage_connection: string;
  bucket_name: string;
  key_prefix: string | null;
  created_at: string;
}

export async function listStorageColumnLinks(filters?: {
  connection?: string;
  database?: string;
}): Promise<StorageColumnLink[]> {
  const params = new URLSearchParams();
  if (filters?.connection) params.set("connection", filters.connection);
  if (filters?.database) params.set("database", filters.database);
  const qs = params.toString() ? `?${params}` : "";
  const res = await adminFetch(`storage/column-links${qs}`);
  const data = await res.json();
  return data.links;
}

export async function setStorageColumnLink(data: {
  connection_name: string;
  database_name: string;
  schema_name?: string;
  table_name: string;
  column_name: string;
  storage_connection: string;
  bucket_name: string;
  key_prefix?: string;
}): Promise<void> {
  await adminFetch("storage/column-links", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
}

export async function removeStorageColumnLink(id: number): Promise<void> {
  await adminFetch(`storage/column-links/${id}`, { method: "DELETE" });
}

export async function getActiveStorageColumnLinks(filters?: {
  connection?: string;
  database?: string;
}): Promise<StorageColumnLink[]> {
  const params = new URLSearchParams();
  if (filters?.connection) params.set("connection", filters.connection);
  if (filters?.database) params.set("database", filters.database);
  const qs = params.toString() ? `?${params}` : "";
  const res = await fetch(`${base}/api/lane/storage/column-links${qs}`, {
    headers: headers(),
  });
  if (!res.ok) return [];
  const data = await res.json();
  return data.links;
}

// ============================================================================
// Approvals API
// ============================================================================

export interface ApprovalSummary {
  id: string;
  user_email: string;
  tool_name: string;
  target_connection: string;
  target_database: string;
  context: string;
  created_at: string;
}

export interface ApprovalDetail extends ApprovalSummary {
  sql_statements: string[];
}

export async function listApprovals(): Promise<ApprovalSummary[]> {
  const res = await fetch(`${base}/api/lane/approvals`, { headers: headers() });
  if (!res.ok) return [];
  return res.json();
}

export async function getApproval(id: string): Promise<ApprovalDetail | null> {
  const res = await fetch(`${base}/api/lane/approvals/${id}`, { headers: headers() });
  if (!res.ok) return null;
  return res.json();
}

export async function approveApproval(id: string): Promise<void> {
  const res = await fetch(`${base}/api/lane/approvals/${id}/approve`, {
    method: "POST",
    headers: headers(),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(data.error || `HTTP ${res.status}`);
  }
}

export async function rejectApproval(id: string, reason?: string): Promise<void> {
  const res = await fetch(`${base}/api/lane/approvals/${id}/reject`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ reason }),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(data.error || `HTTP ${res.status}`);
  }
}

// ============================================================================
// Teams & Projects API
// ============================================================================

export interface Team {
  id: string;
  name: string;
  webhook_url: string | null;
  member_count: number;
  project_count: number;
  created_at: string;
}

export interface Project {
  id: string;
  team_id: string;
  name: string;
  member_count: number;
  created_at: string;
}

export interface TeamMember {
  email: string;
  role: "team_lead" | "member";
  display_name: string | null;
}

export interface ProjectMember {
  email: string;
  role: "project_lead" | "member";
  display_name: string | null;
}

export async function listTeams(): Promise<Team[]> {
  const res = await adminFetch("teams");
  return res.json();
}

export async function createTeam(name: string, webhook_url?: string): Promise<{ id: string }> {
  const res = await adminFetch("teams", {
    method: "POST",
    body: JSON.stringify({ name, webhook_url }),
  });
  return res.json();
}

export async function updateTeam(id: string, updates: { name?: string; webhook_url?: string | null }): Promise<void> {
  await adminFetch(`teams/${id}`, {
    method: "PUT",
    body: JSON.stringify(updates),
  });
}

export async function deleteTeam(id: string): Promise<void> {
  await adminFetch(`teams/${id}`, { method: "DELETE" });
}

export async function listTeamMembers(teamId: string): Promise<TeamMember[]> {
  const res = await adminFetch(`teams/${teamId}/members`);
  return res.json();
}

export async function addTeamMember(teamId: string, email: string, role?: string): Promise<void> {
  await adminFetch(`teams/${teamId}/members`, {
    method: "POST",
    body: JSON.stringify({ email, role }),
  });
}

export async function setTeamMemberRole(teamId: string, email: string, role: string): Promise<void> {
  await adminFetch(`teams/${teamId}/members/${encodeURIComponent(email)}`, {
    method: "PUT",
    body: JSON.stringify({ role }),
  });
}

export async function removeTeamMember(teamId: string, email: string): Promise<void> {
  await adminFetch(`teams/${teamId}/members/${encodeURIComponent(email)}`, { method: "DELETE" });
}

export async function listProjects(teamId: string): Promise<Project[]> {
  const res = await adminFetch(`teams/${teamId}/projects`);
  return res.json();
}

export async function createProject(teamId: string, name: string): Promise<{ id: string }> {
  const res = await adminFetch(`teams/${teamId}/projects`, {
    method: "POST",
    body: JSON.stringify({ name }),
  });
  return res.json();
}

export async function updateProject(id: string, name: string): Promise<void> {
  await adminFetch(`projects/${id}`, {
    method: "PUT",
    body: JSON.stringify({ name }),
  });
}

export async function deleteProject(id: string): Promise<void> {
  await adminFetch(`projects/${id}`, { method: "DELETE" });
}

export async function listProjectMembers(projectId: string): Promise<ProjectMember[]> {
  const res = await adminFetch(`projects/${projectId}/members`);
  return res.json();
}

export async function addProjectMember(projectId: string, email: string, role?: string): Promise<void> {
  await adminFetch(`projects/${projectId}/members`, {
    method: "POST",
    body: JSON.stringify({ email, role }),
  });
}

export async function setProjectMemberRole(projectId: string, email: string, role: string): Promise<void> {
  await adminFetch(`projects/${projectId}/members/${encodeURIComponent(email)}`, {
    method: "PUT",
    body: JSON.stringify({ role }),
  });
}

export async function removeProjectMember(projectId: string, email: string): Promise<void> {
  await adminFetch(`projects/${projectId}/members/${encodeURIComponent(email)}`, { method: "DELETE" });
}

// ============================================================================
// Endpoint API
// ============================================================================

export async function listEndpoints(): Promise<EndpointInfo[]> {
  const res = await adminFetch("endpoints");
  return res.json();
}

export async function createEndpoint(data: {
  name: string;
  connection_name: string;
  database_name: string;
  query: string;
  description?: string;
  parameters?: string;
}): Promise<void> {
  await adminFetch("endpoints", {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export async function updateEndpoint(
  name: string,
  data: {
    connection_name: string;
    database_name: string;
    query: string;
    description?: string;
    parameters?: string;
  },
): Promise<void> {
  await adminFetch(`endpoints/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify(data),
  });
}

export async function deleteEndpoint(name: string): Promise<void> {
  await adminFetch(`endpoints/${encodeURIComponent(name)}`, { method: "DELETE" });
}

export async function getEndpointPermissions(name: string): Promise<string[]> {
  const res = await adminFetch(`endpoints/${encodeURIComponent(name)}/permissions`);
  return res.json();
}

export async function setEndpointPermissions(name: string, emails: string[]): Promise<void> {
  await adminFetch(`endpoints/${encodeURIComponent(name)}/permissions`, {
    method: "PUT",
    body: JSON.stringify({ emails }),
  });
}

// ============================================================================
// Search API
// ============================================================================

export interface SchemaSearchResult {
  connection: string;
  database: string;
  schema: string;
  object_name: string;
  object_type: string;
  columns: string;
  snippet: string;
  rank: number;
}

export interface QuerySearchResult {
  email: string;
  connection: string;
  database: string;
  sql_text: string;
  snippet: string;
  rank: number;
}

export interface EndpointSearchResult {
  name: string;
  connection: string;
  database: string;
  description: string;
  query: string;
  snippet: string;
  rank: number;
}

export interface UnifiedSearchResult {
  schema: SchemaSearchResult[];
  queries: QuerySearchResult[];
  endpoints: EndpointSearchResult[];
}

export interface SearchStats {
  schema_objects: number;
  queries: number;
  endpoints: number;
}

export async function searchAll(query: string, limit?: number): Promise<UnifiedSearchResult> {
  const params = new URLSearchParams({ q: query });
  if (limit) params.set("limit", String(limit));
  const res = await fetch(`${base}/api/lane/search?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`Search failed: HTTP ${res.status}`);
  return res.json();
}

export async function searchSchema(query: string, limit?: number): Promise<{ results: SchemaSearchResult[]; total: number }> {
  const params = new URLSearchParams({ q: query });
  if (limit) params.set("limit", String(limit));
  const res = await fetch(`${base}/api/lane/search/schema?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`Search failed: HTTP ${res.status}`);
  return res.json();
}

export async function searchQueries(query: string, limit?: number, email?: string): Promise<{ results: QuerySearchResult[]; total: number }> {
  const params = new URLSearchParams({ q: query });
  if (limit) params.set("limit", String(limit));
  if (email) params.set("email", email);
  const res = await fetch(`${base}/api/lane/search/queries?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`Search failed: HTTP ${res.status}`);
  return res.json();
}

export async function searchEndpoints(query: string, limit?: number): Promise<{ results: EndpointSearchResult[]; total: number }> {
  const params = new URLSearchParams({ q: query });
  if (limit) params.set("limit", String(limit));
  const res = await fetch(`${base}/api/lane/search/endpoints?${params}`, { headers: headers() });
  if (!res.ok) throw new Error(`Search failed: HTTP ${res.status}`);
  return res.json();
}

export async function adminReindex(): Promise<void> {
  await adminFetch("search/reindex", { method: "POST" });
}

export async function adminSearchStats(): Promise<SearchStats> {
  const res = await adminFetch("search/stats");
  return res.json();
}
