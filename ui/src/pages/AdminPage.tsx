import { useState, useEffect, useCallback } from "react";
import {
  listUsers, createUser, updateUser, deleteUser, purgeUserSessions,
  listTokens, generateToken, revokeToken,
  setPermissions, getAuditLog, adminSetPassword,
  listAdminConnections, createConnection, updateConnection,
  deleteConnection, testConnection, testConnectionInline,
  getInventory, getTokenPolicy, setTokenPolicy,
  rotateApiKey, setConnectionPermissions, getStoragePermissions, setStoragePermissions,
  listPiiRules, createPiiRule, updatePiiRule, deletePiiRule, testPiiRule,
  listPiiColumns, setPiiColumn, removePiiColumn, discoverPiiColumns,
  getPiiSettings, setPiiSettings,
  listConnections, listDatabases, listSchemas, listTables, describeTable,
  listStorageColumnLinks, setStorageColumnLink, removeStorageColumnLink,
  storageListConnections, storageListBuckets,
  listServiceAccounts, createServiceAccount, updateServiceAccount,
  deleteServiceAccount, rotateServiceAccountKey,
  setServiceAccountPermissions, setServiceAccountConnections,
  listTeams, createTeam, updateTeam, deleteTeam,
  listTeamMembers, addTeamMember, setTeamMemberRole, removeTeamMember,
  listProjects, createProject, deleteProject,
  listProjectMembers, addProjectMember, setProjectMemberRole, removeProjectMember,
  listEndpoints, createEndpoint, updateEndpoint, deleteEndpoint,
  getEndpointPermissions, setEndpointPermissions,
  listGraphEdges, createGraphEdge, deleteGraphEdge, seedGraph,
} from "../lib/api";
import type {
  UserInfo, TokenRecord, Permission, AuditEntry,
  AdminConnectionInfo, CreateConnectionData, TestConnectionResult,
  InventoryConnection, TokenPolicy,
  PiiRule, PiiColumn, PiiSettings, PiiTestResult, PiiDiscoveryResult,
  ColumnInfo, StorageColumnLink, BucketInfo,
  ServiceAccountInfo, StoragePermission,
  Team, Project, TeamMember, ProjectMember,
  EndpointInfo,
  GraphEdgeExpanded, SeedResult, CreateEdgeData, ConnectionInfo,
  DatabaseInfo, TableInfo,
} from "../lib/api";
import { formatExpiry } from "./MyAccessPage";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle,
  DialogFooter, DialogDescription,
} from "@/components/ui/dialog";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";

// ============================================================================
// Admin Page
// ============================================================================

export default function AdminPage() {
  const [error, setError] = useState<string | null>(null);

  return (
    <div className="h-full flex flex-col p-4 gap-4">
      <h2 className="text-xl font-bold">Admin</h2>
      {error && (
        <div className="bg-destructive/20 border border-destructive text-destructive px-4 py-2 rounded-md text-sm">
          {error}
          <button className="ml-2 underline" onClick={() => setError(null)}>dismiss</button>
        </div>
      )}
      <Tabs defaultValue="connections">
        <TabsList>
          <TabsTrigger value="connections">Connections</TabsTrigger>
          <TabsTrigger value="users">Users</TabsTrigger>
          <TabsTrigger value="tokens">Tokens</TabsTrigger>
          <TabsTrigger value="permissions">Permissions</TabsTrigger>
          <TabsTrigger value="teams">Teams</TabsTrigger>
          <TabsTrigger value="pii">PII</TabsTrigger>
          <TabsTrigger value="service-accounts">Service Accounts</TabsTrigger>
          <TabsTrigger value="audit">Audit Log</TabsTrigger>
          <TabsTrigger value="endpoints">Endpoints</TabsTrigger>
          <TabsTrigger value="storage-links">Storage Links</TabsTrigger>
          <TabsTrigger value="graph">Graph</TabsTrigger>
          <TabsTrigger value="settings">Settings</TabsTrigger>
        </TabsList>
        <TabsContent value="connections"><ConnectionsTab onError={setError} /></TabsContent>
        <TabsContent value="users"><UsersTab onError={setError} /></TabsContent>
        <TabsContent value="tokens"><TokensTab onError={setError} /></TabsContent>
        <TabsContent value="permissions"><PermissionsTab onError={setError} /></TabsContent>
        <TabsContent value="teams"><TeamsTab onError={setError} /></TabsContent>
        <TabsContent value="service-accounts"><ServiceAccountsTab onError={setError} /></TabsContent>
        <TabsContent value="pii"><PiiTab onError={setError} /></TabsContent>
        <TabsContent value="audit"><AuditTab onError={setError} /></TabsContent>
        <TabsContent value="endpoints"><EndpointsTab onError={setError} /></TabsContent>
        <TabsContent value="storage-links"><StorageLinksTab onError={setError} /></TabsContent>
        <TabsContent value="graph"><GraphTab onError={setError} /></TabsContent>
        <TabsContent value="settings"><SettingsTab onError={setError} /></TabsContent>
      </Tabs>
    </div>
  );
}

// ============================================================================
// Connections Tab
// ============================================================================

function StatusBadge({ status, message }: { status: string; message?: string | null }) {
  if (status === "connected") {
    return <Badge variant="outline" className="text-green-400 border-green-400/50">Connected</Badge>;
  }
  if (status === "error") {
    return <span title={message ?? undefined}><Badge variant="destructive">Error</Badge></span>;
  }
  return <Badge variant="outline" className="text-gray-400 border-gray-400/50">Unknown</Badge>;
}

function ConnectionsTab({ onError }: { onError: (msg: string) => void }) {
  const [connections, setConnections] = useState<AdminConnectionInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [editTarget, setEditTarget] = useState<AdminConnectionInfo | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const [unavailable, setUnavailable] = useState(false);

  const refresh = useCallback(async () => {
    try {
      setConnections(await listAdminConnections());
      setUnavailable(false);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes("not enabled")) setUnavailable(true);
      else onError(msg);
    } finally {
      setLoading(false);
    }
  }, [onError]);

  useEffect(() => { refresh(); }, [refresh]);

  // Auto-refresh status every 10s
  useEffect(() => {
    const timer = setInterval(refresh, 10000);
    return () => clearInterval(timer);
  }, [refresh]);

  const handleTest = async (name: string) => {
    try {
      const result = await testConnection(name);
      if (!result.success) {
        onError(`Test failed for "${name}": ${result.message}`);
      }
      refresh();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
  };

  if (loading) return <p className="text-muted-foreground py-4">Loading connections...</p>;
  if (unavailable) return (
    <Card><CardContent className="py-8 text-center text-muted-foreground">
      Access control is not enabled on this server.
    </CardContent></Card>
  );

  return (
    <>
      <div className="flex justify-between items-center mb-3">
        <Button variant="outline" size="sm" onClick={refresh}>Refresh</Button>
        <Button size="sm" onClick={() => setShowCreate(true)}>Add Connection</Button>
      </div>
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Host:Port</TableHead>
                <TableHead>Database</TableHead>
                <TableHead>Default</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="w-[200px]">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {connections.length === 0 ? (
                <TableRow><TableCell colSpan={7} className="text-center text-muted-foreground py-8">No connections configured</TableCell></TableRow>
              ) : connections.map((c) => (
                <TableRow key={c.name}>
                  <TableCell className="font-mono text-xs">{c.name}</TableCell>
                  <TableCell><Badge variant="outline">{c.type}</Badge></TableCell>
                  <TableCell className="text-xs">{c.host}:{c.port}</TableCell>
                  <TableCell className="text-xs">{c.database}</TableCell>
                  <TableCell>{c.is_default ? <Badge>Default</Badge> : null}</TableCell>
                  <TableCell><StatusBadge status={c.status} message={c.status_message} /></TableCell>
                  <TableCell className="space-x-1">
                    <Button variant="ghost" size="sm" onClick={() => setEditTarget(c)}>Edit</Button>
                    <Button variant="ghost" size="sm" onClick={() => handleTest(c.name)}>Test</Button>
                    <Button variant="ghost" size="sm" className="text-destructive" onClick={() => setDeleteTarget(c.name)}>Delete</Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <CreateConnectionDialog open={showCreate} onClose={() => setShowCreate(false)} onCreated={refresh} onError={onError} />
      <EditConnectionDialog connection={editTarget} onClose={() => setEditTarget(null)} onSaved={refresh} onError={onError} />
      <ConfirmDeleteConnectionDialog name={deleteTarget} onClose={() => setDeleteTarget(null)} onDeleted={refresh} onError={onError} />
    </>
  );
}

function CreateConnectionDialog({ open, onClose, onCreated, onError }: {
  open: boolean; onClose: () => void; onCreated: () => void; onError: (msg: string) => void;
}) {
  const [name, setName] = useState("");
  const [connType, setConnType] = useState("mssql");
  const [host, setHost] = useState("");
  const [port, setPort] = useState("");
  const [database, setDatabase] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [sslmode, setSslmode] = useState("");
  const [encrypt, setEncrypt] = useState(false);
  const [trustCert, setTrustCert] = useState(true);
  const [isDefault, setIsDefault] = useState(false);
  const [region, setRegion] = useState("us-east-1");
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<TestConnectionResult | null>(null);
  const [testing, setTesting] = useState(false);

  useEffect(() => {
    if (open) {
      setName(""); setHost(""); setPort(""); setDatabase("");
      setUsername(""); setPassword(""); setSslmode(""); setEncrypt(false);
      setTrustCert(true); setIsDefault(false); setTestResult(null);
      setConnType("mssql"); setRegion("us-east-1");
    }
  }, [open]);

  const defaultPort = connType === "minio" ? "9000" : connType === "postgres" ? "5432" : connType === "clickhouse" ? "8123" : "1433";

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const data: Record<string, unknown> = {
        type: connType, host, database, username, password,
        port: parseInt(port || defaultPort),
      };
      if (connType === "postgres" && sslmode) data.sslmode = sslmode;
      if (connType === "mssql") {
        data.options_json = JSON.stringify({ encrypt, trustServerCertificate: trustCert });
      }
      if (connType === "minio") {
        data.options_json = JSON.stringify({ region, path_style: true });
        data.database = "";
      }
      const result = await testConnectionInline(data as Parameters<typeof testConnectionInline>[0]);
      setTestResult(result);
    } catch (e) { setTestResult({ success: false, message: e instanceof Error ? e.message : String(e) }); }
    finally { setTesting(false); }
  };

  const submit = async () => {
    if (!name.trim() || !host.trim()) return;
    setSaving(true);
    try {
      const data: CreateConnectionData = {
        name: name.trim(), type: connType, host: host.trim(),
        port: parseInt(port || defaultPort),
        database: connType === "minio" ? "" : (database.trim() || (connType === "postgres" ? "postgres" : connType === "clickhouse" ? "default" : "master")),
        username: username.trim(), password,
        is_default: isDefault,
      };
      if (connType === "postgres" && sslmode) data.sslmode = sslmode;
      if (connType === "mssql") {
        data.options_json = JSON.stringify({ encrypt, trustServerCertificate: trustCert });
      }
      if (connType === "minio") {
        data.options_json = JSON.stringify({ region, path_style: true });
      }
      await createConnection(data);
      onClose();
      onCreated();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-lg">
        <DialogHeader><DialogTitle>Add Connection</DialogTitle></DialogHeader>
        <div className="space-y-4 py-2 max-h-[60vh] overflow-y-auto">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>Name</Label>
              <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="production" />
            </div>
            <div className="space-y-2">
              <Label>Type</Label>
              <Select value={connType} onValueChange={(v) => { setConnType(v); setPort(""); }}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="mssql">MSSQL</SelectItem>
                  <SelectItem value="postgres">Postgres</SelectItem>
                  <SelectItem value="clickhouse">ClickHouse</SelectItem>
                  <SelectItem value="minio">MinIO / S3</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          <div className="grid grid-cols-3 gap-4">
            <div className="col-span-2 space-y-2">
              <Label>{connType === "minio" ? "Endpoint" : "Host"}</Label>
              <Input value={host} onChange={(e) => setHost(e.target.value)} placeholder={connType === "minio" ? "http://minio.example.com" : "db.example.com"} />
            </div>
            <div className="space-y-2">
              <Label>Port</Label>
              <Input type="number" value={port} onChange={(e) => setPort(e.target.value)} placeholder={defaultPort} />
            </div>
          </div>
          {connType !== "minio" && (
            <div className="space-y-2">
              <Label>Database</Label>
              <Input value={database} onChange={(e) => setDatabase(e.target.value)} placeholder={connType === "postgres" ? "postgres" : connType === "clickhouse" ? "default" : "master"} />
            </div>
          )}
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>{connType === "minio" ? "Access Key" : "Username"}</Label>
              <Input value={username} onChange={(e) => setUsername(e.target.value)} placeholder={connType === "minio" ? "minioadmin" : connType === "postgres" ? "postgres" : connType === "clickhouse" ? "default" : "sa"} />
            </div>
            <div className="space-y-2">
              <Label>{connType === "minio" ? "Secret Key" : "Password"}</Label>
              <Input type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
            </div>
          </div>
          {connType === "minio" && (
            <div className="space-y-2">
              <Label>Region</Label>
              <Input value={region} onChange={(e) => setRegion(e.target.value)} placeholder="us-east-1" />
            </div>
          )}
          {connType === "postgres" && (
            <div className="space-y-2">
              <Label>SSL Mode</Label>
              <Select value={sslmode || "none"} onValueChange={(v) => setSslmode(v === "none" ? "" : v)}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">Default (prefer)</SelectItem>
                  <SelectItem value="disable">Disable — No TLS</SelectItem>
                  <SelectItem value="prefer">Prefer — TLS, certificate not verified</SelectItem>
                  <SelectItem value="require">Require — TLS, valid certificate required</SelectItem>
                  <SelectItem value="verify-ca">Verify CA — TLS, certificate chain verified</SelectItem>
                  <SelectItem value="verify-full">Verify Full — TLS, certificate and hostname verified</SelectItem>
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">This app defaults to &quot;prefer&quot;: it uses TLS encryption but does not verify the server certificate. Use &quot;require&quot; or higher for production.</p>
            </div>
          )}
          {connType === "mssql" && (
            <div className="flex items-center gap-6">
              <div className="flex items-center gap-2">
                <Switch checked={encrypt} onCheckedChange={setEncrypt} id="create-encrypt" />
                <Label htmlFor="create-encrypt">Encrypt</Label>
              </div>
              <div className="flex items-center gap-2">
                <Switch checked={trustCert} onCheckedChange={setTrustCert} id="create-trust" />
                <Label htmlFor="create-trust">Trust Server Certificate</Label>
              </div>
            </div>
          )}
          <div className="flex items-center gap-2">
            <Switch checked={isDefault} onCheckedChange={setIsDefault} id="create-default" />
            <Label htmlFor="create-default">Set as default connection</Label>
          </div>
          {testResult && (
            <div className={`px-3 py-2 rounded-md text-sm ${testResult.success ? "bg-green-500/20 text-green-400" : "bg-destructive/20 text-destructive"}`}>
              {testResult.success ? "Connection successful" : testResult.message}
            </div>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={handleTest} disabled={testing || !host.trim()}>
            {testing ? "Testing..." : "Test Connection"}
          </Button>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={saving || !name.trim() || !host.trim()}>
            {saving ? "Creating..." : "Create"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function EditConnectionDialog({ connection, onClose, onSaved, onError }: {
  connection: AdminConnectionInfo | null; onClose: () => void; onSaved: () => void; onError: (msg: string) => void;
}) {
  const [connType, setConnType] = useState("mssql");
  const [host, setHost] = useState("");
  const [port, setPort] = useState("");
  const [database, setDatabase] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [sslmode, setSslmode] = useState("");
  const [encrypt, setEncrypt] = useState(false);
  const [trustCert, setTrustCert] = useState(true);
  const [region, setRegion] = useState("us-east-1");
  const [isDefault, setIsDefault] = useState(false);
  const [isEnabled, setIsEnabled] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<TestConnectionResult | null>(null);
  const [testing, setTesting] = useState(false);

  useEffect(() => {
    if (connection) {
      setConnType(connection.type);
      setHost(connection.host);
      setPort(String(connection.port));
      setDatabase(connection.database);
      setUsername("");
      setPassword("");
      setSslmode("");
      setEncrypt(false);
      setTrustCert(true);
      setRegion("us-east-1");
      setIsDefault(connection.is_default);
      setIsEnabled(connection.is_enabled);
      setTestResult(null);
    }
  }, [connection]);

  const handleTest = async () => {
    if (!connection) return;
    setTesting(true);
    setTestResult(null);
    try {
      const result = await testConnection(connection.name);
      setTestResult(result);
    } catch (e) { setTestResult({ success: false, message: e instanceof Error ? e.message : String(e) }); }
    finally { setTesting(false); }
  };

  const submit = async () => {
    if (!connection) return;
    setSaving(true);
    try {
      const data: Record<string, unknown> = {
        type: connType,
        host: host.trim(),
        port: parseInt(port),
        database: database.trim(),
        is_default: isDefault,
        is_enabled: isEnabled,
      };
      if (username.trim()) data.username = username.trim();
      if (password) data.password = password;
      if (connType === "postgres" && sslmode) data.sslmode = sslmode;
      if (connType === "mssql") {
        data.options_json = JSON.stringify({ encrypt, trustServerCertificate: trustCert });
      }
      if (connType === "minio") {
        data.options_json = JSON.stringify({ region, path_style: true });
        data.database = "";
      }
      await updateConnection(connection.name, data);
      onClose();
      onSaved();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  return (
    <Dialog open={!!connection} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-lg">
        <DialogHeader><DialogTitle>Edit Connection: {connection?.name}</DialogTitle></DialogHeader>
        <div className="space-y-4 py-2 max-h-[60vh] overflow-y-auto">
          <div className="grid grid-cols-3 gap-4">
            <div className="col-span-2 space-y-2">
              <Label>{connType === "minio" ? "Endpoint" : "Host"}</Label>
              <Input value={host} onChange={(e) => setHost(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label>Port</Label>
              <Input type="number" value={port} onChange={(e) => setPort(e.target.value)} />
            </div>
          </div>
          {connType !== "minio" && (
            <div className="space-y-2">
              <Label>Database</Label>
              <Input value={database} onChange={(e) => setDatabase(e.target.value)} />
            </div>
          )}
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>{connType === "minio" ? "Access Key" : "Username"}</Label>
              <Input value={username} onChange={(e) => setUsername(e.target.value)} placeholder="(unchanged)" />
            </div>
            <div className="space-y-2">
              <Label>{connType === "minio" ? "Secret Key" : "Password"}</Label>
              <Input type="password" value={password} onChange={(e) => setPassword(e.target.value)} placeholder="(unchanged)" />
            </div>
          </div>
          {connType === "minio" && (
            <div className="space-y-2">
              <Label>Region</Label>
              <Input value={region} onChange={(e) => setRegion(e.target.value)} placeholder="us-east-1" />
            </div>
          )}
          {connType === "postgres" && (
            <div className="space-y-2">
              <Label>SSL Mode</Label>
              <Select value={sslmode || "none"} onValueChange={(v) => setSslmode(v === "none" ? "" : v)}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">Default (prefer)</SelectItem>
                  <SelectItem value="disable">Disable — No TLS</SelectItem>
                  <SelectItem value="prefer">Prefer — TLS, certificate not verified</SelectItem>
                  <SelectItem value="require">Require — TLS, valid certificate required</SelectItem>
                  <SelectItem value="verify-ca">Verify CA — TLS, certificate chain verified</SelectItem>
                  <SelectItem value="verify-full">Verify Full — TLS, certificate and hostname verified</SelectItem>
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">This app defaults to &quot;prefer&quot;: it uses TLS encryption but does not verify the server certificate. Use &quot;require&quot; or higher for production.</p>
            </div>
          )}
          {connType === "mssql" && (
            <div className="flex items-center gap-6">
              <div className="flex items-center gap-2">
                <Switch checked={encrypt} onCheckedChange={setEncrypt} id="edit-encrypt" />
                <Label htmlFor="edit-encrypt">Encrypt</Label>
              </div>
              <div className="flex items-center gap-2">
                <Switch checked={trustCert} onCheckedChange={setTrustCert} id="edit-trust" />
                <Label htmlFor="edit-trust">Trust Server Certificate</Label>
              </div>
            </div>
          )}
          <div className="flex items-center gap-6">
            <div className="flex items-center gap-2">
              <Switch checked={isDefault} onCheckedChange={setIsDefault} id="edit-default" />
              <Label htmlFor="edit-default">Default</Label>
            </div>
            <div className="flex items-center gap-2">
              <Switch checked={isEnabled} onCheckedChange={setIsEnabled} id="edit-enabled" />
              <Label htmlFor="edit-enabled">Enabled</Label>
            </div>
          </div>
          {testResult && (
            <div className={`px-3 py-2 rounded-md text-sm ${testResult.success ? "bg-green-500/20 text-green-400" : "bg-destructive/20 text-destructive"}`}>
              {testResult.success ? "Connection is healthy" : testResult.message}
            </div>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={handleTest} disabled={testing}>
            {testing ? "Testing..." : "Test Connection"}
          </Button>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={saving}>
            {saving ? "Saving..." : "Save"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ConfirmDeleteConnectionDialog({ name, onClose, onDeleted, onError }: {
  name: string | null; onClose: () => void; onDeleted: () => void; onError: (msg: string) => void;
}) {
  const [deleting, setDeleting] = useState(false);

  const confirm = async () => {
    if (!name) return;
    setDeleting(true);
    try {
      await deleteConnection(name);
      onClose();
      onDeleted();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setDeleting(false); }
  };

  return (
    <Dialog open={!!name} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete Connection</DialogTitle>
          <DialogDescription>
            This will permanently delete the connection <span className="font-mono">"{name}"</span> and drop its connection pool.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button variant="destructive" onClick={confirm} disabled={deleting}>
            {deleting ? "Deleting..." : "Delete"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// ============================================================================
// Users Tab
// ============================================================================

function UsersTab({ onError }: { onError: (msg: string) => void }) {
  const [users, setUsers] = useState<UserInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [editUser, setEditUser] = useState<UserInfo | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const [passwordTarget, setPasswordTarget] = useState<string | null>(null);
  const [unavailable, setUnavailable] = useState(false);

  const refresh = useCallback(async () => {
    try {
      setUsers(await listUsers());
      setUnavailable(false);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes("not enabled")) setUnavailable(true);
      else onError(msg);
    } finally {
      setLoading(false);
    }
  }, [onError]);

  useEffect(() => { refresh(); }, [refresh]);

  if (loading) return <p className="text-muted-foreground py-4">Loading users...</p>;
  if (unavailable) return (
    <Card><CardContent className="py-8 text-center text-muted-foreground">
      Access control is not enabled on this server.
    </CardContent></Card>
  );

  return (
    <>
      <div className="flex justify-end mb-3">
        <Button size="sm" onClick={() => setShowCreate(true)}>Create User</Button>
      </div>
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Email</TableHead>
                <TableHead>Name</TableHead>
                <TableHead>Role</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="w-[60px]">MCP</TableHead>
                <TableHead className="w-[90px]">SQL Mode</TableHead>
                <TableHead>PII Mode</TableHead>
                <TableHead className="w-[200px]">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {users.length === 0 ? (
                <TableRow><TableCell colSpan={8} className="text-center text-muted-foreground py-8">No users</TableCell></TableRow>
              ) : users.map((u) => (
                <TableRow key={u.email}>
                  <TableCell className="font-mono text-xs">{u.email}</TableCell>
                  <TableCell className="text-sm">{u.display_name ?? <span className="text-muted-foreground">-</span>}</TableCell>
                  <TableCell>{u.is_admin ? <Badge>Admin</Badge> : <Badge variant="outline">User</Badge>}</TableCell>
                  <TableCell>{u.is_enabled ? <Badge variant="outline" className="text-green-400 border-green-400/50">Enabled</Badge> : <Badge variant="destructive">Disabled</Badge>}</TableCell>
                  <TableCell>
                    <Switch
                      checked={u.mcp_enabled}
                      onCheckedChange={async (v) => {
                        try { await updateUser(u.email, { mcp_enabled: v }); refresh(); }
                        catch (e) { onError(e instanceof Error ? e.message : String(e)); }
                      }}
                    />
                  </TableCell>
                  <TableCell>
                    <Badge variant={u.sql_mode === "full" ? "default" : u.sql_mode === "none" ? "outline" : "secondary"} className="text-xs">
                      {u.sql_mode === "read_only" ? "Read Only" : u.sql_mode === "supervised" ? "Supervised" : u.sql_mode === "confirmed" ? "Confirmed" : u.sql_mode === "full" ? "Full" : "None"}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-xs">{u.pii_mode ?? <span className="text-muted-foreground">inherit</span>}</TableCell>
                  <TableCell className="space-x-1">
                    <Button variant="ghost" size="sm" onClick={() => setEditUser(u)}>Edit</Button>
                    <Button variant="ghost" size="sm" onClick={() => setPasswordTarget(u.email)}>Password</Button>
                    <Button variant="ghost" size="sm" onClick={async () => {
                      if (!window.confirm(`Purge all sessions for ${u.email}? They will be signed out immediately.`)) return;
                      try {
                        await purgeUserSessions(u.email);
                      } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
                    }}>Purge Sessions</Button>
                    <Button variant="ghost" size="sm" className="text-destructive" onClick={() => setDeleteTarget(u.email)}>Delete</Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <CreateUserDialog open={showCreate} onClose={() => setShowCreate(false)} onCreated={refresh} onError={onError} />
      <EditUserDialog user={editUser} onClose={() => setEditUser(null)} onSaved={refresh} onError={onError} />
      <ConfirmDeleteDialog
        email={deleteTarget}
        onClose={() => setDeleteTarget(null)}
        onDeleted={refresh}
        onError={onError}
      />
      <SetPasswordDialog
        email={passwordTarget}
        onClose={() => setPasswordTarget(null)}
        onError={onError}
      />
    </>
  );
}

function SetPasswordDialog({ email, onClose, onError }: {
  email: string | null; onClose: () => void; onError: (msg: string) => void;
}) {
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [saving, setSaving] = useState(false);
  const [localError, setLocalError] = useState("");

  useEffect(() => {
    if (email) { setPassword(""); setConfirmPassword(""); setLocalError(""); }
  }, [email]);

  const submit = async () => {
    if (!email) return;
    if (password.length < 8) { setLocalError("Password must be at least 8 characters"); return; }
    if (password !== confirmPassword) { setLocalError("Passwords do not match"); return; }
    setSaving(true);
    setLocalError("");
    try {
      await adminSetPassword(email, password);
      onClose();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  return (
    <Dialog open={!!email} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Set Password</DialogTitle>
          <DialogDescription>
            Set a new password for <span className="font-mono">{email}</span>.
          </DialogDescription>
        </DialogHeader>
        {localError && (
          <div className="bg-destructive/20 border border-destructive text-destructive px-4 py-2 rounded-md text-sm">
            {localError}
          </div>
        )}
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label>New Password</Label>
            <Input type="password" value={password} onChange={(e) => setPassword(e.target.value)} placeholder="Min. 8 characters" />
          </div>
          <div className="space-y-2">
            <Label>Confirm Password</Label>
            <Input type="password" value={confirmPassword} onChange={(e) => setConfirmPassword(e.target.value)} placeholder="Confirm"
              onKeyDown={(e) => e.key === "Enter" && submit()} />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={saving}>
            {saving ? "Setting..." : "Set Password"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function CreateUserDialog({ open, onClose, onCreated, onError }: {
  open: boolean; onClose: () => void; onCreated: () => void; onError: (msg: string) => void;
}) {
  const [email, setEmail] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [isAdmin, setIsAdmin] = useState(false);
  const [saving, setSaving] = useState(false);

  const submit = async () => {
    if (!email.trim()) return;
    setSaving(true);
    try {
      await createUser(email.trim(), displayName.trim() || undefined, isAdmin);
      setEmail(""); setDisplayName(""); setIsAdmin(false);
      onClose();
      onCreated();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader><DialogTitle>Create User</DialogTitle></DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label htmlFor="create-email">Email</Label>
            <Input id="create-email" value={email} onChange={(e) => setEmail(e.target.value)} placeholder="user@example.com" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="create-display-name">Display Name</Label>
            <Input id="create-display-name" value={displayName} onChange={(e) => setDisplayName(e.target.value)} placeholder="Optional" />
          </div>
          <div className="flex items-center gap-2">
            <Switch checked={isAdmin} onCheckedChange={setIsAdmin} id="admin-switch" />
            <Label htmlFor="admin-switch">Admin</Label>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={saving || !email.trim()}>
            {saving ? "Creating..." : "Create"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function EditUserDialog({ user, onClose, onSaved, onError }: {
  user: UserInfo | null; onClose: () => void; onSaved: () => void; onError: (msg: string) => void;
}) {
  const [displayName, setDisplayName] = useState("");
  const [isAdmin, setIsAdmin] = useState(false);
  const [isEnabled, setIsEnabled] = useState(true);
  const [piiMode, setPiiMode] = useState("inherit");
  const [sqlMode, setSqlMode] = useState("none");
  const [maxPending, setMaxPending] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (user) {
      setDisplayName(user.display_name ?? "");
      setIsAdmin(user.is_admin);
      setIsEnabled(user.is_enabled);
      setPiiMode(user.pii_mode ?? "inherit");
      setSqlMode(user.sql_mode ?? "none");
      setMaxPending(user.max_pending_approvals != null ? String(user.max_pending_approvals) : "");
    }
  }, [user]);

  const submit = async () => {
    if (!user) return;
    setSaving(true);
    try {
      const parsedMaxPending = maxPending.trim() === "" ? 0 : parseInt(maxPending, 10);
      await updateUser(user.email, {
        display_name: displayName.trim() || undefined,
        is_admin: isAdmin,
        is_enabled: isEnabled,
        pii_mode: piiMode,
        sql_mode: sqlMode,
        max_pending_approvals: parsedMaxPending,
      });
      onClose();
      onSaved();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  return (
    <Dialog open={!!user} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader><DialogTitle>Edit User: {user?.email}</DialogTitle></DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label htmlFor="edit-display-name">Display Name</Label>
            <Input id="edit-display-name" value={displayName} onChange={(e) => setDisplayName(e.target.value)} />
          </div>
          <div className="flex items-center gap-2">
            <Switch checked={isAdmin} onCheckedChange={setIsAdmin} id="edit-admin" />
            <Label htmlFor="edit-admin">Admin</Label>
          </div>
          <div className="flex items-center gap-2">
            <Switch checked={isEnabled} onCheckedChange={setIsEnabled} id="edit-enabled" />
            <Label htmlFor="edit-enabled">Enabled</Label>
          </div>
          <div className="space-y-2">
            <Label>SQL Mode</Label>
            <Select value={sqlMode} onValueChange={setSqlMode}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="none">None</SelectItem>
                <SelectItem value="read_only">Read Only</SelectItem>
                <SelectItem value="supervised">Supervised</SelectItem>
                <SelectItem value="confirmed">Confirmed</SelectItem>
                <SelectItem value="full">Full</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {(sqlMode === "supervised" || sqlMode === "confirmed") && (
            <div className="space-y-2">
              <Label htmlFor="edit-max-pending">Max Pending Approvals</Label>
              <Input
                id="edit-max-pending"
                type="number"
                min={1}
                max={50}
                placeholder="6 (default)"
                value={maxPending}
                onChange={(e) => setMaxPending(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">Maximum concurrent pending approvals for this user. Leave blank for default (6).</p>
            </div>
          )}
          <div className="space-y-2">
            <Label>PII Mode</Label>
            <Select value={piiMode} onValueChange={setPiiMode}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="inherit">Inherit</SelectItem>
                <SelectItem value="scrub">Scrub</SelectItem>

                <SelectItem value="none">None</SelectItem>
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">Default PII mode for all this user's tokens (unless overridden per-token).</p>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={saving}>
            {saving ? "Saving..." : "Save"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ConfirmDeleteDialog({ email, onClose, onDeleted, onError }: {
  email: string | null; onClose: () => void; onDeleted: () => void; onError: (msg: string) => void;
}) {
  const [deleting, setDeleting] = useState(false);

  const confirm = async () => {
    if (!email) return;
    setDeleting(true);
    try {
      await deleteUser(email);
      onClose();
      onDeleted();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setDeleting(false); }
  };

  return (
    <Dialog open={!!email} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete User</DialogTitle>
          <DialogDescription>
            This will permanently delete <span className="font-mono">{email}</span> and all their tokens and permissions.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button variant="destructive" onClick={confirm} disabled={deleting}>
            {deleting ? "Deleting..." : "Delete"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// ============================================================================
// Tokens Tab
// ============================================================================

function TokensTab({ onError }: { onError: (msg: string) => void }) {
  const [tokens, setTokens] = useState<TokenRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [showGenerate, setShowGenerate] = useState(false);
  const [generatedToken, setGeneratedToken] = useState<string | null>(null);
  const [unavailable, setUnavailable] = useState(false);
  const [policy, setPolicy] = useState<TokenPolicy>({ max_lifespan_hours: 0, default_lifespan_hours: 0 });
  const [policySaving, setPolicySaving] = useState(false);
  const [policyDirty, setPolicyDirty] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const [t, p] = await Promise.all([
        listTokens(),
        getTokenPolicy().catch(() => ({ max_lifespan_hours: 0, default_lifespan_hours: 0 })),
      ]);
      setTokens(t);
      setPolicy(p);
      setPolicyDirty(false);
      setUnavailable(false);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes("not enabled")) setUnavailable(true);
      else onError(msg);
    } finally {
      setLoading(false);
    }
  }, [onError]);

  useEffect(() => { refresh(); }, [refresh]);

  const handleRevoke = async (prefix: string) => {
    try {
      await revokeToken(prefix);
      refresh();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
  };

  const savePolicy = async () => {
    setPolicySaving(true);
    try {
      await setTokenPolicy(policy);
      setPolicyDirty(false);
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setPolicySaving(false); }
  };

  if (loading) return <p className="text-muted-foreground py-4">Loading tokens...</p>;
  if (unavailable) return (
    <Card><CardContent className="py-8 text-center text-muted-foreground">
      Access control is not enabled on this server.
    </CardContent></Card>
  );

  return (
    <>
      {/* Token Policy */}
      <Card className="mb-4">
        <CardHeader className="pb-2">
          <h3 className="text-sm font-semibold">Token Policy</h3>
        </CardHeader>
        <CardContent>
          <div className="flex items-end gap-4">
            <div className="space-y-1">
              <Label htmlFor="policy-max" className="text-xs">Max Lifespan (hours)</Label>
              <Input
                id="policy-max"
                type="number"
                className="w-32"
                value={policy.max_lifespan_hours || ""}
                placeholder="0 = unlimited"
                onChange={(e) => {
                  setPolicy({ ...policy, max_lifespan_hours: parseInt(e.target.value) || 0 });
                  setPolicyDirty(true);
                }}
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="policy-default" className="text-xs">Default Lifespan (hours)</Label>
              <Input
                id="policy-default"
                type="number"
                className="w-32"
                value={policy.default_lifespan_hours || ""}
                placeholder="0 = never"
                onChange={(e) => {
                  setPolicy({ ...policy, default_lifespan_hours: parseInt(e.target.value) || 0 });
                  setPolicyDirty(true);
                }}
              />
            </div>
            <Button size="sm" onClick={savePolicy} disabled={policySaving || !policyDirty}>
              {policySaving ? "Saving..." : "Save"}
            </Button>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Set to 0 for unlimited/no default. Max lifespan caps all token generation (self-service and admin).
          </p>
        </CardContent>
      </Card>

      <div className="flex justify-end mb-3">
        <Button size="sm" onClick={() => setShowGenerate(true)}>Generate Token</Button>
      </div>
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Prefix</TableHead>
                <TableHead>Email</TableHead>
                <TableHead>Label</TableHead>
                <TableHead>PII Mode</TableHead>
                <TableHead>Expires</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="w-[80px]">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {tokens.length === 0 ? (
                <TableRow><TableCell colSpan={7} className="text-center text-muted-foreground py-8">No tokens</TableCell></TableRow>
              ) : tokens.map((t) => {
                const exp = formatExpiry(t.expires_at);
                return (
                  <TableRow key={t.token_prefix + t.email}>
                    <TableCell className="font-mono text-xs">{t.token_prefix}...</TableCell>
                    <TableCell className="text-sm">{t.email}</TableCell>
                    <TableCell className="text-sm">{t.label ?? <span className="text-muted-foreground">-</span>}</TableCell>
                    <TableCell className="text-xs">{t.pii_mode ?? <span className="text-muted-foreground">inherit</span>}</TableCell>
                    <TableCell className={`text-xs ${exp.color}`}>
                      {exp.text}
                    </TableCell>
                    <TableCell>
                      {t.is_active
                        ? <Badge variant="outline" className="text-green-400 border-green-400/50">Active</Badge>
                        : <Badge variant="destructive">Revoked</Badge>}
                    </TableCell>
                    <TableCell>
                      {t.is_active && (
                        <Button variant="ghost" size="sm" className="text-destructive" onClick={() => handleRevoke(t.token_prefix)}>
                          Revoke
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <GenerateTokenDialog
        open={showGenerate}
        onClose={() => setShowGenerate(false)}
        onGenerated={(token) => { setGeneratedToken(token); refresh(); }}
        onError={onError}
      />
      <TokenRevealDialog token={generatedToken} onClose={() => setGeneratedToken(null)} />
    </>
  );
}

function GenerateTokenDialog({ open, onClose, onGenerated, onError }: {
  open: boolean; onClose: () => void; onGenerated: (token: string) => void; onError: (msg: string) => void;
}) {
  const [email, setEmail] = useState("");
  const [label, setLabel] = useState("");
  const [expiresHours, setExpiresHours] = useState("");
  const [piiMode, setPiiMode] = useState("inherit");
  const [saving, setSaving] = useState(false);

  const submit = async () => {
    if (!email.trim()) return;
    setSaving(true);
    try {
      const result = await generateToken(
        email.trim(),
        label.trim() || undefined,
        expiresHours ? parseInt(expiresHours) : undefined,
        piiMode === "inherit" ? undefined : piiMode,
      );
      setEmail(""); setLabel(""); setExpiresHours(""); setPiiMode("inherit");
      onClose();
      onGenerated(result.token);
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader><DialogTitle>Generate Token</DialogTitle></DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label htmlFor="token-email">User Email</Label>
            <Input id="token-email" value={email} onChange={(e) => setEmail(e.target.value)} placeholder="user@example.com" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="token-label">Label (optional)</Label>
            <Input id="token-label" value={label} onChange={(e) => setLabel(e.target.value)} placeholder="e.g. CI Pipeline" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="token-expires">Expires in (hours, blank = never)</Label>
            <Input id="token-expires" type="number" value={expiresHours} onChange={(e) => setExpiresHours(e.target.value)} placeholder="e.g. 24" />
          </div>
          <div className="space-y-2">
            <Label>PII Mode</Label>
            <Select value={piiMode} onValueChange={setPiiMode}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="inherit">Inherit</SelectItem>
                <SelectItem value="scrub">Scrub</SelectItem>

                <SelectItem value="none">None</SelectItem>
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">Override PII mode for this token. Inherit = use user/connection default.</p>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={saving || !email.trim()}>
            {saving ? "Generating..." : "Generate"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function TokenRevealDialog({ token, onClose }: { token: string | null; onClose: () => void }) {
  const [copied, setCopied] = useState(false);

  const copy = () => {
    if (token) {
      navigator.clipboard.writeText(token);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  return (
    <Dialog open={!!token} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Token Generated</DialogTitle>
          <DialogDescription>Copy this token now. It cannot be retrieved again.</DialogDescription>
        </DialogHeader>
        <div className="bg-muted p-3 rounded-md font-mono text-xs break-all select-all">{token}</div>
        <DialogFooter>
          <Button variant="outline" onClick={copy}>{copied ? "Copied!" : "Copy"}</Button>
          <Button onClick={onClose}>Done</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// ============================================================================
// Permissions Tab — Tree-based permission builder
// ============================================================================

// Internal state for the tree: tracks which databases/tables have read/write/update/delete enabled
interface PermAccess { read: boolean; write: boolean; update: boolean; delete: boolean }
const emptyAccess: PermAccess = { read: false, write: false, update: false, delete: false };

interface PermState {
  databases: Record<string, PermAccess>;
  tables: Record<string, PermAccess>;
}

function buildPermStateFromPermissions(perms: Permission[]): PermState {
  const state: PermState = { databases: {}, tables: {} };
  for (const p of perms) {
    const access: PermAccess = { read: p.can_read, write: p.can_write, update: p.can_update, delete: p.can_delete };
    if (p.table_pattern === "*") {
      state.databases[p.database_name] = access;
    } else {
      const key = `${p.database_name}::${p.table_pattern}`;
      state.tables[key] = access;
    }
  }
  return state;
}

function permStateToPermissions(state: PermState): { database_name: string; table_pattern: string; can_read: boolean; can_write: boolean; can_update: boolean; can_delete: boolean }[] {
  const result: { database_name: string; table_pattern: string; can_read: boolean; can_write: boolean; can_update: boolean; can_delete: boolean }[] = [];
  for (const [db, access] of Object.entries(state.databases)) {
    result.push({ database_name: db, table_pattern: "*", can_read: access.read, can_write: access.write, can_update: access.update, can_delete: access.delete });
  }
  for (const [key, access] of Object.entries(state.tables)) {
    const [db, table] = key.split("::");
    result.push({ database_name: db, table_pattern: table, can_read: access.read, can_write: access.write, can_update: access.update, can_delete: access.delete });
  }
  return result;
}

function PermissionsTab({ onError }: { onError: (msg: string) => void }) {
  const [users, setUsers] = useState<UserInfo[]>([]);
  const [selectedEmail, setSelectedEmail] = useState("");
  const [inventory, setInventory] = useState<InventoryConnection[]>([]);
  const [permState, setPermState] = useState<PermState>({ databases: {}, tables: {} });
  const [loading, setLoading] = useState(true);
  const [loadingInventory, setLoadingInventory] = useState(false);
  const [saving, setSaving] = useState(false);
  const [unavailable, setUnavailable] = useState(false);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  // Connection access: null = unrestricted, string[] = restricted to listed
  const [connAccess, setConnAccess] = useState<string[] | null>(null);
  // Storage permissions
  const [storagePerms, setStoragePerms] = useState<StoragePermission[]>([]);

  const refreshUsers = useCallback(async () => {
    try {
      const u = await listUsers();
      setUsers(u);
      setUnavailable(false);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes("not enabled")) setUnavailable(true);
      else onError(msg);
    } finally {
      setLoading(false);
    }
  }, [onError]);

  useEffect(() => { refreshUsers(); }, [refreshUsers]);

  // Load inventory on mount
  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoadingInventory(true);
      try {
        const inv = await getInventory();
        if (!cancelled) setInventory(inv);
      } catch (e) {
        if (!cancelled) onError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoadingInventory(false);
      }
    })();
    return () => { cancelled = true; };
  }, [onError]);

  // When user changes, load their permissions into state
  useEffect(() => {
    const user = users.find((u) => u.email === selectedEmail);
    if (user?.permissions) {
      setPermState(buildPermStateFromPermissions(user.permissions));
    } else {
      setPermState({ databases: {}, tables: {} });
    }
    setConnAccess(user?.connection_permissions ?? null);
    // Load storage permissions
    if (selectedEmail) {
      getStoragePermissions(selectedEmail).then(setStoragePerms).catch(() => setStoragePerms([]));
    } else {
      setStoragePerms([]);
    }
  }, [selectedEmail, users]);

  const toggleExpand = (key: string) => {
    setExpanded((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const setDbAccess = (dbName: string, field: keyof PermAccess, value: boolean) => {
    setPermState((prev) => {
      const next = { ...prev, databases: { ...prev.databases } };
      const existing = next.databases[dbName] ?? { ...emptyAccess };
      next.databases[dbName] = { ...existing, [field]: value };
      const updated = next.databases[dbName];
      if (!updated.read && !updated.write && !updated.update && !updated.delete) {
        delete next.databases[dbName];
      }
      return next;
    });
  };

  const setTableAccess = (dbName: string, tableName: string, field: keyof PermAccess, value: boolean) => {
    const key = `${dbName}::${tableName}`;
    setPermState((prev) => {
      const next = { ...prev, tables: { ...prev.tables } };
      const inherited = prev.databases[dbName] ?? prev.databases["*"] ?? { ...emptyAccess };
      const existing = next.tables[key] ?? { ...inherited };
      next.tables[key] = { ...existing, [field]: value };
      const updated = next.tables[key];
      if (updated.read === inherited.read && updated.write === inherited.write && updated.update === inherited.update && updated.delete === inherited.delete) {
        delete next.tables[key];
      }
      return next;
    });
  };

  const getDbAccess = (dbName: string): PermAccess => {
    return permState.databases[dbName] ?? permState.databases["*"] ?? { ...emptyAccess };
  };

  const getTableAccess = (dbName: string, tableName: string): PermAccess => {
    const tableKey = `${dbName}::${tableName}`;
    if (permState.tables[tableKey]) return permState.tables[tableKey];
    return getDbAccess(dbName);
  };

  const hasTableOverride = (dbName: string, tableName: string): boolean => {
    return `${dbName}::${tableName}` in permState.tables;
  };

  const save = async () => {
    if (!selectedEmail) return;
    setSaving(true);
    try {
      await setPermissions(selectedEmail, permStateToPermissions(permState));
      await setConnectionPermissions(selectedEmail, connAccess ?? []);
      await setStoragePermissions(selectedEmail, storagePerms);
      await refreshUsers();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  // Bulk action: enable read on all databases for a connection
  const enableReadAll = (connName: string) => {
    const conn = inventory.find((c) => c.name === connName);
    if (!conn) return;
    setPermState((prev) => {
      const next = { ...prev, databases: { ...prev.databases } };
      for (const db of conn.databases) {
        const existing = next.databases[db.name] ?? { ...emptyAccess };
        next.databases[db.name] = { ...existing, read: true };
      }
      return next;
    });
  };

  // Bulk action: revoke all for a connection
  const revokeAll = (connName: string) => {
    const conn = inventory.find((c) => c.name === connName);
    if (!conn) return;
    setPermState((prev) => {
      const next = { databases: { ...prev.databases }, tables: { ...prev.tables } };
      for (const db of conn.databases) {
        delete next.databases[db.name];
        // Also remove table-level overrides for these databases
        for (const key of Object.keys(next.tables)) {
          if (key.startsWith(`${db.name}::`)) {
            delete next.tables[key];
          }
        }
      }
      return next;
    });
  };

  if (loading) return <p className="text-muted-foreground py-4">Loading...</p>;
  if (unavailable) return (
    <Card><CardContent className="py-8 text-center text-muted-foreground">
      Access control is not enabled on this server.
    </CardContent></Card>
  );

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <Label>User:</Label>
        <Select value={selectedEmail} onValueChange={setSelectedEmail}>
          <SelectTrigger className="w-[280px]">
            <SelectValue placeholder="Select a user" />
          </SelectTrigger>
          <SelectContent>
            {users.map((u) => (
              <SelectItem key={u.email} value={u.email}>{u.email}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {selectedEmail && loadingInventory && (
        <p className="text-muted-foreground py-4">Loading server inventory...</p>
      )}

      {selectedEmail && !loadingInventory && (
        <>
          {/* Wildcard database toggle */}
          <Card>
            <CardContent className="py-3 px-4">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium">All Databases (*)</span>
                <div className="flex items-center gap-4">
                  <div className="flex items-center gap-1.5">
                    <Switch
                      checked={getDbAccess("*").read}
                      onCheckedChange={(v) => setDbAccess("*", "read", v)}
                    />
                    <span className="text-xs text-muted-foreground">Read</span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <Switch
                      checked={getDbAccess("*").write}
                      onCheckedChange={(v) => setDbAccess("*", "write", v)}
                    />
                    <span className="text-xs text-muted-foreground">Insert</span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <Switch
                      checked={getDbAccess("*").update}
                      onCheckedChange={(v) => setDbAccess("*", "update", v)}
                    />
                    <span className="text-xs text-muted-foreground">Update</span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <Switch
                      checked={getDbAccess("*").delete}
                      onCheckedChange={(v) => setDbAccess("*", "delete", v)}
                    />
                    <span className="text-xs text-muted-foreground">Delete</span>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Connection access */}
          <Card>
            <CardContent className="py-3 px-4">
              <div className="flex items-center justify-between">
                <div>
                  <span className="text-sm font-medium">Connection Access</span>
                  <p className="text-xs text-muted-foreground mt-0.5">
                    {connAccess === null
                      ? "Unrestricted — user can see all connections"
                      : `Restricted to ${connAccess.length} connection(s)`}
                  </p>
                </div>
                <div className="flex items-center gap-1.5">
                  <Switch
                    checked={connAccess === null}
                    onCheckedChange={(v) => {
                      if (v) {
                        setConnAccess(null);
                      } else {
                        // Restrict: default to all current connections
                        setConnAccess(inventory.map((c) => c.name));
                      }
                    }}
                  />
                  <span className="text-xs text-muted-foreground">Unrestricted</span>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Storage Permissions */}
          <Card>
            <CardContent className="py-3 px-4 space-y-3">
              <div className="flex items-center justify-between">
                <div>
                  <span className="text-sm font-medium">Storage Permissions</span>
                  <p className="text-xs text-muted-foreground mt-0.5">
                    {storagePerms.length === 0
                      ? "No rules — unrestricted storage access"
                      : `${storagePerms.length} rule(s) configured`}
                  </p>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  className="text-xs h-7"
                  onClick={() =>
                    setStoragePerms((prev) => [
                      ...prev,
                      { connection_name: "", bucket_pattern: "*", can_read: true, can_write: false, can_delete: false },
                    ])
                  }
                >
                  + Add Rule
                </Button>
              </div>
              {storagePerms.map((perm, idx) => (
                <div key={idx} className="flex items-center gap-2 text-xs">
                  <Input
                    className="h-7 text-xs w-32"
                    placeholder="connection"
                    value={perm.connection_name}
                    onChange={(e) => {
                      const next = [...storagePerms];
                      next[idx] = { ...next[idx], connection_name: e.target.value };
                      setStoragePerms(next);
                    }}
                  />
                  <Input
                    className="h-7 text-xs w-28"
                    placeholder="bucket pattern"
                    value={perm.bucket_pattern}
                    onChange={(e) => {
                      const next = [...storagePerms];
                      next[idx] = { ...next[idx], bucket_pattern: e.target.value };
                      setStoragePerms(next);
                    }}
                  />
                  <div className="flex items-center gap-1">
                    <Switch
                      checked={perm.can_read}
                      onCheckedChange={(v) => {
                        const next = [...storagePerms];
                        next[idx] = { ...next[idx], can_read: v };
                        setStoragePerms(next);
                      }}
                    />
                    <span className="text-muted-foreground">R</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <Switch
                      checked={perm.can_write}
                      onCheckedChange={(v) => {
                        const next = [...storagePerms];
                        next[idx] = { ...next[idx], can_write: v };
                        setStoragePerms(next);
                      }}
                    />
                    <span className="text-muted-foreground">W</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <Switch
                      checked={perm.can_delete}
                      onCheckedChange={(v) => {
                        const next = [...storagePerms];
                        next[idx] = { ...next[idx], can_delete: v };
                        setStoragePerms(next);
                      }}
                    />
                    <span className="text-muted-foreground">D</span>
                  </div>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-6 w-6 p-0 text-destructive"
                    onClick={() => setStoragePerms((prev) => prev.filter((_, i) => i !== idx))}
                  >
                    X
                  </Button>
                </div>
              ))}
            </CardContent>
          </Card>

          {/* Connection tree */}
          {inventory.map((conn) => {
            const connKey = `conn::${conn.name}`;
            const connExpanded = expanded[connKey] ?? false;
            const connVisible = connAccess === null || connAccess.includes(conn.name);
            return (
              <Card key={conn.name}>
                <CardHeader className="py-2 px-4">
                  <div className="flex items-center justify-between">
                    <button
                      className="flex items-center gap-2 text-sm font-medium hover:underline"
                      onClick={() => toggleExpand(connKey)}
                    >
                      <span className="text-muted-foreground">{connExpanded ? "\u25BC" : "\u25B6"}</span>
                      <Badge variant="outline" className="text-xs">{conn.type}</Badge>
                      {conn.name}
                    </button>
                    <div className="flex items-center gap-2">
                      {connAccess !== null && (
                        <div className="flex items-center gap-1.5 mr-2">
                          <Switch
                            checked={connVisible}
                            onCheckedChange={(v) => {
                              setConnAccess((prev) => {
                                if (prev === null) return prev;
                                if (v) return [...prev, conn.name];
                                return prev.filter((c) => c !== conn.name);
                              });
                            }}
                          />
                          <span className="text-xs text-muted-foreground">Access</span>
                        </div>
                      )}
                      <Button variant="ghost" size="sm" className="text-xs h-6" onClick={() => enableReadAll(conn.name)}>
                        Enable read all
                      </Button>
                      <Button variant="ghost" size="sm" className="text-xs h-6 text-destructive" onClick={() => revokeAll(conn.name)}>
                        Revoke all
                      </Button>
                    </div>
                  </div>
                </CardHeader>
                {connExpanded && (
                  <CardContent className="pt-0 pb-2 px-4">
                    <div className="space-y-1">
                      {conn.databases.map((db) => {
                        const dbKey = `db::${conn.name}::${db.name}`;
                        const dbExpanded = expanded[dbKey] ?? false;
                        const dbAccess = getDbAccess(db.name);
                        return (
                          <div key={db.name} className="border rounded-md">
                            <div className="flex items-center justify-between px-3 py-1.5">
                              <button
                                className="flex items-center gap-2 text-sm hover:underline"
                                onClick={() => toggleExpand(dbKey)}
                              >
                                <span className="text-muted-foreground text-xs">{dbExpanded ? "\u25BC" : "\u25B6"}</span>
                                <span className="font-mono text-xs">{db.name}</span>
                                <span className="text-xs text-muted-foreground">({db.tables.length} tables)</span>
                              </button>
                              <div className="flex items-center gap-4">
                                <div className="flex items-center gap-1.5">
                                  <Switch
                                    checked={dbAccess.read}
                                    onCheckedChange={(v) => setDbAccess(db.name, "read", v)}
                                  />
                                  <span className="text-xs text-muted-foreground">R</span>
                                </div>
                                <div className="flex items-center gap-1.5">
                                  <Switch
                                    checked={dbAccess.write}
                                    onCheckedChange={(v) => setDbAccess(db.name, "write", v)}
                                  />
                                  <span className="text-xs text-muted-foreground">I</span>
                                </div>
                                <div className="flex items-center gap-1.5">
                                  <Switch
                                    checked={dbAccess.update}
                                    onCheckedChange={(v) => setDbAccess(db.name, "update", v)}
                                  />
                                  <span className="text-xs text-muted-foreground">U</span>
                                </div>
                                <div className="flex items-center gap-1.5">
                                  <Switch
                                    checked={dbAccess.delete}
                                    onCheckedChange={(v) => setDbAccess(db.name, "delete", v)}
                                  />
                                  <span className="text-xs text-muted-foreground">D</span>
                                </div>
                              </div>
                            </div>
                            {dbExpanded && db.tables.length > 0 && (
                              <div className="border-t px-3 py-1">
                                {db.tables.map((tbl) => {
                                  const tblAccess = getTableAccess(db.name, tbl.name);
                                  const isOverride = hasTableOverride(db.name, tbl.name);
                                  return (
                                    <div key={tbl.name} className="flex items-center justify-between py-1 pl-5">
                                      <span className={`font-mono text-xs ${isOverride ? "text-foreground" : "text-muted-foreground"}`}>
                                        {tbl.schema}.{tbl.name}
                                        {isOverride && <span className="text-amber-400 ml-1" title="Table-level override">*</span>}
                                      </span>
                                      <div className="flex items-center gap-4">
                                        <div className="flex items-center gap-1.5">
                                          <Switch
                                            checked={tblAccess.read}
                                            onCheckedChange={(v) => setTableAccess(db.name, tbl.name, "read", v)}
                                          />
                                          <span className="text-xs text-muted-foreground">R</span>
                                        </div>
                                        <div className="flex items-center gap-1.5">
                                          <Switch
                                            checked={tblAccess.write}
                                            onCheckedChange={(v) => setTableAccess(db.name, tbl.name, "write", v)}
                                          />
                                          <span className="text-xs text-muted-foreground">I</span>
                                        </div>
                                        <div className="flex items-center gap-1.5">
                                          <Switch
                                            checked={tblAccess.update}
                                            onCheckedChange={(v) => setTableAccess(db.name, tbl.name, "update", v)}
                                          />
                                          <span className="text-xs text-muted-foreground">U</span>
                                        </div>
                                        <div className="flex items-center gap-1.5">
                                          <Switch
                                            checked={tblAccess.delete}
                                            onCheckedChange={(v) => setTableAccess(db.name, tbl.name, "delete", v)}
                                          />
                                          <span className="text-xs text-muted-foreground">D</span>
                                        </div>
                                        {isOverride && (
                                          <Button
                                            variant="ghost"
                                            size="sm"
                                            className="text-xs h-5 px-1 text-muted-foreground"
                                            onClick={() => {
                                              setPermState((prev) => {
                                                const next = { ...prev, tables: { ...prev.tables } };
                                                delete next.tables[`${db.name}::${tbl.name}`];
                                                return next;
                                              });
                                            }}
                                            title="Remove table-level override (inherit from database)"
                                          >
                                            Reset
                                          </Button>
                                        )}
                                      </div>
                                    </div>
                                  );
                                })}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </CardContent>
                )}
              </Card>
            );
          })}

          <div className="flex justify-end">
            <Button onClick={save} disabled={saving}>{saving ? "Saving..." : "Save Permissions"}</Button>
          </div>
        </>
      )}
    </div>
  );
}

// ============================================================================
// Audit Tab
// ============================================================================

function AuditTab({ onError }: { onError: (msg: string) => void }) {
  const [entries, setEntries] = useState<AuditEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [emailFilter, setEmailFilter] = useState("");
  const [actionFilter, setActionFilter] = useState("");
  const [unavailable, setUnavailable] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setEntries(await getAuditLog({
        email: emailFilter || undefined,
        action: actionFilter || undefined,
        limit: 200,
      }));
      setUnavailable(false);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes("not enabled")) setUnavailable(true);
      else onError(msg);
    } finally {
      setLoading(false);
    }
  }, [emailFilter, actionFilter, onError]);

  useEffect(() => { refresh(); }, [refresh]);

  if (unavailable) return (
    <Card><CardContent className="py-8 text-center text-muted-foreground">
      Access control is not enabled on this server.
    </CardContent></Card>
  );

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-3">
        <Input
          className="w-[200px]"
          placeholder="Filter by email"
          value={emailFilter}
          onChange={(e) => setEmailFilter(e.target.value)}
        />
        <Select value={actionFilter || "all"} onValueChange={(v) => setActionFilter(v === "all" ? "" : v)}>
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="All actions" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All actions</SelectItem>
            <SelectItem value="allowed">Allowed</SelectItem>
            <SelectItem value="denied">Denied</SelectItem>
          </SelectContent>
        </Select>
        <Button variant="outline" size="sm" onClick={refresh}>Refresh</Button>
      </div>

      <Card>
        <CardContent className="p-0">
          {loading ? (
            <p className="text-muted-foreground py-8 text-center">Loading audit log...</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Time</TableHead>
                  <TableHead>Email</TableHead>
                  <TableHead>Action</TableHead>
                  <TableHead>Database</TableHead>
                  <TableHead>Details</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {entries.length === 0 ? (
                  <TableRow><TableCell colSpan={5} className="text-center text-muted-foreground py-8">No audit entries</TableCell></TableRow>
                ) : entries.map((e) => (
                  <TableRow key={e.id}>
                    <TableCell className="text-xs text-muted-foreground whitespace-nowrap">{new Date(e.created_at).toLocaleString()}</TableCell>
                    <TableCell className="text-xs">{e.email ?? "-"}</TableCell>
                    <TableCell>
                      {e.action === "allowed" ? (
                        <Badge variant="outline" className="text-green-400 border-green-400/50">{e.action}</Badge>
                      ) : e.action === "denied" ? (
                        <Badge variant="destructive">{e.action}</Badge>
                      ) : (
                        <span className="text-xs">{e.action ?? "-"}</span>
                      )}
                    </TableCell>
                    <TableCell className="text-xs">{e.database_name ?? "-"}</TableCell>
                    <TableCell className="text-xs max-w-[300px] truncate" title={e.details ?? undefined}>{e.details ?? "-"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

// ============================================================================
// Settings Tab — API Key Rotation
// ============================================================================

// ============================================================================
// PII Tab
// ============================================================================

function PiiTab({ onError }: { onError: (msg: string) => void }) {
  return (
    <div className="space-y-6">
      <PiiSettingsSection onError={onError} />
      <PiiRulesSection onError={onError} />
      <PiiColumnsSection onError={onError} />
    </div>
  );
}

function PiiSettingsSection({ onError }: { onError: (msg: string) => void }) {
  const [settings, setSettings] = useState<PiiSettings | null>(null);
  const [connections, setConnections] = useState<{ name: string }[]>([]);
  const [saving, setSaving] = useState(false);

  const load = useCallback(async () => {
    try {
      const [s, c] = await Promise.all([getPiiSettings(), listConnections()]);
      setSettings(s);
      setConnections(c);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  }, [onError]);

  useEffect(() => { load(); }, [load]);

  const save = async () => {
    if (!settings) return;
    setSaving(true);
    try {
      await setPiiSettings(settings);
      await load();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  if (!settings) return null;

  return (
    <Card>
      <CardHeader className="py-3 px-4">
        <h3 className="text-sm font-medium">PII Settings</h3>
      </CardHeader>
      <CardContent className="py-3 px-4 space-y-4">
        <p className="text-xs text-muted-foreground">
          PII mode is resolved per-request: token-level &rarr; user-level &rarr; connection override &rarr; none.
          Set per-token and per-user PII modes in the Users and Tokens tabs.
        </p>

        {connections.length > 0 && (
          <div className="space-y-2">
            <Label className="text-xs text-muted-foreground">Per-connection overrides</Label>
            {connections.map((conn) => (
              <div key={conn.name} className="flex items-center gap-3">
                <span className="text-sm w-40 truncate">{conn.name}</span>
                <Select
                  value={settings.connection_overrides[conn.name] || ""}
                  onValueChange={(v) => {
                    const overrides = { ...settings.connection_overrides };
                    if (v === "default") {
                      delete overrides[conn.name];
                    } else {
                      overrides[conn.name] = v;
                    }
                    setSettings({ ...settings, connection_overrides: overrides });
                  }}
                >
                  <SelectTrigger className="w-40"><SelectValue placeholder="Default" /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="default">None (default)</SelectItem>
                    <SelectItem value="scrub">Scrub</SelectItem>
        
                    <SelectItem value="none">None (explicit)</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            ))}
          </div>
        )}

        <Button onClick={save} disabled={saving} size="sm">
          {saving ? "Saving..." : "Save Settings"}
        </Button>
      </CardContent>
    </Card>
  );
}

function PiiRulesSection({ onError }: { onError: (msg: string) => void }) {
  const [rules, setRules] = useState<PiiRule[]>([]);
  const [showAdd, setShowAdd] = useState(false);
  const [showTest, setShowTest] = useState<PiiRule | null>(null);
  const [editRule, setEditRule] = useState<PiiRule | null>(null);
  const [testSample, setTestSample] = useState("");
  const [testResult, setTestResult] = useState<PiiTestResult | null>(null);
  const [form, setForm] = useState({ name: "", description: "", regex_pattern: "", replacement_text: "", entity_kind: "" });

  const load = useCallback(async () => {
    try {
      setRules(await listPiiRules());
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  }, [onError]);

  useEffect(() => { load(); }, [load]);

  const handleCreate = async () => {
    try {
      await createPiiRule(form);
      setShowAdd(false);
      setForm({ name: "", description: "", regex_pattern: "", replacement_text: "", entity_kind: "" });
      await load();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleUpdate = async () => {
    if (!editRule) return;
    try {
      await updatePiiRule(editRule.id, {
        name: editRule.name,
        description: editRule.description ?? undefined,
        regex_pattern: editRule.regex_pattern,
        replacement_text: editRule.replacement_text,
        entity_kind: editRule.entity_kind,
      });
      setEditRule(null);
      await load();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleToggle = async (rule: PiiRule, enabled: boolean) => {
    try {
      await updatePiiRule(rule.id, { is_enabled: enabled });
      await load();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleDelete = async (rule: PiiRule) => {
    try {
      await deletePiiRule(rule.id);
      await load();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleTest = async (rule: PiiRule | null) => {
    if (!rule) return;
    try {
      const result = await testPiiRule({
        regex_pattern: rule.regex_pattern,
        replacement_text: rule.replacement_text,
        sample_text: testSample,
      });
      setTestResult(result);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <Card>
      <CardHeader className="py-3 px-4 flex flex-row items-center justify-between">
        <h3 className="text-sm font-medium">PII Rules</h3>
        <Button size="sm" onClick={() => setShowAdd(true)}>Add Rule</Button>
      </CardHeader>
      <CardContent className="py-0 px-4 pb-4">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Kind</TableHead>
              <TableHead>Pattern</TableHead>
              <TableHead>Enabled</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rules.map((rule) => (
              <TableRow key={rule.id}>
                <TableCell className="font-medium">
                  {rule.name}
                  {rule.is_builtin && <Badge variant="outline" className="ml-2 text-xs">Built-in</Badge>}
                </TableCell>
                <TableCell className="text-xs">{rule.entity_kind}</TableCell>
                <TableCell className="font-mono text-xs max-w-48 truncate">{rule.regex_pattern}</TableCell>
                <TableCell>
                  <Switch checked={rule.is_enabled} onCheckedChange={(v) => handleToggle(rule, v)} />
                </TableCell>
                <TableCell className="text-right space-x-1">
                  <Button
                    variant="ghost" size="sm"
                    onClick={() => { setShowTest(rule); setTestSample(""); setTestResult(null); }}
                  >
                    Test
                  </Button>
                  {!rule.is_builtin && (
                    <>
                      <Button variant="ghost" size="sm" onClick={() => setEditRule({ ...rule })}>Edit</Button>
                      <Button variant="ghost" size="sm" className="text-destructive" onClick={() => handleDelete(rule)}>Delete</Button>
                    </>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>

      {/* Add Rule Dialog */}
      <Dialog open={showAdd} onOpenChange={setShowAdd}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add PII Rule</DialogTitle>
            <DialogDescription>Create a custom PII detection rule.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div><Label>Name</Label><Input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></div>
            <div><Label>Description</Label><Input value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} /></div>
            <div><Label>Entity Kind</Label><Input value={form.entity_kind} onChange={(e) => setForm({ ...form, entity_kind: e.target.value })} placeholder="e.g. employee_id" /></div>
            <div><Label>Regex Pattern</Label><Input className="font-mono" value={form.regex_pattern} onChange={(e) => setForm({ ...form, regex_pattern: e.target.value })} /></div>
            <div><Label>Replacement Text</Label><Input value={form.replacement_text} onChange={(e) => setForm({ ...form, replacement_text: e.target.value })} placeholder="e.g. <employee_id>" /></div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowAdd(false)}>Cancel</Button>
            <Button onClick={handleCreate}>Create</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Edit Rule Dialog */}
      <Dialog open={!!editRule} onOpenChange={(v) => !v && setEditRule(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit PII Rule</DialogTitle>
            <DialogDescription>Modify this custom PII rule.</DialogDescription>
          </DialogHeader>
          {editRule && (
            <div className="space-y-3">
              <div><Label>Name</Label><Input value={editRule.name} onChange={(e) => setEditRule({ ...editRule, name: e.target.value })} /></div>
              <div><Label>Description</Label><Input value={editRule.description ?? ""} onChange={(e) => setEditRule({ ...editRule, description: e.target.value })} /></div>
              <div><Label>Entity Kind</Label><Input value={editRule.entity_kind} onChange={(e) => setEditRule({ ...editRule, entity_kind: e.target.value })} /></div>
              <div><Label>Regex Pattern</Label><Input className="font-mono" value={editRule.regex_pattern} onChange={(e) => setEditRule({ ...editRule, regex_pattern: e.target.value })} /></div>
              <div><Label>Replacement Text</Label><Input value={editRule.replacement_text} onChange={(e) => setEditRule({ ...editRule, replacement_text: e.target.value })} /></div>
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditRule(null)}>Cancel</Button>
            <Button onClick={handleUpdate}>Save</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Test Rule Dialog */}
      <Dialog open={!!showTest} onOpenChange={(v) => { if (!v) { setShowTest(null); setTestResult(null); } }}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>Test Rule: {showTest?.name}</DialogTitle>
            <DialogDescription>Enter sample text to test PII detection.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div>
              <Label>Pattern</Label>
              <Input className="font-mono text-xs" value={showTest?.regex_pattern ?? ""} readOnly />
            </div>
            <div>
              <Label>Sample Text</Label>
              <textarea
                className="w-full min-h-[80px] rounded-md border border-input bg-background px-3 py-2 text-sm font-mono"
                value={testSample}
                onChange={(e) => setTestSample(e.target.value)}
                placeholder="Paste sample data here..."
              />
            </div>
            <Button size="sm" onClick={() => handleTest(showTest)}>Run Test</Button>
            {testResult && (
              <div className="space-y-2">
                <div>
                  <Label className="text-xs text-muted-foreground">Matches ({testResult.matches.length})</Label>
                  {testResult.matches.length > 0 ? (
                    <div className="text-xs font-mono bg-muted p-2 rounded max-h-32 overflow-auto">
                      {testResult.matches.map((m, i) => (
                        <div key={i} className="text-yellow-400">
                          [{m.start}..{m.end}] &quot;{m.text}&quot;
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="text-xs text-muted-foreground">No matches found.</p>
                  )}
                </div>
                <div>
                  <Label className="text-xs text-muted-foreground">Scrubbed Output</Label>
                  <div className="text-xs font-mono bg-muted p-2 rounded">{testResult.scrubbed_text}</div>
                </div>
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </Card>
  );
}

function PiiColumnsSection({ onError }: { onError: (msg: string) => void }) {
  const [taggedColumns, setTaggedColumns] = useState<PiiColumn[]>([]);
  const [connections, setConnections] = useState<{ name: string }[]>([]);
  const [databases, setDatabases] = useState<{ name: string }[]>([]);
  const [tables, setTables] = useState<{ TABLE_NAME: string; TABLE_SCHEMA: string }[]>([]);
  const [tableColumns, setTableColumns] = useState<ColumnInfo[]>([]);
  const [loadingColumns, setLoadingColumns] = useState(false);

  const [selConn, setSelConn] = useState("");
  const [selDb, setSelDb] = useState("");
  const [selTable, setSelTable] = useState("");
  const [selSchema, setSelSchema] = useState("dbo");

  const [discovering, setDiscovering] = useState(false);
  const [discoveryResults, setDiscoveryResults] = useState<Record<string, PiiDiscoveryResult>>({});

  // Build a lookup: column_name -> PiiColumn for the current table
  const taggedMap = new Map<string, PiiColumn>();
  for (const tc of taggedColumns) {
    if (tc.connection_name === selConn && tc.database_name === selDb && tc.table_name === selTable) {
      taggedMap.set(tc.column_name, tc);
    }
  }

  const loadTaggedColumns = useCallback(async () => {
    try {
      setTaggedColumns(await listPiiColumns(selConn ? { connection: selConn, database: selDb || undefined } : undefined));
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  }, [onError, selConn, selDb]);

  useEffect(() => { loadTaggedColumns(); }, [loadTaggedColumns]);

  // Load connections on mount
  useEffect(() => {
    listConnections().then(setConnections).catch(() => {});
  }, []);

  // Load databases when connection changes
  useEffect(() => {
    if (selConn) {
      listDatabases(selConn).then(setDatabases).catch(() => setDatabases([]));
      setSelDb("");
      setSelTable("");
      setTables([]);
      setTableColumns([]);
    }
  }, [selConn]);

  // Load tables when database changes
  useEffect(() => {
    if (selConn && selDb) {
      listTables(selDb, selConn).then(setTables).catch(() => setTables([]));
      setSelTable("");
      setTableColumns([]);
    }
  }, [selConn, selDb]);

  // Load columns when table changes
  useEffect(() => {
    if (selConn && selDb && selTable) {
      setLoadingColumns(true);
      setDiscoveryResults({});
      // Find the schema for the selected table
      const tableInfo = tables.find((t) => t.TABLE_NAME === selTable);
      const schema = tableInfo?.TABLE_SCHEMA || "dbo";
      setSelSchema(schema);
      describeTable(selDb, selTable, selConn, schema)
        .then(setTableColumns)
        .catch(() => setTableColumns([]))
        .finally(() => setLoadingColumns(false));
    } else {
      setTableColumns([]);
    }
  }, [selConn, selDb, selTable, tables]);

  const toggleColumn = async (columnName: string) => {
    const existing = taggedMap.get(columnName);
    try {
      if (existing) {
        // Remove the tag
        await removePiiColumn(existing.id);
      } else {
        // Tag it with default "auto" type
        await setPiiColumn({
          connection_name: selConn,
          database_name: selDb,
          schema_name: selSchema,
          table_name: selTable,
          column_name: columnName,
          pii_type: "auto",
        });
      }
      await loadTaggedColumns();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleDiscover = async () => {
    if (!selConn || !selDb || !selTable) return;
    setDiscovering(true);
    setDiscoveryResults({});
    try {
      const results = await discoverPiiColumns({
        connection: selConn,
        database: selDb,
        table: selTable,
      });
      const map: Record<string, PiiDiscoveryResult> = {};
      for (const r of results) {
        map[r.column_name] = r;
      }
      setDiscoveryResults(map);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setDiscovering(false);
    }
  };

  return (
    <Card>
      <CardHeader className="py-3 px-4">
        <h3 className="text-sm font-medium">PII Column Tags</h3>
        <p className="text-xs text-muted-foreground">Select a table, then click columns to tag them as PII. Tagged columns are automatically scrubbed.</p>
      </CardHeader>
      <CardContent className="py-3 px-4 space-y-4">
        {/* Selectors */}
        <div className="flex gap-2 flex-wrap items-center">
          <Select value={selConn} onValueChange={setSelConn}>
            <SelectTrigger className="w-48"><SelectValue placeholder="Connection" /></SelectTrigger>
            <SelectContent>
              {connections.map((c) => (
                <SelectItem key={c.name} value={c.name}>{c.name}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={selDb} onValueChange={setSelDb} disabled={!selConn}>
            <SelectTrigger className="w-48"><SelectValue placeholder="Database" /></SelectTrigger>
            <SelectContent>
              {databases.map((d) => (
                <SelectItem key={d.name} value={d.name}>{d.name}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={selTable} onValueChange={setSelTable} disabled={!selDb}>
            <SelectTrigger className="w-48"><SelectValue placeholder="Table" /></SelectTrigger>
            <SelectContent>
              {tables.map((t) => (
                <SelectItem key={`${t.TABLE_SCHEMA}.${t.TABLE_NAME}`} value={t.TABLE_NAME}>{t.TABLE_SCHEMA}.{t.TABLE_NAME}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Column list for selected table */}
        {loadingColumns && <p className="text-sm text-muted-foreground">Loading columns...</p>}
        {!loadingColumns && tableColumns.length > 0 && (
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label className="text-xs text-muted-foreground">
                {tableColumns.length} columns in {selSchema}.{selTable} — click to toggle PII tagging
              </Label>
              <Button
                size="sm"
                variant="outline"
                disabled={discovering}
                onClick={handleDiscover}
              >
                {discovering ? "Scanning..." : "Auto-Detect PII"}
              </Button>
            </div>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-10">PII</TableHead>
                  <TableHead>Column</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Tag</TableHead>
                  {Object.keys(discoveryResults).length > 0 && (
                    <TableHead>Detected</TableHead>
                  )}
                </TableRow>
              </TableHeader>
              <TableBody>
                {tableColumns.map((col) => {
                  const tagged = taggedMap.get(col.COLUMN_NAME);
                  const discovery = discoveryResults[col.COLUMN_NAME];
                  return (
                    <TableRow
                      key={col.COLUMN_NAME}
                      className={`cursor-pointer ${tagged ? "bg-orange-50 dark:bg-orange-950/20" : "hover:bg-muted/50"}`}
                      onClick={() => toggleColumn(col.COLUMN_NAME)}
                    >
                      <TableCell>
                        <Switch
                          checked={!!tagged}
                          onCheckedChange={() => toggleColumn(col.COLUMN_NAME)}
                          onClick={(e) => e.stopPropagation()}
                        />
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        {col.COLUMN_NAME}
                        {col.IS_PRIMARY_KEY === "YES" && (
                          <Badge variant="outline" className="ml-2 text-[10px]">PK</Badge>
                        )}
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">{col.DATA_TYPE}</TableCell>
                      <TableCell>
                        {tagged && (
                          <Badge className="text-xs bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200 border-orange-300">
                            &lt;pii&gt;
                          </Badge>
                        )}
                      </TableCell>
                      {Object.keys(discoveryResults).length > 0 && (
                        <TableCell>
                          {discovery && discovery.match_count > 0 ? (
                            <div className="flex items-center gap-1">
                              {discovery.detected_types.map((t) => (
                                <Badge key={t} variant="outline" className="text-[10px]">{t}</Badge>
                              ))}
                              <span className="text-[10px] text-muted-foreground">({discovery.match_count})</span>
                            </div>
                          ) : (
                            <span className="text-[10px] text-muted-foreground">—</span>
                          )}
                        </TableCell>
                      )}
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        )}

        {!loadingColumns && selConn && selDb && selTable && tableColumns.length === 0 && (
          <p className="text-sm text-muted-foreground">No columns found for this table.</p>
        )}
        {!selTable && (
          <p className="text-sm text-muted-foreground">Select a connection, database, and table to manage PII column tags.</p>
        )}
      </CardContent>
    </Card>
  );
}

// ============================================================================
// Settings Tab
// ============================================================================

function SettingsTab({ onError }: { onError: (msg: string) => void }) {
  const [showConfirm, setShowConfirm] = useState(false);
  const [rotating, setRotating] = useState(false);
  const [newKey, setNewKey] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const doRotate = async () => {
    setShowConfirm(false);
    setRotating(true);
    try {
      const result = await rotateApiKey();
      setNewKey(result.api_key);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setRotating(false);
    }
  };

  const copyKey = () => {
    if (newKey) {
      navigator.clipboard.writeText(newKey);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="py-3 px-4">
          <h3 className="text-sm font-medium">System API Key</h3>
        </CardHeader>
        <CardContent className="py-3 px-4">
          <p className="text-sm text-muted-foreground mb-3">
            Rotate the system API key. All existing integrations using the current key will stop working immediately.
          </p>
          <Button variant="destructive" onClick={() => setShowConfirm(true)} disabled={rotating}>
            {rotating ? "Rotating..." : "Rotate API Key"}
          </Button>
        </CardContent>
      </Card>

      {/* Confirmation dialog */}
      <Dialog open={showConfirm} onOpenChange={setShowConfirm}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Rotate System API Key?</DialogTitle>
            <DialogDescription>
              This will immediately invalidate the current API key. All integrations, scripts, and MCP configurations
              using the old key will stop working. This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowConfirm(false)}>Cancel</Button>
            <Button variant="destructive" onClick={doRotate}>Rotate Key</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Reveal dialog */}
      <Dialog open={!!newKey} onOpenChange={(v) => !v && setNewKey(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>New API Key</DialogTitle>
            <DialogDescription>Copy this key now. It cannot be retrieved again.</DialogDescription>
          </DialogHeader>
          <div className="bg-muted p-3 rounded-md font-mono text-xs break-all select-all">{newKey}</div>
          <DialogFooter>
            <Button variant="outline" onClick={copyKey}>{copied ? "Copied!" : "Copy"}</Button>
            <Button onClick={() => setNewKey(null)}>Done</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

// ============================================================================
// Storage Links Tab
// ============================================================================

function StorageLinksTab({ onError }: { onError: (msg: string) => void }) {
  const [links, setLinks] = useState<StorageColumnLink[]>([]);
  const [connections, setConnections] = useState<{ name: string }[]>([]);
  const [databases, setDatabases] = useState<{ name: string }[]>([]);
  const [tables, setTables] = useState<{ TABLE_NAME: string; TABLE_SCHEMA: string }[]>([]);
  const [tableColumns, setTableColumns] = useState<ColumnInfo[]>([]);
  const [storageConns, setStorageConns] = useState<string[]>([]);
  const [buckets, setBuckets] = useState<BucketInfo[]>([]);

  const [selConn, setSelConn] = useState("");
  const [selDb, setSelDb] = useState("");
  const [selTable, setSelTable] = useState("");
  const [selColumn, setSelColumn] = useState("");
  const [selStorageConn, setSelStorageConn] = useState("");
  const [selBucket, setSelBucket] = useState("");
  const [keyPrefix, setKeyPrefix] = useState("");
  const [saving, setSaving] = useState(false);

  const loadLinks = useCallback(async () => {
    try {
      setLinks(await listStorageColumnLinks());
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  }, [onError]);

  useEffect(() => { loadLinks(); }, [loadLinks]);

  // Load DB connections + storage connections on mount
  useEffect(() => {
    listConnections().then(setConnections).catch(() => {});
    storageListConnections().then(setStorageConns).catch(() => setStorageConns([]));
  }, []);

  // Load databases when connection changes
  useEffect(() => {
    if (selConn) {
      listDatabases(selConn).then(setDatabases).catch(() => setDatabases([]));
      setSelDb(""); setSelTable(""); setSelColumn("");
      setTables([]); setTableColumns([]);
    }
  }, [selConn]);

  // Load tables when database changes
  useEffect(() => {
    if (selConn && selDb) {
      listTables(selDb, selConn).then(setTables).catch(() => setTables([]));
      setSelTable(""); setSelColumn(""); setTableColumns([]);
    }
  }, [selConn, selDb]);

  // Load columns when table changes
  useEffect(() => {
    if (selConn && selDb && selTable) {
      setSelColumn("");
      const tableInfo = tables.find((t) => t.TABLE_NAME === selTable);
      const schema = tableInfo?.TABLE_SCHEMA || "dbo";
      describeTable(selDb, selTable, selConn, schema)
        .then(setTableColumns)
        .catch(() => setTableColumns([]));
    } else {
      setTableColumns([]);
    }
  }, [selConn, selDb, selTable, tables]);

  // Load buckets when storage connection changes
  useEffect(() => {
    if (selStorageConn) {
      storageListBuckets(selStorageConn).then(setBuckets).catch(() => setBuckets([]));
      setSelBucket("");
    }
  }, [selStorageConn]);

  const handleAdd = async () => {
    if (!selConn || !selDb || !selTable || !selColumn || !selStorageConn || !selBucket) return;
    setSaving(true);
    try {
      const tableInfo = tables.find((t) => t.TABLE_NAME === selTable);
      await setStorageColumnLink({
        connection_name: selConn,
        database_name: selDb,
        schema_name: tableInfo?.TABLE_SCHEMA,
        table_name: selTable,
        column_name: selColumn,
        storage_connection: selStorageConn,
        bucket_name: selBucket,
        key_prefix: keyPrefix || undefined,
      });
      await loadLinks();
      setSelColumn("");
      setKeyPrefix("");
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (id: number) => {
    try {
      await removeStorageColumnLink(id);
      await loadLinks();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <div className="space-y-4">
      {/* Existing links */}
      <Card>
        <CardHeader className="py-3 px-4">
          <h3 className="text-sm font-medium">Storage Column Links</h3>
          <p className="text-xs text-muted-foreground">Link database columns to S3/MinIO buckets. Linked column values render as download links in query results.</p>
        </CardHeader>
        <CardContent className="py-3 px-4">
          {links.length === 0 ? (
            <p className="text-sm text-muted-foreground">No storage column links configured.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Connection</TableHead>
                  <TableHead>Database</TableHead>
                  <TableHead>Table.Column</TableHead>
                  <TableHead>Storage</TableHead>
                  <TableHead>Bucket</TableHead>
                  <TableHead>Prefix</TableHead>
                  <TableHead className="w-10"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {links.map((link) => (
                  <TableRow key={link.id}>
                    <TableCell className="font-mono text-xs">{link.connection_name}</TableCell>
                    <TableCell className="font-mono text-xs">{link.database_name}</TableCell>
                    <TableCell className="font-mono text-xs">{link.table_name}.{link.column_name}</TableCell>
                    <TableCell className="font-mono text-xs">{link.storage_connection}</TableCell>
                    <TableCell className="font-mono text-xs">{link.bucket_name}</TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">{link.key_prefix || "—"}</TableCell>
                    <TableCell>
                      <Button size="sm" variant="destructive" onClick={() => handleDelete(link.id)}>Delete</Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* Add new link */}
      <Card>
        <CardHeader className="py-3 px-4">
          <h3 className="text-sm font-medium">Add Link</h3>
        </CardHeader>
        <CardContent className="py-3 px-4 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            {/* Left: DB selectors */}
            <div className="space-y-2">
              <Label className="text-xs text-muted-foreground">Database Column</Label>
              <Select value={selConn} onValueChange={setSelConn}>
                <SelectTrigger><SelectValue placeholder="Connection" /></SelectTrigger>
                <SelectContent>
                  {connections.map((c) => (
                    <SelectItem key={c.name} value={c.name}>{c.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={selDb} onValueChange={setSelDb} disabled={!selConn}>
                <SelectTrigger><SelectValue placeholder="Database" /></SelectTrigger>
                <SelectContent>
                  {databases.map((d) => (
                    <SelectItem key={d.name} value={d.name}>{d.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={selTable} onValueChange={setSelTable} disabled={!selDb}>
                <SelectTrigger><SelectValue placeholder="Table" /></SelectTrigger>
                <SelectContent>
                  {tables.map((t) => (
                    <SelectItem key={`${t.TABLE_SCHEMA}.${t.TABLE_NAME}`} value={t.TABLE_NAME}>
                      {t.TABLE_SCHEMA}.{t.TABLE_NAME}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={selColumn} onValueChange={setSelColumn} disabled={!selTable}>
                <SelectTrigger><SelectValue placeholder="Column" /></SelectTrigger>
                <SelectContent>
                  {tableColumns.map((c) => (
                    <SelectItem key={c.COLUMN_NAME} value={c.COLUMN_NAME}>
                      {c.COLUMN_NAME} <span className="text-muted-foreground">({c.DATA_TYPE})</span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Right: Storage selectors */}
            <div className="space-y-2">
              <Label className="text-xs text-muted-foreground">Storage Destination</Label>
              <Select value={selStorageConn} onValueChange={setSelStorageConn}>
                <SelectTrigger><SelectValue placeholder="Storage Connection" /></SelectTrigger>
                <SelectContent>
                  {storageConns.map((name) => (
                    <SelectItem key={name} value={name}>{name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={selBucket} onValueChange={setSelBucket} disabled={!selStorageConn}>
                <SelectTrigger><SelectValue placeholder="Bucket" /></SelectTrigger>
                <SelectContent>
                  {buckets.map((b) => (
                    <SelectItem key={b.name} value={b.name}>{b.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Input
                placeholder="Key prefix (optional)"
                value={keyPrefix}
                onChange={(e) => setKeyPrefix(e.target.value)}
              />
            </div>
          </div>
          <Button
            onClick={handleAdd}
            disabled={saving || !selConn || !selDb || !selTable || !selColumn || !selStorageConn || !selBucket}
          >
            {saving ? "Saving..." : "Add Link"}
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}

// ============================================================================
// Service Accounts Tab
// ============================================================================

function ServiceAccountsTab({ onError }: { onError: (msg: string) => void }) {
  const [accounts, setAccounts] = useState<ServiceAccountInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [editAccount, setEditAccount] = useState<ServiceAccountInfo | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const [rotateTarget, setRotateTarget] = useState<string | null>(null);
  const [permTarget, setPermTarget] = useState<ServiceAccountInfo | null>(null);
  const [unavailable, setUnavailable] = useState(false);
  const [newKey, setNewKey] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setAccounts(await listServiceAccounts());
      setUnavailable(false);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes("not enabled")) setUnavailable(true);
      else onError(msg);
    } finally {
      setLoading(false);
    }
  }, [onError]);

  useEffect(() => { refresh(); }, [refresh]);

  if (loading) return <p className="text-muted-foreground py-4">Loading service accounts...</p>;
  if (unavailable) return (
    <Card><CardContent className="py-8 text-center text-muted-foreground">
      Access control is not enabled on this server.
    </CardContent></Card>
  );

  return (
    <>
      <div className="flex justify-end mb-3">
        <Button size="sm" onClick={() => setShowCreate(true)}>Create Service Account</Button>
      </div>
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Description</TableHead>
                <TableHead className="w-[90px]">SQL Mode</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Key Prefix</TableHead>
                <TableHead className="w-[280px]">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {accounts.length === 0 ? (
                <TableRow><TableCell colSpan={6} className="text-center text-muted-foreground py-8">No service accounts</TableCell></TableRow>
              ) : accounts.map((sa) => (
                <TableRow key={sa.name}>
                  <TableCell className="font-mono text-xs">{sa.name}</TableCell>
                  <TableCell className="text-sm">{sa.description ?? <span className="text-muted-foreground">-</span>}</TableCell>
                  <TableCell>
                    <Badge variant={sa.sql_mode === "full" ? "default" : sa.sql_mode === "none" ? "outline" : "secondary"} className="text-xs">
                      {sa.sql_mode === "read_only" ? "Read Only" : sa.sql_mode === "supervised" ? "Supervised" : sa.sql_mode === "confirmed" ? "Confirmed" : sa.sql_mode === "full" ? "Full" : "None"}
                    </Badge>
                  </TableCell>
                  <TableCell>{sa.is_enabled ? <Badge variant="outline" className="text-green-400 border-green-400/50">Enabled</Badge> : <Badge variant="destructive">Disabled</Badge>}</TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">{sa.api_key_prefix}...</TableCell>
                  <TableCell className="space-x-1">
                    <Button variant="ghost" size="sm" onClick={() => setEditAccount(sa)}>Edit</Button>
                    <Button variant="ghost" size="sm" onClick={() => setPermTarget(sa)}>Permissions</Button>
                    <Button variant="ghost" size="sm" onClick={() => setRotateTarget(sa.name)}>Rotate Key</Button>
                    <Button variant="ghost" size="sm" className="text-destructive" onClick={() => setDeleteTarget(sa.name)}>Delete</Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Create Dialog */}
      <CreateServiceAccountDialog
        open={showCreate}
        onClose={() => setShowCreate(false)}
        onCreated={(key) => { setNewKey(key); refresh(); }}
        onError={onError}
      />

      {/* Show New Key Dialog */}
      <Dialog open={!!newKey} onOpenChange={(v) => !v && setNewKey(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Service Account Created</DialogTitle>
            <DialogDescription>
              Copy this API key now. It will not be shown again.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2 py-2">
            <Input readOnly value={newKey ?? ""} className="font-mono text-xs" onClick={(e) => (e.target as HTMLInputElement).select()} />
            <Button size="sm" variant="outline" onClick={() => { navigator.clipboard.writeText(newKey ?? ""); }}>
              Copy to Clipboard
            </Button>
          </div>
          <DialogFooter>
            <Button onClick={() => setNewKey(null)}>Done</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Edit Dialog */}
      <EditServiceAccountDialog
        account={editAccount}
        onClose={() => setEditAccount(null)}
        onSaved={refresh}
        onError={onError}
      />

      {/* Rotate Key Confirm */}
      <RotateKeyDialog
        name={rotateTarget}
        onClose={() => setRotateTarget(null)}
        onRotated={(key) => { setNewKey(key); refresh(); }}
        onError={onError}
      />

      {/* Delete Confirm */}
      <DeleteServiceAccountDialog
        name={deleteTarget}
        onClose={() => setDeleteTarget(null)}
        onDeleted={refresh}
        onError={onError}
      />

      {/* Permissions Dialog */}
      <ServiceAccountPermissionsDialog
        account={permTarget}
        onClose={() => setPermTarget(null)}
        onSaved={refresh}
        onError={onError}
      />
    </>
  );
}

function CreateServiceAccountDialog({ open, onClose, onCreated, onError }: {
  open: boolean; onClose: () => void; onCreated: (key: string) => void; onError: (msg: string) => void;
}) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [sqlMode, setSqlMode] = useState("full");
  const [saving, setSaving] = useState(false);

  useEffect(() => { if (open) { setName(""); setDescription(""); setSqlMode("full"); } }, [open]);

  const submit = async () => {
    if (!name.trim()) return;
    setSaving(true);
    try {
      const result = await createServiceAccount(name.trim(), description || undefined, sqlMode);
      onClose();
      onCreated(result.api_key);
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create Service Account</DialogTitle>
          <DialogDescription>Create a named API key with scoped permissions.</DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label>Name</Label>
            <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. etl-pipeline" />
          </div>
          <div className="space-y-2">
            <Label>Description</Label>
            <Input value={description} onChange={(e) => setDescription(e.target.value)} placeholder="Optional description" />
          </div>
          <div className="space-y-2">
            <Label>SQL Mode</Label>
            <Select value={sqlMode} onValueChange={setSqlMode}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="full">Full</SelectItem>
                <SelectItem value="confirmed">Confirmed</SelectItem>
                <SelectItem value="supervised">Supervised</SelectItem>
                <SelectItem value="read_only">Read Only</SelectItem>
                <SelectItem value="none">None</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={saving || !name.trim()}>{saving ? "Creating..." : "Create"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function EditServiceAccountDialog({ account, onClose, onSaved, onError }: {
  account: ServiceAccountInfo | null; onClose: () => void; onSaved: () => void; onError: (msg: string) => void;
}) {
  const [description, setDescription] = useState("");
  const [sqlMode, setSqlMode] = useState("full");
  const [isEnabled, setIsEnabled] = useState(true);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (account) {
      setDescription(account.description ?? "");
      setSqlMode(account.sql_mode);
      setIsEnabled(account.is_enabled);
    }
  }, [account]);

  const submit = async () => {
    if (!account) return;
    setSaving(true);
    try {
      await updateServiceAccount(account.name, { description: description || undefined, sql_mode: sqlMode, is_enabled: isEnabled });
      onClose();
      onSaved();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  return (
    <Dialog open={!!account} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Edit Service Account</DialogTitle>
          <DialogDescription>Update <span className="font-mono">{account?.name}</span></DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label>Description</Label>
            <Input value={description} onChange={(e) => setDescription(e.target.value)} />
          </div>
          <div className="space-y-2">
            <Label>SQL Mode</Label>
            <Select value={sqlMode} onValueChange={setSqlMode}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="full">Full</SelectItem>
                <SelectItem value="confirmed">Confirmed</SelectItem>
                <SelectItem value="supervised">Supervised</SelectItem>
                <SelectItem value="read_only">Read Only</SelectItem>
                <SelectItem value="none">None</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="flex items-center gap-3">
            <Label>Enabled</Label>
            <Switch checked={isEnabled} onCheckedChange={setIsEnabled} />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={saving}>{saving ? "Saving..." : "Save"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function RotateKeyDialog({ name, onClose, onRotated, onError }: {
  name: string | null; onClose: () => void; onRotated: (key: string) => void; onError: (msg: string) => void;
}) {
  const [saving, setSaving] = useState(false);

  const submit = async () => {
    if (!name) return;
    setSaving(true);
    try {
      const result = await rotateServiceAccountKey(name);
      onClose();
      onRotated(result.api_key);
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  return (
    <Dialog open={!!name} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Rotate API Key</DialogTitle>
          <DialogDescription>
            Generate a new API key for <span className="font-mono">{name}</span>. The old key will stop working immediately.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button variant="destructive" onClick={submit} disabled={saving}>{saving ? "Rotating..." : "Rotate Key"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function DeleteServiceAccountDialog({ name, onClose, onDeleted, onError }: {
  name: string | null; onClose: () => void; onDeleted: () => void; onError: (msg: string) => void;
}) {
  const [saving, setSaving] = useState(false);

  const submit = async () => {
    if (!name) return;
    setSaving(true);
    try {
      await deleteServiceAccount(name);
      onClose();
      onDeleted();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  return (
    <Dialog open={!!name} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete Service Account</DialogTitle>
          <DialogDescription>
            Permanently delete <span className="font-mono">{name}</span> and all its permissions? This cannot be undone.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button variant="destructive" onClick={submit} disabled={saving}>{saving ? "Deleting..." : "Delete"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ServiceAccountPermissionsDialog({ account, onClose, onSaved, onError }: {
  account: ServiceAccountInfo | null; onClose: () => void; onSaved: () => void; onError: (msg: string) => void;
}) {
  const [inventory, setInventory] = useState<InventoryConnection[]>([]);
  const [permState, setPermState] = useState<PermState>({ databases: {}, tables: {} });
  const [connAccess, setConnAccess] = useState<string[] | null>(null);
  const [loadingInventory, setLoadingInventory] = useState(false);
  const [saving, setSaving] = useState(false);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  useEffect(() => {
    if (!account) return;
    let cancelled = false;
    (async () => {
      setLoadingInventory(true);
      try {
        const inv = await getInventory();
        if (!cancelled) setInventory(inv);
      } catch (e) {
        if (!cancelled) onError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoadingInventory(false);
      }
    })();
    return () => { cancelled = true; };
  }, [account, onError]);

  useEffect(() => {
    if (account?.permissions) {
      setPermState(buildPermStateFromPermissions(account.permissions));
    } else {
      setPermState({ databases: {}, tables: {} });
    }
    setConnAccess(account?.connection_permissions ?? null);
  }, [account]);

  const toggleExpand = (key: string) => {
    setExpanded((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const setDbAccess = (dbName: string, field: keyof PermAccess, value: boolean) => {
    setPermState((prev) => {
      const next = { ...prev, databases: { ...prev.databases } };
      const existing = next.databases[dbName] ?? { ...emptyAccess };
      next.databases[dbName] = { ...existing, [field]: value };
      const updated = next.databases[dbName];
      if (!updated.read && !updated.write && !updated.update && !updated.delete) {
        delete next.databases[dbName];
      }
      return next;
    });
  };

  const getDbAccess = (dbName: string): PermAccess => {
    return permState.databases[dbName] ?? permState.databases["*"] ?? { ...emptyAccess };
  };

  const save = async () => {
    if (!account) return;
    setSaving(true);
    try {
      await setServiceAccountPermissions(account.name, permStateToPermissions(permState));
      await setServiceAccountConnections(account.name, connAccess ?? []);
      onClose();
      onSaved();
    } catch (e) { onError(e instanceof Error ? e.message : String(e)); }
    finally { setSaving(false); }
  };

  return (
    <Dialog open={!!account} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-3xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Permissions: {account?.name}</DialogTitle>
          <DialogDescription>Manage database and connection access for this service account.</DialogDescription>
        </DialogHeader>

        {loadingInventory ? (
          <p className="text-muted-foreground py-4">Loading inventory...</p>
        ) : (
          <div className="space-y-4 py-2">
            {/* Wildcard database access */}
            <Card>
              <CardHeader className="py-2 px-4">
                <div className="flex items-center gap-4 text-sm">
                  <span className="font-mono font-bold">* (all databases)</span>
                  {(["read", "write", "update", "delete"] as const).map((f) => (
                    <label key={f} className="flex items-center gap-1 text-xs">
                      <Switch
                        checked={getDbAccess("*")[f]}
                        onCheckedChange={(v) => setDbAccess("*", f, v)}
                      />
                      <span className="capitalize">{f === "write" ? "Insert" : f === "read" ? "Read" : f === "update" ? "Update" : "Delete"}</span>
                    </label>
                  ))}
                </div>
              </CardHeader>
            </Card>

            {/* Per-connection */}
            {inventory.map((conn) => (
              <Card key={conn.name}>
                <CardHeader className="py-2 px-4">
                  <div className="flex items-center justify-between">
                    <span className="font-bold text-sm">{conn.name}</span>
                    <div className="flex items-center gap-2">
                      <label className="flex items-center gap-1 text-xs text-muted-foreground">
                        <Switch
                          checked={connAccess === null || connAccess.includes(conn.name)}
                          onCheckedChange={(v) => {
                            if (v) {
                              setConnAccess((prev) => prev === null ? null : [...prev, conn.name]);
                            } else {
                              setConnAccess((prev) => {
                                const all = inventory.map((c) => c.name);
                                const current = prev ?? all;
                                return current.filter((c) => c !== conn.name);
                              });
                            }
                          }}
                        />
                        Connection Access
                      </label>
                    </div>
                  </div>
                </CardHeader>
                <CardContent className="px-4 pb-2 space-y-1">
                  {conn.databases.map((db) => (
                    <div key={db.name}>
                      <div
                        className="flex items-center gap-4 py-1 cursor-pointer hover:bg-muted/50 rounded px-2"
                        onClick={() => toggleExpand(`${conn.name}::${db.name}`)}
                      >
                        <span className="text-xs w-4">{expanded[`${conn.name}::${db.name}`] ? "\u25BC" : "\u25B6"}</span>
                        <span className="font-mono text-xs flex-1">{db.name}</span>
                        {(["read", "write", "update", "delete"] as const).map((f) => (
                          <label key={f} className="flex items-center gap-1 text-xs" onClick={(e) => e.stopPropagation()}>
                            <Switch
                              checked={getDbAccess(db.name)[f]}
                              onCheckedChange={(v) => setDbAccess(db.name, f, v)}
                            />
                            <span className="capitalize">{f === "write" ? "Ins" : f === "read" ? "R" : f === "update" ? "Upd" : "Del"}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  ))}
                </CardContent>
              </Card>
            ))}
          </div>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={save} disabled={saving}>{saving ? "Saving..." : "Save Permissions"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// ============================================================================
// Endpoints Tab
// ============================================================================

function EndpointsTab({ onError }: { onError: (msg: string) => void }) {
  const [endpoints, setEndpoints] = useState<EndpointInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [editTarget, setEditTarget] = useState<EndpointInfo | null>(null);
  const [permTarget, setPermTarget] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setEndpoints(await listEndpoints());
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [onError]);

  useEffect(() => { refresh(); }, [refresh]);

  const handleDelete = async (name: string) => {
    if (!confirm(`Delete endpoint "${name}"?`)) return;
    try {
      await deleteEndpoint(name);
      refresh();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  if (loading) return <div className="text-muted-foreground text-sm py-4">Loading endpoints...</div>;

  return (
    <>
      <div className="flex justify-between items-center mb-3">
        <Button variant="outline" size="sm" onClick={refresh}>Refresh</Button>
        <Button size="sm" onClick={() => setShowCreate(true)}>Add Endpoint</Button>
      </div>

      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Connection</TableHead>
                <TableHead>Database</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Created By</TableHead>
                <TableHead className="w-[250px]">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {endpoints.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                    No endpoints configured
                  </TableCell>
                </TableRow>
              ) : endpoints.map((ep) => (
                <TableRow key={ep.name}>
                  <TableCell className="font-mono text-sm">{ep.name}</TableCell>
                  <TableCell>{ep.connection_name}</TableCell>
                  <TableCell>{ep.database_name}</TableCell>
                  <TableCell className="max-w-[200px] truncate">{ep.description ?? ""}</TableCell>
                  <TableCell>{ep.created_by ?? ""}</TableCell>
                  <TableCell className="space-x-1">
                    <Button variant="ghost" size="sm" onClick={() => setEditTarget(ep)}>Edit</Button>
                    <Button variant="ghost" size="sm" onClick={() => setPermTarget(ep.name)}>Permissions</Button>
                    <Button variant="ghost" size="sm" onClick={() => handleDelete(ep.name)}>Delete</Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <EndpointFormDialog
        open={showCreate}
        onClose={() => setShowCreate(false)}
        onSaved={refresh}
        onError={onError}
      />
      {editTarget && (
        <EndpointFormDialog
          open
          endpoint={editTarget}
          onClose={() => setEditTarget(null)}
          onSaved={refresh}
          onError={onError}
        />
      )}
      {permTarget && (
        <EndpointPermissionsDialog
          open
          endpointName={permTarget}
          onClose={() => setPermTarget(null)}
          onError={onError}
        />
      )}
    </>
  );
}

function EndpointFormDialog({
  open,
  endpoint,
  onClose,
  onSaved,
  onError,
}: {
  open: boolean;
  endpoint?: EndpointInfo;
  onClose: () => void;
  onSaved: () => void;
  onError: (msg: string) => void;
}) {
  const isEdit = !!endpoint;
  const [name, setName] = useState(endpoint?.name ?? "");
  const [connName, setConnName] = useState(endpoint?.connection_name ?? "");
  const [dbName, setDbName] = useState(endpoint?.database_name ?? "");
  const [query, setQuery] = useState(endpoint?.query ?? "");
  const [description, setDescription] = useState(endpoint?.description ?? "");
  const [paramsJson, setParamsJson] = useState(endpoint?.parameters ?? "");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (open) {
      setName(endpoint?.name ?? "");
      setConnName(endpoint?.connection_name ?? "");
      setDbName(endpoint?.database_name ?? "");
      setQuery(endpoint?.query ?? "");
      setDescription(endpoint?.description ?? "");
      setParamsJson(endpoint?.parameters ?? "");
    }
  }, [open, endpoint]);

  const save = async () => {
    setSaving(true);
    try {
      const data = {
        connection_name: connName,
        database_name: dbName,
        query,
        description: description || undefined,
        parameters: paramsJson || undefined,
      };
      if (isEdit) {
        await updateEndpoint(name, data);
      } else {
        await createEndpoint({ name, ...data });
      }
      onSaved();
      onClose();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
    setSaving(false);
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-lg">
        <DialogHeader><DialogTitle>{isEdit ? "Edit Endpoint" : "Create Endpoint"}</DialogTitle></DialogHeader>
        <div className="space-y-4 py-2 max-h-[60vh] overflow-y-auto">
          <div>
            <Label>Name</Label>
            <Input value={name} onChange={(e) => setName(e.target.value)} disabled={isEdit} placeholder="my-endpoint" />
          </div>
          <div>
            <Label>Connection</Label>
            <Input value={connName} onChange={(e) => setConnName(e.target.value)} placeholder="connection name" />
          </div>
          <div>
            <Label>Database</Label>
            <Input value={dbName} onChange={(e) => setDbName(e.target.value)} placeholder="database name" />
          </div>
          <div>
            <Label>Query</Label>
            <textarea
              className="w-full h-32 rounded-md border border-input bg-background px-3 py-2 text-sm font-mono"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="SELECT * FROM ..."
            />
          </div>
          <div>
            <Label>Description</Label>
            <Input value={description} onChange={(e) => setDescription(e.target.value)} placeholder="Optional" />
          </div>
          <div>
            <Label>Parameters (JSON)</Label>
            <textarea
              className="w-full h-20 rounded-md border border-input bg-background px-3 py-2 text-sm font-mono"
              value={paramsJson}
              onChange={(e) => setParamsJson(e.target.value)}
              placeholder='[{"name":"region","type":"string","default":"US"}]'
            />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={save} disabled={saving}>{saving ? "Saving..." : "Save"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function EndpointPermissionsDialog({
  open,
  endpointName,
  onClose,
  onError,
}: {
  open: boolean;
  endpointName: string;
  onClose: () => void;
  onError: (msg: string) => void;
}) {
  const [emails, setEmails] = useState<string[]>([]);
  const [newEmail, setNewEmail] = useState("");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (open) {
      setLoading(true);
      getEndpointPermissions(endpointName)
        .then(setEmails)
        .catch((e) => onError(e instanceof Error ? e.message : String(e)))
        .finally(() => setLoading(false));
    }
  }, [open, endpointName, onError]);

  const addEmail = () => {
    const e = newEmail.trim();
    if (e && !emails.includes(e)) {
      setEmails([...emails, e]);
      setNewEmail("");
    }
  };

  const removeEmail = (email: string) => {
    setEmails(emails.filter((e) => e !== email));
  };

  const save = async () => {
    setSaving(true);
    try {
      await setEndpointPermissions(endpointName, emails);
      onClose();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
    setSaving(false);
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Permissions: {endpointName}</DialogTitle>
        </DialogHeader>
        {loading ? (
          <div className="py-4 text-muted-foreground text-sm">Loading...</div>
        ) : (
          <div className="space-y-4 py-2 max-h-[60vh] overflow-y-auto">
            <p className="text-sm text-muted-foreground">
              Only listed users can access this endpoint. No users = no access.
            </p>
            <div className="flex gap-2">
              <Input
                value={newEmail}
                onChange={(e) => setNewEmail(e.target.value)}
                placeholder="user@example.com"
                onKeyDown={(e) => e.key === "Enter" && addEmail()}
              />
              <Button size="sm" onClick={addEmail}>Add</Button>
            </div>
            {emails.length > 0 && (
              <div className="space-y-1">
                {emails.map((email) => (
                  <div key={email} className="flex items-center justify-between py-1 px-2 rounded bg-muted">
                    <span className="text-sm">{email}</span>
                    <Button variant="ghost" size="sm" onClick={() => removeEmail(email)}>Remove</Button>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={save} disabled={saving}>{saving ? "Saving..." : "Save"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// ============================================================================
// Teams Tab
// ============================================================================

function TeamsTab({ onError }: { onError: (msg: string) => void }) {
  const [teams, setTeams] = useState<Team[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState("");
  const [newWebhook, setNewWebhook] = useState("");
  const [editTeam, setEditTeam] = useState<Team | null>(null);
  const [editName, setEditName] = useState("");
  const [editWebhook, setEditWebhook] = useState("");
  const [selectedTeam, setSelectedTeam] = useState<Team | null>(null);
  const [members, setMembers] = useState<TeamMember[]>([]);
  const [projects, setProjects] = useState<Project[]>([]);
  const [addMemberEmail, setAddMemberEmail] = useState("");
  const [addMemberRole, setAddMemberRole] = useState("member");
  const [newProjectName, setNewProjectName] = useState("");
  const [selectedProject, setSelectedProject] = useState<Project | null>(null);
  const [projectMembers, setProjectMembers] = useState<ProjectMember[]>([]);
  const [addProjMemberEmail, setAddProjMemberEmail] = useState("");
  const [addProjMemberRole, setAddProjMemberRole] = useState("member");

  const refresh = useCallback(async () => {
    try {
      const data = await listTeams();
      setTeams(data);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [onError]);

  useEffect(() => { refresh(); }, [refresh]);

  const handleCreate = async () => {
    if (!newName.trim()) return;
    try {
      await createTeam(newName.trim(), newWebhook.trim() || undefined);
      setNewName("");
      setNewWebhook("");
      setCreating(false);
      refresh();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleEdit = async () => {
    if (!editTeam) return;
    try {
      await updateTeam(editTeam.id, {
        name: editName.trim() || undefined,
        webhook_url: editWebhook.trim() || null,
      });
      setEditTeam(null);
      refresh();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm("Delete this team and all its projects?")) return;
    try {
      await deleteTeam(id);
      if (selectedTeam?.id === id) setSelectedTeam(null);
      refresh();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const loadTeamDetail = async (team: Team) => {
    setSelectedTeam(team);
    setSelectedProject(null);
    try {
      const [m, p] = await Promise.all([listTeamMembers(team.id), listProjects(team.id)]);
      setMembers(m);
      setProjects(p);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleAddMember = async () => {
    if (!selectedTeam || !addMemberEmail.trim()) return;
    try {
      await addTeamMember(selectedTeam.id, addMemberEmail.trim(), addMemberRole);
      setAddMemberEmail("");
      setAddMemberRole("member");
      const m = await listTeamMembers(selectedTeam.id);
      setMembers(m);
      refresh();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleToggleRole = async (email: string, currentRole: string) => {
    if (!selectedTeam) return;
    const newRole = currentRole === "team_lead" ? "member" : "team_lead";
    try {
      await setTeamMemberRole(selectedTeam.id, email, newRole);
      const m = await listTeamMembers(selectedTeam.id);
      setMembers(m);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleRemoveMember = async (email: string) => {
    if (!selectedTeam) return;
    try {
      await removeTeamMember(selectedTeam.id, email);
      const m = await listTeamMembers(selectedTeam.id);
      setMembers(m);
      refresh();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleCreateProject = async () => {
    if (!selectedTeam || !newProjectName.trim()) return;
    try {
      await createProject(selectedTeam.id, newProjectName.trim());
      setNewProjectName("");
      const p = await listProjects(selectedTeam.id);
      setProjects(p);
      refresh();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleDeleteProject = async (id: string) => {
    if (!confirm("Delete this project?")) return;
    try {
      await deleteProject(id);
      if (selectedProject?.id === id) setSelectedProject(null);
      if (selectedTeam) {
        const p = await listProjects(selectedTeam.id);
        setProjects(p);
      }
      refresh();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const loadProjectDetail = async (proj: Project) => {
    setSelectedProject(proj);
    try {
      const m = await listProjectMembers(proj.id);
      setProjectMembers(m);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleAddProjMember = async () => {
    if (!selectedProject || !addProjMemberEmail.trim()) return;
    try {
      await addProjectMember(selectedProject.id, addProjMemberEmail.trim(), addProjMemberRole);
      setAddProjMemberEmail("");
      setAddProjMemberRole("member");
      const m = await listProjectMembers(selectedProject.id);
      setProjectMembers(m);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleToggleProjRole = async (email: string, currentRole: string) => {
    if (!selectedProject) return;
    const newRole = currentRole === "project_lead" ? "member" : "project_lead";
    try {
      await setProjectMemberRole(selectedProject.id, email, newRole);
      const m = await listProjectMembers(selectedProject.id);
      setProjectMembers(m);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleRemoveProjMember = async (email: string) => {
    if (!selectedProject) return;
    try {
      await removeProjectMember(selectedProject.id, email);
      const m = await listProjectMembers(selectedProject.id);
      setProjectMembers(m);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  if (loading) return <p className="text-sm text-muted-foreground p-4">Loading...</p>;

  return (
    <div className="space-y-4 p-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold">Teams</h3>
        <Button size="sm" onClick={() => setCreating(true)}>Create Team</Button>
      </div>

      {/* Team list */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
        {teams.map((t) => (
          <Card
            key={t.id}
            className={`cursor-pointer transition-colors ${selectedTeam?.id === t.id ? "border-primary" : "hover:bg-accent/30"}`}
            onClick={() => loadTeamDetail(t)}
          >
            <CardContent className="py-3 px-4 flex items-center justify-between">
              <div>
                <span className="font-medium text-sm">{t.name}</span>
                <p className="text-xs text-muted-foreground">
                  {t.member_count} members, {t.project_count} projects
                </p>
              </div>
              <div className="flex gap-1">
                <Button variant="ghost" size="sm" onClick={(e) => {
                  e.stopPropagation();
                  setEditTeam(t);
                  setEditName(t.name);
                  setEditWebhook(t.webhook_url || "");
                }}>Edit</Button>
                <Button variant="ghost" size="sm" className="text-destructive" onClick={(e) => {
                  e.stopPropagation();
                  handleDelete(t.id);
                }}>Delete</Button>
              </div>
            </CardContent>
          </Card>
        ))}
        {teams.length === 0 && (
          <p className="text-sm text-muted-foreground">No teams yet. Create one to enable delegated approvals.</p>
        )}
      </div>

      {/* Team detail panel */}
      {selectedTeam && (
        <Card>
          <CardHeader className="py-3 px-4">
            <h4 className="font-semibold">{selectedTeam.name}</h4>
            {selectedTeam.webhook_url && (
              <p className="text-xs text-muted-foreground">Webhook: {selectedTeam.webhook_url}</p>
            )}
          </CardHeader>
          <CardContent className="px-4 pb-4 space-y-4">
            {/* Members */}
            <div>
              <h5 className="text-sm font-medium mb-2">Members</h5>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Email</TableHead>
                    <TableHead>Name</TableHead>
                    <TableHead>Role</TableHead>
                    <TableHead className="w-24"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {members.map((m) => (
                    <TableRow key={m.email}>
                      <TableCell className="font-mono text-xs">{m.email}</TableCell>
                      <TableCell className="text-xs">{m.display_name || "-"}</TableCell>
                      <TableCell>
                        <Badge
                          variant={m.role === "team_lead" ? "default" : "outline"}
                          className="cursor-pointer"
                          onClick={() => handleToggleRole(m.email, m.role)}
                        >
                          {m.role === "team_lead" ? "Team Lead" : "Member"}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <Button variant="ghost" size="sm" className="text-destructive h-6 text-xs" onClick={() => handleRemoveMember(m.email)}>
                          Remove
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              <div className="flex gap-2 mt-2">
                <Input
                  placeholder="user@example.com"
                  value={addMemberEmail}
                  onChange={(e) => setAddMemberEmail(e.target.value)}
                  className="flex-1"
                />
                <Select value={addMemberRole} onValueChange={setAddMemberRole}>
                  <SelectTrigger className="w-32"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="member">Member</SelectItem>
                    <SelectItem value="team_lead">Team Lead</SelectItem>
                  </SelectContent>
                </Select>
                <Button size="sm" onClick={handleAddMember}>Add</Button>
              </div>
            </div>

            {/* Projects */}
            <div>
              <h5 className="text-sm font-medium mb-2">Projects</h5>
              <div className="space-y-1">
                {projects.map((p) => (
                  <div
                    key={p.id}
                    className={`flex items-center justify-between px-3 py-2 rounded-md cursor-pointer text-sm ${selectedProject?.id === p.id ? "bg-accent" : "hover:bg-accent/30"}`}
                    onClick={() => loadProjectDetail(p)}
                  >
                    <span>{p.name} <span className="text-xs text-muted-foreground">({p.member_count} members)</span></span>
                    <Button variant="ghost" size="sm" className="text-destructive h-6 text-xs" onClick={(e) => {
                      e.stopPropagation();
                      handleDeleteProject(p.id);
                    }}>Delete</Button>
                  </div>
                ))}
              </div>
              <div className="flex gap-2 mt-2">
                <Input
                  placeholder="New project name"
                  value={newProjectName}
                  onChange={(e) => setNewProjectName(e.target.value)}
                  className="flex-1"
                />
                <Button size="sm" onClick={handleCreateProject}>Add Project</Button>
              </div>
            </div>

            {/* Project members */}
            {selectedProject && (
              <div>
                <h5 className="text-sm font-medium mb-2">Project: {selectedProject.name} - Members</h5>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Email</TableHead>
                      <TableHead>Name</TableHead>
                      <TableHead>Role</TableHead>
                      <TableHead className="w-24"></TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {projectMembers.map((m) => (
                      <TableRow key={m.email}>
                        <TableCell className="font-mono text-xs">{m.email}</TableCell>
                        <TableCell className="text-xs">{m.display_name || "-"}</TableCell>
                        <TableCell>
                          <Badge
                            variant={m.role === "project_lead" ? "default" : "outline"}
                            className="cursor-pointer"
                            onClick={() => handleToggleProjRole(m.email, m.role)}
                          >
                            {m.role === "project_lead" ? "Project Lead" : "Member"}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <Button variant="ghost" size="sm" className="text-destructive h-6 text-xs" onClick={() => handleRemoveProjMember(m.email)}>
                            Remove
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
                <div className="flex gap-2 mt-2">
                  <Input
                    placeholder="user@example.com"
                    value={addProjMemberEmail}
                    onChange={(e) => setAddProjMemberEmail(e.target.value)}
                    className="flex-1"
                  />
                  <Select value={addProjMemberRole} onValueChange={setAddProjMemberRole}>
                    <SelectTrigger className="w-32"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="member">Member</SelectItem>
                      <SelectItem value="project_lead">Project Lead</SelectItem>
                    </SelectContent>
                  </Select>
                  <Button size="sm" onClick={handleAddProjMember}>Add</Button>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Create Team Dialog */}
      <Dialog open={creating} onOpenChange={(v) => !v && setCreating(false)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create Team</DialogTitle>
            <DialogDescription>Teams enable delegated approval workflows.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div>
              <Label>Name</Label>
              <Input value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="Engineering" />
            </div>
            <div>
              <Label>Webhook URL (optional)</Label>
              <Input value={newWebhook} onChange={(e) => setNewWebhook(e.target.value)} placeholder="https://hooks.slack.com/..." />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreating(false)}>Cancel</Button>
            <Button onClick={handleCreate}>Create</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Edit Team Dialog */}
      <Dialog open={!!editTeam} onOpenChange={(v) => !v && setEditTeam(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Team</DialogTitle>
            <DialogDescription>Update team name or webhook URL.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div>
              <Label>Name</Label>
              <Input value={editName} onChange={(e) => setEditName(e.target.value)} />
            </div>
            <div>
              <Label>Webhook URL</Label>
              <Input value={editWebhook} onChange={(e) => setEditWebhook(e.target.value)} placeholder="https://hooks.slack.com/..." />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditTeam(null)}>Cancel</Button>
            <Button onClick={handleEdit}>Save</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

// ============================================================================
// Graph Tab
// ============================================================================

function formatNode(node: { connection_name: string; database_name: string; schema_name: string; table_name: string }) {
  const parts = [node.connection_name, node.database_name];
  if (node.schema_name) parts.push(node.schema_name);
  if (node.table_name) parts.push(node.table_name);
  return parts.join(" / ");
}

function GraphTab({ onError }: { onError: (msg: string) => void }) {
  const [edges, setEdges] = useState<GraphEdgeExpanded[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterEdgeType, setFilterEdgeType] = useState("");
  const [filterConnection, setFilterConnection] = useState("");
  const [showCreateEdge, setShowCreateEdge] = useState(false);
  const [connections, setConnections] = useState<ConnectionInfo[]>([]);
  const [seedConnection, setSeedConnection] = useState("");
  const [seeding, setSeeding] = useState(false);
  const [seedResult, setSeedResult] = useState<SeedResult | null>(null);

  const refresh = useCallback(async () => {
    try {
      setLoading(true);
      const [edgeData, connData] = await Promise.all([
        listGraphEdges(filterEdgeType || undefined),
        listConnections(),
      ]);
      setEdges(edgeData);
      setConnections(connData);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [onError, filterEdgeType]);

  useEffect(() => { refresh(); }, [refresh]);

  const handleSeed = async () => {
    try {
      setSeeding(true);
      setSeedResult(null);
      const result = await seedGraph(seedConnection || undefined);
      setSeedResult(result);
      refresh();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setSeeding(false);
    }
  };

  const handleDeleteEdge = async (id: number) => {
    try {
      await deleteGraphEdge(id);
      refresh();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    }
  };

  const filtered = filterConnection
    ? edges.filter(e => e.source.connection_name === filterConnection || e.target.connection_name === filterConnection)
    : edges;

  return (
    <>
      {/* Seed Section */}
      <Card className="mb-4">
        <CardContent className="pt-4">
          <div className="flex items-center gap-3 flex-wrap">
            <Label className="text-sm font-medium">Seed from Foreign Keys:</Label>
            <Select value={seedConnection} onValueChange={setSeedConnection}>
              <SelectTrigger className="w-48"><SelectValue placeholder="All connections" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">All connections</SelectItem>
                {connections.map(c => (
                  <SelectItem key={c.name} value={c.name}>{c.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button size="sm" onClick={handleSeed} disabled={seeding}>
              {seeding ? "Seeding..." : "Seed"}
            </Button>
            {seedResult && (
              <span className="text-sm text-muted-foreground">
                {seedResult.edges_seeded} edges from {seedResult.connections_processed} connection(s)
                {seedResult.errors.length > 0 && (
                  <span className="text-destructive ml-1">({seedResult.errors.length} errors)</span>
                )}
              </span>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Filters + Actions */}
      <div className="flex justify-between items-center mb-3 gap-2 flex-wrap">
        <div className="flex items-center gap-2">
          <Select value={filterEdgeType} onValueChange={setFilterEdgeType}>
            <SelectTrigger className="w-40"><SelectValue placeholder="All types" /></SelectTrigger>
            <SelectContent>
              <SelectItem value="__all__">All types</SelectItem>
              <SelectItem value="join_key">join_key</SelectItem>
              <SelectItem value="derives_from">derives_from</SelectItem>
              <SelectItem value="references">references</SelectItem>
            </SelectContent>
          </Select>
          <Select value={filterConnection} onValueChange={setFilterConnection}>
            <SelectTrigger className="w-40"><SelectValue placeholder="All connections" /></SelectTrigger>
            <SelectContent>
              <SelectItem value="__all__">All connections</SelectItem>
              {connections.map(c => (
                <SelectItem key={c.name} value={c.name}>{c.name}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button variant="outline" size="sm" onClick={refresh}>Refresh</Button>
        </div>
        <Button size="sm" onClick={() => setShowCreateEdge(true)}>+ Add Edge</Button>
      </div>

      {/* Edge Table */}
      <Card>
        {loading ? (
          <CardContent className="py-8 text-center text-muted-foreground">Loading...</CardContent>
        ) : filtered.length === 0 ? (
          <CardContent className="py-8 text-center text-muted-foreground">
            No edges found. Use "Seed" to auto-discover FK relationships or "Add Edge" to create one.
          </CardContent>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Source</TableHead>
                <TableHead>Target</TableHead>
                <TableHead>Columns</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Created By</TableHead>
                <TableHead className="w-16"></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.map(edge => (
                <TableRow key={edge.id}>
                  <TableCell className="text-xs font-mono">{formatNode(edge.source)}</TableCell>
                  <TableCell className="text-xs font-mono">{formatNode(edge.target)}</TableCell>
                  <TableCell className="text-xs font-mono">
                    {edge.source_columns?.join(", ") || "—"}
                    <span className="text-muted-foreground mx-1">&rarr;</span>
                    {edge.target_columns?.join(", ") || "—"}
                  </TableCell>
                  <TableCell><Badge variant="outline">{edge.edge_type}</Badge></TableCell>
                  <TableCell className="text-xs text-muted-foreground">{edge.created_by || "—"}</TableCell>
                  <TableCell>
                    <Button variant="ghost" size="sm" className="text-destructive" onClick={() => handleDeleteEdge(edge.id)}>
                      Delete
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </Card>

      <CreateEdgeDialog
        open={showCreateEdge}
        onClose={() => setShowCreateEdge(false)}
        onCreated={refresh}
        onError={onError}
        connections={connections}
      />
    </>
  );
}

// ============================================================================
// Create Edge Dialog
// ============================================================================

function CascadingTableSelect({
  label,
  connections,
  connection, setConnection,
  database, setDatabase,
  schema, setSchema,
  table, setTable,
}: {
  label: string;
  connections: ConnectionInfo[];
  connection: string; setConnection: (v: string) => void;
  database: string; setDatabase: (v: string) => void;
  schema: string; setSchema: (v: string) => void;
  table: string; setTable: (v: string) => void;
}) {
  const [databases, setDatabases] = useState<DatabaseInfo[]>([]);
  const [schemas, setSchemas] = useState<{ schema_name: string }[]>([]);
  const [tables, setTables] = useState<TableInfo[]>([]);

  useEffect(() => {
    if (!connection) { setDatabases([]); return; }
    listDatabases(connection).then(setDatabases).catch(() => setDatabases([]));
  }, [connection]);

  useEffect(() => {
    if (!database || !connection) { setSchemas([]); return; }
    listSchemas(database, connection).then(setSchemas).catch(() => setSchemas([]));
  }, [database, connection]);

  useEffect(() => {
    if (!database || !connection || !schema) { setTables([]); return; }
    listTables(database, connection, schema).then(setTables).catch(() => setTables([]));
  }, [database, connection, schema]);

  return (
    <div className="space-y-2">
      <Label className="text-sm font-medium">{label}</Label>
      <Select value={connection} onValueChange={(v) => { setConnection(v); setDatabase(""); setSchema(""); setTable(""); }}>
        <SelectTrigger><SelectValue placeholder="Connection" /></SelectTrigger>
        <SelectContent>
          {connections.map(c => <SelectItem key={c.name} value={c.name}>{c.name}</SelectItem>)}
        </SelectContent>
      </Select>
      <Select value={database} onValueChange={(v) => { setDatabase(v); setSchema(""); setTable(""); }} disabled={!connection}>
        <SelectTrigger><SelectValue placeholder="Database" /></SelectTrigger>
        <SelectContent>
          {databases.map(d => <SelectItem key={d.name} value={d.name}>{d.name}</SelectItem>)}
        </SelectContent>
      </Select>
      <Select value={schema} onValueChange={(v) => { setSchema(v); setTable(""); }} disabled={!database}>
        <SelectTrigger><SelectValue placeholder="Schema" /></SelectTrigger>
        <SelectContent>
          {schemas.map(s => <SelectItem key={s.schema_name} value={s.schema_name}>{s.schema_name}</SelectItem>)}
        </SelectContent>
      </Select>
      <Select value={table} onValueChange={setTable} disabled={!schema}>
        <SelectTrigger><SelectValue placeholder="Table" /></SelectTrigger>
        <SelectContent>
          {tables.map(t => <SelectItem key={t.TABLE_NAME} value={t.TABLE_NAME}>{t.TABLE_NAME}</SelectItem>)}
        </SelectContent>
      </Select>
    </div>
  );
}

function CreateEdgeDialog({
  open, onClose, onCreated, onError, connections,
}: {
  open: boolean;
  onClose: () => void;
  onCreated: () => void;
  onError: (msg: string) => void;
  connections: ConnectionInfo[];
}) {
  const [srcConn, setSrcConn] = useState("");
  const [srcDb, setSrcDb] = useState("");
  const [srcSchema, setSrcSchema] = useState("");
  const [srcTable, setSrcTable] = useState("");
  const [tgtConn, setTgtConn] = useState("");
  const [tgtDb, setTgtDb] = useState("");
  const [tgtSchema, setTgtSchema] = useState("");
  const [tgtTable, setTgtTable] = useState("");
  const [srcCols, setSrcCols] = useState("");
  const [tgtCols, setTgtCols] = useState("");
  const [edgeType, setEdgeType] = useState("join_key");
  const [submitting, setSubmitting] = useState(false);

  const reset = () => {
    setSrcConn(""); setSrcDb(""); setSrcSchema(""); setSrcTable("");
    setTgtConn(""); setTgtDb(""); setTgtSchema(""); setTgtTable("");
    setSrcCols(""); setTgtCols(""); setEdgeType("join_key");
  };

  const handleSubmit = async () => {
    if (!srcConn || !srcDb || !tgtConn || !tgtDb) {
      onError("Source and target connection + database are required");
      return;
    }
    try {
      setSubmitting(true);
      const data: CreateEdgeData = {
        source_connection: srcConn,
        source_database: srcDb,
        source_schema: srcSchema || undefined,
        source_table: srcTable || undefined,
        target_connection: tgtConn,
        target_database: tgtDb,
        target_schema: tgtSchema || undefined,
        target_table: tgtTable || undefined,
        edge_type: edgeType,
        source_columns: srcCols || undefined,
        target_columns: tgtCols || undefined,
      };
      await createGraphEdge(data);
      reset();
      onClose();
      onCreated();
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(v) => { if (!v) { reset(); onClose(); } }}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Create Graph Edge</DialogTitle>
          <DialogDescription>Define a relationship between two tables, optionally across different connections.</DialogDescription>
        </DialogHeader>
        <div className="grid grid-cols-2 gap-4 py-2">
          <CascadingTableSelect
            label="Source"
            connections={connections}
            connection={srcConn} setConnection={setSrcConn}
            database={srcDb} setDatabase={setSrcDb}
            schema={srcSchema} setSchema={setSrcSchema}
            table={srcTable} setTable={setSrcTable}
          />
          <CascadingTableSelect
            label="Target"
            connections={connections}
            connection={tgtConn} setConnection={setTgtConn}
            database={tgtDb} setDatabase={setTgtDb}
            schema={tgtSchema} setSchema={setTgtSchema}
            table={tgtTable} setTable={setTgtTable}
          />
        </div>
        <div className="grid grid-cols-3 gap-3 py-2">
          <div>
            <Label className="text-xs">Source Columns</Label>
            <Input placeholder="e.g. user_id" value={srcCols} onChange={e => setSrcCols(e.target.value)} />
          </div>
          <div>
            <Label className="text-xs">Target Columns</Label>
            <Input placeholder="e.g. customer_id" value={tgtCols} onChange={e => setTgtCols(e.target.value)} />
          </div>
          <div>
            <Label className="text-xs">Edge Type</Label>
            <Select value={edgeType} onValueChange={setEdgeType}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="join_key">join_key</SelectItem>
                <SelectItem value="derives_from">derives_from</SelectItem>
                <SelectItem value="references">references</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => { reset(); onClose(); }}>Cancel</Button>
          <Button onClick={handleSubmit} disabled={submitting || !srcConn || !srcDb || !tgtConn || !tgtDb}>
            {submitting ? "Creating..." : "Create Edge"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
