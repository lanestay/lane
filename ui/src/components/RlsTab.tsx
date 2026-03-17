import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  listRlsPolicies,
  getRlsStatus,
  generateRlsSql,
  executeQuery,
} from "../lib/api";
import type { RlsPolicyInfo, RlsStatus, GenerateRlsSqlRequest } from "../lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogDescription,
} from "@/components/ui/dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

interface Props {
  database: string;
  schema: string;
  table: string;
  connection: string;
  connectionType: string;
}

export default function RlsTab({ database, schema, table, connection, connectionType }: Props) {
  const navigate = useNavigate();
  const isPostgres = connectionType === "postgres";
  const isMssql = connectionType === "mssql";

  const [policies, setPolicies] = useState<RlsPolicyInfo[]>([]);
  const [status, setStatus] = useState<RlsStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // SQL preview dialog
  const [sqlPreview, setSqlPreview] = useState<string | null>(null);
  const [sqlExecuting, setSqlExecuting] = useState(false);
  const [sqlResult, setSqlResult] = useState<{ success: boolean; message: string } | null>(null);

  // Create policy dialog
  const [showCreate, setShowCreate] = useState(false);
  const [createForm, setCreateForm] = useState<GenerateRlsSqlRequest>({
    policy_name: "",
    command: "ALL",
    permissive: "true",
    roles: "PUBLIC",
    using_expr: "",
    with_check_expr: "",
    predicate_type: "FILTER",
    predicate_function: "",
    predicate_args: "",
  });
  const [createSql, setCreateSql] = useState<string | null>(null);

  const conn = connection || undefined;

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [policiesRes, statusRes] = await Promise.allSettled([
        listRlsPolicies(database, table, conn, schema),
        getRlsStatus(database, table, conn, schema),
      ]);
      if (policiesRes.status === "fulfilled") setPolicies(policiesRes.value);
      if (statusRes.status === "fulfilled") setStatus(statusRes.value);
      if (policiesRes.status === "rejected" && statusRes.status === "rejected") {
        setError(policiesRes.reason?.message || "Failed to load RLS data");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load RLS data");
    } finally {
      setLoading(false);
    }
  }, [database, table, conn, schema]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleGenerateAndPreview = async (action: string, body: GenerateRlsSqlRequest) => {
    try {
      const result = await generateRlsSql(database, table, action, body, conn, schema);
      setSqlPreview(result.sql);
      setSqlResult(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to generate SQL");
    }
  };

  const handleExecuteSql = async (sql: string) => {
    setSqlExecuting(true);
    setSqlResult(null);
    try {
      await executeQuery(sql, database, conn);
      setSqlResult({ success: true, message: "Executed successfully" });
      // Refresh data after successful execution
      await fetchData();
    } catch (e) {
      setSqlResult({ success: false, message: e instanceof Error ? e.message : "Execution failed" });
    } finally {
      setSqlExecuting(false);
    }
  };

  const openInEditor = (sql: string) => {
    navigate("/", { state: { prefillQuery: sql, database, connection } });
  };

  const handleToggleRls = (action: string) => {
    handleGenerateAndPreview(action, {});
  };

  const handleDropPolicy = (policyName: string) => {
    handleGenerateAndPreview("drop_policy", { policy_name: policyName });
  };

  const handleMssqlTogglePolicy = (policyName: string, enable: boolean) => {
    handleGenerateAndPreview(enable ? "enable_policy" : "disable_policy", { policy_name: policyName });
  };

  const handlePreviewCreate = async () => {
    try {
      const result = await generateRlsSql(database, table, "create_policy", createForm, conn, schema);
      setCreateSql(result.sql);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to generate SQL");
    }
  };

  const handleExecuteCreate = async () => {
    if (!createSql) return;
    setSqlExecuting(true);
    setSqlResult(null);
    try {
      await executeQuery(createSql, database, conn);
      setSqlResult({ success: true, message: "Policy created successfully" });
      setShowCreate(false);
      setCreateSql(null);
      setCreateForm({
        policy_name: "",
        command: "ALL",
        permissive: "true",
        roles: "PUBLIC",
        using_expr: "",
        with_check_expr: "",
        predicate_type: "FILTER",
        predicate_function: "",
        predicate_args: "",
      });
      await fetchData();
    } catch (e) {
      setSqlResult({ success: false, message: e instanceof Error ? e.message : "Execution failed" });
    } finally {
      setSqlExecuting(false);
    }
  };

  if (loading) {
    return <div className="text-muted-foreground text-sm py-4">Loading RLS data...</div>;
  }

  return (
    <div className="flex flex-col gap-4">
      {error && (
        <div className="bg-destructive/10 text-destructive px-3 py-2 rounded text-sm">{error}</div>
      )}

      {/* Status Bar */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">RLS Status</CardTitle>
        </CardHeader>
        <CardContent>
          {isPostgres && status && (
            <div className="flex items-center gap-6">
              <div className="flex items-center gap-2">
                <Switch
                  checked={status.rls_enabled}
                  onCheckedChange={(checked) =>
                    handleToggleRls(checked ? "enable_rls" : "disable_rls")
                  }
                />
                <Label className="text-sm">RLS Enabled</Label>
              </div>
              <div className="flex items-center gap-2">
                <Switch
                  checked={status.rls_forced ?? false}
                  onCheckedChange={(checked) =>
                    handleToggleRls(checked ? "force_rls" : "no_force_rls")
                  }
                />
                <Label className="text-sm">Force RLS (table owner)</Label>
              </div>
              <Badge variant={status.rls_enabled ? "default" : "secondary"}>
                {policies.length} {policies.length === 1 ? "policy" : "policies"}
              </Badge>
            </div>
          )}
          {isMssql && status && (
            <div className="flex items-center gap-4">
              <Badge variant={status.rls_enabled ? "default" : "secondary"}>
                {status.policy_count ?? 0} {(status.policy_count ?? 0) === 1 ? "policy" : "policies"}
              </Badge>
              <Badge variant="outline">
                {status.enabled_count ?? 0} enabled
              </Badge>
            </div>
          )}
          {!status && (
            <div className="text-muted-foreground text-sm">No RLS information available</div>
          )}
        </CardContent>
      </Card>

      {/* Policies Table */}
      <Card>
        <CardHeader className="pb-2 flex flex-row items-center justify-between">
          <CardTitle className="text-sm font-medium">Policies</CardTitle>
          <Button size="sm" onClick={() => setShowCreate(true)}>
            Create Policy
          </Button>
        </CardHeader>
        <CardContent>
          {policies.length === 0 ? (
            <div className="text-muted-foreground text-sm py-2">No policies defined</div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  {isPostgres && (
                    <>
                      <TableHead>Command</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Roles</TableHead>
                      <TableHead>USING</TableHead>
                      <TableHead>WITH CHECK</TableHead>
                    </>
                  )}
                  {isMssql && (
                    <>
                      <TableHead>Enabled</TableHead>
                      <TableHead>Predicate Type</TableHead>
                      <TableHead>Definition</TableHead>
                      <TableHead>Operation</TableHead>
                    </>
                  )}
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {policies.map((policy, i) => (
                  <TableRow key={`${policy.policy_name}-${i}`}>
                    <TableCell className="font-mono text-xs">{policy.policy_name}</TableCell>
                    {isPostgres && (
                      <>
                        <TableCell>{policy.command}</TableCell>
                        <TableCell>
                          <Badge variant={policy.is_permissive ? "default" : "secondary"} className="text-xs">
                            {policy.is_permissive ? "Permissive" : "Restrictive"}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-xs">{policy.roles || "PUBLIC"}</TableCell>
                        <TableCell className="font-mono text-xs max-w-[200px] truncate" title={policy.using_expr}>
                          {policy.using_expr || "—"}
                        </TableCell>
                        <TableCell className="font-mono text-xs max-w-[200px] truncate" title={policy.with_check_expr}>
                          {policy.with_check_expr || "—"}
                        </TableCell>
                      </>
                    )}
                    {isMssql && (
                      <>
                        <TableCell>
                          <Badge variant={policy.is_enabled ? "default" : "secondary"} className="text-xs">
                            {policy.is_enabled ? "Yes" : "No"}
                          </Badge>
                        </TableCell>
                        <TableCell>{policy.predicate_type}</TableCell>
                        <TableCell className="font-mono text-xs max-w-[200px] truncate" title={policy.predicate_definition}>
                          {policy.predicate_definition || "—"}
                        </TableCell>
                        <TableCell>{policy.operation}</TableCell>
                      </>
                    )}
                    <TableCell className="text-right">
                      <div className="flex gap-1 justify-end">
                        {isMssql && (
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => handleMssqlTogglePolicy(policy.policy_name, !policy.is_enabled)}
                          >
                            {policy.is_enabled ? "Disable" : "Enable"}
                          </Button>
                        )}
                        <Button
                          size="sm"
                          variant="destructive"
                          onClick={() => handleDropPolicy(policy.policy_name)}
                        >
                          Drop
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* SQL Preview Dialog */}
      <Dialog open={sqlPreview !== null} onOpenChange={(open) => { if (!open) { setSqlPreview(null); setSqlResult(null); } }}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>SQL Preview</DialogTitle>
            <DialogDescription>Review the generated SQL before executing.</DialogDescription>
          </DialogHeader>
          <pre className="bg-muted p-3 rounded text-sm font-mono whitespace-pre-wrap overflow-auto max-h-[300px]">
            {sqlPreview}
          </pre>
          {sqlResult && (
            <div className={`px-3 py-2 rounded text-sm ${sqlResult.success ? "bg-green-500/10 text-green-700 dark:text-green-400" : "bg-destructive/10 text-destructive"}`}>
              {sqlResult.message}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => sqlPreview && openInEditor(sqlPreview)}>
              Open in SQL Editor
            </Button>
            <Button
              onClick={() => sqlPreview && handleExecuteSql(sqlPreview)}
              disabled={sqlExecuting || sqlResult?.success === true}
            >
              {sqlExecuting ? "Executing..." : "Execute"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Create Policy Dialog */}
      <Dialog open={showCreate} onOpenChange={(open) => { if (!open) { setShowCreate(false); setCreateSql(null); setSqlResult(null); } }}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Create {isPostgres ? "RLS" : "Security"} Policy</DialogTitle>
            <DialogDescription>
              {isPostgres
                ? "Define a row-level security policy for this table."
                : "Define a security policy with filter/block predicates."}
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-3">
            <div className="grid gap-1">
              <Label htmlFor="policy_name">Policy Name</Label>
              <Input
                id="policy_name"
                value={createForm.policy_name}
                onChange={(e) => setCreateForm({ ...createForm, policy_name: e.target.value })}
                placeholder="my_policy"
              />
            </div>
            {isPostgres && (
              <>
                <div className="grid grid-cols-2 gap-3">
                  <div className="grid gap-1">
                    <Label>Command</Label>
                    <Select value={createForm.command} onValueChange={(v) => setCreateForm({ ...createForm, command: v })}>
                      <SelectTrigger><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="ALL">ALL</SelectItem>
                        <SelectItem value="SELECT">SELECT</SelectItem>
                        <SelectItem value="INSERT">INSERT</SelectItem>
                        <SelectItem value="UPDATE">UPDATE</SelectItem>
                        <SelectItem value="DELETE">DELETE</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="grid gap-1">
                    <Label>Type</Label>
                    <Select value={createForm.permissive} onValueChange={(v) => setCreateForm({ ...createForm, permissive: v })}>
                      <SelectTrigger><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="true">Permissive</SelectItem>
                        <SelectItem value="false">Restrictive</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>
                <div className="grid gap-1">
                  <Label htmlFor="roles">Roles</Label>
                  <Input
                    id="roles"
                    value={createForm.roles}
                    onChange={(e) => setCreateForm({ ...createForm, roles: e.target.value })}
                    placeholder="PUBLIC"
                  />
                </div>
                <div className="grid gap-1">
                  <Label htmlFor="using_expr">USING Expression</Label>
                  <Textarea
                    id="using_expr"
                    value={createForm.using_expr}
                    onChange={(e) => setCreateForm({ ...createForm, using_expr: e.target.value })}
                    placeholder="e.g. tenant_id = current_setting('app.tenant_id')::int"
                    rows={2}
                  />
                </div>
                <div className="grid gap-1">
                  <Label htmlFor="with_check_expr">WITH CHECK Expression</Label>
                  <Textarea
                    id="with_check_expr"
                    value={createForm.with_check_expr}
                    onChange={(e) => setCreateForm({ ...createForm, with_check_expr: e.target.value })}
                    placeholder="e.g. tenant_id = current_setting('app.tenant_id')::int"
                    rows={2}
                  />
                </div>
              </>
            )}
            {isMssql && (
              <>
                <div className="grid gap-1">
                  <Label>Predicate Type</Label>
                  <Select value={createForm.predicate_type} onValueChange={(v) => setCreateForm({ ...createForm, predicate_type: v })}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="FILTER">FILTER</SelectItem>
                      <SelectItem value="BLOCK">BLOCK</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="grid gap-1">
                  <Label htmlFor="predicate_function">Predicate Function</Label>
                  <Input
                    id="predicate_function"
                    value={createForm.predicate_function}
                    onChange={(e) => setCreateForm({ ...createForm, predicate_function: e.target.value })}
                    placeholder="e.g. dbo.fn_security_predicate"
                  />
                </div>
                <div className="grid gap-1">
                  <Label htmlFor="predicate_args">Predicate Arguments</Label>
                  <Input
                    id="predicate_args"
                    value={createForm.predicate_args}
                    onChange={(e) => setCreateForm({ ...createForm, predicate_args: e.target.value })}
                    placeholder="e.g. @TenantId, TenantId"
                  />
                </div>
              </>
            )}
          </div>

          {createSql && (
            <pre className="bg-muted p-3 rounded text-sm font-mono whitespace-pre-wrap overflow-auto max-h-[200px]">
              {createSql}
            </pre>
          )}
          {sqlResult && (
            <div className={`px-3 py-2 rounded text-sm ${sqlResult.success ? "bg-green-500/10 text-green-700 dark:text-green-400" : "bg-destructive/10 text-destructive"}`}>
              {sqlResult.message}
            </div>
          )}

          <DialogFooter>
            {!createSql ? (
              <Button onClick={handlePreviewCreate} disabled={!createForm.policy_name}>
                Preview SQL
              </Button>
            ) : (
              <>
                <Button variant="outline" onClick={() => openInEditor(createSql)}>
                  Open in SQL Editor
                </Button>
                <Button onClick={handleExecuteCreate} disabled={sqlExecuting}>
                  {sqlExecuting ? "Executing..." : "Execute"}
                </Button>
              </>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
