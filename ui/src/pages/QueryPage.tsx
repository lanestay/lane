import { useState, useRef, useEffect, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import ConnectionPicker from "../components/ConnectionPicker";
import ResultsTable, { ErrorBanner } from "../components/ResultsTable";
import ChartView from "../components/ChartView";
import SqlEditor from "../components/SqlEditor";
import type { SqlEditorHandle } from "../components/SqlEditor";
import TemplatePicker from "../components/TemplatePicker";
import QueryHistory from "../components/QueryHistory";
import { executeQuery, createEndpoint, getActiveStorageColumnLinks } from "../lib/api";
import { useAuth } from "../lib/auth";
import type { QueryResult, StorageColumnLink, EndpointParam } from "../lib/api";
import ExportButton from "../components/ExportButton";
import { loadSchema, clearSchema, createCompletionSource } from "../lib/schema-cache";
import type { CompletionSource } from "@codemirror/autocomplete";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter,
} from "@/components/ui/dialog";

function toSlug(sql: string): string {
  const words = sql.replace(/[^a-zA-Z0-9\s]/g, "").trim().split(/\s+/).slice(0, 5);
  return words.join("-").toLowerCase().substring(0, 40) || "endpoint";
}

function detectParams(sql: string): EndpointParam[] {
  const re = /\{\{(\w+)\}\}/g;
  const seen = new Set<string>();
  const params: EndpointParam[] = [];
  let m;
  while ((m = re.exec(sql)) !== null) {
    if (!seen.has(m[1])) {
      seen.add(m[1]);
      params.push({ name: m[1], type: "string" });
    }
  }
  return params;
}

function SaveEndpointDialog({
  open,
  onClose,
  query,
  connection,
  database,
}: {
  open: boolean;
  onClose: () => void;
  query: string;
  connection: string;
  database: string;
}) {
  const [name, setName] = useState(() => toSlug(query));
  const [description, setDescription] = useState("");
  const [params, setParams] = useState<EndpointParam[]>(() => detectParams(query));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (open) {
      setName(toSlug(query));
      setParams(detectParams(query));
      setDescription("");
      setError(null);
    }
  }, [open, query]);

  const handleSave = async () => {
    if (!name.trim()) { setError("Name is required"); return; }
    setSaving(true);
    setError(null);
    try {
      await createEndpoint({
        name: name.trim(),
        connection_name: connection,
        database_name: database,
        query,
        description: description || undefined,
        parameters: params.length > 0 ? JSON.stringify(params) : undefined,
      });
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setSaving(false);
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-lg">
        <DialogHeader><DialogTitle>Save as Endpoint</DialogTitle></DialogHeader>
        <div className="space-y-4 py-2 max-h-[60vh] overflow-y-auto">
          <div>
            <Label>Name (slug)</Label>
            <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="my-endpoint" />
          </div>
          <div>
            <Label>Description</Label>
            <Input value={description} onChange={(e) => setDescription(e.target.value)} placeholder="Optional description" />
          </div>
          <div>
            <Label>Connection</Label>
            <Input value={connection} disabled />
          </div>
          <div>
            <Label>Database</Label>
            <Input value={database} disabled />
          </div>
          {params.length > 0 && (
            <div>
              <Label>Parameters (detected from {"{{...}}"} in query)</Label>
              <div className="mt-2 space-y-2">
                {params.map((p, i) => (
                  <div key={p.name} className="flex items-center gap-2">
                    <Input value={p.name} disabled className="w-32" />
                    <Input
                      placeholder="default value"
                      value={p.default || ""}
                      onChange={(e) => {
                        const next = [...params];
                        next[i] = { ...next[i], default: e.target.value || undefined };
                        setParams(next);
                      }}
                      className="flex-1"
                    />
                  </div>
                ))}
              </div>
            </div>
          )}
          {error && <p className="text-sm text-destructive">{error}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={handleSave} disabled={saving}>{saving ? "Saving..." : "Save"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default function QueryPage() {
  const [connection, setConnection] = useState("");
  const [database, setDatabase] = useState("");
  const [connectionType, setConnectionType] = useState<string>("mssql");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [lastQuery, setLastQuery] = useState("");
  const [showHistory, setShowHistory] = useState(false);
  const [historyTrigger, setHistoryTrigger] = useState(0);
  const [completionSource, setCompletionSource] = useState<CompletionSource | null>(null);
  const [storageLinks, setStorageLinks] = useState<StorageColumnLink[]>([]);
  const [showSaveEndpoint, setShowSaveEndpoint] = useState(false);
  const { user } = useAuth();
  const isAdmin = user?.is_admin ?? false;
  const editorRef = useRef<SqlEditorHandle>(null);
  const [searchParams] = useSearchParams();

  // Deep-link: pre-fill SQL from ?sql= parameter
  useEffect(() => {
    const sql = searchParams.get("sql");
    if (sql && editorRef.current) {
      editorRef.current.replaceAll(sql);
    }
  }, [searchParams]);

  // Fetch storage column links when connection/database changes
  useEffect(() => {
    if (connection && database) {
      getActiveStorageColumnLinks({ connection, database })
        .then(setStorageLinks)
        .catch(() => setStorageLinks([]));
    } else {
      setStorageLinks([]);
    }
  }, [connection, database]);

  const run = useCallback(async () => {
    const sql = editorRef.current?.getValue() ?? "";
    if (!sql.trim()) {
      setError("Query cannot be empty");
      return;
    }
    setLoading(true);
    setError(null);
    setResult(null);
    setLastQuery(sql);
    try {
      const res = await executeQuery(sql, database, connection || undefined);
      setResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setLoading(false);
    setHistoryTrigger((n) => n + 1);
  }, [connection, database]);

  // Global Ctrl/Cmd+Enter handler (works even when CodeMirror has focus)
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
        e.preventDefault();
        run();
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [run]);

  // Load schema for autocomplete when connection/database changes
  useEffect(() => {
    if (!connection || !database) {
      setCompletionSource(null);
      return;
    }
    clearSchema(connection, database);
    loadSchema(connection, database).then(() => {
      setCompletionSource(() => createCompletionSource(connection, database));
    });
  }, [connection, database]);

  const handleTemplateSelect = (sql: string) => {
    editorRef.current?.replaceAll(sql);
  };

  const handleHistorySelect = (sql: string) => {
    editorRef.current?.replaceAll(sql);
  };

  return (
    <div className="h-full flex flex-col p-4 gap-4">
      <div className="flex items-center justify-between gap-3">
        <ConnectionPicker
          connection={connection}
          database={database}
          onConnectionChange={(name, defaultDb, connType) => {
            setConnection(name);
            setDatabase(defaultDb);
            setConnectionType(connType);
          }}
          onDatabaseChange={setDatabase}
        />
        <div className="flex items-center gap-2">
          <TemplatePicker dialect={connectionType} onSelect={handleTemplateSelect} />
          <Button
            variant={showHistory ? "secondary" : "outline"}
            size="sm"
            onClick={() => setShowHistory((v) => !v)}
          >
            History
          </Button>
          <Button disabled={loading} onClick={run}>
            {loading ? "Running..." : "Run (Ctrl+Enter)"}
          </Button>
        </div>
      </div>
      <SqlEditor
        ref={editorRef}
        dialect={connectionType}
        completionSource={completionSource}
        onExecute={run}
        placeholder="SELECT * FROM ..."
      />
      {showHistory && (
        <QueryHistory
          onSelect={handleHistorySelect}
          refreshTrigger={historyTrigger}
        />
      )}
      <div className="flex-1 overflow-auto">
        {error ? (
          <ErrorBanner message={error} />
        ) : result ? (
          <Tabs defaultValue="table" className="h-full">
            {/* Shared header bar */}
            <div className="flex items-center justify-between px-4 py-2 bg-card border border-border rounded-t-lg">
              <div className="flex items-center gap-3">
                <TabsList className="h-7">
                  <TabsTrigger value="table" className="text-xs px-3 py-1">
                    Table
                  </TabsTrigger>
                  <TabsTrigger value="chart" className="text-xs px-3 py-1">
                    Chart
                  </TabsTrigger>
                </TabsList>
                <Badge variant="secondary" className="text-green-400">
                  {result.total_rows} rows
                </Badge>
                <span className="text-muted-foreground text-sm">
                  {result.execution_time_ms}ms
                </span>
                <span className="text-muted-foreground text-sm">
                  {result.rows_per_second.toFixed(0)} rows/sec
                </span>
              </div>
              <div className="flex items-center gap-2">
                {isAdmin && (
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setShowSaveEndpoint(true)}
                  >
                    Save as Endpoint
                  </Button>
                )}
                <ExportButton
                  query={lastQuery}
                  database={database}
                  connection={connection || undefined}
                  result={result}
                />
              </div>
            </div>
            <TabsContent value="table" className="mt-0">
              <ResultsTable
                result={result}
                storageLinks={storageLinks}
                connection={connection || undefined}
                database={database || undefined}
                showHeader={false}
              />
            </TabsContent>
            <TabsContent value="chart" className="mt-0 border border-t-0 border-border rounded-b-lg p-4">
              <ChartView result={result} />
            </TabsContent>
          </Tabs>
        ) : (
          <div className="text-muted-foreground text-sm text-center py-8">
            Run a query to see results
          </div>
        )}
      </div>
      <SaveEndpointDialog
        open={showSaveEndpoint}
        onClose={() => setShowSaveEndpoint(false)}
        query={lastQuery}
        connection={connection}
        database={database}
      />
    </div>
  );
}
