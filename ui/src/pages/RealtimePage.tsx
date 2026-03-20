import { useState, useEffect, useRef, useCallback } from "react";
import {
  listRealtimeTables, enableRealtime, disableRealtime,
  listConnections, listDatabases, listTables,
  listRealtimeWebhooks, createRealtimeWebhook, updateRealtimeWebhook, deleteRealtimeWebhook,
} from "../lib/api";
import type { RealtimeTableEntry, RealtimeEvent, ConnectionInfo, RealtimeWebhook } from "../lib/api";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import {
  Drawer, DrawerContent, DrawerHeader, DrawerTitle, DrawerDescription,
} from "@/components/ui/drawer";

function getSessionToken(): string {
  return localStorage.getItem("session_token") ?? "";
}

// ============================================================================
// Realtime Page
// ============================================================================

export default function RealtimePage() {
  const [error, setError] = useState<string | null>(null);
  const [tables, setTables] = useState<RealtimeTableEntry[]>([]);
  const [connections, setConnections] = useState<ConnectionInfo[]>([]);
  const [loading, setLoading] = useState(true);

  // Add form state
  const [addConn, setAddConn] = useState("");
  const [addDb, setAddDb] = useState("");
  const [addTable, setAddTable] = useState("");
  const [databases, setDatabases] = useState<string[]>([]);
  const [dbTables, setDbTables] = useState<string[]>([]);
  const [adding, setAdding] = useState(false);

  // Live stream state
  const [watchEntry, setWatchEntry] = useState<RealtimeTableEntry | null>(null);
  const [events, setEvents] = useState<RealtimeEvent[]>([]);
  const [showFull, setShowFull] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);

  // Webhooks state
  const [webhooks, setWebhooks] = useState<RealtimeWebhook[]>([]);
  const [showWebhookForm, setShowWebhookForm] = useState(false);
  const [webhookConn, setWebhookConn] = useState("");
  const [webhookDb, setWebhookDb] = useState("");
  const [webhookTable, setWebhookTable] = useState("");
  const [webhookUrl, setWebhookUrl] = useState("");
  const [webhookEvents, setWebhookEvents] = useState<string[]>(["INSERT", "UPDATE", "DELETE"]);
  const [webhookSecret, setWebhookSecret] = useState("");
  const [webhookUseSecret, setWebhookUseSecret] = useState(false);
  const [addingWebhook, setAddingWebhook] = useState(false);
  const [revealedSecrets, setRevealedSecrets] = useState<Set<number>>(new Set());
  const [webhookDbs, setWebhookDbs] = useState<string[]>([]);
  const [webhookTables, setWebhookTables] = useState<string[]>([]);

  const refresh = useCallback(async () => {
    try {
      const [t, c, w] = await Promise.all([
        listRealtimeTables(),
        listConnections(),
        listRealtimeWebhooks(),
      ]);
      setTables(t);
      setConnections(c);
      setWebhooks(w);
      if (!addConn && c.length > 0) {
        const def = c.find((x) => x.is_default) ?? c[0];
        setAddConn(def.name);
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [addConn]);

  useEffect(() => { refresh(); }, [refresh]);

  // Load databases when connection changes
  useEffect(() => {
    if (!addConn) return;
    listDatabases(addConn).then((dbs) => {
      setDatabases(dbs.map((d) => d.name));
      setAddDb("");
      setDbTables([]);
      setAddTable("");
    }).catch(() => {});
  }, [addConn]);

  // Load tables when database changes
  useEffect(() => {
    if (!addConn || !addDb) { setDbTables([]); setAddTable(""); return; }
    listTables(addDb, addConn).then((tbls) => {
      setDbTables(tbls.map((t) => t.TABLE_NAME));
      setAddTable("");
    }).catch(() => {});
  }, [addConn, addDb]);

  // Load databases for webhook form connection
  useEffect(() => {
    if (!webhookConn) { setWebhookDbs([]); return; }
    listDatabases(webhookConn).then((dbs) => {
      setWebhookDbs(dbs.map((d) => d.name));
      setWebhookDb("");
      setWebhookTables([]);
      setWebhookTable("");
    }).catch(() => {});
  }, [webhookConn]);

  // Load tables for webhook form database
  useEffect(() => {
    if (!webhookConn || !webhookDb) { setWebhookTables([]); setWebhookTable(""); return; }
    listTables(webhookDb, webhookConn).then((tbls) => {
      setWebhookTables(tbls.map((t) => t.TABLE_NAME));
      setWebhookTable("");
    }).catch(() => {});
  }, [webhookConn, webhookDb]);

  const handleEnable = async () => {
    if (!addConn || !addDb || !addTable) return;
    setAdding(true);
    try {
      await enableRealtime(addConn, addDb, addTable);
      setAddTable("");
      await refresh();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setAdding(false);
    }
  };

  const handleDisable = async (entry: RealtimeTableEntry) => {
    try {
      await disableRealtime(entry.connection_name, entry.database_name, entry.table_name);
      if (watchEntry && entryKey(watchEntry) === entryKey(entry)) stopWatching();
      await refresh();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const entryKey = (e: RealtimeTableEntry) =>
    `${e.connection_name}/${e.database_name}/${e.table_name}`;

  const startWatching = (entry: RealtimeTableEntry) => {
    stopWatching();
    setWatchEntry(entry);
    setEvents([]);

    const params = new URLSearchParams({
      connection: entry.connection_name,
      database: entry.database_name,
      table: entry.table_name,
      token: getSessionToken(),
    });
    const es = new EventSource(`/api/lane/realtime/subscribe?${params}`);
    es.addEventListener("change", (e) => {
      try {
        const data: RealtimeEvent = JSON.parse(e.data);
        setEvents((prev) => [data, ...prev].slice(0, 200));
      } catch { /* ignore */ }
    });
    es.onerror = () => {};
    eventSourceRef.current = es;
  };

  const stopWatching = () => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    setWatchEntry(null);
  };

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) eventSourceRef.current.close();
    };
  }, []);

  // Webhook handlers
  const handleCreateWebhook = async () => {
    if (!webhookConn || !webhookDb || !webhookTable || !webhookUrl || webhookEvents.length === 0) return;
    setAddingWebhook(true);
    try {
      await createRealtimeWebhook(
        webhookConn, webhookDb, webhookTable, webhookUrl, webhookEvents,
        webhookUseSecret ? webhookSecret : undefined,
      );
      setShowWebhookForm(false);
      setWebhookUrl("");
      setWebhookSecret("");
      setWebhookUseSecret(false);
      setWebhookEvents(["INSERT", "UPDATE", "DELETE"]);
      await refresh();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setAddingWebhook(false);
    }
  };

  const handleToggleWebhook = async (hook: RealtimeWebhook) => {
    try {
      await updateRealtimeWebhook(hook.id, { is_enabled: !hook.is_enabled });
      await refresh();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleDeleteWebhook = async (id: number) => {
    try {
      await deleteRealtimeWebhook(id);
      setRevealedSecrets((prev) => { const next = new Set(prev); next.delete(id); return next; });
      await refresh();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const toggleRevealSecret = (id: number) => {
    setRevealedSecrets((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  const toggleWebhookEvent = (evt: string) => {
    setWebhookEvents((prev) =>
      prev.includes(evt) ? prev.filter((e) => e !== evt) : [...prev, evt]
    );
  };

  if (loading) return <div className="p-4 text-muted-foreground">Loading...</div>;

  const watchKey = watchEntry ? entryKey(watchEntry) : null;

  return (
    <div className="h-full flex flex-col p-4 gap-4">
      <h2 className="text-xl font-bold">Realtime Monitoring</h2>
      {error && (
        <div className="bg-destructive/20 border border-destructive text-destructive px-4 py-2 rounded-md text-sm">
          {error}
          <button className="ml-2 underline" onClick={() => setError(null)}>dismiss</button>
        </div>
      )}

      {/* Enable form */}
      <Card>
        <CardHeader>
          <p className="text-sm font-medium">Enable Realtime Monitoring on a Table</p>
        </CardHeader>
        <CardContent>
          <div className="flex gap-2 items-end flex-wrap">
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">Connection</label>
              <Select value={addConn} onValueChange={setAddConn}>
                <SelectTrigger className="w-40"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {connections.map((c) => (
                    <SelectItem key={c.name} value={c.name}>{c.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">Database</label>
              <Select value={addDb} onValueChange={setAddDb}>
                <SelectTrigger className="w-44"><SelectValue placeholder="Select..." /></SelectTrigger>
                <SelectContent>
                  {databases.map((d) => (
                    <SelectItem key={d} value={d}>{d}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">Table</label>
              {dbTables.length > 0 ? (
                <Select value={addTable} onValueChange={setAddTable}>
                  <SelectTrigger className="w-48"><SelectValue placeholder="Select..." /></SelectTrigger>
                  <SelectContent>
                    {dbTables.map((t) => (
                      <SelectItem key={t} value={t}>{t}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              ) : (
                <Input
                  className="w-48"
                  placeholder="Table name"
                  value={addTable}
                  onChange={(e) => setAddTable(e.target.value)}
                />
              )}
            </div>
            <Button
              onClick={handleEnable}
              disabled={adding || !addConn || !addDb || !addTable}
              size="sm"
            >
              {adding ? "Enabling..." : "Enable"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Enabled tables */}
      <Card>
        <CardHeader>
          <p className="text-sm font-medium">Enabled Tables ({tables.length})</p>
        </CardHeader>
        <CardContent>
          {tables.length === 0 ? (
            <p className="text-sm text-muted-foreground">No tables have realtime enabled yet.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Connection</TableHead>
                  <TableHead>Database</TableHead>
                  <TableHead>Table</TableHead>
                  <TableHead>Enabled</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {tables.map((entry) => {
                  const key = entryKey(entry);
                  const isWatching = watchKey === key;
                  return (
                    <TableRow key={key}>
                      <TableCell><Badge variant="outline">{entry.connection_name}</Badge></TableCell>
                      <TableCell>{entry.database_name}</TableCell>
                      <TableCell className="font-mono">{entry.table_name}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{entry.created_at}</TableCell>
                      <TableCell className="text-right space-x-2">
                        <Button
                          size="sm"
                          variant={isWatching ? "default" : "outline"}
                          onClick={() => isWatching ? stopWatching() : startWatching(entry)}
                        >
                          {isWatching ? "Stop" : "Watch"}
                        </Button>
                        <Button
                          size="sm"
                          variant="destructive"
                          onClick={() => handleDisable(entry)}
                        >
                          Disable
                        </Button>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* Webhooks */}
      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <p className="text-sm font-medium">Webhooks ({webhooks.length})</p>
          <Button size="sm" variant="outline" onClick={() => {
            setShowWebhookForm(!showWebhookForm);
            if (!webhookConn && connections.length > 0) {
              const def = connections.find((x) => x.is_default) ?? connections[0];
              setWebhookConn(def.name);
            }
          }}>
            {showWebhookForm ? "Cancel" : "Add Webhook"}
          </Button>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Create webhook form */}
          {showWebhookForm && (
            <div className="border rounded-lg p-4 space-y-3 bg-muted/30">
              <div className="flex gap-2 items-end flex-wrap">
                <div className="space-y-1">
                  <label className="text-xs text-muted-foreground">Connection</label>
                  <Select value={webhookConn} onValueChange={setWebhookConn}>
                    <SelectTrigger className="w-40"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {connections.map((c) => (
                        <SelectItem key={c.name} value={c.name}>{c.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-1">
                  <label className="text-xs text-muted-foreground">Database</label>
                  <Select value={webhookDb} onValueChange={setWebhookDb}>
                    <SelectTrigger className="w-44"><SelectValue placeholder="Select..." /></SelectTrigger>
                    <SelectContent>
                      {webhookDbs.map((d) => (
                        <SelectItem key={d} value={d}>{d}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-1">
                  <label className="text-xs text-muted-foreground">Table</label>
                  {webhookTables.length > 0 ? (
                    <Select value={webhookTable} onValueChange={setWebhookTable}>
                      <SelectTrigger className="w-48"><SelectValue placeholder="Select..." /></SelectTrigger>
                      <SelectContent>
                        {webhookTables.map((t) => (
                          <SelectItem key={t} value={t}>{t}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  ) : (
                    <Input className="w-48" placeholder="Table name" value={webhookTable} onChange={(e) => setWebhookTable(e.target.value)} />
                  )}
                </div>
              </div>
              <div className="space-y-1">
                <label className="text-xs text-muted-foreground">Destination URL</label>
                <Input placeholder="External URL for egress (e.g. https://destination-app.com/endpoint)" value={webhookUrl} onChange={(e) => setWebhookUrl(e.target.value)} />
              </div>
              <div className="space-y-1">
                <label className="text-xs text-muted-foreground">Events</label>
                <div className="flex gap-2">
                  {["INSERT", "UPDATE", "DELETE"].map((evt) => (
                    <Button
                      key={evt}
                      size="sm"
                      variant={webhookEvents.includes(evt) ? "default" : "outline"}
                      onClick={() => toggleWebhookEvent(evt)}
                    >
                      {evt}
                    </Button>
                  ))}
                </div>
              </div>
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <Switch checked={webhookUseSecret} onCheckedChange={(checked) => {
                    setWebhookUseSecret(checked);
                    if (checked && !webhookSecret) {
                      const bytes = new Uint8Array(32);
                      crypto.getRandomValues(bytes);
                      setWebhookSecret(Array.from(bytes).map(b => b.toString(16).padStart(2, "0")).join(""));
                    }
                  }} />
                  <label className="text-xs text-muted-foreground">HMAC-SHA256 Signing</label>
                </div>
                {webhookUseSecret && (
                  <div className="flex gap-2">
                    <Input
                      readOnly
                      className="font-mono text-xs select-all"
                      value={webhookSecret}
                    />
                    <Button size="sm" variant="outline" onClick={() => {
                      const bytes = new Uint8Array(32);
                      crypto.getRandomValues(bytes);
                      setWebhookSecret(Array.from(bytes).map(b => b.toString(16).padStart(2, "0")).join(""));
                    }}>
                      Regenerate
                    </Button>
                  </div>
                )}
              </div>
              <Button
                size="sm"
                disabled={addingWebhook || !webhookConn || !webhookDb || !webhookTable || !webhookUrl || webhookEvents.length === 0}
                onClick={handleCreateWebhook}
              >
                {addingWebhook ? "Creating..." : "Create Webhook"}
              </Button>
            </div>
          )}

          {/* Webhook list */}
          {webhooks.length === 0 && !showWebhookForm ? (
            <p className="text-sm text-muted-foreground">No webhooks configured yet.</p>
          ) : webhooks.length > 0 && (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Table</TableHead>
                  <TableHead>URL</TableHead>
                  <TableHead>Events</TableHead>
                  <TableHead>Secret</TableHead>
                  <TableHead>Enabled</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {webhooks.map((hook) => (
                  <TableRow key={hook.id}>
                    <TableCell>
                      <div className="space-y-0.5">
                        <Badge variant="outline" className="text-[10px]">{hook.connection_name}</Badge>
                        <div className="text-xs text-muted-foreground">{hook.database_name}</div>
                        <div className="font-mono text-sm">{hook.table_name}</div>
                      </div>
                    </TableCell>
                    <TableCell>
                      <span className="font-mono text-xs break-all">{hook.url}</span>
                    </TableCell>
                    <TableCell>
                      <div className="flex gap-1 flex-wrap">
                        {hook.events.split(",").map((e) => (
                          <Badge key={e} variant="secondary" className="text-[10px]">{e.trim()}</Badge>
                        ))}
                      </div>
                    </TableCell>
                    <TableCell>
                      {hook.secret ? (
                        <div className="space-y-1">
                          <button
                            className="text-xs text-blue-500 hover:underline"
                            onClick={() => toggleRevealSecret(hook.id)}
                          >
                            {revealedSecrets.has(hook.id) ? "Hide" : "Reveal"}
                          </button>
                          {revealedSecrets.has(hook.id) && (
                            <div className="font-mono text-xs bg-muted rounded px-2 py-1 break-all select-all">
                              {hook.secret}
                            </div>
                          )}
                        </div>
                      ) : (
                        <span className="text-xs text-muted-foreground">None</span>
                      )}
                    </TableCell>
                    <TableCell>
                      <Switch
                        checked={hook.is_enabled}
                        onCheckedChange={() => handleToggleWebhook(hook)}
                      />
                    </TableCell>
                    <TableCell className="text-right">
                      <Button size="sm" variant="destructive" onClick={() => handleDeleteWebhook(hook.id)}>
                        Delete
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* Watch drawer */}
      <Drawer
        open={!!watchEntry}
        onOpenChange={(open) => { if (!open) stopWatching(); }}
      >
        <DrawerContent className="h-[50vh]">
          <DrawerHeader className="flex-row items-center justify-between shrink-0">
            <div className="flex items-center gap-3">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
                <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500" />
              </span>
              <div>
                <DrawerTitle className="text-sm">
                  Watching: <span className="font-mono">{watchKey}</span>
                </DrawerTitle>
                <DrawerDescription className="text-xs">
                  {events.length} event{events.length !== 1 ? "s" : ""}
                </DrawerDescription>
              </div>
            </div>
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-2">
                <label className="text-xs text-muted-foreground">Full details</label>
                <Switch checked={showFull} onCheckedChange={setShowFull} />
              </div>
              <Button size="sm" variant="ghost" onClick={() => setEvents([])}>Clear</Button>
              <Button size="sm" variant="outline" onClick={stopWatching}>Close</Button>
            </div>
          </DrawerHeader>

          <div className="flex-1 min-h-0 overflow-y-auto px-4 pb-4">
            {events.length === 0 ? (
              <p className="text-sm text-muted-foreground text-center py-8">
                Waiting for events...
              </p>
            ) : (
              <div className="space-y-1.5 font-mono text-xs">
                {events.map((evt) => {
                  const dataStr = evt.data ? JSON.stringify(evt.data) : null;
                  const truncated = dataStr && dataStr.length > 120 && !showFull
                    ? dataStr.slice(0, 120) + "..."
                    : dataStr;

                  return (
                    <div
                      key={evt.id}
                      className="px-3 py-2 rounded bg-accent/30 border border-border"
                    >
                      <div className="flex items-center gap-3">
                        <Badge
                          variant={
                            evt.query_type === "INSERT" ? "default" :
                            evt.query_type === "DELETE" ? "destructive" :
                            "outline"
                          }
                          className="w-16 justify-center text-[10px] shrink-0"
                        >
                          {evt.query_type}
                        </Badge>
                        <span className="text-muted-foreground shrink-0">
                          {new Date(evt.timestamp).toLocaleTimeString()}
                        </span>
                        {evt.row_count != null && (
                          <span className="shrink-0">{evt.row_count} row{evt.row_count !== 1 ? "s" : ""}</span>
                        )}
                        {evt.user && (
                          <span className="text-muted-foreground ml-auto truncate max-w-48">{evt.user}</span>
                        )}
                      </div>
                      {truncated && (
                        <div className="mt-1.5 text-[11px] text-muted-foreground">
                          {showFull ? (
                            <pre className="whitespace-pre-wrap break-all bg-muted/50 rounded p-2 border border-border">
                              {JSON.stringify(evt.data, null, 2)}
                            </pre>
                          ) : (
                            <span className="break-all">{truncated}</span>
                          )}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </DrawerContent>
      </Drawer>
    </div>
  );
}
