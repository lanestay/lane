import { useState, useEffect, useRef, useCallback } from "react";
import {
  listRealtimeTables, enableRealtime, disableRealtime,
  listConnections, listDatabases, listTables,
} from "../lib/api";
import type { RealtimeTableEntry, RealtimeEvent, ConnectionInfo } from "../lib/api";
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

  const refresh = useCallback(async () => {
    try {
      const [t, c] = await Promise.all([listRealtimeTables(), listConnections()]);
      setTables(t);
      setConnections(c);
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
