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
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";

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
  const [watchKey, setWatchKey] = useState<string | null>(null);
  const [events, setEvents] = useState<RealtimeEvent[]>([]);
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
      // Stop watching if we were watching this one
      const key = `${entry.connection_name}/${entry.database_name}/${entry.table_name}`;
      if (watchKey === key) stopWatching();
      await refresh();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const startWatching = (entry: RealtimeTableEntry) => {
    stopWatching();
    const key = `${entry.connection_name}/${entry.database_name}/${entry.table_name}`;
    setWatchKey(key);
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
    es.onerror = () => {
      // will auto-reconnect
    };
    eventSourceRef.current = es;
  };

  const stopWatching = () => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    setWatchKey(null);
  };

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) eventSourceRef.current.close();
    };
  }, []);

  if (loading) return <div className="p-4 text-muted-foreground">Loading...</div>;

  return (
    <div className="h-full flex flex-col p-4 gap-4">
      <h2 className="text-xl font-bold">Realtime</h2>
      {error && (
        <div className="bg-destructive/20 border border-destructive text-destructive px-4 py-2 rounded-md text-sm">
          {error}
          <button className="ml-2 underline" onClick={() => setError(null)}>dismiss</button>
        </div>
      )}

      {/* Enable form */}
      <Card>
        <CardHeader>
          <p className="text-sm font-medium">Enable Realtime on a Table</p>
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
                  const key = `${entry.connection_name}/${entry.database_name}/${entry.table_name}`;
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

      {/* Live event stream */}
      {watchKey && (
        <Card className="flex-1 min-h-0 flex flex-col">
          <CardHeader className="flex-row items-center justify-between py-2">
            <div className="flex items-center gap-2">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
                <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500" />
              </span>
              <p className="text-sm font-medium">
                Watching: <span className="font-mono">{watchKey}</span>
              </p>
            </div>
            <Button size="sm" variant="ghost" onClick={() => setEvents([])}>Clear</Button>
          </CardHeader>
          <CardContent className="flex-1 min-h-0 overflow-auto">
            {events.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                Waiting for events... Execute a write query against this table to see events here.
              </p>
            ) : (
              <div className="space-y-1 font-mono text-xs">
                {events.map((evt) => (
                  <div
                    key={evt.id}
                    className="flex items-center gap-3 px-2 py-1.5 rounded bg-accent/30 border border-border"
                  >
                    <Badge
                      variant={
                        evt.query_type === "INSERT" ? "default" :
                        evt.query_type === "DELETE" ? "destructive" :
                        "outline"
                      }
                      className="w-16 justify-center text-[10px]"
                    >
                      {evt.query_type}
                    </Badge>
                    <span className="text-muted-foreground">
                      {new Date(evt.timestamp).toLocaleTimeString()}
                    </span>
                    {evt.row_count != null && (
                      <span>{evt.row_count} row{evt.row_count !== 1 ? "s" : ""}</span>
                    )}
                    {evt.user && (
                      <span className="text-muted-foreground ml-auto truncate max-w-48">{evt.user}</span>
                    )}
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
