import { useState, useEffect, useCallback } from "react";
import {
  listConnections, listDatabases, listSchemas, listTables,
  traverseGraph,
} from "../lib/api";
import type {
  ConnectionInfo, DatabaseInfo, TableInfo,
  TraversalResult, TraversalPath,
} from "../lib/api";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";

function formatNode(node: { connection_name: string; database_name: string; schema_name: string; table_name: string }) {
  const parts = [node.connection_name, node.database_name];
  if (node.schema_name) parts.push(node.schema_name);
  if (node.table_name) parts.push(node.table_name);
  return parts.join(" / ");
}

export default function GraphPage() {
  const [connections, setConnections] = useState<ConnectionInfo[]>([]);
  const [databases, setDatabases] = useState<DatabaseInfo[]>([]);
  const [schemas, setSchemas] = useState<{ schema_name: string }[]>([]);
  const [tables, setTables] = useState<TableInfo[]>([]);

  const [connection, setConnection] = useState("");
  const [database, setDatabase] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [maxDepth, setMaxDepth] = useState("3");
  const [edgeTypes, setEdgeTypes] = useState("");

  const [result, setResult] = useState<TraversalResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    listConnections().then(setConnections).catch(() => {});
  }, []);

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

  const handleExplore = useCallback(async () => {
    if (!connection || !database) return;
    try {
      setLoading(true);
      setError(null);
      const res = await traverseGraph({
        connection_name: connection,
        database_name: database,
        schema_name: schema || undefined,
        table_name: table || undefined,
        max_depth: parseInt(maxDepth) || 3,
        edge_types: edgeTypes || undefined,
      });
      setResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setResult(null);
    } finally {
      setLoading(false);
    }
  }, [connection, database, schema, table, maxDepth, edgeTypes]);

  // Group results by depth
  const grouped: Map<number, TraversalPath[]> = new Map();
  if (result) {
    for (const path of result.reachable) {
      const existing = grouped.get(path.depth) || [];
      existing.push(path);
      grouped.set(path.depth, existing);
    }
  }
  const depths = Array.from(grouped.keys()).sort((a, b) => a - b);

  return (
    <div className="h-full flex flex-col p-4 gap-4">
      {error && (
        <div className="bg-destructive/20 border border-destructive text-destructive px-4 py-2 rounded-md text-sm">
          {error}
          <button className="ml-2 underline" onClick={() => setError(null)}>dismiss</button>
        </div>
      )}

      {/* Start Node Selector */}
      <Card>
        <CardHeader className="pb-2">
          <h3 className="text-sm font-medium">Start Table</h3>
          <p className="text-xs text-muted-foreground">Select a table to discover how it connects to other tables — including across different database connections.</p>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-3">
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
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-1.5">
              <Label className="text-xs">Max Depth:</Label>
              <Input
                type="number"
                min={1}
                max={10}
                value={maxDepth}
                onChange={e => setMaxDepth(e.target.value)}
                className="w-16 h-8 text-sm"
              />
            </div>
            <div className="flex items-center gap-1.5">
              <Label className="text-xs">Edge Types:</Label>
              <Input
                placeholder="all (or e.g. join_key)"
                value={edgeTypes}
                onChange={e => setEdgeTypes(e.target.value)}
                className="w-48 h-8 text-sm"
              />
            </div>
            <Button size="sm" onClick={handleExplore} disabled={loading || !connection || !database}>
              {loading ? "Exploring..." : "Explore"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Results */}
      {result && (
        <Card>
          <CardHeader className="pb-2">
            <h3 className="text-sm font-medium">
              Starting from: <span className="font-mono">{formatNode(result.start_node)}</span>
            </h3>
            <p className="text-xs text-muted-foreground">
              {result.reachable.length} reachable table{result.reachable.length !== 1 ? "s" : ""} — each row shows the exact join chain needed to get there
            </p>
          </CardHeader>
          <CardContent>
            {result.reachable.length === 0 ? (
              <p className="text-sm text-muted-foreground py-4 text-center">
                No connected tables found. Seed the graph from foreign keys in Admin &rarr; Graph tab, or manually add cross-connection edges.
              </p>
            ) : (
              <div className="space-y-4">
                {depths.map(depth => {
                  const paths = grouped.get(depth) || [];
                  return (
                    <div key={depth}>
                      <h4 className="text-xs font-medium text-muted-foreground mb-2">
                        Depth {depth} ({paths.length} table{paths.length !== 1 ? "s" : ""})
                      </h4>
                      <div className="space-y-2 pl-3 border-l-2 border-border">
                        {paths.map((path, i) => (
                          <div key={i} className="text-sm">
                            <div className="font-mono font-medium">
                              {formatNode(path.node)}
                            </div>
                            <div className="text-xs text-muted-foreground mt-0.5">
                              via:{" "}
                              {path.edges.map((edge, j) => (
                                <span key={j}>
                                  {j > 0 && <span className="mx-1">&rarr;</span>}
                                  <span>
                                    {edge.source.table_name}
                                    {edge.source_columns?.length ? `.${edge.source_columns.join(",")}` : ""}
                                    <span className="mx-1">&rarr;</span>
                                    {edge.target.table_name}
                                    {edge.target_columns?.length ? `.${edge.target_columns.join(",")}` : ""}
                                  </span>
                                  <Badge variant="outline" className="ml-1 text-[10px] px-1 py-0">{edge.edge_type}</Badge>
                                </span>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
