import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import ConnectionPicker from "../components/ConnectionPicker";
import { listSchemas, listViews, listRoutines, getObjectDefinition } from "../lib/api";
import type { ViewInfo, RoutineInfo, ObjectDefinition } from "../lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

type ObjectTab = "views" | "procedures" | "functions";

interface ObjectEntry {
  name: string;
  schema_name: string;
  type: string;
  routine_type?: string;
  create_date?: string;
  modify_date?: string;
}

export default function ObjectsPage() {
  const navigate = useNavigate();
  const [connection, setConnection] = useState("");
  const [database, setDatabase] = useState("");
  const [, setConnectionType] = useState("mssql");
  const [availableSchemas, setAvailableSchemas] = useState<string[]>([]);
  const [schemaFilter, setSchemaFilter] = useState<string>("__all__");
  const [activeTab, setActiveTab] = useState<ObjectTab>("views");

  const [views, setViews] = useState<ViewInfo[]>([]);
  const [routines, setRoutines] = useState<RoutineInfo[]>([]);
  const [selectedObject, setSelectedObject] = useState<ObjectEntry | null>(null);
  const [definition, setDefinition] = useState<ObjectDefinition | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Fetch schemas when database changes
  useEffect(() => {
    if (!database) {
      setAvailableSchemas([]);
      setSchemaFilter("__all__");
      return;
    }
    listSchemas(database, connection || undefined)
      .then((result) => setAvailableSchemas(result.map((r) => r.schema_name)))
      .catch(() => setAvailableSchemas([]));
    setSchemaFilter("__all__");
  }, [database, connection]);

  // Fetch objects when database/schema changes
  useEffect(() => {
    if (!database) {
      setViews([]);
      setRoutines([]);
      return;
    }
    setSelectedObject(null);
    setDefinition(null);
    setError(null);

    const schema = schemaFilter === "__all__" ? undefined : schemaFilter;

    listViews(database, connection || undefined, schema)
      .then(setViews)
      .catch(() => setViews([]));

    listRoutines(database, connection || undefined, schema)
      .then(setRoutines)
      .catch(() => setRoutines([]));
  }, [database, connection, schemaFilter]);

  const selectObject = async (obj: ObjectEntry) => {
    setSelectedObject(obj);
    setDefinition(null);
    setError(null);
    setLoading(true);

    const objType = obj.routine_type
      ? obj.routine_type.toLowerCase().replace("_", " ").includes("function")
        ? "function"
        : "procedure"
      : obj.type === "MATERIALIZED VIEW"
        ? "materialized_view"
        : "view";

    try {
      const def = await getObjectDefinition(
        database,
        obj.name,
        objType,
        connection || undefined,
        obj.schema_name,
      );
      setDefinition(def);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const openInEditor = () => {
    if (!definition?.definition) return;
    navigate(`/?sql=${encodeURIComponent(definition.definition)}`);
  };

  // Build grouped objects for the sidebar
  const currentObjects: ObjectEntry[] = (() => {
    if (activeTab === "views") {
      return views.map((v) => ({
        name: v.name,
        schema_name: v.schema_name,
        type: v.type,
        create_date: v.create_date,
        modify_date: v.modify_date,
      }));
    }
    const filtered = routines.filter((r) => {
      if (activeTab === "procedures") return r.routine_type === "PROCEDURE";
      return r.routine_type !== "PROCEDURE"; // functions
    });
    return filtered.map((r) => ({
      name: r.name,
      schema_name: r.schema_name,
      type: r.routine_type,
      routine_type: r.routine_type,
      create_date: r.create_date,
      modify_date: r.modify_date,
    }));
  })();

  // Group by schema
  const schemaGroups = new Map<string, ObjectEntry[]>();
  for (const obj of currentObjects) {
    const group = schemaGroups.get(obj.schema_name) ?? [];
    group.push(obj);
    schemaGroups.set(obj.schema_name, group);
  }

  return (
    <div className="h-full flex flex-col p-4 gap-4">
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

      <Tabs value={activeTab} onValueChange={(v) => { setActiveTab(v as ObjectTab); setSelectedObject(null); setDefinition(null); }}>
        <TabsList>
          <TabsTrigger value="views">Views ({views.length})</TabsTrigger>
          <TabsTrigger value="procedures">
            Procedures ({routines.filter((r) => r.routine_type === "PROCEDURE").length})
          </TabsTrigger>
          <TabsTrigger value="functions">
            Functions ({routines.filter((r) => r.routine_type !== "PROCEDURE").length})
          </TabsTrigger>
        </TabsList>
      </Tabs>

      <div className="flex-1 flex gap-4 overflow-hidden">
        {/* Sidebar */}
        <Card className="w-64 shrink-0 overflow-y-auto">
          <CardHeader className="pb-2 space-y-2">
            <CardTitle className="text-sm">
              {activeTab === "views" ? "Views" : activeTab === "procedures" ? "Procedures" : "Functions"}
            </CardTitle>
            {availableSchemas.length > 0 && (
              <Select value={schemaFilter} onValueChange={setSchemaFilter}>
                <SelectTrigger className="h-7 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__all__">All schemas</SelectItem>
                  {availableSchemas.map((s) => (
                    <SelectItem key={s} value={s}>{s}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
          </CardHeader>
          <CardContent className="p-2">
            {currentObjects.length === 0 ? (
              <p className="text-muted-foreground text-sm px-2 py-4 text-center">
                {database ? "No objects found" : "Select a database"}
              </p>
            ) : (
              Array.from(schemaGroups.entries()).map(([schema, objects]) => (
                <SchemaObjectTree
                  key={schema}
                  schema={schema}
                  objects={objects}
                  selectedObject={selectedObject}
                  onSelect={selectObject}
                />
              ))
            )}
          </CardContent>
        </Card>

        {/* Detail panel */}
        <div className="flex-1 flex flex-col gap-4 overflow-y-auto">
          {error ? (
            <Card className="border-destructive">
              <CardContent className="pt-4">
                <p className="text-destructive text-sm">{error}</p>
              </CardContent>
            </Card>
          ) : selectedObject ? (
            <>
              <Card>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base">
                      {selectedObject.schema_name}.{selectedObject.name}
                    </CardTitle>
                    <div className="flex items-center gap-2">
                      <Badge variant="outline">{selectedObject.type}</Badge>
                      {definition && (
                        <Button size="sm" variant="outline" onClick={openInEditor}>
                          Open in SQL Editor
                        </Button>
                      )}
                    </div>
                  </div>
                  {(selectedObject.create_date || selectedObject.modify_date) && (
                    <div className="flex gap-4 text-xs text-muted-foreground">
                      {selectedObject.create_date && <span>Created: {selectedObject.create_date}</span>}
                      {selectedObject.modify_date && <span>Modified: {selectedObject.modify_date}</span>}
                    </div>
                  )}
                </CardHeader>
                <CardContent>
                  {loading ? (
                    <p className="text-muted-foreground text-sm">Loading definition...</p>
                  ) : definition ? (
                    <div className="flex flex-col gap-4">
                      {/* Parameters table for procs/functions */}
                      {definition.parameters && definition.parameters.length > 0 && (
                        <div>
                          <h4 className="text-sm font-medium mb-2">Parameters</h4>
                          <Table>
                            <TableHeader>
                              <TableRow>
                                <TableHead>Name</TableHead>
                                <TableHead>Type</TableHead>
                                <TableHead>Max Length</TableHead>
                                <TableHead>Direction</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {definition.parameters.map((p, i) => (
                                <TableRow key={i}>
                                  <TableCell className="font-mono text-xs">{p.param_name || "(return)"}</TableCell>
                                  <TableCell className="font-mono text-xs text-muted-foreground">{p.type_name}</TableCell>
                                  <TableCell className="text-xs text-muted-foreground">{p.max_length ?? "-"}</TableCell>
                                  <TableCell className="text-xs">
                                    {p.is_output ? (
                                      <Badge variant="outline" className="text-amber-400 border-amber-400/50">OUTPUT</Badge>
                                    ) : (
                                      <span className="text-muted-foreground">INPUT</span>
                                    )}
                                  </TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </div>
                      )}

                      {/* Postgres function signature */}
                      {definition.arguments && (
                        <div className="text-xs text-muted-foreground">
                          <span className="font-medium">Arguments:</span> {definition.arguments}
                          {definition.return_type && (
                            <span className="ml-4"><span className="font-medium">Returns:</span> {definition.return_type}</span>
                          )}
                        </div>
                      )}

                      {/* SQL Definition */}
                      <div>
                        <h4 className="text-sm font-medium mb-2">Definition</h4>
                        <pre className="bg-muted rounded-md p-4 text-xs font-mono overflow-x-auto whitespace-pre-wrap max-h-[600px] overflow-y-auto">
                          {definition.definition}
                        </pre>
                      </div>
                    </div>
                  ) : null}
                </CardContent>
              </Card>
            </>
          ) : (
            <div className="text-muted-foreground text-center py-12">
              Select a {activeTab === "views" ? "view" : activeTab === "procedures" ? "procedure" : "function"} to view its definition
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function SchemaObjectTree({ schema, objects, selectedObject, onSelect }: {
  schema: string;
  objects: ObjectEntry[];
  selectedObject: ObjectEntry | null;
  onSelect: (obj: ObjectEntry) => void;
}) {
  const [expanded, setExpanded] = useState(true);

  return (
    <div className="mb-1">
      <Button
        variant="ghost"
        size="sm"
        className="w-full justify-start text-muted-foreground px-2"
        onClick={() => setExpanded(!expanded)}
      >
        <span className="text-xs mr-1">{expanded ? "\u25BC" : "\u25B6"}</span>
        {schema}
        <span className="ml-auto text-xs text-muted-foreground">{objects.length}</span>
      </Button>
      {expanded && (
        <div className="ml-4">
          {objects.map((obj) => (
            <Button
              key={obj.name}
              variant={selectedObject?.name === obj.name && selectedObject?.schema_name === obj.schema_name ? "secondary" : "ghost"}
              size="sm"
              className="w-full justify-start px-2 h-7 text-xs"
              onClick={() => onSelect(obj)}
            >
              {obj.name}
            </Button>
          ))}
        </div>
      )}
    </div>
  );
}
