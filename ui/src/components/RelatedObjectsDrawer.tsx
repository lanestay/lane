import { useState, useEffect } from "react";
import { listTriggers, getTriggerDefinition, getRelatedObjects } from "../lib/api";
import type { TriggerInfo, RelatedObject } from "../lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ChevronDown, ChevronUp, ExternalLink } from "lucide-react";
import { useNavigate } from "react-router-dom";

interface Props {
  database: string;
  schema: string;
  table: string;
  connection: string;
  connectionType: string;
}

export default function RelatedObjectsDrawer({ database, schema, table, connection }: Props) {
  const navigate = useNavigate();
  const [expanded, setExpanded] = useState(false);
  const [triggers, setTriggers] = useState<TriggerInfo[]>([]);
  const [relatedObjects, setRelatedObjects] = useState<RelatedObject[]>([]);
  const [selectedDef, setSelectedDef] = useState<string | null>(null);
  const [selectedName, setSelectedName] = useState<string | null>(null);
  const [loadingDef, setLoadingDef] = useState(false);

  const views = relatedObjects.filter((o) => o.object_type === "VIEW");
  const procedures = relatedObjects.filter((o) => o.object_type !== "VIEW");

  // Fetch data when table changes
  useEffect(() => {
    setExpanded(false);
    setSelectedDef(null);
    setSelectedName(null);
    setTriggers([]);
    setRelatedObjects([]);

    if (!database || !table) return;

    const conn = connection || undefined;
    Promise.allSettled([
      listTriggers(database, table, conn, schema),
      getRelatedObjects(database, table, conn, schema),
    ]).then(([trigResult, relResult]) => {
      setTriggers(trigResult.status === "fulfilled" ? trigResult.value : []);
      setRelatedObjects(relResult.status === "fulfilled" ? relResult.value : []);
    });
  }, [database, schema, table, connection]);

  const loadTriggerDefinition = async (name: string) => {
    if (selectedName === name) {
      setSelectedDef(null);
      setSelectedName(null);
      return;
    }
    setLoadingDef(true);
    setSelectedName(name);
    try {
      const def = await getTriggerDefinition(database, name, connection || undefined, schema);
      setSelectedDef(def.definition);
    } catch {
      setSelectedDef("-- Failed to load definition");
    } finally {
      setLoadingDef(false);
    }
  };

  const openInEditor = (sql: string) => {
    navigate("/", { state: { prefillQuery: sql, database, connection } });
  };

  const totalCount = triggers.length + views.length + procedures.length;
  if (totalCount === 0) return null;

  return (
    <div className="border-t border-border mt-4">
      {/* Collapsed bar */}
      <button
        className="w-full flex items-center justify-between px-3 py-2 hover:bg-muted/50 transition-colors cursor-pointer"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          {triggers.length > 0 && (
            <Badge variant="secondary" className="text-xs">
              Triggers ({triggers.length})
            </Badge>
          )}
          {views.length > 0 && (
            <Badge variant="secondary" className="text-xs">
              Views ({views.length})
            </Badge>
          )}
          {procedures.length > 0 && (
            <Badge variant="secondary" className="text-xs">
              Procedures ({procedures.length})
            </Badge>
          )}
        </div>
        {expanded ? <ChevronDown className="size-4 text-muted-foreground" /> : <ChevronUp className="size-4 text-muted-foreground" />}
      </button>

      {/* Expanded panel */}
      {expanded && (
        <div className="max-h-[300px] overflow-y-auto px-3 pb-3">
          <Tabs defaultValue={triggers.length > 0 ? "triggers" : views.length > 0 ? "views" : "procedures"}>
            <TabsList className="h-8">
              {triggers.length > 0 && <TabsTrigger value="triggers" className="text-xs">Triggers</TabsTrigger>}
              {views.length > 0 && <TabsTrigger value="views" className="text-xs">Views</TabsTrigger>}
              {procedures.length > 0 && <TabsTrigger value="procedures" className="text-xs">Procedures</TabsTrigger>}
            </TabsList>

            {triggers.length > 0 && (
              <TabsContent value="triggers" className="mt-2">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="text-xs">Name</TableHead>
                      <TableHead className="text-xs">Events</TableHead>
                      <TableHead className="text-xs">Status</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {triggers.map((t) => (
                      <TableRow
                        key={t.name}
                        className={`cursor-pointer ${selectedName === t.name ? "bg-muted" : ""}`}
                        onClick={() => loadTriggerDefinition(t.name)}
                      >
                        <TableCell className="font-mono text-xs">{t.name}</TableCell>
                        <TableCell className="text-xs text-muted-foreground">{t.events}</TableCell>
                        <TableCell className="text-xs">
                          {t.is_disabled ? (
                            <Badge variant="outline" className="text-yellow-400 border-yellow-400/50">Disabled</Badge>
                          ) : (
                            <Badge variant="outline" className="text-green-400 border-green-400/50">Enabled</Badge>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
                {loadingDef && <p className="text-xs text-muted-foreground mt-2">Loading definition...</p>}
                {selectedDef && !loadingDef && (
                  <div className="mt-2">
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-xs font-medium text-muted-foreground">{selectedName}</span>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-6 text-xs gap-1"
                        onClick={() => openInEditor(selectedDef)}
                      >
                        <ExternalLink className="size-3" /> Open in SQL Editor
                      </Button>
                    </div>
                    <pre className="text-xs bg-muted/50 p-2 rounded overflow-x-auto max-h-[200px] overflow-y-auto">
                      {selectedDef}
                    </pre>
                  </div>
                )}
              </TabsContent>
            )}

            {views.length > 0 && (
              <TabsContent value="views" className="mt-2">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="text-xs">Name</TableHead>
                      <TableHead className="text-xs">Schema</TableHead>
                      {views.some((v) => v.modify_date) && <TableHead className="text-xs">Modified</TableHead>}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {views.map((v) => (
                      <TableRow key={v.object_name}>
                        <TableCell className="font-mono text-xs">{v.object_name}</TableCell>
                        <TableCell className="text-xs text-muted-foreground">{v.schema_name}</TableCell>
                        {views.some((o) => o.modify_date) && (
                          <TableCell className="text-xs text-muted-foreground">{v.modify_date ?? "-"}</TableCell>
                        )}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TabsContent>
            )}

            {procedures.length > 0 && (
              <TabsContent value="procedures" className="mt-2">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="text-xs">Name</TableHead>
                      <TableHead className="text-xs">Type</TableHead>
                      <TableHead className="text-xs">Schema</TableHead>
                      {procedures.some((p) => p.modify_date) && <TableHead className="text-xs">Modified</TableHead>}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {procedures.map((p) => (
                      <TableRow key={p.object_name + p.object_type}>
                        <TableCell className="font-mono text-xs">{p.object_name}</TableCell>
                        <TableCell className="text-xs text-muted-foreground">{p.object_type}</TableCell>
                        <TableCell className="text-xs text-muted-foreground">{p.schema_name}</TableCell>
                        {procedures.some((o) => o.modify_date) && (
                          <TableCell className="text-xs text-muted-foreground">{p.modify_date ?? "-"}</TableCell>
                        )}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TabsContent>
            )}
          </Tabs>
        </div>
      )}
    </div>
  );
}
