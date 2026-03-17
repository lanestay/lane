import { useState, useEffect, useRef, useCallback } from "react";
import ConnectionPicker from "../components/ConnectionPicker";
import { ErrorBanner } from "../components/ResultsTable";
import EditableTable from "../components/EditableTable";
import MermaidERD from "../components/MermaidERD";
import { listSchemas, listTables, describeTable, executeQuery } from "../lib/api";
import type { ColumnInfo, QueryResult } from "../lib/api";
import { buildPreviewQuery, type ColumnFilter, type Dialect, type SortSpec } from "../lib/sql-gen";
import {
  fetchForeignKeys,
  fetchIndexes,
  getForeignKeysForTable,
  getReferencingForeignKeys,
} from "../lib/schema-queries";
import type { ForeignKeyInfo, IndexInfo } from "../lib/schema-queries";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Search, X } from "lucide-react";
import ExportButton from "../components/ExportButton";
import RelatedObjectsDrawer from "../components/RelatedObjectsDrawer";
import RlsTab from "../components/RlsTab";

interface SchemaGroup {
  schema: string;
  tables: string[];
}

export default function TablesPage() {
  const [connection, setConnection] = useState("");
  const [database, setDatabase] = useState("");
  const [connectionType, setConnectionType] = useState("mssql");
  const [availableSchemas, setAvailableSchemas] = useState<string[]>([]);
  const [schemaFilter, setSchemaFilter] = useState<string>("__all__");
  const [schemaGroups, setSchemaGroups] = useState<SchemaGroup[]>([]);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [selectedSchema, setSelectedSchema] = useState("dbo");
  const [columns, setColumns] = useState<ColumnInfo[] | null>(null);
  const [preview, setPreview] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [foreignKeys, setForeignKeys] = useState<ForeignKeyInfo[]>([]);
  const [indexes, setIndexes] = useState<IndexInfo[]>([]);
  const [fkLoading, setFkLoading] = useState(false);
  const [lastPreviewQuery, setLastPreviewQuery] = useState("");

  // Filter & sort state
  const [globalSearch, setGlobalSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [columnFilters, setColumnFilters] = useState<ColumnFilter[]>([]);
  const [sort, setSort] = useState<SortSpec | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  // Debounce global search
  useEffect(() => {
    debounceRef.current = setTimeout(() => setDebouncedSearch(globalSearch), 400);
    return () => clearTimeout(debounceRef.current);
  }, [globalSearch]);

  // Fetch available schemas when database changes
  useEffect(() => {
    if (!database) {
      setAvailableSchemas([]);
      setSchemaFilter("__all__");
      return;
    }
    listSchemas(database, connection || undefined)
      .then((result) => {
        setAvailableSchemas(result.map((r) => r.schema_name));
      })
      .catch(() => setAvailableSchemas([]));
    setSchemaFilter("__all__");
  }, [database, connection]);

  useEffect(() => {
    if (!database) return;
    setSelectedTable(null);
    setColumns(null);
    setPreview(null);
    listTables(database, connection || undefined).then((tables) => {
      const map = new Map<string, string[]>();
      for (const t of tables) {
        const schema = t.TABLE_SCHEMA || "dbo";
        if (!map.has(schema)) map.set(schema, []);
        map.get(schema)!.push(t.TABLE_NAME);
      }
      setSchemaGroups(
        Array.from(map.entries()).map(([schema, tables]) => ({ schema, tables: tables.sort() }))
      );
    });
  }, [database, connection]);

  // Fetch FK + index data when database changes
  useEffect(() => {
    if (!database) {
      setForeignKeys([]);
      setIndexes([]);
      return;
    }
    const dialect = connectionType as Dialect;
    const conn = connection || undefined;
    setFkLoading(true);

    Promise.allSettled([
      fetchForeignKeys(database, dialect, conn),
      fetchIndexes(database, dialect, conn),
    ]).then(([fkResult, idxResult]) => {
      setForeignKeys(fkResult.status === "fulfilled" ? fkResult.value : []);
      setIndexes(idxResult.status === "fulfilled" ? idxResult.value : []);
      setFkLoading(false);
    });
  }, [database, connection, connectionType]);

  const getDisplayColumns = useCallback((): string[] => {
    if (!preview) return columns?.map((c) => c.COLUMN_NAME) ?? [];
    return preview.metadata
      ? preview.metadata.columns.map((c) => c.name)
      : preview.data.length > 0
        ? Object.keys(preview.data[0])
        : [];
  }, [preview, columns]);

  const refreshPreview = useCallback(async (
    schema: string,
    table: string,
    search = debouncedSearch,
    filters = columnFilters,
    sortSpec = sort,
    displayCols?: string[],
  ) => {
    const dialect = connectionType as Dialect;
    const cols = displayCols ?? getDisplayColumns();
    const query = buildPreviewQuery(schema, table, dialect, search, filters, sortSpec, cols);
    setLastPreviewQuery(query);
    try {
      const res = await executeQuery(query, database, connection || undefined);
      setPreview(res);
    } catch (e) {
      if (!error) setError(e instanceof Error ? e.message : String(e));
    }
  }, [connectionType, database, connection, error, debouncedSearch, columnFilters, sort, getDisplayColumns]);

  // Re-query when filters/sort/search change
  useEffect(() => {
    if (!selectedTable || !selectedSchema) return;
    refreshPreview(selectedSchema, selectedTable);
  }, [debouncedSearch, columnFilters, sort]); // eslint-disable-line react-hooks/exhaustive-deps

  const selectTable = async (schema: string, table: string) => {
    setSelectedTable(table);
    setSelectedSchema(schema);
    setError(null);
    setColumns(null);
    setPreview(null);
    setGlobalSearch("");
    setDebouncedSearch("");
    setColumnFilters([]);
    setSort(null);

    try {
      const cols = await describeTable(database, table, connection || undefined, schema);
      setColumns(cols);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }

    // Initial preview with no filters — pass empty values explicitly
    const dialect = connectionType as Dialect;
    const query = buildPreviewQuery(schema, table, dialect, "", [], null, []);
    setLastPreviewQuery(query);
    try {
      const res = await executeQuery(query, database, connection || undefined);
      setPreview(res);
    } catch (e) {
      if (!error) setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleColumnFilterChange = (column: string, filter: ColumnFilter | null) => {
    setColumnFilters((prev) => {
      const next = prev.filter((f) => f.column !== column);
      if (filter) next.push(filter);
      return next;
    });
  };

  const clearAllFilters = () => {
    setGlobalSearch("");
    setDebouncedSearch("");
    setColumnFilters([]);
    setSort(null);
  };

  const hasAnyFilter = globalSearch !== "" || columnFilters.length > 0 || sort !== null;
  const filterFingerprint = JSON.stringify({ debouncedSearch, columnFilters, sort });

  const tableFks = selectedTable ? getForeignKeysForTable(foreignKeys, selectedSchema, selectedTable) : [];
  const referencingFks = selectedTable ? getReferencingForeignKeys(foreignKeys, selectedSchema, selectedTable) : [];

  // Build lookup maps for the current table
  const fkByColumn = new Map<string, ForeignKeyInfo>();
  for (const fk of tableFks) {
    fkByColumn.set(fk.PARENT_COLUMN, fk);
  }

  const indexByColumn = new Map<string, IndexInfo[]>();
  if (selectedTable) {
    for (const idx of indexes) {
      if (idx.TABLE_SCHEMA === selectedSchema && idx.TABLE_NAME === selectedTable) {
        const list = indexByColumn.get(idx.COLUMN_NAME) ?? [];
        list.push(idx);
        indexByColumn.set(idx.COLUMN_NAME, list);
      }
    }
  }

  const erdTables = schemaGroups.flatMap((g) =>
    g.tables.map((t) => ({ schema: g.schema, name: t }))
  );

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
      <div className="flex-1 flex gap-4 overflow-hidden">
        {/* Schema tree */}
        <Card className="w-64 shrink-0 overflow-y-auto">
          <CardHeader className="pb-2 space-y-2">
            <CardTitle className="text-sm">Schemas & Tables</CardTitle>
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
            {schemaGroups.length === 0 ? (
              <p className="text-muted-foreground text-sm px-2 py-4 text-center">Select a database</p>
            ) : (
              (schemaFilter === "__all__" ? schemaGroups : schemaGroups.filter((g) => g.schema === schemaFilter)).map((group) => (
                <SchemaTree
                  key={group.schema}
                  schema={group.schema}
                  tables={group.tables}
                  selectedTable={selectedTable}
                  onSelect={(table) => selectTable(group.schema, table)}
                />
              ))
            )}
          </CardContent>
        </Card>
        {/* Detail panel */}
        <div className="flex-1 flex flex-col gap-4 overflow-y-auto">
          {error ? (
            <ErrorBanner message={error} />
          ) : selectedTable ? (
            <>
            <Tabs defaultValue="columns">
              <TabsList>
                <TabsTrigger value="columns">Columns</TabsTrigger>
                <TabsTrigger value="erd">ERD</TabsTrigger>
                {connectionType !== "duckdb" && <TabsTrigger value="rls">RLS</TabsTrigger>}
              </TabsList>
              <TabsContent value="columns" className="flex flex-col gap-4">
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-base">
                      {selectedSchema}.{selectedTable}
                      {fkLoading && <span className="text-muted-foreground text-xs ml-2">(loading FK data...)</span>}
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    {columns ? (
                      <ColumnTable
                        columns={columns}
                        fkByColumn={fkByColumn}
                        indexByColumn={indexByColumn}
                        onFkClick={(schema, table) => selectTable(schema, table)}
                      />
                    ) : (
                      <p className="text-muted-foreground text-sm">Loading columns...</p>
                    )}
                    {referencingFks.length > 0 && (
                      <div className="mt-4 pt-3 border-t border-border">
                        <p className="text-xs font-medium text-muted-foreground mb-2">Referenced By</p>
                        <div className="flex flex-wrap gap-2">
                          {referencingFks.map((fk) => (
                            <Button
                              key={fk.FK_NAME + fk.PARENT_COLUMN}
                              variant="ghost"
                              size="sm"
                              className="h-6 text-xs text-blue-400 hover:text-blue-300"
                              onClick={() => selectTable(fk.PARENT_SCHEMA, fk.PARENT_TABLE)}
                            >
                              {fk.PARENT_SCHEMA}.{fk.PARENT_TABLE}.{fk.PARENT_COLUMN}
                            </Button>
                          ))}
                        </div>
                      </div>
                    )}
                  </CardContent>
                </Card>
                {preview && columns && (
                  <div>
                    <div className="flex items-center justify-between mb-2">
                      <h3 className="text-muted-foreground font-medium text-sm">
                        {hasAnyFilter ? "Preview (filtered)" : "Preview (first 100 rows)"}
                      </h3>
                      <ExportButton
                        query={lastPreviewQuery}
                        database={database}
                        connection={connection || undefined}
                        result={preview}
                      />
                    </div>
                    {/* Filter bar */}
                    <div className="flex flex-col gap-2 mb-2">
                      <div className="flex items-center gap-2">
                        <div className="relative flex-1 max-w-sm">
                          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-4 text-muted-foreground" />
                          <Input
                            className="h-8 pl-8 text-sm"
                            placeholder="Search all columns..."
                            value={globalSearch}
                            onChange={(e) => setGlobalSearch(e.target.value)}
                          />
                        </div>
                        {hasAnyFilter && (
                          <Button variant="ghost" size="sm" className="text-xs" onClick={clearAllFilters}>
                            Clear all
                          </Button>
                        )}
                      </div>
                      {/* Active filter badges */}
                      {(columnFilters.length > 0 || sort) && (
                        <div className="flex flex-wrap gap-1.5">
                          {columnFilters.map((f) => (
                            <Badge key={f.column} variant="secondary" className="gap-1 text-xs">
                              {f.column} {f.operator} {f.value}
                              <button
                                className="ml-0.5 hover:text-foreground cursor-pointer"
                                onClick={() => handleColumnFilterChange(f.column, null)}
                              >
                                <X className="size-3" />
                              </button>
                            </Badge>
                          ))}
                          {sort && (
                            <Badge variant="secondary" className="gap-1 text-xs">
                              Sort: {sort.column} {sort.direction}
                              <button
                                className="ml-0.5 hover:text-foreground cursor-pointer"
                                onClick={() => setSort(null)}
                              >
                                <X className="size-3" />
                              </button>
                            </Badge>
                          )}
                        </div>
                      )}
                    </div>
                    <EditableTable
                      key={filterFingerprint}
                      result={preview}
                      columns={columns}
                      schema={selectedSchema}
                      table={selectedTable}
                      database={database}
                      connection={connection}
                      dialect={connectionType as Dialect}
                      onRefresh={() => refreshPreview(selectedSchema, selectedTable)}
                      sort={sort}
                      onSortChange={setSort}
                      columnFilters={columnFilters}
                      onColumnFilterChange={handleColumnFilterChange}
                    />
                  </div>
                )}
              </TabsContent>
              <TabsContent value="erd">
                <MermaidERD
                  tables={erdTables}
                  foreignKeys={foreignKeys}
                  onTableClick={(schema, table) => selectTable(schema, table)}
                />
              </TabsContent>
              {connectionType !== "duckdb" && (
                <TabsContent value="rls">
                  <RlsTab
                    database={database}
                    schema={selectedSchema}
                    table={selectedTable}
                    connection={connection}
                    connectionType={connectionType}
                  />
                </TabsContent>
              )}
            </Tabs>
            <RelatedObjectsDrawer
              database={database}
              schema={selectedSchema}
              table={selectedTable}
              connection={connection}
              connectionType={connectionType}
            />
            </>
          ) : (
            <div className="text-muted-foreground text-center py-12">
              Select a table to view its columns and data
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function SchemaTree({ schema, tables, selectedTable, onSelect }: {
  schema: string;
  tables: string[];
  selectedTable: string | null;
  onSelect: (table: string) => void;
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
      </Button>
      {expanded && (
        <div className="ml-4">
          {tables.map((table) => (
            <Button
              key={table}
              variant={selectedTable === table ? "secondary" : "ghost"}
              size="sm"
              className="w-full justify-start px-2 h-7 text-xs"
              onClick={() => onSelect(table)}
            >
              {table}
            </Button>
          ))}
        </div>
      )}
    </div>
  );
}

function ColumnTable({ columns, fkByColumn, indexByColumn, onFkClick }: {
  columns: ColumnInfo[];
  fkByColumn: Map<string, ForeignKeyInfo>;
  indexByColumn: Map<string, IndexInfo[]>;
  onFkClick: (schema: string, table: string) => void;
}) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Column</TableHead>
          <TableHead>Type</TableHead>
          <TableHead>Nullable</TableHead>
          <TableHead>Default</TableHead>
          <TableHead>PK</TableHead>
          <TableHead>FK</TableHead>
          <TableHead>Indexes</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {columns.map((col) => {
          const fk = fkByColumn.get(col.COLUMN_NAME);
          const colIndexes = indexByColumn.get(col.COLUMN_NAME) ?? [];
          return (
            <TableRow key={col.COLUMN_NAME}>
              <TableCell className="font-mono text-xs">{col.COLUMN_NAME}</TableCell>
              <TableCell className="font-mono text-xs text-muted-foreground">{col.DATA_TYPE}</TableCell>
              <TableCell className="text-xs text-muted-foreground">{col.IS_NULLABLE}</TableCell>
              <TableCell className="font-mono text-xs text-muted-foreground">{col.COLUMN_DEFAULT ?? "-"}</TableCell>
              <TableCell className="text-xs">
                {col.IS_PRIMARY_KEY === "YES" ? (
                  <Badge variant="outline" className="text-yellow-400 border-yellow-400/50">PK</Badge>
                ) : (
                  <span className="text-muted-foreground">-</span>
                )}
              </TableCell>
              <TableCell className="text-xs">
                {fk ? (
                  <button
                    className="inline-flex items-center gap-1 cursor-pointer hover:underline"
                    onClick={() => onFkClick(fk.REFERENCED_SCHEMA, fk.REFERENCED_TABLE)}
                    title={`${fk.FK_NAME} → ${fk.REFERENCED_SCHEMA}.${fk.REFERENCED_TABLE}.${fk.REFERENCED_COLUMN}`}
                  >
                    <Badge variant="outline" className="text-blue-400 border-blue-400/50">FK</Badge>
                    <span className="text-blue-400 text-xs">{fk.REFERENCED_TABLE}</span>
                  </button>
                ) : (
                  <span className="text-muted-foreground">-</span>
                )}
              </TableCell>
              <TableCell className="text-xs">
                {colIndexes.length > 0 ? (
                  <div className="flex gap-1 flex-wrap">
                    {colIndexes.map((idx) => (
                      <Badge
                        key={idx.INDEX_NAME}
                        variant="outline"
                        className={idx.IS_UNIQUE ? "text-green-400 border-green-400/50" : "text-muted-foreground border-muted-foreground/50"}
                        title={idx.INDEX_NAME}
                      >
                        {idx.IS_UNIQUE ? "UQ" : "IDX"}
                      </Badge>
                    ))}
                  </div>
                ) : (
                  <span className="text-muted-foreground">-</span>
                )}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}
