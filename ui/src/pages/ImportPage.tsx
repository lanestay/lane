import { useState, useEffect, useRef, useCallback } from "react";
import ConnectionPicker from "../components/ConnectionPicker";
import {
  listSchemas,
  listTables,
  previewImport,
  executeImport,
} from "../lib/api";
import type {
  ImportPreviewColumn,
  ImportPreviewResult,
  ImportExecuteResult,
} from "../lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
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

type Step = "upload" | "preview" | "result";

export default function ImportPage() {
  const [step, setStep] = useState<Step>("upload");

  // Connection state
  const [connection, setConnection] = useState("");
  const [database, setDatabase] = useState("");
  const [connType, setConnType] = useState("mssql");

  // Upload form
  const [schemas, setSchemas] = useState<string[]>([]);
  const [schema, setSchema] = useState("");
  const [customSchema, setCustomSchema] = useState(false);
  const [tables, setTables] = useState<string[]>([]);
  const [tableName, setTableName] = useState("");
  const [newTable, setNewTable] = useState(false);
  const [hasHeader, setHasHeader] = useState(true);
  const [file, setFile] = useState<File | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Preview state
  const [preview, setPreview] = useState<ImportPreviewResult | null>(null);
  const [columns, setColumns] = useState<(ImportPreviewColumn & { include: boolean })[]>([]);
  const [ifExists, setIfExists] = useState<"create" | "append">("create");

  // Result state
  const [result, setResult] = useState<ImportExecuteResult | null>(null);

  // Shared
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const defaultSchema = connType === "postgres" ? "public" : "dbo";

  // Fetch schemas when database changes
  useEffect(() => {
    if (!database) {
      setSchemas([]);
      setSchema("");
      setCustomSchema(false);
      return;
    }
    setCustomSchema(false);
    listSchemas(database, connection || undefined)
      .then((result) => {
        const names = result.map((r) => r.schema_name);
        setSchemas(names);
        // Pre-select default schema
        const preferred = connType === "postgres" ? "public" : "dbo";
        if (names.includes(preferred)) {
          setSchema(preferred);
        } else if (names.length > 0) {
          setSchema(names[0]);
        } else {
          setSchema("");
        }
      })
      .catch(() => {
        setSchemas([]);
        setSchema("");
      });
  }, [database, connection, connType]);

  // Fetch tables when schema changes
  useEffect(() => {
    if (!database || !schema) {
      setTables([]);
      return;
    }
    listTables(database, connection || undefined, schema)
      .then((result) => {
        setTables(result.map((t) => t.TABLE_NAME));
      })
      .catch(() => {
        setTables([]);
      });
  }, [database, connection, schema]);

  const handleConnectionChange = useCallback(
    (name: string, defaultDb: string, type_: string) => {
      setConnection(name);
      setDatabase(defaultDb);
      setConnType(type_);
    },
    [],
  );

  const handleDatabaseChange = useCallback((name: string) => {
    setDatabase(name);
  }, []);

  // Handle file drop
  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const f = e.dataTransfer.files[0];
    if (f) setFile(f);
  }, []);

  const handleUpload = async () => {
    if (!file || !database || !tableName) return;
    setLoading(true);
    setError(null);

    try {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("database", database);
      formData.append("table_name", tableName);
      if (connection) formData.append("connection", connection);
      if (schema) formData.append("schema", schema);
      formData.append("has_header", hasHeader ? "true" : "false");

      const result = await previewImport(formData);
      setPreview(result);
      setColumns(
        result.columns.map((c) => ({ ...c, include: true })),
      );
      setStep("preview");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleImport = async () => {
    if (!preview) return;
    setLoading(true);
    setError(null);

    try {
      const res = await executeImport({
        preview_id: preview.preview_id,
        connection: connection || undefined,
        database,
        schema: schema || defaultSchema,
        table_name: tableName,
        if_exists: ifExists,
        columns: columns.map((c) => ({
          name: c.name,
          sql_type: c.sql_type,
          include: c.include,
        })),
      });
      setResult(res);
      setStep("result");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleReset = () => {
    setStep("upload");
    setPreview(null);
    setColumns([]);
    setResult(null);
    setFile(null);
    setTableName("");
    setNewTable(false);
    setError(null);
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  const updateColumnType = (idx: number, sql_type: string) => {
    setColumns((prev) =>
      prev.map((c, i) => (i === idx ? { ...c, sql_type } : c)),
    );
  };

  const toggleColumnInclude = (idx: number) => {
    setColumns((prev) =>
      prev.map((c, i) => (i === idx ? { ...c, include: !c.include } : c)),
    );
  };

  return (
    <div className="p-6 max-w-6xl mx-auto space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Import Data</h1>
        {step !== "upload" && (
          <Button variant="outline" size="sm" onClick={handleReset}>
            Start Over
          </Button>
        )}
      </div>

      {/* Step indicator */}
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Badge variant={step === "upload" ? "default" : "secondary"}>
          1. Upload
        </Badge>
        <span>→</span>
        <Badge variant={step === "preview" ? "default" : "secondary"}>
          2. Preview
        </Badge>
        <span>→</span>
        <Badge variant={step === "result" ? "default" : "secondary"}>
          3. Import
        </Badge>
      </div>

      {error && (
        <div className="bg-destructive/10 border border-destructive/30 text-destructive rounded-md p-3 text-sm">
          {error}
        </div>
      )}

      {/* Step 1: Upload */}
      {step === "upload" && (
        <Card>
          <CardHeader>
            <CardTitle>Upload File</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <ConnectionPicker
              connection={connection}
              database={database}
              onConnectionChange={handleConnectionChange}
              onDatabaseChange={handleDatabaseChange}
            />

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-1.5">
                <label className="text-sm font-medium">Schema</label>
                {customSchema ? (
                  <div className="flex gap-2">
                    <Input
                      value={schema}
                      onChange={(e) => setSchema(e.target.value)}
                      placeholder="schema_name"
                      className="flex-1"
                    />
                    <Button
                      variant="ghost"
                      size="sm"
                      className="shrink-0 text-xs"
                      onClick={() => {
                        setCustomSchema(false);
                        const preferred = connType === "postgres" ? "public" : "dbo";
                        if (schemas.includes(preferred)) setSchema(preferred);
                        else if (schemas.length > 0) setSchema(schemas[0]);
                      }}
                    >
                      Back to list
                    </Button>
                  </div>
                ) : (
                  <Select
                    value={schema}
                    onValueChange={(v) => {
                      if (v === "__custom__") {
                        setCustomSchema(true);
                        setSchema("");
                      } else {
                        setSchema(v);
                      }
                    }}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder={schemas.length ? "Select schema" : defaultSchema} />
                    </SelectTrigger>
                    <SelectContent>
                      {schemas.map((s) => (
                        <SelectItem key={s} value={s}>
                          {s}
                        </SelectItem>
                      ))}
                      <SelectItem value="__custom__">Custom...</SelectItem>
                    </SelectContent>
                  </Select>
                )}
              </div>
              <div className="space-y-1.5">
                <label className="text-sm font-medium">Table Name</label>
                {newTable ? (
                  <div className="flex gap-2">
                    <Input
                      value={tableName}
                      onChange={(e) => setTableName(e.target.value)}
                      placeholder="my_table"
                      className="flex-1"
                    />
                    {tables.length > 0 && (
                      <Button
                        variant="ghost"
                        size="sm"
                        className="shrink-0 text-xs"
                        onClick={() => {
                          setNewTable(false);
                          setTableName("");
                          setIfExists("create");
                        }}
                      >
                        Back to list
                      </Button>
                    )}
                  </div>
                ) : (
                  <Select
                    value={tableName}
                    onValueChange={(v) => {
                      if (v === "__new__") {
                        setNewTable(true);
                        setTableName("");
                        setIfExists("create");
                      } else {
                        setTableName(v);
                        setIfExists("append");
                      }
                    }}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder={tables.length ? "Select table" : "my_table"} />
                    </SelectTrigger>
                    <SelectContent>
                      {tables.map((t) => (
                        <SelectItem key={t} value={t}>
                          {t}
                        </SelectItem>
                      ))}
                      <SelectItem value="__new__">New table...</SelectItem>
                    </SelectContent>
                  </Select>
                )}
              </div>
            </div>

            <div className="flex items-center gap-3">
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={hasHeader}
                  onChange={(e) => setHasHeader(e.target.checked)}
                  className="rounded"
                />
                First row is header
              </label>
            </div>

            {/* File drop zone */}
            <div
              onDrop={handleDrop}
              onDragOver={(e) => e.preventDefault()}
              className="border-2 border-dashed border-border rounded-lg p-8 text-center cursor-pointer hover:border-primary/50 transition-colors"
              onClick={() => fileInputRef.current?.click()}
            >
              <input
                ref={fileInputRef}
                type="file"
                accept=".csv,.tsv,.xlsx,.xls,.xlsb,.ods"
                className="hidden"
                onChange={(e) => setFile(e.target.files?.[0] ?? null)}
              />
              {file ? (
                <div className="space-y-1">
                  <p className="font-medium">{file.name}</p>
                  <p className="text-sm text-muted-foreground">
                    {(file.size / 1024).toFixed(1)} KB — Click or drop to change
                  </p>
                </div>
              ) : (
                <div className="space-y-1">
                  <p className="text-muted-foreground">
                    Drop a CSV or Excel file here, or click to browse
                  </p>
                  <p className="text-xs text-muted-foreground">
                    Supports .csv, .tsv, .xlsx, .xls, .xlsb, .ods (max 50MB)
                  </p>
                </div>
              )}
            </div>

            <Button
              onClick={handleUpload}
              disabled={!file || !database || !tableName || loading}
            >
              {loading ? "Uploading..." : "Upload & Preview"}
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Step 2: Preview */}
      {step === "preview" && preview && (
        <>
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                <span>Schema Preview</span>
                <span className="text-sm font-normal text-muted-foreground">
                  {preview.file_name} — {preview.total_rows.toLocaleString()} rows
                </span>
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex items-center gap-4">
                <label className="text-sm font-medium">Mode:</label>
                <Select
                  value={ifExists}
                  onValueChange={(v) => setIfExists(v as "create" | "append")}
                >
                  <SelectTrigger className="w-[200px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="create">Create new table</SelectItem>
                    <SelectItem value="append">Append to existing</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {/* Column schema table */}
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-10">Include</TableHead>
                    <TableHead>Column</TableHead>
                    <TableHead>Detected Type</TableHead>
                    <TableHead>SQL Type</TableHead>
                    <TableHead>Nullable</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {columns.map((col, idx) => (
                    <TableRow
                      key={col.name}
                      className={!col.include ? "opacity-50" : ""}
                    >
                      <TableCell>
                        <input
                          type="checkbox"
                          checked={col.include}
                          onChange={() => toggleColumnInclude(idx)}
                        />
                      </TableCell>
                      <TableCell className="font-mono text-sm">
                        {col.name}
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">{col.inferred_type}</Badge>
                      </TableCell>
                      <TableCell>
                        <Input
                          value={col.sql_type}
                          onChange={(e) =>
                            updateColumnType(idx, e.target.value)
                          }
                          className="h-8 w-48 font-mono text-xs"
                        />
                      </TableCell>
                      <TableCell>
                        {col.nullable ? (
                          <Badge variant="secondary">NULL</Badge>
                        ) : (
                          <Badge variant="outline">NOT NULL</Badge>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          {/* Data preview */}
          <Card>
            <CardHeader>
              <CardTitle>Data Preview (first {preview.preview_rows.length} rows)</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      {preview.columns.map((col) => (
                        <TableHead key={col.name} className="font-mono text-xs">
                          {col.name}
                        </TableHead>
                      ))}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {preview.preview_rows.map((row, i) => (
                      <TableRow key={i}>
                        {row.map((cell, j) => (
                          <TableCell
                            key={j}
                            className="text-xs max-w-[200px] truncate"
                          >
                            {cell ?? (
                              <span className="text-muted-foreground italic">
                                NULL
                              </span>
                            )}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>

          <div className="flex items-center gap-3">
            <Button onClick={handleImport} disabled={loading}>
              {loading
                ? "Importing..."
                : `Import ${preview.total_rows.toLocaleString()} rows`}
            </Button>
            <span className="text-sm text-muted-foreground">
              into {schema || defaultSchema}.{tableName}
            </span>
          </div>
        </>
      )}

      {/* Step 3: Result */}
      {step === "result" && result && (
        <Card>
          <CardHeader>
            <CardTitle>Import Complete</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid grid-cols-2 gap-4 max-w-md">
              <div className="text-sm text-muted-foreground">Rows imported</div>
              <div className="font-medium">
                {result.rows_imported.toLocaleString()}
              </div>
              <div className="text-sm text-muted-foreground">Batches</div>
              <div className="font-medium">{result.batches}</div>
              <div className="text-sm text-muted-foreground">Table created</div>
              <div className="font-medium">
                {result.table_created ? "Yes" : "No (appended)"}
              </div>
              <div className="text-sm text-muted-foreground">Time</div>
              <div className="font-medium">
                {(result.execution_time_ms / 1000).toFixed(2)}s
              </div>
            </div>

            <Button onClick={handleReset} className="mt-4">
              Import Another File
            </Button>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
