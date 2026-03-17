import { useState, useEffect, useRef, useCallback } from "react";
import SqlEditor, { type SqlEditorHandle } from "../components/SqlEditor";
import ResultsTable, { ErrorBanner } from "../components/ResultsTable";
import type { QueryResult, WorkspaceTable } from "../lib/api";
import {
  workspaceListTables,
  workspaceUpload,
  workspaceDeleteTable,
  workspaceClear,
  workspaceQuery,
} from "../lib/api";
import { Download } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

const ACCEPTED_EXTENSIONS = ".csv,.tsv,.json,.parquet,.xlsx,.xls";

export default function WorkspacePage() {
  const [tables, setTables] = useState<WorkspaceTable[]>([]);
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [showClearDialog, setShowClearDialog] = useState(false);
  const [tableName, setTableName] = useState("");
  const [file, setFile] = useState<File | null>(null);

  const editorRef = useRef<SqlEditorHandle>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const loadTables = useCallback(async () => {
    try {
      const t = await workspaceListTables();
      setTables(t);
    } catch {
      // silently ignore — workspace may not be enabled
    }
  }, []);

  useEffect(() => {
    loadTables();
  }, [loadTables]);

  // Upload
  const handleUpload = async () => {
    if (!file) return;
    setUploading(true);
    setError(null);
    try {
      const formData = new FormData();
      formData.append("file", file);
      if (tableName.trim()) formData.append("table_name", tableName.trim());
      await workspaceUpload(formData);
      setFile(null);
      setTableName("");
      if (fileInputRef.current) fileInputRef.current.value = "";
      await loadTables();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setUploading(false);
    }
  };

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const f = e.dataTransfer.files[0];
    if (f) setFile(f);
  }, []);

  // Delete
  const handleDelete = async (name: string) => {
    try {
      await workspaceDeleteTable(name);
      await loadTables();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  // Clear all
  const handleClearAll = async () => {
    try {
      await workspaceClear();
      setShowClearDialog(false);
      await loadTables();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  // Query
  const handleRun = async () => {
    const sql = editorRef.current?.getValue()?.trim();
    if (!sql) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const r = await workspaceQuery(sql);
      setResult(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  // Select table → populate editor and run
  const handleSelectTable = async (name: string) => {
    const sql = `SELECT * FROM ${name}`;
    editorRef.current?.replaceAll(sql);
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const r = await workspaceQuery(sql);
      setResult(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  // Download table as CSV
  const handleDownloadTable = async (name: string) => {
    try {
      const r = await workspaceQuery(`SELECT * FROM ${name}`);
      if (r.data.length === 0) return;
      const cols = r.metadata
        ? r.metadata.columns.map((c) => c.name)
        : Object.keys(r.data[0]);
      const escape = (v: unknown) => {
        const s = v === null || v === undefined ? "" : String(v);
        return s.includes(",") || s.includes('"') || s.includes("\n")
          ? `"${s.replace(/"/g, '""')}"`
          : s;
      };
      const rows = [
        cols.join(","),
        ...r.data.map((row) => cols.map((c) => escape(row[c])).join(",")),
      ];
      const blob = new Blob([rows.join("\n")], { type: "text/csv" });
      downloadBlob(blob, `${name}.csv`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  // Client-side export
  const downloadJson = () => {
    if (!result) return;
    const blob = new Blob([JSON.stringify(result.data, null, 2)], { type: "application/json" });
    downloadBlob(blob, "workspace-results.json");
  };

  const downloadCsv = () => {
    if (!result || result.data.length === 0) return;
    const cols = result.metadata
      ? result.metadata.columns.map((c) => c.name)
      : Object.keys(result.data[0]);
    const escape = (v: unknown) => {
      const s = v === null || v === undefined ? "" : String(v);
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    };
    const rows = [
      cols.join(","),
      ...result.data.map((row) => cols.map((c) => escape(row[c])).join(",")),
    ];
    const blob = new Blob([rows.join("\n")], { type: "text/csv" });
    downloadBlob(blob, "workspace-results.csv");
  };

  return (
    <div className="flex flex-col h-full">
      {/* Top half: Upload + Tables */}
      <div className="flex gap-4 p-4 min-h-0" style={{ flex: "0 0 auto" }}>
        {/* Left — File drop zone */}
        <div className="w-80 shrink-0 space-y-3">
          <h2 className="text-lg font-semibold">Upload</h2>
          <div
            onDrop={handleDrop}
            onDragOver={(e) => e.preventDefault()}
            className="border-2 border-dashed border-border rounded-lg p-6 text-center cursor-pointer hover:border-primary/50 transition-colors"
            onClick={() => fileInputRef.current?.click()}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept={ACCEPTED_EXTENSIONS}
              className="hidden"
              onChange={(e) => setFile(e.target.files?.[0] ?? null)}
            />
            {file ? (
              <div className="space-y-1">
                <p className="font-medium text-sm">{file.name}</p>
                <p className="text-xs text-muted-foreground">
                  {(file.size / 1024).toFixed(1)} KB — Click or drop to change
                </p>
              </div>
            ) : (
              <div className="space-y-1">
                <p className="text-sm text-muted-foreground">
                  Drop a file here, or click to browse
                </p>
                <p className="text-xs text-muted-foreground">
                  .csv, .tsv, .json, .parquet, .xlsx, .xls
                </p>
              </div>
            )}
          </div>
          <Input
            value={tableName}
            onChange={(e) => setTableName(e.target.value)}
            placeholder="Table name (optional)"
          />
          <Button
            onClick={handleUpload}
            disabled={!file || uploading}
            className="w-full"
          >
            {uploading ? "Uploading..." : "Upload"}
          </Button>
        </div>

        {/* Right — Tables list */}
        <div className="flex-1 min-w-0 space-y-2">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold">Tables</h2>
            {tables.length > 0 && (
              <Button
                variant="destructive"
                size="sm"
                onClick={() => setShowClearDialog(true)}
              >
                Clear All
              </Button>
            )}
          </div>
          {tables.length === 0 ? (
            <p className="text-sm text-muted-foreground py-4">
              No tables in workspace. Upload a file or import via MCP.
            </p>
          ) : (
            <div className="overflow-auto border rounded-lg" style={{ maxHeight: "240px" }}>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Source</TableHead>
                    <TableHead className="text-right">Rows</TableHead>
                    <TableHead className="text-right">Cols</TableHead>
                    <TableHead>Uploaded</TableHead>
                    <TableHead className="w-10" />
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {tables.map((t) => (
                    <TableRow key={t.table_name}>
                      <TableCell>
                        <button
                          type="button"
                          className="font-mono text-sm text-primary hover:underline cursor-pointer"
                          onClick={() => handleSelectTable(t.table_name)}
                        >
                          {t.table_name}
                        </button>
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground max-w-[150px] truncate">
                        {t.original_filename}
                      </TableCell>
                      <TableCell className="text-right">{t.row_count.toLocaleString()}</TableCell>
                      <TableCell className="text-right">{t.column_count}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {new Date(t.uploaded_at).toLocaleString()}
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0"
                            onClick={() => handleDownloadTable(t.table_name)}
                            title="Download as CSV"
                          >
                            <Download className="size-3.5" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0 text-destructive hover:text-destructive"
                            onClick={() => handleDelete(t.table_name)}
                            title="Delete table"
                          >
                            &times;
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </div>
      </div>

      {/* Error banner (shared) */}
      {error && (
        <div className="px-4">
          <ErrorBanner message={error} />
        </div>
      )}

      {/* Bottom half: Query editor + Results */}
      <div className="flex-1 flex flex-col min-h-0 p-4 pt-0 gap-3">
        <div className="flex items-center gap-2">
          <h2 className="text-lg font-semibold">Query</h2>
          <Badge variant="secondary" className="text-xs">DuckDB</Badge>
        </div>
        <SqlEditor ref={editorRef} onExecute={handleRun} placeholder="Enter DuckDB SQL..." />
        <div className="flex items-center gap-2">
          <Button onClick={handleRun} disabled={loading}>
            {loading ? "Running..." : "Run"}
          </Button>
          <span className="text-xs text-muted-foreground">Ctrl+Enter</span>
        </div>

        {result && (
          <div className="flex-1 overflow-auto">
            <ResultsTable
              result={result}
              actions={
                <div className="flex gap-2">
                  <Button variant="outline" size="sm" onClick={downloadCsv}>
                    CSV
                  </Button>
                  <Button variant="outline" size="sm" onClick={downloadJson}>
                    JSON
                  </Button>
                </div>
              }
            />
          </div>
        )}
      </div>

      {/* Clear All confirmation dialog */}
      <Dialog open={showClearDialog} onOpenChange={setShowClearDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Clear Workspace</DialogTitle>
            <DialogDescription>
              This will drop all {tables.length} table{tables.length !== 1 ? "s" : ""}. This cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowClearDialog(false)}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={handleClearAll}>
              Clear All
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
