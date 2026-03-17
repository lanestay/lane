import { useState } from "react";
import type { QueryResult, ColumnInfo } from "../lib/api";
import { executeQuery } from "../lib/api";
import { generateUpdate, type CellChange, type Dialect } from "../lib/sql-gen";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

interface StatementResult {
  status: "pending" | "success" | "error" | "warning";
  message?: string;
}

interface ReviewChangesDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  dirty: Map<number, Map<string, CellChange>>;
  result: QueryResult;
  columns: ColumnInfo[];
  schema: string;
  table: string;
  database: string;
  connection: string;
  dialect: Dialect;
  onCommitted: () => void;
}

export default function ReviewChangesDialog({
  open,
  onOpenChange,
  dirty,
  result,
  columns,
  schema,
  table,
  database,
  connection,
  dialect,
  onCommitted,
}: ReviewChangesDialogProps) {
  const [executing, setExecuting] = useState(false);
  const [results, setResults] = useState<Map<number, StatementResult>>(new Map());

  // Build statements for each changed row
  const entries = Array.from(dirty.entries())
    .filter(([, changes]) => changes.size > 0)
    .map(([rowIndex, changes]) => {
      const rowData = result.data[rowIndex];
      const sql = generateUpdate(schema, table, columns, rowData, changes, dialect);
      return { rowIndex, changes, rowData, sql };
    });

  const executeAll = async () => {
    setExecuting(true);
    const newResults = new Map<number, StatementResult>();
    let allSuccess = true;

    for (const entry of entries) {
      try {
        const res = await executeQuery(entry.sql, database, connection || undefined);
        if (res.total_rows === 0) {
          newResults.set(entry.rowIndex, {
            status: "warning",
            message: "0 rows affected — row may have been modified or deleted",
          });
          allSuccess = false;
        } else {
          newResults.set(entry.rowIndex, { status: "success" });
        }
      } catch (e) {
        newResults.set(entry.rowIndex, {
          status: "error",
          message: e instanceof Error ? e.message : String(e),
        });
        allSuccess = false;
      }
      setResults(new Map(newResults));
    }

    setExecuting(false);
    if (allSuccess) {
      onCommitted();
    }
  };

  const hasResults = results.size > 0;
  const allDone = hasResults && entries.every((e) => results.has(e.rowIndex));
  const allSuccess = allDone && entries.every((e) => results.get(e.rowIndex)?.status === "success");

  return (
    <Dialog open={open} onOpenChange={executing ? undefined : onOpenChange}>
      <DialogContent className="sm:max-w-2xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Review Changes</DialogTitle>
          <DialogDescription>
            {entries.length} UPDATE statement{entries.length !== 1 ? "s" : ""} will be executed on {schema}.{table}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {entries.map(({ rowIndex, changes, sql }) => {
            const stmtResult = results.get(rowIndex);
            return (
              <div key={rowIndex} className="border border-border rounded-lg p-3 space-y-2">
                <div className="flex items-center gap-2">
                  <span className="text-xs text-muted-foreground">Row {rowIndex + 1}</span>
                  {stmtResult?.status === "success" && (
                    <Badge variant="secondary" className="text-green-400">Success</Badge>
                  )}
                  {stmtResult?.status === "warning" && (
                    <Badge variant="secondary" className="text-yellow-400">Warning</Badge>
                  )}
                  {stmtResult?.status === "error" && (
                    <Badge variant="destructive">Error</Badge>
                  )}
                </div>

                {/* Diff */}
                <div className="space-y-1">
                  {Array.from(changes.entries()).map(([colName, change]) => (
                    <div key={colName} className="flex items-center gap-2 text-xs font-mono">
                      <span className="text-muted-foreground w-32 truncate shrink-0">{colName}</span>
                      <span className="text-red-400 line-through">
                        {change.original === null || change.original === undefined ? "NULL" : String(change.original)}
                      </span>
                      <span className="text-muted-foreground">→</span>
                      <span className="text-green-400">
                        {change.current === null || change.current === undefined ? "NULL" : String(change.current)}
                      </span>
                    </div>
                  ))}
                </div>

                {/* SQL */}
                <pre className="text-xs bg-muted p-2 rounded overflow-x-auto whitespace-pre-wrap break-all">
                  {sql}
                </pre>

                {/* Error message */}
                {stmtResult?.message && (
                  <p className={`text-xs ${stmtResult.status === "error" ? "text-destructive" : "text-yellow-400"}`}>
                    {stmtResult.message}
                  </p>
                )}
              </div>
            );
          })}
        </div>

        <DialogFooter>
          {allSuccess ? (
            <Button onClick={onCommitted}>Done</Button>
          ) : (
            <>
              <Button
                variant="ghost"
                onClick={() => onOpenChange(false)}
                disabled={executing}
              >
                Cancel
              </Button>
              <Button onClick={executeAll} disabled={executing}>
                {executing
                  ? "Executing..."
                  : hasResults
                    ? "Retry"
                    : `Execute ${entries.length} UPDATE${entries.length !== 1 ? "s" : ""}`
                }
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
