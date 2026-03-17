import { useState, useRef, useEffect, useCallback } from "react";
import type { QueryResult, ColumnInfo } from "../lib/api";
import { hasPrimaryKey, isNumericType, type Dialect, type CellChange, type SortSpec, type ColumnFilter } from "../lib/sql-gen";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ArrowUp, ArrowDown, ChevronsUpDown, ListFilter } from "lucide-react";
import ColumnFilterPopover from "./ColumnFilterPopover";
import ReviewChangesDialog from "./ReviewChangesDialog";

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

interface EditableCellProps {
  value: unknown;
  isEditing: boolean;
  isDirty: boolean;
  isPk: boolean;
  dataType: string;
  onStartEdit: () => void;
  onCommit: (value: unknown) => void;
  onCancel: () => void;
}

function EditableCell({
  value,
  isEditing,
  isDirty,
  isPk,
  dataType,
  onStartEdit,
  onCommit,
  onCancel,
}: EditableCellProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [editValue, setEditValue] = useState("");
  const [isNull, setIsNull] = useState(false);

  useEffect(() => {
    if (isEditing && inputRef.current) {
      const isNullVal = value === null || value === undefined;
      setIsNull(isNullVal);
      setEditValue(isNullVal ? "" : formatValue(value));
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [isEditing, value]);

  const commit = useCallback(() => {
    if (isNull) {
      onCommit(null);
    } else {
      const trimmed = editValue;
      if (isNumericType(dataType) && trimmed !== "" && !isNaN(Number(trimmed))) {
        onCommit(Number(trimmed));
      } else {
        onCommit(trimmed);
      }
    }
  }, [isNull, editValue, dataType, onCommit]);

  if (isEditing) {
    return (
      <div className="flex items-center gap-1 min-w-[120px]">
        <Input
          ref={inputRef}
          className="h-7 text-xs font-mono px-1"
          value={isNull ? "" : editValue}
          disabled={isNull}
          placeholder={isNull ? "NULL" : ""}
          onChange={(e) => { setEditValue(e.target.value); setIsNull(false); }}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === "Tab") {
              e.preventDefault();
              commit();
            } else if (e.key === "Escape") {
              onCancel();
            }
          }}
          onBlur={commit}
        />
        <Button
          variant={isNull ? "secondary" : "ghost"}
          size="sm"
          className="h-7 px-1.5 text-[10px] shrink-0"
          onMouseDown={(e) => e.preventDefault()} // prevent blur before click
          onClick={() => {
            if (isNull) {
              setIsNull(false);
              setEditValue("");
              inputRef.current?.focus();
            } else {
              setIsNull(true);
              setEditValue("");
            }
          }}
        >
          NULL
        </Button>
      </div>
    );
  }

  const display = formatValue(value);
  const isNullDisplay = value === null || value === undefined;

  return (
    <div
      className={`whitespace-nowrap font-mono text-xs cursor-pointer px-1 py-0.5 rounded -mx-1 -my-0.5 ${
        isDirty ? "border-l-2 border-yellow-400 pl-1.5" : ""
      } ${isPk ? "cursor-default text-muted-foreground" : "hover:bg-accent"} ${
        isNullDisplay ? "text-muted-foreground italic" : ""
      }`}
      onClick={isPk ? undefined : onStartEdit}
    >
      {display}
    </div>
  );
}

interface EditableTableProps {
  result: QueryResult;
  columns: ColumnInfo[];
  schema: string;
  table: string;
  database: string;
  connection: string;
  dialect: Dialect;
  onRefresh: () => void;
  sort: SortSpec | null;
  onSortChange: (sort: SortSpec | null) => void;
  columnFilters: ColumnFilter[];
  onColumnFilterChange: (column: string, filter: ColumnFilter | null) => void;
}

export default function EditableTable({
  result,
  columns,
  schema,
  table,
  database,
  connection,
  dialect,
  onRefresh,
  sort,
  onSortChange,
  columnFilters,
  onColumnFilterChange,
}: EditableTableProps) {
  // Map<rowIndex, Map<columnName, CellChange>>
  const [dirty, setDirty] = useState<Map<number, Map<string, CellChange>>>(new Map());
  const [editingCell, setEditingCell] = useState<{ rowIndex: number; columnName: string } | null>(null);
  const [reviewOpen, setReviewOpen] = useState(false);

  const canEdit = hasPrimaryKey(columns);
  const pkSet = new Set(columns.filter((c) => c.IS_PRIMARY_KEY === "YES").map((c) => c.COLUMN_NAME));
  const colMap = new Map(columns.map((c) => [c.COLUMN_NAME, c]));

  const displayColumns = result.metadata
    ? result.metadata.columns.map((c) => c.name)
    : result.data.length > 0
      ? Object.keys(result.data[0])
      : [];

  const [openFilterCol, setOpenFilterCol] = useState<string | null>(null);
  const filterByColumn = new Map(columnFilters.map((f) => [f.column, f]));

  const cycleSort = (col: string) => {
    if (!sort || sort.column !== col) {
      onSortChange({ column: col, direction: "ASC" });
    } else if (sort.direction === "ASC") {
      onSortChange({ column: col, direction: "DESC" });
    } else {
      onSortChange(null);
    }
  };

  const totalChangedRows = dirty.size;
  const totalChangedCells = Array.from(dirty.values()).reduce((n, m) => n + m.size, 0);

  const getCellValue = (rowIndex: number, colName: string): unknown => {
    const change = dirty.get(rowIndex)?.get(colName);
    return change ? change.current : result.data[rowIndex][colName];
  };

  const commitEdit = (rowIndex: number, colName: string, newValue: unknown) => {
    setEditingCell(null);
    const originalValue = result.data[rowIndex][colName];

    // If value unchanged, remove from dirty
    const same = newValue === originalValue
      || (newValue === null && (originalValue === null || originalValue === undefined))
      || String(newValue) === String(originalValue);

    setDirty((prev) => {
      const next = new Map(prev);
      if (same) {
        const rowChanges = next.get(rowIndex);
        if (rowChanges) {
          rowChanges.delete(colName);
          if (rowChanges.size === 0) next.delete(rowIndex);
        }
      } else {
        if (!next.has(rowIndex)) next.set(rowIndex, new Map());
        next.get(rowIndex)!.set(colName, { original: originalValue, current: newValue });
      }
      return next;
    });
  };

  const discardAll = () => {
    setDirty(new Map());
    setEditingCell(null);
  };

  const handleCommitted = () => {
    setDirty(new Map());
    setEditingCell(null);
    setReviewOpen(false);
    onRefresh();
  };

  return (
    <div className="flex flex-col">
      {/* Header bar */}
      <div className="flex items-center gap-3 px-4 py-2 bg-card border border-border rounded-t-lg">
        <Badge variant="secondary" className="text-green-400">
          {result.total_rows} rows
        </Badge>
        <span className="text-muted-foreground text-sm">{result.execution_time_ms}ms</span>
        {!canEdit && (
          <span className="text-muted-foreground text-sm italic">Read-only (no primary key)</span>
        )}
        {totalChangedRows > 0 && (
          <>
            <Badge variant="outline" className="text-yellow-400 border-yellow-400/50">
              {totalChangedCells} changed
            </Badge>
            <div className="ml-auto flex items-center gap-2">
              <Button variant="ghost" size="sm" onClick={discardAll}>
                Discard
              </Button>
              <Button size="sm" onClick={() => setReviewOpen(true)}>
                Review Changes
              </Button>
            </div>
          </>
        )}
      </div>

      {/* Table */}
      <div className="overflow-x-auto border border-t-0 border-border rounded-b-lg">
        <Table>
          <TableHeader>
            <TableRow>
              {displayColumns.map((col) => {
                const isSorted = sort?.column === col;
                const colInfo = colMap.get(col);
                const hasFilter = filterByColumn.has(col);

                return (
                  <TableHead key={col} className="whitespace-nowrap px-2">
                    <div className="flex items-center gap-0.5">
                      <button
                        className="flex items-center gap-1 hover:text-foreground cursor-pointer select-none"
                        onClick={() => cycleSort(col)}
                      >
                        {col}
                        {pkSet.has(col) && (
                          <span className="text-yellow-400 text-[10px]">PK</span>
                        )}
                        {isSorted ? (
                          sort.direction === "ASC" ? (
                            <ArrowUp className="size-3.5 text-blue-400" />
                          ) : (
                            <ArrowDown className="size-3.5 text-blue-400" />
                          )
                        ) : (
                          <ChevronsUpDown className="size-3.5 opacity-30" />
                        )}
                      </button>
                      <ColumnFilterPopover
                        columnName={col}
                        dataType={colInfo?.DATA_TYPE ?? "varchar"}
                        isOpen={openFilterCol === col}
                        onOpenChange={(open) => setOpenFilterCol(open ? col : null)}
                        currentFilter={filterByColumn.get(col)}
                        onApply={(f) => onColumnFilterChange(col, f)}
                        onClear={() => onColumnFilterChange(col, null)}
                      >
                        <button
                          className={`p-0.5 rounded hover:bg-accent cursor-pointer ${hasFilter ? "text-blue-400" : "opacity-30 hover:opacity-70"}`}
                        >
                          <ListFilter className="size-3.5" />
                        </button>
                      </ColumnFilterPopover>
                    </div>
                  </TableHead>
                );
              })}
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.data.map((_, rowIdx) => (
              <TableRow key={rowIdx}>
                {displayColumns.map((col) => {
                  const isPk = pkSet.has(col);
                  const isEditing = editingCell?.rowIndex === rowIdx && editingCell?.columnName === col;
                  const isDirty = dirty.get(rowIdx)?.has(col) ?? false;
                  const cellValue = getCellValue(rowIdx, col);
                  const colInfo = colMap.get(col);

                  return (
                    <TableCell key={col} className="py-1 px-2">
                      <EditableCell
                        value={cellValue}
                        isEditing={isEditing}
                        isDirty={isDirty}
                        isPk={isPk || !canEdit}
                        dataType={colInfo?.DATA_TYPE ?? "varchar"}
                        onStartEdit={() => setEditingCell({ rowIndex: rowIdx, columnName: col })}
                        onCommit={(val) => commitEdit(rowIdx, col, val)}
                        onCancel={() => setEditingCell(null)}
                      />
                    </TableCell>
                  );
                })}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      {/* Review dialog */}
      {reviewOpen && (
        <ReviewChangesDialog
          open={reviewOpen}
          onOpenChange={setReviewOpen}
          dirty={dirty}
          result={result}
          columns={columns}
          schema={schema}
          table={table}
          database={database}
          connection={connection}
          dialect={dialect}
          onCommitted={handleCommitted}
        />
      )}
    </div>
  );
}
