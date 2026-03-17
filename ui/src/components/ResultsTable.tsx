import { useState, useEffect, useMemo } from "react";
import type { QueryResult, StorageColumnLink } from "../lib/api";
import { storageDownloadObject } from "../lib/api";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

const IMAGE_EXTENSIONS = new Set(["png", "jpg", "jpeg", "gif", "webp", "svg"]);

function isImageFile(filename: string): boolean {
  const ext = filename.split(".").pop()?.toLowerCase() || "";
  return IMAGE_EXTENSIONS.has(ext);
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

/** Thumbnail that fetches image via authenticated API and displays as blob URL */
function StorageImage({ storageConnection, bucket, objectKey }: {
  storageConnection: string;
  bucket: string;
  objectKey: string;
}) {
  const [src, setSrc] = useState<string | null>(null);

  useEffect(() => {
    let revoke: string | null = null;
    storageDownloadObject(storageConnection, bucket, objectKey)
      .then((blob) => {
        const url = URL.createObjectURL(blob);
        revoke = url;
        setSrc(url);
      })
      .catch(() => setSrc(null));
    return () => { if (revoke) URL.revokeObjectURL(revoke); };
  }, [storageConnection, bucket, objectKey]);

  if (!src) return <span className="text-blue-500 text-xs">{objectKey.split("/").pop()}</span>;
  return <img src={src} alt={objectKey} className="h-8 w-8 object-cover rounded" />;
}

/** Renders a cell value linked to storage */
function LinkedCell({ value, link }: { value: string; link: StorageColumnLink }) {
  const objectKey = (link.key_prefix || "") + value;

  const handleDownload = async () => {
    try {
      const blob = await storageDownloadObject(link.storage_connection, link.bucket_name, objectKey);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = value;
      a.click();
      URL.revokeObjectURL(url);
    } catch {
      // silently fail
    }
  };

  if (isImageFile(value)) {
    return (
      <button onClick={handleDownload} className="inline-flex items-center gap-1" title={value}>
        <StorageImage storageConnection={link.storage_connection} bucket={link.bucket_name} objectKey={objectKey} />
      </button>
    );
  }

  return (
    <button onClick={handleDownload} className="text-blue-500 hover:underline text-xs" title={`Download ${value}`}>
      {value}
    </button>
  );
}

export default function ResultsTable({
  result,
  actions,
  storageLinks,
  connection,
  database,
  showHeader = true,
}: {
  result: QueryResult;
  actions?: React.ReactNode;
  storageLinks?: StorageColumnLink[];
  connection?: string;
  database?: string;
  showHeader?: boolean;
}) {
  const columns = result.metadata
    ? result.metadata.columns.map((c) => c.name)
    : result.data.length > 0
      ? Object.keys(result.data[0])
      : [];

  // Build a map of column_name -> StorageColumnLink for matching connection+database
  const linkMap = useMemo(() => {
    const map = new Map<string, StorageColumnLink>();
    if (!storageLinks || !connection || !database) return map;
    for (const link of storageLinks) {
      if (link.connection_name === connection && link.database_name === database) {
        map.set(link.column_name, link);
      }
    }
    return map;
  }, [storageLinks, connection, database]);

  const renderCell = (col: string, value: unknown) => {
    const formatted = formatValue(value);
    if (formatted === "NULL" || formatted === "") return formatted;

    const link = linkMap.get(col);
    if (link && typeof value === "string" && value) {
      return <LinkedCell value={value} link={link} />;
    }
    return formatted;
  };

  return (
    <div className="flex flex-col">
      {showHeader && (
        <div className="flex items-center justify-between px-4 py-2 bg-card border border-border rounded-t-lg">
          <div className="flex items-center gap-3">
            <Badge variant="secondary" className="text-green-400">
              {result.total_rows} rows
            </Badge>
            <span className="text-muted-foreground text-sm">{result.execution_time_ms}ms</span>
            <span className="text-muted-foreground text-sm">{result.rows_per_second.toFixed(0)} rows/sec</span>
          </div>
          {actions}
        </div>
      )}
      <div className={`overflow-x-auto border border-border ${showHeader ? "border-t-0 rounded-b-lg" : "rounded-lg"}`}>
        <Table>
          <TableHeader>
            <TableRow>
              {columns.map((col) => (
                <TableHead key={col} className="whitespace-nowrap">
                  {col}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.data.map((row, i) => (
              <TableRow key={i}>
                {columns.map((col) => (
                  <TableCell key={col} className="whitespace-nowrap font-mono text-xs">
                    {renderCell(col, row[col])}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

export function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="bg-destructive/20 border border-destructive rounded-lg p-4">
      <p className="text-destructive font-mono text-sm whitespace-pre-wrap">{message}</p>
    </div>
  );
}
