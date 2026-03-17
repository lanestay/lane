import { useState } from "react";
import { Download } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { exportQuery } from "../lib/api";
import type { QueryResult } from "../lib/api";

interface ExportButtonProps {
  query: string;
  database: string;
  connection?: string;
  result: QueryResult;
}

export default function ExportButton({
  query,
  database,
  connection,
  result,
}: ExportButtonProps) {
  const [open, setOpen] = useState(false);
  const [exporting, setExporting] = useState<string | null>(null);

  const handleExport = async (format: "csv" | "xlsx" | "json") => {
    if (format === "json") {
      // Client-side JSON download — no server round-trip
      const blob = new Blob([JSON.stringify(result.data, null, 2)], {
        type: "application/json",
      });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "results.json";
      a.click();
      URL.revokeObjectURL(url);
      setOpen(false);
      return;
    }

    setExporting(format);
    try {
      const res = await exportQuery(query, database, format, connection);
      if (res.download_url) {
        // Trigger browser download
        const a = document.createElement("a");
        a.href = res.download_url;
        a.download = `results.${format}`;
        a.click();
      }
    } catch (e) {
      console.error("Export failed:", e);
    }
    setExporting(null);
    setOpen(false);
  };

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm">
          <Download />
          Export
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-44 p-1">
        <button
          className="w-full text-left px-3 py-1.5 text-sm rounded hover:bg-accent disabled:opacity-50"
          onClick={() => handleExport("csv")}
          disabled={!!exporting}
        >
          {exporting === "csv" ? "Exporting..." : "CSV (.csv)"}
        </button>
        <button
          className="w-full text-left px-3 py-1.5 text-sm rounded hover:bg-accent disabled:opacity-50"
          onClick={() => handleExport("xlsx")}
          disabled={!!exporting}
        >
          {exporting === "xlsx" ? "Exporting..." : "Excel (.xlsx)"}
        </button>
        <button
          className="w-full text-left px-3 py-1.5 text-sm rounded hover:bg-accent disabled:opacity-50"
          onClick={() => handleExport("json")}
          disabled={!!exporting}
        >
          JSON (.json)
        </button>
      </PopoverContent>
    </Popover>
  );
}
