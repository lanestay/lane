import { useEffect, useState, useCallback } from "react";
import { listHistory, toggleHistoryFavorite, deleteHistoryEntry } from "../lib/api";
import type { QueryHistoryEntry } from "../lib/api";
import { Button } from "@/components/ui/button";

interface Props {
  onSelect: (sql: string) => void;
  refreshTrigger: number;
}

export default function QueryHistory({ onSelect, refreshTrigger }: Props) {
  const [entries, setEntries] = useState<QueryHistoryEntry[]>([]);
  const [tab, setTab] = useState<"recent" | "favorites">("recent");
  const [search, setSearch] = useState("");

  const load = useCallback(async () => {
    try {
      const data = await listHistory({
        limit: 50,
        search: search || undefined,
        favorites_only: tab === "favorites" ? true : undefined,
      });
      setEntries(data);
    } catch {
      // silently fail
    }
  }, [tab, search]);

  useEffect(() => {
    load();
  }, [load, refreshTrigger]);

  const handleFavorite = async (entry: QueryHistoryEntry) => {
    try {
      await toggleHistoryFavorite(entry.id, !entry.is_favorite);
      load();
    } catch {
      // ignore
    }
  };

  const handleDelete = async (id: number) => {
    try {
      await deleteHistoryEntry(id);
      load();
    } catch {
      // ignore
    }
  };

  return (
    <div className="border rounded-md p-3 space-y-3 bg-muted/30">
      <div className="flex items-center gap-2">
        <div className="flex rounded-md border overflow-hidden text-sm">
          <button
            className={`px-3 py-1 ${tab === "recent" ? "bg-primary text-primary-foreground" : "hover:bg-muted"}`}
            onClick={() => setTab("recent")}
          >
            Recent
          </button>
          <button
            className={`px-3 py-1 ${tab === "favorites" ? "bg-primary text-primary-foreground" : "hover:bg-muted"}`}
            onClick={() => setTab("favorites")}
          >
            Favorites
          </button>
        </div>
        <input
          type="text"
          placeholder="Search queries..."
          className="flex-1 text-sm border rounded-md px-2 py-1 bg-background"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>

      <div className="max-h-64 overflow-y-auto space-y-1">
        {entries.length === 0 ? (
          <div className="text-sm text-muted-foreground text-center py-4">
            No queries found
          </div>
        ) : (
          entries.map((entry) => (
            <div
              key={entry.id}
              className="flex items-start gap-2 p-2 rounded hover:bg-muted cursor-pointer text-sm group"
              onClick={() => onSelect(entry.sql_text)}
            >
              <span
                className={`shrink-0 mt-0.5 w-2 h-2 rounded-full ${entry.is_success ? "bg-green-500" : "bg-red-500"}`}
              />
              <div className="flex-1 min-w-0">
                <div className="font-mono text-xs truncate">{entry.sql_text}</div>
                <div className="text-xs text-muted-foreground">
                  {entry.execution_time_ms}ms
                  {entry.row_count != null && ` \u00b7 ${entry.row_count} rows`}
                  {entry.connection_name && ` \u00b7 ${entry.connection_name}`}
                </div>
              </div>
              <div className="flex gap-1 shrink-0 opacity-0 group-hover:opacity-100">
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 w-6 p-0"
                  onClick={(e) => {
                    e.stopPropagation();
                    handleFavorite(entry);
                  }}
                >
                  {entry.is_favorite ? "\u2605" : "\u2606"}
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 w-6 p-0 text-destructive"
                  onClick={(e) => {
                    e.stopPropagation();
                    handleDelete(entry.id);
                  }}
                >
                  \u00d7
                </Button>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
