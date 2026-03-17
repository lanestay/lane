import { useState, useEffect, useRef, useCallback } from "react";
import { Search, Table2, ScrollText, Blocks, X } from "lucide-react";
import { Dialog, DialogContent } from "@/components/ui/dialog";
import {
  searchAll,
  type UnifiedSearchResult,
  type SchemaSearchResult,
  type QuerySearchResult,
  type EndpointSearchResult,
} from "@/lib/api";

export default function SearchBar() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<UnifiedSearchResult | null>(null);
  const [loading, setLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  // Cmd+K / Ctrl+K shortcut
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setOpen((prev) => !prev);
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, []);

  // Focus input when dialog opens
  useEffect(() => {
    if (open) {
      setTimeout(() => inputRef.current?.focus(), 50);
    } else {
      setQuery("");
      setResults(null);
    }
  }, [open]);

  // Debounced search
  const doSearch = useCallback(async (q: string) => {
    if (!q.trim()) {
      setResults(null);
      return;
    }
    setLoading(true);
    try {
      const res = await searchAll(q, 10);
      setResults(res);
    } catch {
      // ignore
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => doSearch(query), 300);
    return () => clearTimeout(debounceRef.current);
  }, [query, doSearch]);

  const handleSchemaSelect = (item: SchemaSearchResult) => {
    setOpen(false);
    const params = new URLSearchParams();
    if (item.connection) params.set("connection", item.connection);
    if (item.database) params.set("database", item.database);
    window.location.href = `/?${params}`;
  };

  const handleQuerySelect = (item: QuerySearchResult) => {
    setOpen(false);
    // Store SQL in sessionStorage for the editor to pick up
    sessionStorage.setItem("search_sql", item.sql_text);
    window.location.href = "/";
  };

  const handleEndpointSelect = (_item: EndpointSearchResult) => {
    setOpen(false);
    window.location.href = "/admin";
  };

  const totalResults =
    (results?.schema?.length ?? 0) +
    (results?.queries?.length ?? 0) +
    (results?.endpoints?.length ?? 0);

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="flex items-center gap-2 rounded-full border border-border bg-muted/50 px-3 py-1.5 text-sm text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
      >
        <Search className="size-3.5" />
        <span className="hidden sm:inline">Search...</span>
        <kbd className="ml-1 hidden rounded border border-border bg-background px-1.5 py-0.5 text-[0.65rem] font-medium text-muted-foreground sm:inline">
          {navigator.platform.includes("Mac") ? "\u2318" : "Ctrl"}K
        </kbd>
      </button>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent
          showCloseButton={false}
          className="top-[20%] translate-y-0 gap-0 overflow-hidden p-0 sm:max-w-xl"
        >
          <div className="flex items-center gap-3 border-b px-4 py-3">
            <Search className="size-4 shrink-0 text-muted-foreground" />
            <input
              ref={inputRef}
              type="text"
              placeholder="Search tables, queries, endpoints..."
              className="flex-1 bg-transparent text-sm outline-none placeholder:text-muted-foreground"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Escape") setOpen(false);
              }}
            />
            {query && (
              <button
                type="button"
                onClick={() => setQuery("")}
                className="rounded p-0.5 text-muted-foreground hover:text-foreground"
              >
                <X className="size-3.5" />
              </button>
            )}
          </div>

          <div className="max-h-[60vh] overflow-y-auto">
            {loading && (
              <div className="px-4 py-8 text-center text-sm text-muted-foreground">
                Searching...
              </div>
            )}

            {!loading && query && totalResults === 0 && (
              <div className="px-4 py-8 text-center text-sm text-muted-foreground">
                No results found for "{query}"
              </div>
            )}

            {!loading && results && totalResults > 0 && (
              <div className="py-2">
                {results.schema.length > 0 && (
                  <ResultGroup label="Schema">
                    {results.schema.map((item, i) => (
                      <ResultItem
                        key={`s-${i}`}
                        icon={<Table2 className="size-3.5" />}
                        title={`${item.schema}.${item.object_name}`}
                        subtitle={`${item.connection} / ${item.database} \u00b7 ${item.object_type}`}
                        detail={item.columns ? item.columns.split(" ").slice(0, 6).join(", ") + (item.columns.split(" ").length > 6 ? "..." : "") : ""}
                        onClick={() => handleSchemaSelect(item)}
                      />
                    ))}
                  </ResultGroup>
                )}

                {results.queries.length > 0 && (
                  <ResultGroup label="Query History">
                    {results.queries.map((item, i) => (
                      <ResultItem
                        key={`q-${i}`}
                        icon={<ScrollText className="size-3.5" />}
                        title={item.sql_text.slice(0, 80) + (item.sql_text.length > 80 ? "..." : "")}
                        subtitle={`${item.email} \u00b7 ${item.connection || "default"} / ${item.database}`}
                        onClick={() => handleQuerySelect(item)}
                      />
                    ))}
                  </ResultGroup>
                )}

                {results.endpoints.length > 0 && (
                  <ResultGroup label="Endpoints">
                    {results.endpoints.map((item, i) => (
                      <ResultItem
                        key={`e-${i}`}
                        icon={<Blocks className="size-3.5" />}
                        title={item.name}
                        subtitle={`${item.connection} / ${item.database}${item.description ? ` \u00b7 ${item.description}` : ""}`}
                        onClick={() => handleEndpointSelect(item)}
                      />
                    ))}
                  </ResultGroup>
                )}
              </div>
            )}

            {!query && (
              <div className="px-4 py-8 text-center text-sm text-muted-foreground">
                Type to search tables, columns, queries, and endpoints
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}

function ResultGroup({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="mb-1">
      <div className="px-4 py-1.5 text-[0.68rem] font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      {children}
    </div>
  );
}

function ResultItem({
  icon,
  title,
  subtitle,
  detail,
  onClick,
}: {
  icon: React.ReactNode;
  title: string;
  subtitle: string;
  detail?: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      className="flex w-full items-start gap-3 px-4 py-2.5 text-left transition-colors hover:bg-muted/50"
      onClick={onClick}
    >
      <span className="mt-0.5 flex size-7 shrink-0 items-center justify-center rounded-lg bg-muted text-muted-foreground">
        {icon}
      </span>
      <div className="min-w-0 flex-1">
        <p className="truncate text-sm font-medium">{title}</p>
        <p className="truncate text-xs text-muted-foreground">{subtitle}</p>
        {detail && (
          <p className="mt-0.5 truncate text-xs text-muted-foreground/70">{detail}</p>
        )}
      </div>
    </button>
  );
}
