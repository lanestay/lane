import { useState, useEffect, useCallback, useRef } from "react";
import { listActiveQueries, killQuery, listConnections } from "../lib/api";
import type { ActiveQuery, ConnectionInfo } from "../lib/api";
import { useAuth } from "../lib/auth";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import {
  Dialog, DialogContent, DialogDescription, DialogFooter,
  DialogHeader, DialogTitle,
} from "@/components/ui/dialog";

const REFRESH_INTERVAL = 5000;
const WARN_THRESHOLD_SECONDS = 30;

export default function MonitorPage() {
  const { user } = useAuth();
  const [connections, setConnections] = useState<ConnectionInfo[]>([]);
  const [selectedConn, setSelectedConn] = useState("");
  const [queries, setQueries] = useState<ActiveQuery[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [killTarget, setKillTarget] = useState<ActiveQuery | null>(null);
  const [killing, setKilling] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Load connections on mount
  useEffect(() => {
    listConnections()
      .then((conns) => {
        setConnections(conns);
        if (conns.length > 0) {
          const def = conns.find((c) => c.is_default) ?? conns[0];
          setSelectedConn(def.name);
        }
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, []);

  const fetchQueries = useCallback(async () => {
    if (!selectedConn) return;
    try {
      const q = await listActiveQueries(selectedConn);
      setQueries(q);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [selectedConn]);

  // Fetch on connection change
  useEffect(() => {
    setLoading(true);
    fetchQueries();
  }, [fetchQueries]);

  // Auto-refresh
  useEffect(() => {
    if (autoRefresh && selectedConn) {
      intervalRef.current = setInterval(fetchQueries, REFRESH_INTERVAL);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [autoRefresh, fetchQueries, selectedConn]);

  const handleKill = async () => {
    if (!killTarget) return;
    setKilling(true);
    try {
      await killQuery(killTarget.spid, selectedConn);
      setKillTarget(null);
      await fetchQueries();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setKilling(false);
    }
  };

  function formatDuration(seconds: number | null): string {
    if (seconds == null) return "-";
    if (seconds < 60) return `${seconds}s`;
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return `${m}m ${s}s`;
  }

  function truncate(text: string | null, maxLen: number): string {
    if (!text) return "-";
    return text.length > maxLen ? text.slice(0, maxLen) + "..." : text;
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold">Query Monitor</h2>
        <div className="flex items-center gap-3">
          <Select value={selectedConn} onValueChange={setSelectedConn}>
            <SelectTrigger className="w-48">
              <SelectValue placeholder="Connection" />
            </SelectTrigger>
            <SelectContent>
              {connections.map((c) => (
                <SelectItem key={c.name} value={c.name}>
                  {c.name}{c.is_default ? " (default)" : ""}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button
            variant={autoRefresh ? "default" : "outline"}
            size="sm"
            onClick={() => setAutoRefresh(!autoRefresh)}
          >
            {autoRefresh ? "Pause" : "Resume"}
          </Button>
          <Button variant="outline" size="sm" onClick={fetchQueries}>
            Refresh
          </Button>
        </div>
      </div>

      {error && (
        <div className="bg-destructive/10 text-destructive p-3 rounded-md text-sm">
          {error}
        </div>
      )}

      {loading ? (
        <p className="text-muted-foreground">Loading...</p>
      ) : queries.length === 0 ? (
        <p className="text-muted-foreground">No active queries.</p>
      ) : (
        <div className="border rounded-md">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-16">SPID</TableHead>
                <TableHead className="w-24">Status</TableHead>
                <TableHead className="w-28">Database</TableHead>
                <TableHead className="w-24">Duration</TableHead>
                <TableHead className="w-28">Wait Type</TableHead>
                <TableHead>Query</TableHead>
                {user?.is_admin && <TableHead className="w-16">Action</TableHead>}
              </TableRow>
            </TableHeader>
            <TableBody>
              {queries.map((q) => {
                const isLong = q.duration_seconds != null && q.duration_seconds >= WARN_THRESHOLD_SECONDS;
                return (
                  <TableRow
                    key={q.spid}
                    className={isLong ? "bg-yellow-500/10" : ""}
                  >
                    <TableCell className="font-mono text-xs">{q.spid}</TableCell>
                    <TableCell>
                      <Badge variant={q.status === "running" || q.status === "active" ? "default" : "secondary"}>
                        {q.status ?? "-"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-sm">{q.database_name ?? "-"}</TableCell>
                    <TableCell className={`text-sm ${isLong ? "text-yellow-600 font-medium" : ""}`}>
                      {formatDuration(q.duration_seconds)}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {q.wait_type ?? "-"}
                    </TableCell>
                    <TableCell className="text-xs font-mono max-w-md truncate" title={q.query_text ?? ""}>
                      {truncate(q.query_text, 120)}
                    </TableCell>
                    {user?.is_admin && (
                      <TableCell>
                        <Button
                          variant="destructive"
                          size="sm"
                          onClick={() => setKillTarget(q)}
                        >
                          Kill
                        </Button>
                      </TableCell>
                    )}
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      )}

      <p className="text-xs text-muted-foreground">
        {queries.length} active {queries.length === 1 ? "query" : "queries"}
        {autoRefresh ? ` \u00b7 refreshing every ${REFRESH_INTERVAL / 1000}s` : " \u00b7 paused"}
      </p>

      {/* Kill confirmation dialog */}
      <Dialog open={!!killTarget} onOpenChange={(open) => !open && setKillTarget(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Kill Query</DialogTitle>
            <DialogDescription>
              Are you sure you want to terminate process <strong>{killTarget?.spid}</strong>?
              This will immediately kill the running query.
            </DialogDescription>
          </DialogHeader>
          <div className="bg-muted p-3 rounded text-xs font-mono max-h-32 overflow-auto">
            {killTarget?.query_text ?? "No query text"}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setKillTarget(null)} disabled={killing}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={handleKill} disabled={killing}>
              {killing ? "Killing..." : "Kill Process"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
