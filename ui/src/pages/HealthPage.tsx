import { useState, useEffect, useCallback, useRef } from "react";
import { getConnectionsHealth } from "../lib/api";
import type { ConnectionHealth } from "../lib/api";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

const REFRESH_INTERVAL = 15000;

export default function HealthPage() {
  const [connections, setConnections] = useState<ConnectionHealth[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchHealth = useCallback(async () => {
    try {
      const data = await getConnectionsHealth();
      setConnections(data);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to fetch health");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchHealth();
  }, [fetchHealth]);

  useEffect(() => {
    if (autoRefresh) {
      intervalRef.current = setInterval(fetchHealth, REFRESH_INTERVAL);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [autoRefresh, fetchHealth]);

  if (loading) {
    return (
      <div className="p-6">
        <p className="text-muted-foreground">Loading health data...</p>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold">Connection Health</h2>
        <div className="flex items-center gap-2">
          <Button
            variant={autoRefresh ? "default" : "outline"}
            size="sm"
            onClick={() => setAutoRefresh(!autoRefresh)}
          >
            {autoRefresh ? "Pause" : "Resume"}
          </Button>
          <Button variant="outline" size="sm" onClick={fetchHealth}>
            Refresh
          </Button>
        </div>
      </div>

      {error && (
        <div className="text-sm text-destructive bg-destructive/10 rounded-md p-3">
          {error}
        </div>
      )}

      {connections.length === 0 && !error && (
        <p className="text-muted-foreground text-sm">No connections found.</p>
      )}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {connections.map((conn) => (
          <ConnectionCard key={conn.name} conn={conn} />
        ))}
      </div>
    </div>
  );
}

function ConnectionCard({ conn }: { conn: ConnectionHealth }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base">{conn.name}</CardTitle>
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="text-xs">
              {conn.dialect}
            </Badge>
            <StatusBadge status={conn.status} />
          </div>
        </div>
        {conn.status_message && (
          <p className="text-xs text-destructive truncate mt-1" title={conn.status_message}>
            {conn.status_message}
          </p>
        )}
      </CardHeader>
      <CardContent className="space-y-4">
        {conn.pool && <PoolBar pool={conn.pool} />}
        {!conn.pool && (
          <p className="text-xs text-muted-foreground">No pool stats available</p>
        )}
        <div>
          <p className="text-xs text-muted-foreground mb-1">24h Status Timeline</p>
          <StatusTimeline history={conn.history} />
        </div>
      </CardContent>
    </Card>
  );
}

function StatusBadge({ status }: { status: string }) {
  if (status === "connected") {
    return <Badge className="bg-emerald-500/15 text-emerald-600 border-emerald-500/30 text-xs">Connected</Badge>;
  }
  if (status === "error") {
    return <Badge className="bg-red-500/15 text-red-600 border-red-500/30 text-xs">Error</Badge>;
  }
  return <Badge variant="secondary" className="text-xs">Unknown</Badge>;
}

function PoolBar({ pool }: { pool: ConnectionHealth["pool"] }) {
  if (!pool) return null;
  const pct = pool.max_size > 0 ? Math.round((pool.active_connections / pool.max_size) * 100) : 0;

  return (
    <div>
      <div className="flex items-center justify-between text-xs text-muted-foreground mb-1">
        <span>Pool Utilization</span>
        <span>{pool.active_connections} active / {pool.idle_connections} idle / {pool.max_size} max</span>
      </div>
      <div className="h-3 bg-muted rounded-full overflow-hidden flex">
        {pool.active_connections > 0 && (
          <div
            className="bg-blue-500 transition-all duration-300"
            style={{ width: `${(pool.active_connections / pool.max_size) * 100}%` }}
            title={`${pool.active_connections} active`}
          />
        )}
        {pool.idle_connections > 0 && (
          <div
            className="bg-emerald-500/40 transition-all duration-300"
            style={{ width: `${(pool.idle_connections / pool.max_size) * 100}%` }}
            title={`${pool.idle_connections} idle`}
          />
        )}
      </div>
      <p className="text-xs text-muted-foreground mt-1 text-right">{pct}% utilized</p>
    </div>
  );
}

/**
 * 24h timeline: 48 segments of 30 min each.
 * Green = connected, Red = error, Gray = no data.
 */
function StatusTimeline({ history }: { history: ConnectionHealth["history"] }) {
  const SEGMENTS = 48;
  const SEGMENT_MS = 30 * 60 * 1000; // 30 minutes
  const now = Date.now();
  const start = now - SEGMENTS * SEGMENT_MS;

  // Build segment data
  const segments: { status: "connected" | "error" | "unknown"; tooltip: string }[] = [];

  for (let i = 0; i < SEGMENTS; i++) {
    const segStart = start + i * SEGMENT_MS;
    const segEnd = segStart + SEGMENT_MS;

    // Find checks within this segment
    const checks = history.filter((h) => {
      const t = new Date(h.checked_at + "Z").getTime();
      return t >= segStart && t < segEnd;
    });

    if (checks.length === 0) {
      const timeLabel = formatSegmentTime(segStart, segEnd);
      segments.push({ status: "unknown", tooltip: `${timeLabel}: No data` });
    } else {
      const hasError = checks.some((c) => c.status === "error");
      const timeLabel = formatSegmentTime(segStart, segEnd);
      if (hasError) {
        const errMsg = checks.find((c) => c.status === "error")?.error_message || "Error";
        segments.push({ status: "error", tooltip: `${timeLabel}: Error - ${errMsg}` });
      } else {
        segments.push({ status: "connected", tooltip: `${timeLabel}: Connected` });
      }
    }
  }

  return (
    <div className="flex gap-px h-4">
      {segments.map((seg, i) => (
        <div
          key={i}
          className={`flex-1 rounded-sm transition-colors ${
            seg.status === "connected"
              ? "bg-emerald-500"
              : seg.status === "error"
              ? "bg-red-500"
              : "bg-muted-foreground/20"
          }`}
          title={seg.tooltip}
        />
      ))}
    </div>
  );
}

function formatSegmentTime(startMs: number, endMs: number): string {
  const s = new Date(startMs);
  const e = new Date(endMs);
  const fmt = (d: Date) =>
    d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  return `${fmt(s)}\u2013${fmt(e)}`;
}
