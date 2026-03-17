import { useState, useEffect, useCallback, useRef, type ReactNode } from "react";
import {
  ArrowRight,
  CheckCircle2,
  Clock3,
  Database,
  Server,
  ShieldX,
  UserRound,
} from "lucide-react";
import {
  listApprovals, getApproval, approveApproval, rejectApproval, getSessionToken,
} from "../lib/api";
import type { ApprovalSummary, ApprovalDetail } from "../lib/api";
import { useAuth } from "../lib/auth";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";

export default function ApprovalsPage() {
  const { user } = useAuth();
  const [approvals, setApprovals] = useState<ApprovalSummary[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selected, setSelected] = useState<ApprovalDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [rejectId, setRejectId] = useState<string | null>(null);
  const [rejectReason, setRejectReason] = useState("");
  const [acting, setActing] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [sseConnected, setSseConnected] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);
  const detailRequestRef = useRef(0);

  const refresh = useCallback(async () => {
    try {
      const items = await listApprovals();
      setApprovals(items);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  const loadDetail = useCallback(async (id: string) => {
    const requestId = detailRequestRef.current + 1;
    detailRequestRef.current = requestId;
    setSelectedId(id);
    setDetailLoading(true);
    setSelected((current) => (current?.id === id ? current : null));

    try {
      const detail = await getApproval(id);
      if (detailRequestRef.current !== requestId) return;
      setSelected(detail);
      if (!detail) {
        setError("That approval is no longer available.");
      }
    } catch (e) {
      if (detailRequestRef.current !== requestId) return;
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      if (detailRequestRef.current === requestId) {
        setDetailLoading(false);
      }
    }
  }, []);

  // SSE connection for real-time updates
  useEffect(() => {
    const token = getSessionToken();
    if (!token) return;

    const url = `/api/lane/approvals/events?token=${encodeURIComponent(token)}`;
    const es = new EventSource(url);
    eventSourceRef.current = es;

    es.onopen = () => setSseConnected(true);
    es.onerror = () => setSseConnected(false);

    es.addEventListener("new_approval", () => refresh());
    es.addEventListener("resolved", () => refresh());

    return () => {
      es.close();
      eventSourceRef.current = null;
      setSseConnected(false);
    };
  }, [refresh]);

  // Fallback polling if SSE not connected
  useEffect(() => {
    void refresh();
    if (!sseConnected) {
      const interval = setInterval(refresh, 3000);
      return () => clearInterval(interval);
    }
  }, [refresh, sseConnected]);

  useEffect(() => {
    if (approvals.length === 0) {
      setSelectedId(null);
      setSelected(null);
      return;
    }

    const stillExists = selectedId && approvals.some((approval) => approval.id === selectedId);
    if (!stillExists) {
      void loadDetail(approvals[0].id);
    }
  }, [approvals, selectedId, loadDetail]);

  const handleApprove = async () => {
    if (!selected) return;
    setActing(true);
    try {
      await approveApproval(selected.id);
      setSelected(null);
      setSelectedId(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setActing(false);
    }
  };

  const handleReject = async () => {
    if (!rejectId) return;
    setActing(true);
    try {
      await rejectApproval(rejectId, rejectReason || undefined);
      setRejectId(null);
      setRejectReason("");
      setSelected(null);
      setSelectedId(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setActing(false);
    }
  };

  const isDelegated = useCallback((approval: ApprovalSummary) => approval.user_email !== user?.email, [user?.email]);

  const delegatedCount = approvals.filter(isDelegated).length;
  const personalCount = approvals.length - delegatedCount;
  const targetCount = new Set(approvals.map((approval) => `${approval.target_connection}:${approval.target_database}`)).size;
  const selectedSummary = approvals.find((approval) => approval.id === selectedId) ?? null;

  return (
    <div className="mx-auto flex w-full max-w-[1650px] flex-col gap-6 px-4 py-6 sm:px-6 xl:px-8">
      <div className="grid gap-3 sm:grid-cols-3">
        <MetricCard label="Pending" value={String(approvals.length)} description="Total items in queue" />
        <MetricCard label="Assigned to you" value={String(personalCount)} description="Direct approvals" />
        <MetricCard label="Targets" value={String(targetCount)} description="Connection and database pairs" />
      </div>

      {error && (
        <div className="rounded-2xl border border-destructive/25 bg-destructive/10 px-4 py-3 text-sm text-destructive shadow-sm">
          {error}
          <button className="ml-2 underline underline-offset-4" onClick={() => setError(null)}>dismiss</button>
        </div>
      )}

      <div className="grid gap-6 xl:grid-cols-2">
        <Card className="app-panel gap-0 rounded-[1.75rem] border-0">
          <CardHeader className="px-6 pt-6 pb-4 sm:px-7">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div>
                <CardTitle className="text-xl tracking-tight">Approvals</CardTitle>
                <CardDescription className="mt-2 text-sm">
                  Prioritized queue with the latest request selected automatically when the current one resolves.
                </CardDescription>
              </div>
              <div className="flex flex-wrap gap-2">
                <MiniBadge label="Delegated" value={delegatedCount} tone="info" />
                <MiniBadge label="Mine" value={personalCount} tone="neutral" />
              </div>
            </div>
          </CardHeader>
          <CardContent className="px-4 pb-4 sm:px-5">
            {approvals.length === 0 ? (
              <div className="rounded-[1.5rem] border border-dashed border-border bg-muted/30 px-6 py-16 text-center">
                <p className="text-base font-medium">No pending approvals.</p>
                <p className="mt-2 text-sm text-muted-foreground">
                  New requests will appear here as soon as they reach your queue.
                </p>
              </div>
            ) : (
              <div className="space-y-3">
                {approvals.map((approval, index) => {
                  const delegated = isDelegated(approval);
                  const active = approval.id === selectedId;
                  return (
                    <button
                      key={approval.id}
                      type="button"
                      onClick={() => void loadDetail(approval.id)}
                      className={cn(
                        "group flex w-full flex-col gap-4 rounded-[1.4rem] border px-4 py-4 text-left transition-all",
                        active
                          ? "border-primary/30 bg-primary/10 shadow-[0_18px_34px_rgba(15,109,117,0.12)]"
                          : "border-border bg-card hover:border-primary/20 hover:bg-muted/50",
                      )}
                    >
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div className="min-w-0 flex-1">
                          <div className="flex flex-wrap items-center gap-2">
                            <Badge variant="outline" className="border-amber-500/30 bg-amber-500/10 text-amber-400">
                              Pending
                            </Badge>
                            {delegated && (
                              <Badge variant="outline" className="border-sky-500/25 bg-sky-500/10 text-sky-400">
                                Delegated
                              </Badge>
                            )}
                            <span className="truncate text-base font-semibold tracking-tight">
                              {approval.tool_name}
                            </span>
                          </div>
                          <p className="mt-2 line-clamp-2 text-sm leading-6 text-muted-foreground">
                            {approval.context}
                          </p>
                        </div>
                        <span className="rounded-full bg-muted px-3 py-1 text-xs font-medium text-muted-foreground">
                          #{index + 1}
                        </span>
                      </div>

                      <div className="grid gap-3 text-sm text-muted-foreground sm:grid-cols-3">
                        <QueueMeta
                          icon={<UserRound className="size-4" />}
                          label="Requester"
                          value={delegated ? approval.user_email : "You"}
                        />
                        <QueueMeta
                          icon={<Server className="size-4" />}
                          label="Connection"
                          value={approval.target_connection}
                        />
                        <QueueMeta
                          icon={<Clock3 className="size-4" />}
                          label="Age"
                          value={formatAge(approval.created_at)}
                        />
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="app-panel gap-0 rounded-[1.75rem] border-0 xl:sticky xl:top-24 xl:self-start">
          <CardHeader className="px-6 pt-6 pb-4 sm:px-7">
            <CardTitle className="text-xl tracking-tight">Selected Request</CardTitle>
            <CardDescription className="mt-2 text-sm">
              Inspect context, target, and SQL before acting.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-5 px-6 pb-6 sm:px-7">
            {detailLoading && (
              <div className="rounded-[1.4rem] border border-dashed border-border bg-muted/30 px-5 py-12 text-center text-sm text-muted-foreground">
                Loading approval details...
              </div>
            )}

            {!detailLoading && !selected && approvals.length > 0 && (
              <div className="rounded-[1.4rem] border border-dashed border-border bg-muted/30 px-5 py-12 text-center text-sm text-muted-foreground">
                Select an approval from the queue to review the SQL payload and resolve it.
              </div>
            )}

            {!detailLoading && approvals.length === 0 && (
              <div className="rounded-[1.4rem] border border-dashed border-border bg-muted/30 px-5 py-12 text-center text-sm text-muted-foreground">
                Your queue is clear.
              </div>
            )}

            {!detailLoading && selected && (
              <>
                <div className="space-y-4 rounded-[1.5rem] border border-border bg-card p-5">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <div className="flex flex-wrap items-center gap-2">
                        <Badge className="border-0 bg-primary/12 text-primary shadow-none">
                          {selected.tool_name}
                        </Badge>
                        {selected.user_email !== user?.email && (
                          <Badge variant="outline" className="border-sky-500/25 bg-sky-500/10 text-sky-400">
                            Delegated
                          </Badge>
                        )}
                      </div>
                      <p className="mt-3 text-sm leading-6 text-muted-foreground">
                        {selected.context}
                      </p>
                    </div>
                    {selectedSummary && (
                      <span className="rounded-full bg-muted px-3 py-1 text-xs font-medium text-muted-foreground">
                        {formatExactTime(selectedSummary.created_at)}
                      </span>
                    )}
                  </div>

                  <div className="grid gap-3 sm:grid-cols-2">
                    <DetailMeta label="Requester" value={selected.user_email} icon={<UserRound className="size-4" />} />
                    <DetailMeta label="Connection" value={selected.target_connection} icon={<Server className="size-4" />} />
                    <DetailMeta label="Database" value={selected.target_database} icon={<Database className="size-4" />} />
                    <DetailMeta label="Statements" value={String(selected.sql_statements.length)} icon={<ArrowRight className="size-4" />} />
                  </div>
                </div>

                <div className="space-y-3">
                  <div className="flex items-center justify-between gap-3">
                    <h3 className="text-sm font-semibold uppercase tracking-[0.24em] text-muted-foreground">
                      SQL Statements
                    </h3>
                    <span className="text-xs text-muted-foreground">
                      Review carefully before approving.
                    </span>
                  </div>
                  <div className="space-y-3">
                    {selected.sql_statements.map((sql, index) => (
                      <div key={index} className="overflow-hidden rounded-[1.35rem] border border-slate-900/10 bg-slate-950 text-slate-100 shadow-[0_18px_32px_rgba(15,23,42,0.18)]">
                        <div className="border-b border-white/10 px-4 py-3 text-xs uppercase tracking-[0.22em] text-slate-300/70">
                          Statement {index + 1}
                        </div>
                        <pre className="max-h-64 overflow-auto p-4 text-xs leading-6 whitespace-pre-wrap break-all">
                          {sql}
                        </pre>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="flex flex-col gap-3 pt-2 sm:flex-row">
                  <Button
                    variant="destructive"
                    className="sm:flex-1"
                    onClick={() => {
                      setRejectId(selected.id);
                      setRejectReason("");
                    }}
                    disabled={acting}
                  >
                    <ShieldX className="size-4" />
                    Reject
                  </Button>
                  <Button
                    onClick={() => void handleApprove()}
                    disabled={acting}
                    className="sm:flex-1"
                  >
                    <CheckCircle2 className="size-4" />
                    {acting ? "Approving..." : "Approve"}
                  </Button>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </div>

      <Dialog
        open={!!rejectId}
        onOpenChange={(open) => {
          if (!open) {
            setRejectId(null);
            setRejectReason("");
          }
        }}
      >
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>Reject Approval</DialogTitle>
            <DialogDescription>
              Add a reason if the requester needs context before they revise and resubmit.
            </DialogDescription>
          </DialogHeader>
          <Textarea
            placeholder="Optional rejection reason"
            value={rejectReason}
            onChange={(event) => setRejectReason(event.target.value)}
            className="min-h-28"
          />
          <DialogFooter className="gap-2">
            <Button
              variant="outline"
              onClick={() => {
                setRejectId(null);
                setRejectReason("");
              }}
            >
              Cancel
            </Button>
            <Button variant="destructive" onClick={() => void handleReject()} disabled={acting}>
              {acting ? "Rejecting..." : "Reject"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

function MetricCard({ label, value, description }: { label: string; value: string; description: string }) {
  return (
    <div className="rounded-[1.5rem] border border-border bg-muted/50 px-4 py-4 shadow-sm">
      <p className="text-xs uppercase tracking-[0.24em] text-muted-foreground">{label}</p>
      <p className="mt-3 text-3xl font-semibold tracking-tight text-foreground">{value}</p>
      <p className="mt-2 text-sm text-muted-foreground">{description}</p>
    </div>
  );
}

function MiniBadge({ label, value, tone }: { label: string; value: number; tone: "info" | "neutral" }) {
  return (
    <span
      className={cn(
        "rounded-full px-3 py-1 text-xs font-semibold",
        tone === "info"
          ? "bg-sky-500/10 text-sky-400 ring-1 ring-sky-500/20"
          : "bg-muted/50 text-muted-foreground ring-1 ring-border",
      )}
    >
      {label}: {value}
    </span>
  );
}

function QueueMeta({ icon, label, value }: { icon: ReactNode; label: string; value: string }) {
  return (
    <div className="flex items-center gap-3 rounded-2xl bg-muted/60 px-3 py-2">
      <span className="text-muted-foreground">{icon}</span>
      <div className="min-w-0">
        <p className="text-[0.68rem] uppercase tracking-[0.22em] text-muted-foreground">{label}</p>
        <p className="truncate text-sm font-medium text-foreground">{value}</p>
      </div>
    </div>
  );
}

function DetailMeta({ label, value, icon }: { label: string; value: string; icon: ReactNode }) {
  return (
    <div className="rounded-[1.2rem] bg-muted/60 px-4 py-3">
      <div className="flex items-center gap-2 text-[0.68rem] uppercase tracking-[0.22em] text-muted-foreground">
        {icon}
        <span>{label}</span>
      </div>
      <p className="mt-2 break-all text-sm font-medium text-foreground">{value}</p>
    </div>
  );
}

function formatAge(createdAt: string) {
  const ms = Date.now() - new Date(createdAt).getTime();
  const secs = Math.floor(ms / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

function formatExactTime(createdAt: string) {
  return new Date(createdAt).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}
