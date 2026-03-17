import { useState, useEffect, useCallback, type ReactNode } from "react";
import {
  Copy,
  Globe2,
  KeyRound,
  LockKeyhole,
  PlugZap,
  ShieldCheck,
  TimerReset,
} from "lucide-react";
import {
  selfListTokens, selfGenerateToken, selfRevokeToken,
  getTokenPolicy,
} from "../lib/api";
import type { TokenRecord, TokenPolicy } from "../lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle,
  DialogFooter, DialogDescription,
} from "@/components/ui/dialog";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

export function formatExpiry(expiresAt: string | null): { text: string; color: string } {
  if (!expiresAt) return { text: "Never", color: "" };

  const now = Date.now();
  const exp = new Date(expiresAt + (expiresAt.includes("Z") || expiresAt.includes("+") ? "" : "Z")).getTime();
  const diffMs = exp - now;

  if (diffMs < 0) {
    const ago = formatDuration(-diffMs);
    return { text: `Expired ${ago} ago`, color: "text-red-500" };
  }

  const text = `in ${formatDuration(diffMs)}`;

  if (diffMs < 7 * 24 * 60 * 60 * 1000) {
    return { text, color: "text-amber-600" };
  }

  return { text, color: "text-muted-foreground" };
}

function formatDuration(ms: number): string {
  const mins = Math.floor(ms / 60000);
  if (mins < 60) return `${mins}m`;
  const hours = Math.floor(mins / 60);
  if (hours < 48) return `${hours}h`;
  const days = Math.floor(hours / 24);
  return `${days} days`;
}

export default function MyAccessPage() {
  const [tokens, setTokens] = useState<TokenRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showGenerate, setShowGenerate] = useState(false);
  const [revealState, setRevealState] = useState<{ token: string; label?: string } | null>(null);
  const [policy, setPolicy] = useState<TokenPolicy | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [tokenList, tokenPolicy] = await Promise.all([
        selfListTokens(),
        getTokenPolicy().catch(() => null),
      ]);
      setTokens(tokenList);
      setPolicy(tokenPolicy);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const handleRevoke = async (prefix: string) => {
    try {
      await selfRevokeToken(prefix);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const serverUrl = window.location.origin;
  const activeTokens = tokens.filter((token) => token.is_active).length;
  const expiringSoon = tokens.filter((token) => token.is_active && isExpiringSoon(token.expires_at)).length;
  const neverExpires = tokens.filter((token) => token.is_active && !token.expires_at).length;
  const defaultLifespan = policy?.default_lifespan_hours
    ? policy.default_lifespan_hours >= 24
      ? `${Math.floor(policy.default_lifespan_hours / 24)} days`
      : `${policy.default_lifespan_hours}h`
    : "Server default";
  const maxLifespan = policy?.max_lifespan_hours
    ? policy.max_lifespan_hours >= 24
      ? `${Math.floor(policy.max_lifespan_hours / 24)} days`
      : `${policy.max_lifespan_hours}h`
    : "Not enforced";

  return (
    <div className="mx-auto flex w-full max-w-[1650px] flex-col gap-6 px-4 py-6 sm:px-6 xl:px-8">
      <Card className="app-panel overflow-hidden border-0">
        <CardContent className="grid gap-6 px-6 py-6 lg:grid-cols-[minmax(0,1.2fr)_minmax(280px,360px)] lg:px-8">
          <div className="space-y-4">
            <div className="flex flex-wrap items-center gap-2">
              <Badge className="border-0 bg-primary/12 text-primary shadow-none">User Access</Badge>
              <Badge variant="outline" className="border-border bg-muted/50 text-muted-foreground">
                <Globe2 className="mr-1 size-3.5" />
                {serverUrl}
              </Badge>
            </div>
            <div className="space-y-2">
              <h1 className="text-3xl font-semibold tracking-tight text-balance sm:text-4xl">
                My Access
              </h1>
              <p className="max-w-2xl text-sm leading-6 text-muted-foreground sm:text-base">
                Manage your API tokens and view your permissions.
              </p>
            </div>
            <div className="flex flex-wrap gap-3">
              <Button onClick={() => setShowGenerate(true)}>
                <KeyRound className="size-4" />
                Generate New Token
              </Button>
              <Button variant="outline" onClick={() => void refresh()}>
                <TimerReset className="size-4" />
                Refresh
              </Button>
            </div>
          </div>
          <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-1">
            <MetricCard label="Active" value={String(activeTokens)} description="Usable tokens right now" />
            <MetricCard label="Expiring soon" value={String(expiringSoon)} description="Within the next 7 days" />
            <MetricCard label="Never expires" value={String(neverExpires)} description="Tokens without expiry" />
          </div>
        </CardContent>
      </Card>

      {error && (
        <div className="rounded-2xl border border-destructive/25 bg-destructive/10 px-4 py-3 text-sm text-destructive shadow-sm">
          {error}
          <button className="ml-2 underline underline-offset-4" onClick={() => setError(null)}>dismiss</button>
        </div>
      )}

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.18fr)_minmax(320px,380px)]">
        <Card className="app-panel gap-0 rounded-[1.75rem] border-0">
          <CardHeader className="px-6 pt-6 pb-4 sm:px-7">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div>
                <CardTitle className="text-xl tracking-tight">Your Tokens</CardTitle>
                <CardDescription className="mt-2 text-sm">
                  Generate, review, and revoke personal access tokens without leaving this page.
                </CardDescription>
              </div>
              <div className="flex items-center gap-2">
                <Badge variant="outline" className="border-border bg-muted/50 text-muted-foreground">
                  {tokens.length} total
                </Badge>
                <Button size="sm" onClick={() => setShowGenerate(true)}>Generate New Token</Button>
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-4 px-4 pb-5 sm:px-5">
            {loading ? (
              <div className="rounded-[1.4rem] border border-dashed border-border bg-muted/30 px-6 py-16 text-center text-sm text-muted-foreground">
                Loading tokens...
              </div>
            ) : tokens.length === 0 ? (
              <div className="rounded-[1.4rem] border border-dashed border-border bg-muted/30 px-6 py-16 text-center">
                <p className="text-base font-medium">No tokens yet.</p>
                <p className="mt-2 text-sm text-muted-foreground">
                  Generate one to connect Claude Desktop, scripts, or your own tooling.
                </p>
              </div>
            ) : (
              <>
                <div className="grid gap-3 md:hidden">
                  {tokens.map((token) => (
                    <TokenCard key={token.token_prefix} token={token} onRevoke={handleRevoke} />
                  ))}
                </div>

                <div className="hidden overflow-hidden rounded-[1.5rem] border border-border bg-card md:block">
                  <Table>
                    <TableHeader>
                      <TableRow className="border-border/70">
                        <TableHead>Prefix</TableHead>
                        <TableHead>Label</TableHead>
                        <TableHead>Expires</TableHead>
                        <TableHead>Status</TableHead>
                        <TableHead className="w-[96px] text-right">Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {tokens.map((token) => {
                        const expiry = formatExpiry(token.expires_at);
                        return (
                          <TableRow key={token.token_prefix} className="border-border/60">
                            <TableCell className="font-mono text-xs">{token.token_prefix}...</TableCell>
                            <TableCell className="text-sm">
                              {token.label ?? <span className="text-muted-foreground">Unlabeled</span>}
                            </TableCell>
                            <TableCell className={cn("text-xs", expiry.color)}>{expiry.text}</TableCell>
                            <TableCell>
                              <TokenStatusBadge active={token.is_active} />
                            </TableCell>
                            <TableCell className="text-right">
                              {token.is_active && (
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="text-destructive hover:text-destructive"
                                  onClick={() => void handleRevoke(token.token_prefix)}
                                >
                                  Revoke
                                </Button>
                              )}
                            </TableCell>
                          </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                </div>
              </>
            )}
          </CardContent>
        </Card>

        <div className="space-y-6 xl:sticky xl:top-24 xl:self-start">
          <Card className="app-panel gap-0 rounded-[1.75rem] border-0">
            <CardHeader className="px-6 pt-6 pb-4">
              <CardTitle className="text-xl tracking-tight">
                {revealState ? "How to Use Your New Token" : "How to Use"}
              </CardTitle>
              <CardDescription className="mt-2 text-sm">
                Copy a ready-to-use configuration for MCP clients or direct API calls.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4 px-6 pb-6">
              {revealState ? (
                <UsageSnippets token={revealState.token} serverUrl={serverUrl} />
              ) : (
                <>
                  <SnippetBlock
                    title="MCP Configuration (Claude Desktop / Claude Code)"
                    code={`{\n  "type": "http",\n  "url": "${serverUrl}/mcp",\n  "headers": {\n    "x-lane-key": "YOUR_TOKEN_HERE"\n  }\n}`}
                  />
                  <SnippetBlock
                    title="API Usage"
                    code={`curl -H "x-lane-key: YOUR_TOKEN_HERE" \\\n  ${serverUrl}/api/lane \\\n  -d '{"query":"SELECT 1","database":"master"}'`}
                  />
                </>
              )}
            </CardContent>
          </Card>

          <Card className="app-panel gap-0 rounded-[1.75rem] border-0">
            <CardHeader className="px-6 pt-6 pb-4">
              <CardTitle className="text-xl tracking-tight">Policy Snapshot</CardTitle>
              <CardDescription className="mt-2 text-sm">
                Useful defaults before you mint tokens for users, automation, or local tools.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-3 px-6 pb-6">
              <PolicyRow icon={<ShieldCheck className="size-4" />} label="Default lifespan" value={defaultLifespan} />
              <PolicyRow icon={<LockKeyhole className="size-4" />} label="Maximum lifespan" value={maxLifespan} />
              <PolicyRow icon={<PlugZap className="size-4" />} label="Endpoint" value={serverUrl} />
            </CardContent>
          </Card>
        </div>
      </div>

      <GenerateDialog
        open={showGenerate}
        policy={policy}
        onClose={() => setShowGenerate(false)}
        onGenerated={(token, label) => {
          setRevealState({ token, label });
          void refresh();
        }}
        onError={setError}
      />

      <TokenRevealDialog
        state={revealState}
        serverUrl={serverUrl}
        onClose={() => setRevealState(null)}
      />
    </div>
  );
}

function UsageSnippets({ token, serverUrl }: { token: string; serverUrl: string }) {
  return (
    <>
      <SnippetBlock
        title="MCP Configuration (Claude Desktop / Claude Code)"
        code={`{\n  "type": "http",\n  "url": "${serverUrl}/mcp",\n  "headers": {\n    "x-lane-key": "${token}"\n  }\n}`}
      />
      <SnippetBlock
        title="API Usage"
        code={`curl -H "x-lane-key: ${token}" \\\n  ${serverUrl}/api/lane \\\n  -d '{"query":"SELECT 1","database":"master"}'`}
      />
    </>
  );
}

function SnippetBlock({ title, code }: { title: string; code: string }) {
  const [copied, setCopied] = useState(false);

  const copy = () => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="rounded-[1.4rem] border border-border bg-muted/30 p-4">
      <div className="mb-3 flex items-start justify-between gap-3">
        <p className="text-sm font-medium text-foreground">{title}</p>
        <Button variant="ghost" size="sm" className="shrink-0" onClick={copy}>
          <Copy className="size-4" />
          {copied ? "Copied!" : "Copy"}
        </Button>
      </div>
      <pre className="overflow-auto rounded-[1rem] bg-slate-950 p-4 text-xs leading-6 text-slate-100 whitespace-pre-wrap break-all">
        {code}
      </pre>
    </div>
  );
}

const EXPIRY_OPTIONS = [
  { label: "1 hour", hours: 1 },
  { label: "24 hours", hours: 24 },
  { label: "7 days", hours: 168 },
  { label: "30 days", hours: 720 },
  { label: "90 days", hours: 2160 },
];

function GenerateDialog({ open, policy, onClose, onGenerated, onError }: {
  open: boolean;
  policy: TokenPolicy | null;
  onClose: () => void;
  onGenerated: (token: string, label?: string) => void;
  onError: (msg: string) => void;
}) {
  const [label, setLabel] = useState("");
  const [expiryHours, setExpiryHours] = useState("168");
  const [saving, setSaving] = useState(false);

  const maxHours = policy?.max_lifespan_hours || 0;
  const filteredOptions = maxHours > 0
    ? EXPIRY_OPTIONS.filter((option) => option.hours <= maxHours)
    : EXPIRY_OPTIONS;

  const submit = async () => {
    setSaving(true);
    try {
      const hours = parseInt(expiryHours, 10);
      const result = await selfGenerateToken(
        label.trim() || undefined,
        Number.isNaN(hours) ? undefined : hours,
      );
      const trimmedLabel = label.trim() || undefined;
      setLabel("");
      setExpiryHours("168");
      onClose();
      onGenerated(result.token, trimmedLabel);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(value) => !value && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Generate New Token</DialogTitle>
          <DialogDescription>
            Create a fresh token for a device, integration, or workflow and copy it before closing.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label htmlFor="self-token-label">Label (optional)</Label>
            <Input
              id="self-token-label"
              value={label}
              onChange={(event) => setLabel(event.target.value)}
              placeholder="e.g. Claude Desktop, CI Pipeline"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="self-token-expiry">Expires in</Label>
            <Select value={expiryHours} onValueChange={setExpiryHours}>
              <SelectTrigger id="self-token-expiry">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {filteredOptions.map((option) => (
                  <SelectItem key={option.hours} value={String(option.hours)}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {maxHours > 0 && (
              <p className="text-xs text-muted-foreground">
                Max allowed: {maxHours >= 24 ? `${Math.floor(maxHours / 24)} days` : `${maxHours}h`}
              </p>
            )}
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={() => void submit()} disabled={saving}>
            {saving ? "Generating..." : "Generate"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function TokenRevealDialog({ state, serverUrl, onClose }: {
  state: { token: string; label?: string } | null;
  serverUrl: string;
  onClose: () => void;
}) {
  const [copied, setCopied] = useState(false);

  const copy = () => {
    if (!state) return;
    navigator.clipboard.writeText(state.token);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <Dialog open={!!state} onOpenChange={(value) => !value && onClose()}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Token Generated</DialogTitle>
          <DialogDescription>
            Copy this token now. It cannot be retrieved again after you close this dialog.
          </DialogDescription>
        </DialogHeader>
        <div className="rounded-[1.2rem] bg-slate-950 p-4 font-mono text-xs leading-6 text-slate-100 break-all select-all">
          {state?.token}
        </div>

        <div className="space-y-4">
          <SnippetBlock
            title="MCP Configuration"
            code={`{\n  "type": "http",\n  "url": "${serverUrl}/mcp",\n  "headers": {\n    "x-lane-key": "${state?.token ?? ""}"\n  }\n}`}
          />
          <SnippetBlock
            title="API Usage"
            code={`curl -H "x-lane-key: ${state?.token ?? ""}" \\\n  ${serverUrl}/api/lane \\\n  -d '{"query":"SELECT 1","database":"master"}'`}
          />
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={copy}>
            <Copy className="size-4" />
            {copied ? "Copied token!" : "Copy Token"}
          </Button>
          <Button onClick={onClose}>Done</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
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

function PolicyRow({ icon, label, value }: { icon: ReactNode; label: string; value: string }) {
  return (
    <div className="rounded-[1.2rem] border border-border bg-muted/30 px-4 py-3">
      <div className="flex items-center gap-2 text-[0.68rem] uppercase tracking-[0.22em] text-muted-foreground">
        {icon}
        <span>{label}</span>
      </div>
      <p className="mt-2 break-all text-sm font-medium text-foreground">{value}</p>
    </div>
  );
}

function TokenCard({ token, onRevoke }: { token: TokenRecord; onRevoke: (prefix: string) => Promise<void> }) {
  const expiry = formatExpiry(token.expires_at);

  return (
    <div className="rounded-[1.4rem] border border-border bg-muted/30 p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="font-mono text-xs text-muted-foreground">{token.token_prefix}...</p>
          <p className="mt-2 truncate text-base font-semibold tracking-tight">
            {token.label ?? "Unlabeled token"}
          </p>
        </div>
        <TokenStatusBadge active={token.is_active} />
      </div>
      <div className="mt-4 grid gap-3 sm:grid-cols-2">
        <div className="rounded-2xl bg-muted/60 px-3 py-2">
          <p className="text-[0.68rem] uppercase tracking-[0.22em] text-muted-foreground">Expires</p>
          <p className={cn("mt-2 text-sm font-medium", expiry.color)}>{expiry.text}</p>
        </div>
        <div className="rounded-2xl bg-muted/60 px-3 py-2">
          <p className="text-[0.68rem] uppercase tracking-[0.22em] text-muted-foreground">Created</p>
          <p className="mt-2 text-sm font-medium text-foreground">
            {new Date(token.created_at).toLocaleDateString()}
          </p>
        </div>
      </div>
      {token.is_active && (
        <Button
          variant="ghost"
          className="mt-4 w-full justify-center text-destructive hover:text-destructive"
          onClick={() => void onRevoke(token.token_prefix)}
        >
          Revoke
        </Button>
      )}
    </div>
  );
}

function TokenStatusBadge({ active }: { active: boolean }) {
  if (active) {
    return <Badge variant="outline" className="border-emerald-500/25 bg-emerald-500/10 text-emerald-400">Active</Badge>;
  }

  return <Badge variant="destructive">Revoked</Badge>;
}

function isExpiringSoon(expiresAt: string | null) {
  if (!expiresAt) return false;
  const exp = new Date(expiresAt + (expiresAt.includes("Z") || expiresAt.includes("+") ? "" : "Z")).getTime();
  const diff = exp - Date.now();
  return diff > 0 && diff < 7 * 24 * 60 * 60 * 1000;
}
