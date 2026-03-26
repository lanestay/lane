import { useState, useEffect, useCallback, useRef, type ReactNode } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";
import {
  Activity,
  Blocks,
  GitGraph,
  FolderKanban,
  HardDrive,
  HeartPulse,
  KeyRound,
  LogOut,
  Moon,
  Sun,
  Menu,
  PanelLeftClose,
  PanelLeftOpen,
  Radio,
  ScrollText,
  ShieldCheck,
  Table2,
  Upload,
  UsersRound,
  Waypoints,
  X,
} from "lucide-react";
import { useAuth } from "../lib/auth";
import { listApprovals, getSessionToken } from "../lib/api";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import SearchBar from "@/components/SearchBar";

type NavItem = {
  to: string;
  label: string;
  icon: ReactNode;
  badge?: number;
};

type NavSection = {
  label: string;
  items: NavItem[];
};

const PAGE_META: Record<string, { eyebrow: string; title: string; description: string }> = {
  "/": {
    eyebrow: "Workspace",
    title: "SQL Editor",
    description: "Run SQL queries across your connected databases.",
  },
  "/tables": {
    eyebrow: "Explore",
    title: "Tables",
    description: "Browse tables, columns, and indexes across your databases.",
  },
  "/objects": {
    eyebrow: "Explore",
    title: "Objects",
    description: "Views, stored procedures, and other database objects.",
  },
  "/graph": {
    eyebrow: "Explore",
    title: "Graph",
    description: "Explore table relationships and join paths across databases.",
  },
  "/realtime": {
    eyebrow: "Operations",
    title: "Realtime",
    description: "Stream live table changes via server-sent events.",
  },
  "/import": {
    eyebrow: "Workspace",
    title: "Import",
    description: "Upload CSV and Excel files into your databases.",
  },
  "/workspace": {
    eyebrow: "Workspace",
    title: "Workspace",
    description: "Query and join data across databases with DuckDB.",
  },
  "/monitor": {
    eyebrow: "Operations",
    title: "Monitor",
    description: "View currently running queries across connections.",
  },
  "/health": {
    eyebrow: "Operations",
    title: "Health",
    description: "Check connection status and latency.",
  },
  "/storage": {
    eyebrow: "Operations",
    title: "Storage",
    description: "Browse and manage S3/MinIO buckets and objects.",
  },
  "/access": {
    eyebrow: "Access",
    title: "My Access",
    description: "Your API tokens, permissions, and integration setup.",
  },
  "/approvals": {
    eyebrow: "Access",
    title: "Approvals",
    description: "Approve or reject pending write requests.",
  },
  "/admin": {
    eyebrow: "Admin",
    title: "Admin",
    description: "Manage users, connections, permissions, and platform settings.",
  },
};

export default function Layout() {
  const { user, logout, authProviders } = useAuth();
  const location = useLocation();
  const [pendingCount, setPendingCount] = useState(0);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [collapsed, setCollapsed] = useState(
    () => localStorage.getItem("sidebar-collapsed") === "true",
  );
  const eventSourceRef = useRef<EventSource | null>(null);

  const toggleCollapsed = () => {
    setCollapsed((prev) => {
      const next = !prev;
      localStorage.setItem("sidebar-collapsed", String(next));
      return next;
    });
  };

  const refreshCount = useCallback(async () => {
    try {
      const items = await listApprovals();
      setPendingCount(items.length);
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    setMobileNavOpen(false);
  }, [location.pathname]);

  // SSE for real-time badge updates
  useEffect(() => {
    const token = getSessionToken();
    if (!token) return;

    refreshCount();

    const url = `/api/lane/approvals/events?token=${encodeURIComponent(token)}`;
    const es = new EventSource(url);
    eventSourceRef.current = es;

    es.addEventListener("new_approval", () => refreshCount());
    es.addEventListener("resolved", () => refreshCount());

    // Fallback polling
    const interval = setInterval(refreshCount, 10000);

    return () => {
      es.close();
      eventSourceRef.current = null;
      clearInterval(interval);
    };
  }, [refreshCount]);

  const navSections: NavSection[] = [
    {
      label: "Workspace",
      items: [
        { to: "/", label: "SQL Editor", icon: <ScrollText className="size-4" /> },
        { to: "/workspace", label: "Workspace", icon: <FolderKanban className="size-4" /> },
        { to: "/import", label: "Import", icon: <Upload className="size-4" /> },
      ],
    },
    {
      label: "Explore",
      items: [
        { to: "/tables", label: "Tables", icon: <Table2 className="size-4" /> },
        { to: "/objects", label: "Objects", icon: <Blocks className="size-4" /> },
        { to: "/graph", label: "Graph", icon: <GitGraph className="size-4" /> },
      ],
    },
    {
      label: "Operations",
      items: [
        { to: "/realtime", label: "Realtime", icon: <Radio className="size-4" /> },
        { to: "/monitor", label: "Monitor", icon: <Activity className="size-4" /> },
        { to: "/health", label: "Health", icon: <HeartPulse className="size-4" /> },
        { to: "/storage", label: "Storage", icon: <HardDrive className="size-4" /> },
      ],
    },
    {
      label: "Access",
      items: [
        { to: "/access", label: "My Access", icon: <KeyRound className="size-4" /> },
        {
          to: "/approvals",
          label: "Approvals",
          icon: <ShieldCheck className="size-4" />,
          badge: pendingCount > 0 ? pendingCount : undefined,
        },
        ...(user?.is_admin
          ? [{ to: "/admin", label: "Admin", icon: <UsersRound className="size-4" /> }]
          : []),
      ],
    },
  ];

  const pageMeta = PAGE_META[location.pathname] ?? {
    eyebrow: "Lane DB",
    title: "Workspace",
    description: "",
  };

  return (
    <div className="app-shell min-h-screen text-foreground">
      {mobileNavOpen && (
        <>
          <button
            type="button"
            aria-label="Close navigation"
            className="fixed inset-0 z-40 bg-slate-950/35 backdrop-blur-sm lg:hidden"
            onClick={() => setMobileNavOpen(false)}
          />
          <aside className="fixed inset-y-3 left-3 z-50 flex w-[min(88vw,22rem)] flex-col rounded-[2rem] border border-white/15 bg-sidebar px-4 py-4 text-sidebar-foreground shadow-[0_28px_90px_rgba(15,23,42,0.35)] lg:hidden">
            <MobileHeader email={user?.email} onClose={() => setMobileNavOpen(false)} />
            <SidebarSections sections={navSections} />
            <SidebarFooter email={user?.email} onLogout={logout} authProviders={authProviders} />
          </aside>
        </>
      )}

      <div
        className={cn(
          "mx-auto min-h-screen max-w-[1800px] lg:grid lg:gap-5 lg:px-4 lg:py-4 transition-[grid-template-columns] duration-300",
          collapsed
            ? "lg:grid-cols-[64px_minmax(0,1fr)]"
            : "lg:grid-cols-[290px_minmax(0,1fr)]",
        )}
      >
        <aside
          className={cn(
            "app-sidebar-panel sticky top-4 hidden h-[calc(100vh-2rem)] flex-col overflow-hidden rounded-[2rem] transition-all duration-300 lg:flex",
            collapsed ? "w-16" : "w-[290px]",
          )}
        >
          <div className={cn("py-6", collapsed ? "px-2" : "px-6")}>
            <div className={cn("flex items-center", collapsed ? "justify-center" : "gap-3")}>
              <div className="flex size-11 shrink-0 items-center justify-center rounded-2xl bg-white/10 ring-1 ring-white/10">
                <Waypoints className="size-5" />
              </div>
              {!collapsed && (
                <div>
                  <p className="text-[0.68rem] uppercase tracking-[0.28em] text-sidebar-foreground/60">
                    Lane
                  </p>
                  <h1 className="text-lg font-semibold tracking-tight">DB Control</h1>
                </div>
              )}
            </div>
            {!collapsed && (
              <p className="mt-4 text-sm leading-6 text-sidebar-foreground/70">
                A calmer shell for query work, approvals, and access operations.
              </p>
            )}
          </div>
          <Separator className="bg-white/10" />
          <SidebarSections sections={navSections} collapsed={collapsed} />
          <div className={cn("flex px-3 pb-1", collapsed ? "justify-center" : "justify-end")}>
            <Button
              variant="ghost"
              size="icon-sm"
              className="rounded-xl text-sidebar-foreground/50 hover:bg-white/10 hover:text-sidebar-foreground"
              onClick={toggleCollapsed}
              aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
            >
              {collapsed ? (
                <PanelLeftOpen className="size-4" />
              ) : (
                <PanelLeftClose className="size-4" />
              )}
            </Button>
          </div>
          <SidebarFooter email={user?.email} onLogout={logout} collapsed={collapsed} authProviders={authProviders} />
        </aside>

        <div className="flex min-h-screen min-w-0 flex-col lg:min-h-[calc(100vh-2rem)]">
          <header className="border-b border-border/70 bg-background/75 backdrop-blur-xl lg:rounded-[2rem] lg:border lg:bg-background/[0.85] lg:shadow-[0_14px_40px_rgba(15,23,42,0.08)] mb-4">
            <div className="flex items-center gap-4 px-4 py-4 sm:px-6 lg:px-5">
              <Button
                variant="outline"
                size="icon-sm"
                className="shrink-0 rounded-xl border-border bg-muted/50 lg:hidden"
                onClick={() => setMobileNavOpen(true)}
                aria-label="Open navigation"
              >
                <Menu className="size-4" />
              </Button>
              <div className="min-w-0">
                <p className="text-[0.68rem] uppercase tracking-[0.28em] text-muted-foreground">
                  {pageMeta.eyebrow}
                </p>
                <div className="flex items-center gap-2">
                  <h2 className="truncate text-lg font-semibold tracking-tight sm:text-xl">
                    {pageMeta.title}
                  </h2>
                  {pendingCount > 0 && location.pathname !== "/approvals" && (
                    <span className="rounded-full bg-amber-500/12 px-2 py-1 text-[0.68rem] font-semibold text-amber-700 ring-1 ring-amber-500/25">
                      {pendingCount} pending
                    </span>
                  )}
                </div>
                <p className="hidden text-sm text-muted-foreground md:block">
                  {pageMeta.description}
                </p>
              </div>
              <div className="ml-auto hidden items-center gap-3 sm:flex">
                <SearchBar />
                <div className="hidden rounded-full border border-border bg-muted/50 px-3 py-1.5 text-sm text-foreground xl:block">
                  {user?.email}
                </div>
                <button
                  onClick={() => {
                    const html = document.documentElement;
                    const isDark = html.classList.toggle("dark");
                    localStorage.setItem("theme", isDark ? "dark" : "light");
                  }}
                  className="flex size-10 items-center justify-center rounded-2xl bg-primary text-primary-foreground shadow-[0_12px_30px_rgba(15,109,117,0.24)] transition-colors hover:bg-primary/90"
                >
                  <Sun className="size-4 hidden dark:block" />
                  <Moon className="size-4 block dark:hidden" />
                </button>
              </div>
            </div>
          </header>

          <main className="min-w-0 flex-1 overflow-x-hidden border-b border-border/70 bg-background/75 backdrop-blur-xl lg:rounded-[2rem] lg:border lg:bg-background/[0.85] lg:shadow-[0_14px_40px_rgba(15,23,42,0.08)]">
            <Outlet />
          </main>
        </div>
      </div>
    </div>
  );
}

function SidebarSections({
  sections,
  collapsed = false,
}: {
  sections: NavSection[];
  collapsed?: boolean;
}) {
  return (
    <div className="flex-1 overflow-y-auto px-3 py-4">
      {sections.map((section) => (
        <div key={section.label} className="mb-6 last:mb-0">
          {!collapsed && (
            <p className="px-3 text-[0.68rem] font-medium uppercase tracking-[0.24em] text-sidebar-foreground/45">
              {section.label}
            </p>
          )}
          <div className={cn("space-y-1.5", !collapsed && "mt-2")}>
            {section.items.map((item) => (
              <SidebarLink
                key={item.to}
                to={item.to}
                icon={item.icon}
                badge={item.badge}
                collapsed={collapsed}
              >
                {item.label}
              </SidebarLink>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function SidebarFooter({
  email,
  onLogout,
  collapsed = false,
  authProviders = [],
}: {
  email?: string;
  onLogout: () => Promise<void>;
  collapsed?: boolean;
  authProviders?: string[];
}) {
  const tailscaleOnly = authProviders.length === 1 && authProviders[0] === "tailscale";

  return (
    <>
      <Separator className="bg-white/10" />
      <div className={cn("space-y-3 py-4", collapsed ? "px-2" : "px-4")}>
        {email && !collapsed && (
          <div className="rounded-2xl border border-white/10 bg-white/[0.06] px-3 py-3">
            <p className="text-[0.68rem] uppercase tracking-[0.24em] text-sidebar-foreground/45">
              Signed in
            </p>
            <p className="mt-1 truncate text-sm text-sidebar-foreground/80" title={email}>
              {email}
            </p>
          </div>
        )}
        {!tailscaleOnly && (
          collapsed ? (
            <Button
              variant="ghost"
              size="icon-sm"
              className="mx-auto flex rounded-xl text-sidebar-foreground/70 hover:bg-white/10 hover:text-sidebar-foreground"
              onClick={() => void onLogout()}
              aria-label="Sign out"
            >
              <LogOut className="size-4" />
            </Button>
          ) : (
            <Button
              variant="ghost"
              size="sm"
              className="w-full justify-start rounded-xl px-3 text-sidebar-foreground/70 hover:bg-white/10 hover:text-sidebar-foreground"
              onClick={() => void onLogout()}
            >
              Sign out
            </Button>
          )
        )}
      </div>
    </>
  );
}

function MobileHeader({ email, onClose }: { email?: string; onClose: () => void }) {
  return (
    <div className="mb-3 flex items-start justify-between gap-4 px-2 pb-3">
      <div>
        <p className="text-[0.68rem] uppercase tracking-[0.28em] text-sidebar-foreground/60">
          Lane
        </p>
        <h2 className="mt-1 text-lg font-semibold tracking-tight">DB Control</h2>
        {email && <p className="mt-1 text-sm text-sidebar-foreground/70">{email}</p>}
      </div>
      <Button
        variant="ghost"
        size="icon-sm"
        className="rounded-xl text-sidebar-foreground/80 hover:bg-white/10 hover:text-sidebar-foreground"
        onClick={onClose}
        aria-label="Close navigation"
      >
        <X className="size-4" />
      </Button>
    </div>
  );
}

function SidebarLink({
  to,
  children,
  icon,
  badge,
  collapsed = false,
}: {
  to: string;
  children: ReactNode;
  icon: ReactNode;
  badge?: number;
  collapsed?: boolean;
}) {
  return (
    <NavLink
      to={to}
      end={to === "/"}
      className={({ isActive }) =>
        cn(
          "group flex items-center rounded-2xl text-sm transition-all",
          collapsed ? "justify-center px-1 py-2" : "justify-between gap-3 px-3 py-3",
          isActive
            ? "bg-white text-slate-900 shadow-[0_18px_40px_rgba(15,23,42,0.22)]"
            : "text-sidebar-foreground/70 hover:bg-white/[0.08] hover:text-sidebar-foreground",
        )
      }
    >
      {({ isActive }) => (
        <>
          <span className={cn("flex min-w-0 items-center", collapsed ? "justify-center" : "gap-3")}>
            <span
              className={cn(
                "flex size-9 shrink-0 items-center justify-center rounded-xl transition-colors",
                isActive ? "bg-slate-100 text-slate-900" : "bg-white/[0.08] text-sidebar-foreground/80",
              )}
            >
              {icon}
            </span>
            {!collapsed && <span className="truncate font-medium">{children}</span>}
          </span>
          {badge !== undefined && !collapsed && (
            <span
              className={cn(
                "min-w-6 rounded-full px-2 py-1 text-center text-[0.68rem] font-semibold",
                isActive ? "bg-amber-100 text-amber-800" : "bg-amber-500/14 text-amber-200",
              )}
            >
              {badge}
            </span>
          )}
        </>
      )}
    </NavLink>
  );
}
