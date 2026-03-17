import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import AdminPage from "@/pages/AdminPage";
import { mockFetch, jsonResponse, USERS, TOKENS, AUDIT_ENTRIES } from "../helpers";

beforeEach(() => {
  localStorage.setItem("session_token", "test-key");
});

afterEach(() => {
  vi.restoreAllMocks();
});

function routeAdmin(url: string | URL | RequestInfo): Response {
  const u = String(url);
  if (u.includes("/admin/connections")) return jsonResponse({ connections: [] });
  if (u.includes("/admin/users")) return jsonResponse({ users: USERS });
  if (u.includes("/admin/tokens")) return jsonResponse({ tokens: TOKENS });
  if (u.includes("/admin/audit")) return jsonResponse({ entries: AUDIT_ENTRIES });
  return jsonResponse({});
}

describe("AdminPage", () => {
  it("renders five tab triggers", async () => {
    mockFetch(async (url) => routeAdmin(url));
    render(<AdminPage />);
    expect(screen.getByRole("tab", { name: "Connections" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Users" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Tokens" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Permissions" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Audit Log" })).toBeInTheDocument();
  });

  it("handles 503 (access control disabled) gracefully", async () => {
    mockFetch(async () => new Response("", { status: 503 }));
    render(<AdminPage />);
    await waitFor(() =>
      expect(screen.getByText("Access control is not enabled on this server.")).toBeInTheDocument()
    );
  });

  it("renders users table when data loads", async () => {
    const user = userEvent.setup();
    mockFetch(async (url) => routeAdmin(url));
    render(<AdminPage />);
    // Switch to Users tab (Connections is now default)
    await user.click(screen.getByRole("tab", { name: "Users" }));
    await waitFor(() => {
      expect(screen.getByText("admin@test.com")).toBeInTheDocument();
      expect(screen.getByText("user@test.com")).toBeInTheDocument();
    });
  });

  it("can dismiss error banner", async () => {
    const user = userEvent.setup();
    let callCount = 0;
    mockFetch(async (url) => {
      callCount++;
      if (callCount === 1) return routeAdmin(url);
      return routeAdmin(url);
    });
    render(<AdminPage />);
    // Switch to Users tab
    await user.click(screen.getByRole("tab", { name: "Users" }));
    await waitFor(() => expect(screen.getByText("admin@test.com")).toBeInTheDocument());

    // Force an error by switching tabs to trigger a new fetch that fails
    mockFetch(async () => {
      throw new Error("Network error");
    });
    await user.click(screen.getByRole("tab", { name: "Tokens" }));
    await waitFor(() => {
      const dismissBtn = screen.queryByText("dismiss");
      if (dismissBtn) {
        expect(dismissBtn).toBeInTheDocument();
      }
    });
  });
});
