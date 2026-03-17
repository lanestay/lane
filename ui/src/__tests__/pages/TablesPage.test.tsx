import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import TablesPage from "@/pages/TablesPage";
import {
  mockFetch,
  jsonResponse,
  CONNECTIONS,
  DATABASES,
  TABLES,
  COLUMNS,
  QUERY_RESULT,
  FOREIGN_KEYS_RESULT,
  INDEXES_RESULT,
} from "../helpers";

beforeEach(() => {
  localStorage.setItem("session_token", "test-key");
});

afterEach(() => {
  vi.restoreAllMocks();
});

function routeFetch(url: string | URL | RequestInfo, init?: RequestInit): Response {
  const u = String(url);
  if (u.includes("/connections")) return jsonResponse(CONNECTIONS);
  if (u.includes("/databases")) return jsonResponse(DATABASES);
  if (u.includes("/describe")) return jsonResponse(COLUMNS);
  if (u.includes("/tables")) return jsonResponse(TABLES);
  if (u.includes("/lane") && init?.method === "POST") {
    const body = init.body ? JSON.parse(init.body as string) : {};
    const sql: string = body.query ?? "";
    // FK query
    if (sql.includes("sys.foreign_keys") || sql.includes("table_constraints")) {
      return jsonResponse(FOREIGN_KEYS_RESULT);
    }
    // Index query
    if (sql.includes("sys.indexes") || sql.includes("pg_index")) {
      return jsonResponse(INDEXES_RESULT);
    }
    return jsonResponse(QUERY_RESULT);
  }
  return jsonResponse([]);
}

describe("TablesPage", () => {
  it("shows initial state before database selection", async () => {
    mockFetch(async (url, init) => routeFetch(url, init));
    render(<TablesPage />);
    expect(screen.getByText("Schemas & Tables")).toBeInTheDocument();
  });

  it("loads tables when database is set", async () => {
    mockFetch(async (url, init) => routeFetch(url, init));
    render(<TablesPage />);
    // After mount, ConnectionPicker auto-selects default => triggers table load
    await waitFor(() => {
      expect(screen.getByText("users")).toBeInTheDocument();
      expect(screen.getByText("orders")).toBeInTheDocument();
    });
  });

  it("generates MSSQL SELECT TOP query for mssql connections", async () => {
    const user = userEvent.setup();
    const spy = mockFetch(async (url, init) => routeFetch(url, init));
    render(<TablesPage />);
    // Wait for tables to load
    await waitFor(() => expect(screen.getByText("users")).toBeInTheDocument());
    // Click on a table
    await user.click(screen.getByText("users"));
    await waitFor(() => {
      const postCalls = spy.mock.calls.filter(
        ([, init]) => (init as RequestInit | undefined)?.method === "POST"
      );
      const previewCall = postCalls.find(([, init]) => {
        const body = JSON.parse((init as RequestInit).body as string);
        return body.query?.includes("SELECT TOP 100");
      });
      expect(previewCall).toBeDefined();
    });
  });

  it("generates Postgres LIMIT query for postgres connections", async () => {
    const user = userEvent.setup();
    // Return postgres as first (default) connection
    const pgConnections = [
      { name: "dev-pg", is_default: true, type: "postgres", default_database: "postgres" },
    ];
    const spy = mockFetch(async (url, init) => {
      const u = String(url);
      if (u.includes("/connections")) return jsonResponse(pgConnections);
      return routeFetch(url, init);
    });
    render(<TablesPage />);
    await waitFor(() => expect(screen.getByText("users")).toBeInTheDocument());
    await user.click(screen.getByText("users"));
    await waitFor(() => {
      const postCalls = spy.mock.calls.filter(
        ([, init]) => (init as RequestInit | undefined)?.method === "POST"
      );
      const previewCall = postCalls.find(([, init]) => {
        const body = JSON.parse((init as RequestInit).body as string);
        return body.query?.includes("LIMIT 100");
      });
      expect(previewCall).toBeDefined();
    });
  });

  it("shows Columns and ERD tabs when a table is selected", async () => {
    const user = userEvent.setup();
    mockFetch(async (url, init) => routeFetch(url, init));
    render(<TablesPage />);
    await waitFor(() => expect(screen.getByText("users")).toBeInTheDocument());
    await user.click(screen.getByText("users"));
    await waitFor(() => {
      expect(screen.getByRole("tab", { name: "Columns" })).toBeInTheDocument();
      expect(screen.getByRole("tab", { name: "ERD" })).toBeInTheDocument();
    });
  });

  it("global search triggers re-query with WHERE clause containing LIKE", async () => {
    const user = userEvent.setup();
    const spy = mockFetch(async (url, init) => routeFetch(url, init));
    render(<TablesPage />);
    await waitFor(() => expect(screen.getByText("users")).toBeInTheDocument());
    await user.click(screen.getByText("users"));
    await waitFor(() => expect(screen.getByPlaceholderText("Search all columns...")).toBeInTheDocument());

    await user.type(screen.getByPlaceholderText("Search all columns..."), "alice");

    // Wait for debounce + re-query
    await waitFor(() => {
      const postCalls = spy.mock.calls.filter(
        ([, init]) => (init as RequestInit | undefined)?.method === "POST"
      );
      const filterCall = postCalls.find(([, init]) => {
        const body = JSON.parse((init as RequestInit).body as string);
        return body.query?.includes("LIKE") && body.query?.includes("alice");
      });
      expect(filterCall).toBeDefined();
    }, { timeout: 2000 });
  });

  it("clicking column header adds ORDER BY to query", async () => {
    const user = userEvent.setup();
    const spy = mockFetch(async (url, init) => routeFetch(url, init));
    render(<TablesPage />);
    await waitFor(() => expect(screen.getByText("users")).toBeInTheDocument());
    await user.click(screen.getByText("users"));
    await waitFor(() => expect(screen.getByText("Preview (first 100 rows)")).toBeInTheDocument());

    // Find the "id" column header button in the preview table and click it
    const previewSection = screen.getByText("Preview (first 100 rows)").parentElement!;
    const idButtons = Array.from(previewSection.querySelectorAll("button")).filter(
      (btn) => btn.textContent?.includes("id") && btn.textContent?.includes("PK")
    );
    if (idButtons.length > 0) {
      await user.click(idButtons[0]);
    }

    await waitFor(() => {
      const postCalls = spy.mock.calls.filter(
        ([, init]) => (init as RequestInit | undefined)?.method === "POST"
      );
      const sortCall = postCalls.find(([, init]) => {
        const body = JSON.parse((init as RequestInit).body as string);
        return body.query?.includes("ORDER BY");
      });
      expect(sortCall).toBeDefined();
    });
  });

  it("clear filters resets to base query", async () => {
    const user = userEvent.setup();
    const spy = mockFetch(async (url, init) => routeFetch(url, init));
    render(<TablesPage />);
    await waitFor(() => expect(screen.getByText("users")).toBeInTheDocument());
    await user.click(screen.getByText("users"));
    await waitFor(() => expect(screen.getByPlaceholderText("Search all columns...")).toBeInTheDocument());

    // Type a search term
    await user.type(screen.getByPlaceholderText("Search all columns..."), "test");
    await waitFor(() => expect(screen.getByText("Clear all")).toBeInTheDocument(), { timeout: 2000 });

    // Click clear all
    await user.click(screen.getByText("Clear all"));

    // Verify the search input is cleared
    const input = screen.getByPlaceholderText("Search all columns...") as HTMLInputElement;
    expect(input.value).toBe("");
  });

  it("shows FK badge on columns with foreign keys", async () => {
    const user = userEvent.setup();
    // Use columns that include the FK parent column
    const columnsWithFk = [
      { COLUMN_NAME: "id", DATA_TYPE: "int", IS_NULLABLE: "NO", IS_PRIMARY_KEY: "YES" },
      { COLUMN_NAME: "user_id", DATA_TYPE: "int", IS_NULLABLE: "NO", IS_PRIMARY_KEY: "NO" },
    ];
    // Return "orders" table columns so FK_orders_users matches
    mockFetch(async (url, init) => {
      const u = String(url);
      if (u.includes("/describe")) return jsonResponse(columnsWithFk);
      return routeFetch(url, init);
    });
    render(<TablesPage />);
    await waitFor(() => expect(screen.getByText("orders")).toBeInTheDocument());
    await user.click(screen.getByText("orders"));
    await waitFor(() => {
      // "FK" appears as both a table header and badge — check for the badge specifically
      const badges = screen.getAllByText("FK");
      const fkBadge = badges.find((el) => el.getAttribute("data-slot") === "badge");
      expect(fkBadge).toBeDefined();
    });
  });
});
