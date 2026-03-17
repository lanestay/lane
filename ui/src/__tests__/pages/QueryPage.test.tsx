import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { mockFetch, jsonResponse, CONNECTIONS, DATABASES, QUERY_RESULT } from "../helpers";

// Mock SqlEditor as a plain textarea so tests work without a real CodeMirror DOM
let mockValue = "";

vi.mock("@/components/SqlEditor", () => {
  const MockSqlEditor = React.forwardRef(function MockSqlEditor(
    props: { onExecute?: () => void; placeholder?: string },
    ref: React.Ref<{ getValue: () => string; replaceAll: (t: string) => void; view: null }>,
  ) {
    React.useImperativeHandle(ref, () => ({
      getValue: () => mockValue,
      replaceAll: (t: string) => { mockValue = t; },
      view: null,
    }));
    return (
      <textarea
        data-testid="sql-editor"
        placeholder={props.placeholder}
        onChange={(e) => { mockValue = e.target.value; }}
        onKeyDown={(e) => {
          if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
            e.preventDefault();
            props.onExecute?.();
          }
        }}
      />
    );
  });
  return { __esModule: true, default: MockSqlEditor };
});

// Must import after mock setup
const { default: QueryPage } = await import("@/pages/QueryPage");

beforeEach(() => {
  localStorage.setItem("session_token", "test-key");
  mockValue = "";
});

afterEach(() => {
  vi.restoreAllMocks();
});

function routeFetch(url: string | URL | RequestInfo, init?: RequestInit): Response {
  const u = String(url);
  if (u.includes("/connections")) return jsonResponse(CONNECTIONS);
  if (u.includes("/databases")) return jsonResponse(DATABASES);
  if (u.includes("/lane") && init?.method === "POST") return jsonResponse(QUERY_RESULT);
  return jsonResponse([]);
}

describe("QueryPage", () => {
  it("renders the SQL editor and Run button", async () => {
    mockFetch(async (url, init) => routeFetch(url, init));
    render(<QueryPage />);
    expect(screen.getByTestId("sql-editor")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Run/i })).toBeInTheDocument();
  });

  it("shows error when running empty query", async () => {
    const user = userEvent.setup();
    mockFetch(async (url, init) => routeFetch(url, init));
    render(<QueryPage />);
    await user.click(screen.getByRole("button", { name: /Run/i }));
    expect(screen.getByText("Query cannot be empty")).toBeInTheDocument();
  });

  it("executes query and shows results", async () => {
    const user = userEvent.setup();
    mockFetch(async (url, init) => routeFetch(url, init));
    render(<QueryPage />);
    const textarea = screen.getByTestId("sql-editor");
    await user.type(textarea, "SELECT 1");
    await user.click(screen.getByRole("button", { name: /Run/i }));
    await waitFor(() => expect(screen.getByText("2 rows")).toBeInTheDocument());
  });

  it("shows error banner on query failure", async () => {
    const user = userEvent.setup();
    mockFetch(async (url, init) => {
      const u = String(url);
      if (u.includes("/lane") && init?.method === "POST") {
        return jsonResponse(
          { error: { message: "Invalid object name 'foo'" } },
          400
        );
      }
      return routeFetch(url, init);
    });
    render(<QueryPage />);
    const textarea = screen.getByTestId("sql-editor");
    await user.type(textarea, "SELECT * FROM foo");
    await user.click(screen.getByRole("button", { name: /Run/i }));
    await waitFor(() => expect(screen.getByText(/Invalid object name/)).toBeInTheDocument());
  });

  it("runs query on Ctrl+Enter", async () => {
    const user = userEvent.setup();
    const spy = mockFetch(async (url, init) => routeFetch(url, init));
    render(<QueryPage />);
    const textarea = screen.getByTestId("sql-editor");
    await user.type(textarea, "SELECT 1");
    await user.keyboard("{Control>}{Enter}{/Control}");
    await waitFor(() => {
      const postCalls = spy.mock.calls.filter(
        ([, init]) => (init as RequestInit | undefined)?.method === "POST"
      );
      expect(postCalls.length).toBeGreaterThan(0);
    });
  });
});
