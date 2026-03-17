import { mockFetch, jsonResponse, CONNECTIONS, DATABASES } from "../helpers";
import {
  listConnections,
  listDatabases,
  listTables,
  describeTable,
  executeQuery,
} from "@/lib/api";

beforeEach(() => {
  localStorage.setItem("session_token", "test-key");
});

afterEach(() => {
  localStorage.removeItem("session_token");
  vi.restoreAllMocks();
});

describe("auth helpers", () => {
  it("session token round-trip via localStorage", () => {
    localStorage.removeItem("session_token");
    expect(!!localStorage.getItem("session_token")).toBe(false);
    localStorage.setItem("session_token", "abc");
    expect(!!localStorage.getItem("session_token")).toBe(true);
  });

  it("removeItem clears the token", () => {
    localStorage.setItem("session_token", "abc");
    localStorage.removeItem("session_token");
    expect(!!localStorage.getItem("session_token")).toBe(false);
  });
});

describe("listConnections", () => {
  it("calls correct URL with auth header", async () => {
    const spy = mockFetch(async () => jsonResponse(CONNECTIONS));
    await listConnections();
    expect(spy).toHaveBeenCalledTimes(1);
    const [url, init] = spy.mock.calls[0];
    expect(url).toBe("/api/lane/connections");
    expect((init as RequestInit).headers).toMatchObject({ "Authorization": "Bearer test-key" });
  });

  it("throws on non-ok response", async () => {
    mockFetch(async () => new Response("fail", { status: 401 }));
    await expect(listConnections()).rejects.toThrow("401");
  });
});

describe("listDatabases", () => {
  it("appends connection query param when provided", async () => {
    const spy = mockFetch(async () => jsonResponse(DATABASES));
    await listDatabases("my-conn");
    const url = spy.mock.calls[0][0] as string;
    expect(url).toContain("connection=my-conn");
  });

  it("omits connection param when not provided", async () => {
    const spy = mockFetch(async () => jsonResponse(DATABASES));
    await listDatabases();
    const url = spy.mock.calls[0][0] as string;
    expect(url).toBe("/api/lane/databases");
  });
});

describe("executeQuery", () => {
  it("posts to correct URL with body", async () => {
    const spy = mockFetch(async () =>
      jsonResponse({ success: true, total_rows: 1, execution_time_ms: 5, rows_per_second: 200, data: [{ val: 1 }] })
    );
    await executeQuery("SELECT 1", "master");
    expect(spy).toHaveBeenCalledTimes(1);
    const [url, init] = spy.mock.calls[0];
    expect(url).toBe("/api/lane");
    expect((init as RequestInit).method).toBe("POST");
    const body = JSON.parse((init as RequestInit).body as string);
    expect(body.query).toBe("SELECT 1");
    expect(body.database).toBe("master");
    expect(body.includeMetadata).toBe(true);
  });

  it("parses error with suggestion", async () => {
    mockFetch(async () =>
      jsonResponse(
        { error: { message: "bad sql", suggestion: "check syntax" } },
        400
      )
    );
    await expect(executeQuery("BAD", "master")).rejects.toThrow("Suggestion: check syntax");
  });

  it("falls back to HTTP status on unparseable error", async () => {
    mockFetch(async () => new Response("server error", { status: 500 }));
    await expect(executeQuery("SELECT 1", "master")).rejects.toThrow("HTTP 500");
  });
});

describe("listTables and describeTable", () => {
  it("listTables builds query params correctly", async () => {
    const spy = mockFetch(async () => jsonResponse([]));
    await listTables("mydb", "conn1", "public");
    const url = spy.mock.calls[0][0] as string;
    expect(url).toContain("database=mydb");
    expect(url).toContain("connection=conn1");
    expect(url).toContain("schema=public");
  });

  it("describeTable builds query params correctly", async () => {
    const spy = mockFetch(async () => jsonResponse([]));
    await describeTable("mydb", "users", "conn1", "dbo");
    const url = spy.mock.calls[0][0] as string;
    expect(url).toContain("database=mydb");
    expect(url).toContain("table=users");
    expect(url).toContain("connection=conn1");
    expect(url).toContain("schema=dbo");
  });
});
