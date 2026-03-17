/**
 * Integration tests that hit a real running server.
 * Skipped unless TEST_BASE_URL is set.
 *
 * Run with:
 *   TEST_BASE_URL=http://localhost:3401 TEST_ADMIN_EMAIL=admin@example.com TEST_ADMIN_PASSWORD=yourpassword npx vitest run --config vitest.integration.config.ts
 */

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const env = (import.meta as any).env ?? {};
const BASE: string | undefined = env.VITE_TEST_BASE_URL;
const ADMIN_EMAIL: string = env.VITE_TEST_ADMIN_EMAIL ?? "admin@example.com";
const ADMIN_PASSWORD: string = env.VITE_TEST_ADMIN_PASSWORD;

const skip = !BASE;

let sessionToken: string | undefined;

async function login(): Promise<string> {
  if (sessionToken) return sessionToken;
  const res = await fetch(`${BASE}/api/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email: ADMIN_EMAIL, password: ADMIN_PASSWORD }),
  });
  if (res.status !== 200) throw new Error(`Login failed: ${res.status}`);
  const body = await res.json();
  sessionToken = body.session_token ?? body.token;
  if (!sessionToken) throw new Error("No token in login response");
  return sessionToken;
}

async function authHeaders(): Promise<Record<string, string>> {
  const token = await login();
  return { "Content-Type": "application/json", "Authorization": `Bearer ${token}` };
}

describe.skipIf(skip)("Integration: REST API", () => {
  it("GET /health returns 200", async () => {
    const res = await fetch(`${BASE}/health`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.status).toBe("healthy");
  });

  it("GET /api/lane/connections returns array", async () => {
    const res = await fetch(`${BASE}/api/lane/connections`, { headers: await authHeaders() });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(Array.isArray(body)).toBe(true);
    expect(body.length).toBeGreaterThan(0);
  });

  it("rejects missing auth with 401", async () => {
    const res = await fetch(`${BASE}/api/lane/connections`);
    expect(res.status).toBe(401);
  });

  it("GET /api/lane/databases returns array", async () => {
    const res = await fetch(`${BASE}/api/lane/databases`, { headers: await authHeaders() });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(Array.isArray(body)).toBe(true);
  });

  it("POST /api/lane with SELECT 1", async () => {
    const res = await fetch(`${BASE}/api/lane`, {
      method: "POST",
      headers: await authHeaders(),
      body: JSON.stringify({ database: "master", query: "SELECT 1 AS val" }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.success).toBe(true);
    expect(body.data.length).toBeGreaterThan(0);
  });

  it("POST /api/lane with bad SQL returns error", async () => {
    const res = await fetch(`${BASE}/api/lane`, {
      method: "POST",
      headers: await authHeaders(),
      body: JSON.stringify({ database: "master", query: "SELECTTTT BOGUS" }),
    });
    expect(res.status).toBeGreaterThanOrEqual(400);
    const body = await res.json();
    expect(body.success).toBe(false);
  });
});
