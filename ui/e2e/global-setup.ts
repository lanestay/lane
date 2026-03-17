import { chromium, type FullConfig } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";

const BACKEND_URL = process.env.E2E_BACKEND_URL ?? "http://localhost:3401";
const BASE_URL = "http://localhost:5173";
const E2E_EMAIL = process.env.E2E_EMAIL ?? "admin@test.com";
const E2E_PASSWORD = process.env.E2E_PASSWORD;
if (!E2E_PASSWORD) throw new Error("E2E_PASSWORD env var must be set");
const AUTH_DIR = path.join(process.cwd(), ".auth");
const STORAGE_STATE = path.join(AUTH_DIR, "storage-state.json");

async function globalSetup(_config: FullConfig) {
  // 1. Pre-check backend health
  try {
    const res = await fetch(`${BACKEND_URL}/health`, { signal: AbortSignal.timeout(5000) });
    if (!res.ok) throw new Error(`Health check returned ${res.status}`);
  } catch (e) {
    throw new Error(
      `Backend not reachable at ${BACKEND_URL}/health. Start the server first.\n${e}`
    );
  }

  // 2. Ensure .auth directory exists
  fs.mkdirSync(AUTH_DIR, { recursive: true });

  // 3. Check if setup is needed and run it
  const statusRes = await fetch(`${BACKEND_URL}/api/auth/status`);
  const status = await statusRes.json();

  if (status.needs_setup) {
    const setupRes = await fetch(`${BACKEND_URL}/api/auth/setup`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email: E2E_EMAIL,
        display_name: "E2E Admin",
        password: E2E_PASSWORD,
      }),
    });
    if (!setupRes.ok) {
      const body = await setupRes.text();
      throw new Error(`Setup failed: ${body}`);
    }
  }

  // 4. Launch browser, login, and save storage state
  const browser = await chromium.launch();
  const page = await browser.newPage();

  await page.goto(BASE_URL);
  await page.getByLabel("Email").fill(E2E_EMAIL);
  await page.getByLabel("Password").fill(E2E_PASSWORD);
  await page.getByRole("button", { name: "Sign In" }).click();

  // Wait for the app shell (sidebar) to appear
  await page.getByRole("link", { name: "SQL Editor" }).waitFor({ timeout: 15_000 });

  // 5. Save storage state
  await page.context().storageState({ path: STORAGE_STATE });
  await browser.close();
}

export default globalSetup;
