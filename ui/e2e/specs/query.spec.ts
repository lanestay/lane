import { test, expect } from "../fixtures";
import { waitForApi } from "../helpers/api-waiter";

// Helper: focus the CodeMirror editor and type SQL
async function typeInEditor(page: import("@playwright/test").Page, text: string) {
  const editor = page.locator('[data-testid="sql-editor"] .cm-content');
  await editor.click();
  // Select all existing content and replace
  await page.keyboard.press("ControlOrMeta+a");
  await page.keyboard.type(text);
}

test.describe("SQL Editor", () => {
  test("ConnectionPicker auto-populates", async ({ appPage: page }) => {
    // Connection and database selects should have values after auto-load
    const connectionTrigger = page.locator('[data-slot="select-trigger"]').first();
    await expect(connectionTrigger).not.toHaveText("Select");
    // The trigger should contain a value (not be empty placeholder)
    await expect(connectionTrigger.locator('[data-slot="select-value"]')).not.toBeEmpty();
  });

  test("can switch database", async ({ appPage: page }) => {
    // The database select is the second trigger
    const dbTrigger = page.locator('[data-slot="select-trigger"]').nth(1);
    await expect(dbTrigger.locator('[data-slot="select-value"]')).not.toBeEmpty();

    // Open the dropdown — there should be at least one item
    await dbTrigger.click();
    const items = page.locator('[data-slot="select-item"]');
    await expect(items.first()).toBeVisible();

    // Click the first available item
    await items.first().click();
  });

  test("empty query shows error", async ({ appPage: page }) => {
    await page.getByRole("button", { name: "Run (Ctrl+Enter)" }).click();

    await expect(page.getByText("Query cannot be empty")).toBeVisible();
  });

  test("SELECT returns results", async ({ appPage: page }) => {
    await typeInEditor(page, "SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLES");

    const apiWait = waitForApi(page, "/api/lane", "POST");
    await page.getByRole("button", { name: "Run (Ctrl+Enter)" }).click();
    await apiWait;

    // Results table should show with row count badge and column header
    await expect(page.locator('[data-slot="badge"]').filter({ hasText: /rows$/ })).toBeVisible({ timeout: 10_000 });
    await expect(page.getByRole("columnheader", { name: "TABLE_NAME" })).toBeVisible();
  });

  test("bad SQL shows error banner", async ({ appPage: page }) => {
    await typeInEditor(page, "SELECTTTTTT BADQUERY");

    const apiWait = waitForApi(page, "/api/lane", "POST");
    await page.getByRole("button", { name: "Run (Ctrl+Enter)" }).click();
    await apiWait;

    // Error banner — uses class bg-destructive/20 with text-destructive inside
    await expect(page.locator(".text-destructive.font-mono")).toBeVisible({ timeout: 10_000 });
  });

  test("Ctrl+Enter executes query", async ({ appPage: page }) => {
    await typeInEditor(page, "SELECT TOP 1 TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES");

    const apiWait = waitForApi(page, "/api/lane", "POST");
    // Document-level keydown handler catches Ctrl/Cmd+Enter
    await page.keyboard.press("Control+Enter");
    await apiWait;

    await expect(page.locator('[data-slot="badge"]').filter({ hasText: /rows$/ })).toBeVisible({ timeout: 10_000 });
    await expect(page.getByRole("columnheader", { name: "TABLE_SCHEMA" })).toBeVisible();
  });

  test("results show stats", async ({ appPage: page }) => {
    await typeInEditor(page, "SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLES");

    const apiWait = waitForApi(page, "/api/lane", "POST");
    await page.getByRole("button", { name: "Run (Ctrl+Enter)" }).click();
    await apiWait;

    // Row count badge
    await expect(page.locator('[data-slot="badge"]').filter({ hasText: /rows$/ })).toBeVisible({ timeout: 10_000 });
    // Execution time (e.g. "12ms")
    await expect(page.getByText(/\d+ms/)).toBeVisible();
  });

  test("template picker inserts SQL", async ({ appPage: page }) => {
    // Click the template picker (3rd select trigger: connection, database, template)
    const templateTrigger = page.locator('[data-slot="select-trigger"]').nth(2);
    await expect(templateTrigger).toContainText("Insert template...");
    await templateTrigger.click();

    // Select the first template
    const items = page.locator('[data-slot="select-item"]');
    await expect(items.first()).toBeVisible();
    await items.first().click();

    // Editor should now contain SQL (check it has some content via CodeMirror)
    const editor = page.locator('[data-testid="sql-editor"] .cm-content');
    await expect(editor).not.toBeEmpty();
  });
});
