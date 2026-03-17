import { test, expect } from "../fixtures";
import { navigateTo } from "../helpers/navigation";

test.describe("Tables Browser", () => {
  test.beforeEach(async ({ appPage: page }) => {
    await navigateTo(page, "Tables");
  });

  test("page loads with schema tree", async ({ appPage: page }) => {
    await expect(page.getByText("Schemas & Tables")).toBeVisible();
  });

  test("tables populate after database loads", async ({ appPage: page }) => {
    // Wait for schema groups to populate (connection auto-selects default db)
    // Schema tree buttons should appear (e.g., "dbo", "public", etc.)
    const schemaButtons = page.locator("button").filter({ hasText: /^(▼|▶)/ });
    await expect(schemaButtons.first()).toBeVisible({ timeout: 10_000 });
  });

  test("click table shows column details", async ({ appPage: page }) => {
    // Wait for tables to load
    const schemaButtons = page.locator("button").filter({ hasText: /^(▼|▶)/ });
    await expect(schemaButtons.first()).toBeVisible({ timeout: 10_000 });

    // Find the first table button (inside the expanded schema, under ml-4)
    const firstTable = page.locator(".ml-4 button").first();
    await expect(firstTable).toBeVisible();
    const tableName = await firstTable.textContent();

    await firstTable.click();

    // Detail card should show with schema.table title
    await expect(page.getByText(`.${tableName}`)).toBeVisible({ timeout: 10_000 });

    // Column headers should be visible
    await expect(page.getByRole("columnheader", { name: "Column" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Type" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Nullable" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "PK" })).toBeVisible();
  });

  test("column types render", async ({ appPage: page }) => {
    const schemaButtons = page.locator("button").filter({ hasText: /^(▼|▶)/ });
    await expect(schemaButtons.first()).toBeVisible({ timeout: 10_000 });

    const firstTable = page.locator(".ml-4 button").first();
    await firstTable.click();

    // Wait for column details to load — look for common data type patterns
    await expect(page.getByRole("columnheader", { name: "Type" })).toBeVisible({ timeout: 10_000 });
    // At least one data cell should be visible in the column table
    const columnRows = page.locator("table").first().locator("tbody tr");
    await expect(columnRows.first()).toBeVisible();
  });

  test("preview loads automatically", async ({ appPage: page }) => {
    const schemaButtons = page.locator("button").filter({ hasText: /^(▼|▶)/ });
    await expect(schemaButtons.first()).toBeVisible({ timeout: 10_000 });

    const firstTable = page.locator(".ml-4 button").first();
    await firstTable.click();

    // Preview section should appear
    await expect(page.getByText("Preview (first 100 rows)")).toBeVisible({ timeout: 15_000 });
  });

  test("schema expand/collapse toggles", async ({ appPage: page }) => {
    const schemaButtons = page.locator("button").filter({ hasText: /^(▼|▶)/ });
    await expect(schemaButtons.first()).toBeVisible({ timeout: 10_000 });

    // Tables should be visible initially (expanded by default, ▼ arrow)
    const tables = page.locator(".ml-4 button");
    await expect(tables.first()).toBeVisible();

    // Click the schema toggle to collapse
    await schemaButtons.first().click();

    // Tables should be hidden after collapse
    await expect(tables.first()).toBeHidden();

    // Click again to re-expand
    await schemaButtons.first().click();
    await expect(tables.first()).toBeVisible();
  });
});
