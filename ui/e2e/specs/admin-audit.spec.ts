import { test, expect } from "../fixtures";
import { navigateTo } from "../helpers/navigation";
import { isAccessControlEnabled, switchAdminTab } from "../helpers/admin";
import { selectOption } from "../helpers/radix-select";

test.describe("Admin — Audit Log", () => {
  let skipRest = false;

  test.beforeEach(async ({ appPage: page }) => {
    if (skipRest) test.skip();
    await navigateTo(page, "Admin");
    await switchAdminTab(page, "Audit Log");
  });

  test("filters and table render", async ({ appPage: page }) => {
    await page.waitForTimeout(2000);

    // Check for access control
    const enabled = await isAccessControlEnabled(page);
    if (!enabled) {
      skipRest = true;
      test.skip();
      return;
    }

    // Filter controls
    await expect(page.getByPlaceholder("Filter by email")).toBeVisible();
    await expect(page.getByRole("button", { name: "Refresh" })).toBeVisible();

    // Table headers
    await expect(page.getByRole("columnheader", { name: "Time" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Email" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Action" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Database" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Details" })).toBeVisible();
  });

  test("skip if no access control", async ({ appPage: page }) => {
    await page.waitForTimeout(2000);
    const enabled = await isAccessControlEnabled(page);
    if (!enabled) {
      skipRest = true;
      test.skip();
    }
  });

  test("action filter dropdown has options", async ({ appPage: page }) => {
    await page.waitForTimeout(1000);

    // Open the action filter dropdown
    const trigger = page.locator('[data-slot="select-trigger"]');
    await trigger.click();

    // Check for filter options
    await expect(page.locator('[data-slot="select-item"]').filter({ hasText: "All actions" })).toBeVisible();
    await expect(page.locator('[data-slot="select-item"]').filter({ hasText: "Allowed" })).toBeVisible();
    await expect(page.locator('[data-slot="select-item"]').filter({ hasText: "Denied" })).toBeVisible();

    // Close by selecting current value
    await page.locator('[data-slot="select-item"]').filter({ hasText: "All actions" }).click();
  });

  test("refresh reloads data", async ({ appPage: page }) => {
    await page.waitForTimeout(1000);

    const refreshBtn = page.getByRole("button", { name: "Refresh" });
    await expect(refreshBtn).toBeVisible();

    // Click refresh — should show loading state briefly
    await refreshBtn.click();

    // The table should reappear after refresh (loading state passes)
    await expect(page.getByRole("columnheader", { name: "Time" })).toBeVisible({ timeout: 10_000 });
  });
});
