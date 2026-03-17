import { test, expect } from "../fixtures";
import { navigateTo } from "../helpers/navigation";
import { isAccessControlEnabled, switchAdminTab, createTestUser, deleteTestUser } from "../helpers/admin";
import { selectOption } from "../helpers/radix-select";

const PERM_USER_EMAIL = `e2e-perm-${Date.now()}@test.com`;

test.describe("Admin — Permissions", () => {
  test.describe.configure({ mode: "serial" });

  let skipRest = false;

  test.beforeEach(async ({ appPage: page }) => {
    if (skipRest) test.skip();
    await navigateTo(page, "Admin");
    await switchAdminTab(page, "Permissions");
  });

  test("user select dropdown renders", async ({ appPage: page }) => {
    await page.waitForTimeout(2000);

    // Check for access control
    const enabled = await isAccessControlEnabled(page);
    if (!enabled) {
      skipRest = true;
      test.skip();
      return;
    }

    await expect(page.getByText("User:")).toBeVisible();
    await expect(page.locator('[data-slot="select-trigger"]')).toBeVisible();
  });

  test("skip if no access control", async ({ appPage: page }) => {
    await page.waitForTimeout(2000);
    const enabled = await isAccessControlEnabled(page);
    if (!enabled) {
      skipRest = true;
      test.skip();
    }

    // Create a test user for permissions testing
    await switchAdminTab(page, "Users");
    await page.waitForTimeout(1000);
    await createTestUser(page, PERM_USER_EMAIL, "Perm Test User");
    await switchAdminTab(page, "Permissions");
  });

  test("select user shows permissions card", async ({ appPage: page }) => {
    // Wait for the user select to be available
    await page.waitForTimeout(1000);
    const trigger = page.locator('[data-slot="select-trigger"]');
    await selectOption(trigger, PERM_USER_EMAIL);

    // Permissions card should appear
    await expect(page.getByText(`Permissions for ${PERM_USER_EMAIL}`)).toBeVisible({ timeout: 5_000 });
    await expect(page.getByRole("button", { name: "Add Rule" })).toBeVisible();
  });

  test("add rule creates editable row", async ({ appPage: page }) => {
    await page.waitForTimeout(1000);
    const trigger = page.locator('[data-slot="select-trigger"]');
    await selectOption(trigger, PERM_USER_EMAIL);

    await page.getByRole("button", { name: "Add Rule" }).click();

    // Should have Database and Table Pattern inputs
    await expect(page.getByRole("columnheader", { name: "Database" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Table Pattern" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Read" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Write" })).toBeVisible();
  });

  test("save permissions button works", async ({ appPage: page }) => {
    await page.waitForTimeout(1000);
    const trigger = page.locator('[data-slot="select-trigger"]');
    await selectOption(trigger, PERM_USER_EMAIL);

    // Add a rule and save
    await page.getByRole("button", { name: "Add Rule" }).click();

    const saveBtn = page.getByRole("button", { name: "Save Permissions" });
    await expect(saveBtn).toBeVisible();
    await saveBtn.click();

    // Button should show "Saving..." briefly
    await expect(saveBtn).toBeEnabled({ timeout: 5_000 });

    // Cleanup: delete the test user
    await switchAdminTab(page, "Users");
    await page.waitForTimeout(1000);
    await deleteTestUser(page, PERM_USER_EMAIL);
  });
});
