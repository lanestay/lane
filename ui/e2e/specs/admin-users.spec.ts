import { test, expect } from "../fixtures";
import { navigateTo } from "../helpers/navigation";
import { isAccessControlEnabled, switchAdminTab, createTestUser, deleteTestUser } from "../helpers/admin";

const TEST_EMAIL = `e2e-user-${Date.now()}@test.com`;
const TEST_NAME = "E2E Test User";

test.describe("Admin — Users", () => {
  test.describe.configure({ mode: "serial" });

  let skipRest = false;

  test.beforeEach(async ({ appPage: page }) => {
    if (skipRest) test.skip();
    await navigateTo(page, "Admin");
    await switchAdminTab(page, "Users");
  });

  test("admin tabs are visible", async ({ appPage: page }) => {
    await expect(page.getByRole("tab", { name: "Users" })).toBeVisible();
    await expect(page.getByRole("tab", { name: "Tokens" })).toBeVisible();
    await expect(page.getByRole("tab", { name: "Permissions" })).toBeVisible();
    await expect(page.getByRole("tab", { name: "Audit Log" })).toBeVisible();
  });

  test("skip if no access control", async ({ appPage: page }) => {
    // Wait for users tab content to load
    await page.waitForTimeout(2000);
    const enabled = await isAccessControlEnabled(page);
    if (!enabled) {
      skipRest = true;
      test.skip();
    }
  });

  test("users table has correct headers", async ({ appPage: page }) => {
    await expect(page.getByRole("columnheader", { name: "Email" })).toBeVisible({ timeout: 10_000 });
    await expect(page.getByRole("columnheader", { name: "Name" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Role" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Status" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Actions" })).toBeVisible();
  });

  test("create user dialog", async ({ appPage: page }) => {
    await createTestUser(page, TEST_EMAIL, TEST_NAME);

    // New row should appear with the test email
    await expect(page.getByText(TEST_EMAIL)).toBeVisible({ timeout: 5_000 });
    await expect(page.getByText(TEST_NAME)).toBeVisible();
  });

  test("edit user dialog", async ({ appPage: page }) => {
    const row = page.locator("tr").filter({ hasText: TEST_EMAIL });
    await row.getByRole("button", { name: "Edit" }).click();

    const dialog = page.locator('[data-slot="dialog-content"]');
    await dialog.waitFor();

    // Change display name
    const nameInput = dialog.getByLabel("Display Name");
    await nameInput.clear();
    await nameInput.fill("Updated Name");

    await dialog.getByRole("button", { name: "Save" }).click();
    await dialog.waitFor({ state: "hidden" });

    // Row should reflect the update
    await expect(page.getByText("Updated Name")).toBeVisible({ timeout: 5_000 });
  });

  test("delete user dialog", async ({ appPage: page }) => {
    await deleteTestUser(page, TEST_EMAIL);

    // Row should be gone
    await expect(page.getByText(TEST_EMAIL)).toBeHidden({ timeout: 5_000 });
  });

  test("admin badge renders for admin users", async ({ appPage: page }) => {
    const adminEmail = `e2e-admin-${Date.now()}@test.com`;

    await createTestUser(page, adminEmail, "Admin User", true);

    // The row should show an "Admin" badge
    const row = page.locator("tr").filter({ hasText: adminEmail });
    await expect(row.getByText("Admin", { exact: true })).toBeVisible();

    // Cleanup
    await deleteTestUser(page, adminEmail);
  });
});
