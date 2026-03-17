import { test, expect } from "../fixtures";
import { navigateTo } from "../helpers/navigation";
import { isAccessControlEnabled, switchAdminTab, createTestUser, deleteTestUser } from "../helpers/admin";

const TOKEN_USER_EMAIL = `e2e-token-${Date.now()}@test.com`;

test.describe("Admin — Tokens", () => {
  test.describe.configure({ mode: "serial" });

  let skipRest = false;

  test.beforeEach(async ({ appPage: page }) => {
    if (skipRest) test.skip();
    await navigateTo(page, "Admin");
    await switchAdminTab(page, "Tokens");
  });

  test("tokens table has correct headers", async ({ appPage: page }) => {
    // Wait for loading to finish
    await page.waitForTimeout(2000);

    // Check if access control is available
    const enabled = await isAccessControlEnabled(page);
    if (!enabled) {
      skipRest = true;
      test.skip();
      return;
    }

    await expect(page.getByRole("columnheader", { name: "Prefix" })).toBeVisible({ timeout: 10_000 });
    await expect(page.getByRole("columnheader", { name: "Email" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Label" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Expires" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Status" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Actions" })).toBeVisible();
  });

  test("skip if no access control", async ({ appPage: page }) => {
    await page.waitForTimeout(2000);
    const enabled = await isAccessControlEnabled(page);
    if (!enabled) {
      skipRest = true;
      test.skip();
    }

    // Create a test user for token generation (switch to Users tab first)
    await switchAdminTab(page, "Users");
    await page.waitForTimeout(1000);
    await createTestUser(page, TOKEN_USER_EMAIL, "Token Test User");
    await switchAdminTab(page, "Tokens");
  });

  test("generate token dialog", async ({ appPage: page }) => {
    await page.getByRole("button", { name: "Generate Token" }).click();

    const dialog = page.getByRole("dialog", { name: "Generate Token" });
    await dialog.waitFor();

    await dialog.getByLabel("User Email").fill(TOKEN_USER_EMAIL);
    await dialog.getByLabel("Label (optional)").fill("E2E test token");

    await dialog.getByRole("button", { name: "Generate" }).click();

    // Token reveal dialog should appear (different dialog)
    const revealDialog = page.getByRole("dialog", { name: "Token Generated" });
    await revealDialog.waitFor({ timeout: 10_000 });
    await expect(revealDialog.getByText("Token Generated")).toBeVisible();
  });

  test("token reveal has copy button", async ({ appPage: page }) => {
    // Generate another token to get the reveal dialog
    await page.getByRole("button", { name: "Generate Token" }).click();

    const dialog = page.getByRole("dialog", { name: "Generate Token" });
    await dialog.waitFor();
    await dialog.getByLabel("User Email").fill(TOKEN_USER_EMAIL);
    await dialog.getByRole("button", { name: "Generate" }).click();

    // Token reveal dialog
    const revealDialog = page.getByRole("dialog", { name: "Token Generated" });
    await revealDialog.waitFor({ timeout: 10_000 });

    // Token text should be visible (starts with bq_)
    await expect(revealDialog.locator(".font-mono")).toBeVisible();
    await expect(revealDialog.getByRole("button", { name: "Copy" })).toBeVisible();
    await expect(revealDialog.getByRole("button", { name: "Done" })).toBeVisible();

    // Close
    await revealDialog.getByRole("button", { name: "Done" }).click();
  });

  test("revoke token", async ({ appPage: page }) => {
    // Find an Active token and get its prefix to pin the row
    const activeRow = page.locator("tr").filter({ hasText: "Active" }).first();
    await expect(activeRow).toBeVisible({ timeout: 5_000 });
    const prefix = await activeRow.locator("td").first().textContent();

    await activeRow.getByRole("button", { name: "Revoke" }).click();

    // Wait for the specific row (by prefix) to show "Revoked"
    const revokedRow = page.locator("tr").filter({ hasText: prefix! });
    await expect(revokedRow.getByText("Revoked")).toBeVisible({ timeout: 5_000 });

    // Cleanup: delete the test user (switch to Users tab)
    await switchAdminTab(page, "Users");
    await page.waitForTimeout(1000);
    await deleteTestUser(page, TOKEN_USER_EMAIL);
  });
});
