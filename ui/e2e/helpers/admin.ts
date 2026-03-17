import type { Page } from "@playwright/test";

/**
 * Check if the "Access control is not enabled" message is shown.
 */
export async function isAccessControlEnabled(page: Page): Promise<boolean> {
  const unavailable = page.getByText("Access control is not enabled on this server.");
  // Short timeout — if the message appears quickly, access control is off
  const visible = await unavailable.isVisible().catch(() => false);
  return !visible;
}

/**
 * Switch between admin tabs: Users, Tokens, Permissions, Audit Log
 */
export async function switchAdminTab(page: Page, tab: "Users" | "Tokens" | "Permissions" | "Audit Log") {
  await page.getByRole("tab", { name: tab }).click();
}

/**
 * Create a test user via the Admin UI.
 * Assumes the Users tab is active.
 */
export async function createTestUser(page: Page, email: string, name?: string, isAdmin = false) {
  await page.getByRole("button", { name: "Create User" }).click();

  const dialog = page.locator('[data-slot="dialog-content"]');
  await dialog.waitFor();

  await dialog.getByLabel("Email").fill(email);
  if (name) {
    await dialog.getByLabel("Display Name").fill(name);
  }
  if (isAdmin) {
    await dialog.getByLabel("Admin").click();
  }

  await dialog.getByRole("button", { name: "Create" }).click();
  // Wait for dialog to close
  await dialog.waitFor({ state: "hidden" });
}

/**
 * Delete a test user via the Admin UI.
 * Assumes the Users tab is active and user exists.
 */
export async function deleteTestUser(page: Page, email: string) {
  const row = page.locator("tr").filter({ hasText: email });
  await row.getByRole("button", { name: "Delete" }).click();

  const dialog = page.locator('[data-slot="dialog-content"]');
  await dialog.waitFor();
  await dialog.getByRole("button", { name: "Delete" }).click();
  await dialog.waitFor({ state: "hidden" });
}
