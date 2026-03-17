import type { Page } from "@playwright/test";

export type SidebarLink = "SQL Editor" | "Tables" | "Admin";

export async function navigateTo(page: Page, link: SidebarLink) {
  await page.getByRole("link", { name: link }).click();
  // Wait for navigation to settle
  await page.waitForLoadState("networkidle");
}
