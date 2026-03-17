import { test as base, type Page } from "@playwright/test";

type AppFixtures = {
  appPage: Page;
};

export const test = base.extend<AppFixtures>({
  appPage: async ({ page }, use) => {
    // Set up response listener BEFORE navigation to avoid race condition
    const dbResponse = page.waitForResponse(
      (res) => res.url().includes("/api/lane/databases") && res.status() === 200,
      { timeout: 15_000 },
    );
    await page.goto("/");
    // Wait for sidebar to confirm we're authenticated and app is loaded
    await page.getByRole("link", { name: "SQL Editor" }).waitFor({ timeout: 10_000 });
    // Wait for ConnectionPicker to fully load: connections fetched, then databases fetched
    await dbResponse;
    await use(page);
  },
});

export { expect } from "@playwright/test";
