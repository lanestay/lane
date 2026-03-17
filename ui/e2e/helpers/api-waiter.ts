import type { Page } from "@playwright/test";

/**
 * Create a promise that resolves when a matching API request completes.
 * Call BEFORE triggering the action, then await AFTER the action.
 *
 * Usage:
 *   const wait = waitForApi(page, '/api/lane', 'POST');
 *   await page.click('#run');
 *   const response = await wait;
 */
export function waitForApi(page: Page, urlPattern: string | RegExp, method?: string) {
  return page.waitForResponse((res) => {
    const url = typeof urlPattern === "string"
      ? res.url().includes(urlPattern)
      : urlPattern.test(res.url());
    const methodMatch = method ? res.request().method() === method.toUpperCase() : true;
    return url && methodMatch;
  });
}
