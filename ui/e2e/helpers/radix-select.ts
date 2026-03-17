import type { Locator } from "@playwright/test";

/**
 * Interact with a shadcn/Radix Select component.
 * Clicks the trigger, waits for the dropdown content, then clicks the matching item.
 */
export async function selectOption(trigger: Locator, itemText: string) {
  await trigger.click();
  const page = trigger.page();
  await page
    .locator('[data-slot="select-item"]')
    .filter({ hasText: itemText })
    .click();
}
