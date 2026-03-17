import type { Page } from "@playwright/test";

const E2E_EMAIL = process.env.E2E_EMAIL ?? "admin@test.com";
const E2E_PASSWORD = process.env.E2E_PASSWORD;
if (!E2E_PASSWORD) throw new Error("E2E_PASSWORD env var must be set");

export async function login(page: Page, email: string = E2E_EMAIL, password: string = E2E_PASSWORD) {
  await page.getByLabel("Email").fill(email);
  await page.getByLabel("Password").fill(password);
  await page.getByRole("button", { name: "Sign In" }).click();
}

export async function logout(page: Page) {
  await page.getByRole("button", { name: "Logout" }).click();
}
