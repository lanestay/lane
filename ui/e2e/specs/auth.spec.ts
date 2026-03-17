import { test, expect } from "@playwright/test";
import { login, logout } from "../helpers/auth";

// Auth tests run unauthenticated — no saved storage state
test.use({ storageState: { cookies: [], origins: [] } });

test.describe("Authentication", () => {
  test("login page renders", async ({ page }) => {
    await page.goto("/");

    await expect(page.getByText("Batch Query")).toBeVisible();
    await expect(page.getByLabel("Email")).toBeVisible();
    await expect(page.getByLabel("Password")).toBeVisible();
    await expect(page.getByRole("button", { name: "Sign In" })).toBeVisible();
  });

  test("empty fields shows validation error", async ({ page }) => {
    await page.goto("/");
    await page.getByRole("button", { name: "Sign In" }).click();

    await expect(page.getByText("Email and password are required")).toBeVisible();
  });

  test("invalid credentials shows error", async ({ page }) => {
    await page.goto("/");
    await login(page, "wrong@test.com", "wrongpassword");

    await expect(page.getByText(/Invalid email or password/)).toBeVisible();
  });

  test("valid credentials navigates to app shell", async ({ page }) => {
    await page.goto("/");
    await login(page);

    // Sidebar links should be visible
    await expect(page.getByRole("link", { name: "SQL Editor" })).toBeVisible();
    await expect(page.getByRole("link", { name: "Tables" })).toBeVisible();
  });

  test("logout returns to login page", async ({ page }) => {
    await page.goto("/");
    await login(page);

    // Confirm we're in the app
    await expect(page.getByRole("link", { name: "SQL Editor" })).toBeVisible();

    await logout(page);

    // Login form should reappear
    await expect(page.getByLabel("Email")).toBeVisible();
    await expect(page.getByRole("button", { name: "Sign In" })).toBeVisible();
  });
});
