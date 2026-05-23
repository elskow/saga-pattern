import { test, expect } from "@playwright/test";

test.describe("Route Availability", () => {
  const routes = [
    { path: "/", pattern: /^http:\/\/127\.0\.0\.1:6173\/$/ },
    { path: "/checkout", pattern: /^http:\/\/127\.0\.0\.1:6173\/checkout/ },
    { path: "/admin", pattern: /^http:\/\/127\.0\.0\.1:6173\/admin/ },
    { path: "/admin/inventory", pattern: /^http:\/\/127\.0\.0\.1:6173\/admin\/inventory/ },
    { path: "/admin/orders", pattern: /^http:\/\/127\.0\.0\.1:6173\/admin\/orders/ },
    { path: "/admin/payment", pattern: /^http:\/\/127\.0\.0\.1:6173\/admin\/payment/ },
    {
      path: "/admin/shipments",
      pattern: /^http:\/\/127\.0\.0\.1:6173\/admin\/shipments/,
    },
    { path: "/orders", pattern: /^http:\/\/127\.0\.0\.1:6173\/orders/ },
  ];

  for (const { path, pattern } of routes) {
    test(`loads ${path} without 404`, async ({ page }) => {
      await page.goto(path);
      await page.waitForLoadState("networkidle");
      await page.waitForTimeout(500);

      await expect(page).toHaveURL(pattern);
      await expect(page).not.toContainText("404 - Not Found");
      await expect(page.locator("main")).not.toContainText("404 - Not Found");
    });
  }
});
