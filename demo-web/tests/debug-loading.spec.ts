import { test, expect } from "@playwright/test";

test("debug inventory and shipments loading", async ({ page }) => {
  const consoleMessages: string[] = [];
  const networkErrors: string[] = [];
  
  page.on("console", msg => {
    const text = `${msg.type()}: ${msg.text()}`;
    consoleMessages.push(text);
    console.log(text);
  });
  
  page.on("pageerror", err => {
    consoleMessages.push(`PAGE ERROR: ${err.message}`);
    console.error("PAGE ERROR:", err.message);
  });

  page.on("requestfailed", request => {
    const text = `REQUEST FAILED: ${request.url()} - ${request.failure()?.errorText}`;
    networkErrors.push(text);
    console.error(text);
  });

  // Intercept fetch/XHR responses
  await page.route("**/proxy/**", async route => {
    const response = await route.fetch();
    const status = response.status();
    const url = route.request().url();
    console.log(`PROXY RESPONSE: ${url} -> ${status}`);
    await route.fulfill({ response });
  });

  // Test inventory page
  console.log("\n=== TESTING INVENTORY PAGE ===\n");
  await page.goto("http://localhost:6173/admin/inventory");
  
  // Wait up to 10 seconds
  await page.waitForTimeout(10000);
  
  // Check for skeletons
  const skeletons = await page.locator("[data-testid='skeleton'], .animate-pulse, [role='presentation']").count();
  console.log(`Skeleton count after 10s: ${skeletons}`);
  
  // Check for table rows
  const tableRows = await page.locator("table tbody tr").count();
  console.log(`Table rows: ${tableRows}`);
  
  // Check for error messages
  const errorDivs = await page.locator(".border-amber-200, .text-destructive").count();
  console.log(`Error divs: ${errorDivs}`);
  
  // Check page text
  const pageText = await page.locator("table").first().allTextContents().catch(() => ["no table found"]);
  console.log(`Table content preview: ${pageText[0]?.substring(0, 200)}`);

  // Test shipments page
  console.log("\n=== TESTING SHIPMENTS PAGE ===\n");
  await page.goto("http://localhost:6173/admin/shipments");
  
  await page.waitForTimeout(10000);
  
  const skeletons2 = await page.locator("[data-testid='skeleton'], .animate-pulse, [role='presentation']").count();
  console.log(`Skeleton count after 10s: ${skeletons2}`);
  
  const tableRows2 = await page.locator("table tbody tr").count();
  console.log(`Table rows: ${tableRows2}`);
  
  const errorDivs2 = await page.locator(".border-amber-200, .text-destructive").count();
  console.log(`Error divs: ${errorDivs2}`);

  // Summary
  console.log("\n=== SUMMARY ===\n");
  console.log(`Console messages: ${consoleMessages.length}`);
  console.log(`Network errors: ${networkErrors.length}`);
  if (networkErrors.length > 0) {
    console.log("Network errors:", networkErrors);
  }
  if (consoleMessages.length > 0) {
    console.log("Console messages:", consoleMessages);
  }
});
