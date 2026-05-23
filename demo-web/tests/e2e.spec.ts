import { test, expect } from '@playwright/test';

test.describe('E2E Store & Admin Flow', () => {
  test('Storefront checkout and saga processing', async ({ page }) => {
    // 1. Visit the store and verify it loads
    await page.goto('/');
    await expect(page.locator('h1')).toContainText('Curated essentials');
    
    // 2. Add item to cart
    await page.click('button:has-text("Add")');
    await expect(page.locator('.sonner-toast, [data-sonner-toast]')).toContainText('added', { ignoreCase: true, timeout: 10000 });

    // 3. Go to checkout
    await page.goto('/checkout');
    await expect(page.locator('h1')).toContainText('Checkout');
    
    // Wait for network/hydration
    await page.waitForTimeout(1000);
    
    // 4. Place order
    await page.click('button:has-text("Place order")');
    
    // 5. Verify redirect to order tracking
    await page.waitForURL(/\/orders\/.*/, { timeout: 15000 });
    await expect(page.locator('h1')).toContainText('Order Tracking');
    
    // 6. Wait for saga to complete (should be COMPLETED after a few seconds)
    await expect(page.locator('.tracking-wide.uppercase')).toContainText('COMPLETED', { timeout: 20000 });
  });

  test('Admin Dashboard and Inventory', async ({ page }) => {
    // 1. Visit Admin Dashboard
    await page.goto('/admin');
    await expect(page.locator('h1')).toContainText('Dashboard');
    
    // 2. Verify Metrics load (Total Orders shouldn't be empty)
    await expect(page.locator('.text-3xl').first()).not.toBeEmpty();

    // 3. Visit Inventory
    await page.goto('/admin/inventory');
    await expect(page.locator('h1')).toContainText('Inventory');
    
    // 4. Verify products load in table
    await expect(page.locator('table tbody tr')).not.toHaveCount(0);
  });
});
