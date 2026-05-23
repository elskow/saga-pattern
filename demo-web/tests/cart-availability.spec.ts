import { expect, test } from "@playwright/test";

const seededCart = {
  state: {
    items: [
      {
        product: {
          id: "PROD-001",
          productId: "PROD-001",
          name: "Hydration Test Laptop",
          description: "Seeded cart item for checkout hydration coverage.",
          price: 999.99,
          image: "/laptop.png",
          category: "Computing",
          stock: 5,
        },
        quantity: 1,
      },
    ],
    pattern: "choreography",
  },
  version: 0,
};

test.describe("Cart availability", () => {
  test("hydrates persisted cart before showing checkout empty-cart state", async ({ page }) => {
    await page.addInitScript((cart) => {
      window.localStorage.setItem("saga-demo-cart", JSON.stringify(cart));

      const trackedWindow = window as Window & { __emptyCartRenderCount?: number };
      trackedWindow.__emptyCartRenderCount = 0;

      const recordEmptyCartRender = () => {
        if (document.body?.innerText.includes("Your cart is empty")) {
          trackedWindow.__emptyCartRenderCount = (trackedWindow.__emptyCartRenderCount ?? 0) + 1;
        }
      };

      const observeCartState = () => {
        recordEmptyCartRender();
        new MutationObserver(recordEmptyCartRender).observe(document.documentElement, {
          childList: true,
          characterData: true,
          subtree: true,
        });
      };

      if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", observeCartState, { once: true });
      } else {
        observeCartState();
      }
    }, seededCart);

    await page.goto("/checkout", { waitUntil: "domcontentloaded" });

    await expect(page.getByRole("heading", { name: "Secure Checkout" })).toBeVisible({ timeout: 1_500 });
    await expect(page.getByText("Add some premium tech to your cart before proceeding to checkout.")).toHaveCount(0);

    const emptyCartRenderCount = await page.evaluate(() => {
      return (window as Window & { __emptyCartRenderCount?: number }).__emptyCartRenderCount ?? 0;
    });
    expect(emptyCartRenderCount).toBe(0);
  });

  test("uses client navigation from the cart checkout button", async ({ page }) => {
    await page.addInitScript((cart) => {
      window.localStorage.setItem("saga-demo-cart", JSON.stringify(cart));
    }, seededCart);

    await page.goto("/", { waitUntil: "domcontentloaded" });
    const cartTrigger = page.getByRole("button", { name: "Open cart" });
    await expect(cartTrigger).toBeVisible({ timeout: 1_500 });
    await expect(cartTrigger.getByText("1")).toBeVisible({ timeout: 1_500 });

    await page.evaluate(() => {
      (window as Window & { __cartNavigationMarker?: string }).__cartNavigationMarker = "client";
    });
    await cartTrigger.click();
    await expect(page.getByRole("heading", { name: "Your Cart" })).toBeVisible({ timeout: 1_500 });
    await expect(page.getByText("Qty: 1")).toBeVisible({ timeout: 1_500 });
    await page.getByRole("button", { name: "Go to checkout" }).click();

    await expect(page).toHaveURL(/\/checkout/);
    await expect(page.getByRole("heading", { name: "Secure Checkout" })).toBeVisible({ timeout: 1_500 });
    await expect(page.getByText("Qty 1")).toBeVisible();

    const navigationMarker = await page.evaluate(() => {
      return (window as Window & { __cartNavigationMarker?: string }).__cartNavigationMarker;
    });
    expect(navigationMarker).toBe("client");
  });

  test("shows the cart trigger quickly when the catalog refresh is unavailable", async ({ page }) => {
    await page.route("**/_serverFn/**", (route) =>
      route.fulfill({ status: 503, body: "Catalog unavailable" })
    );
    await page.goto("/", { waitUntil: "domcontentloaded" });

    await expect(page.getByText("Catalog unavailable")).toBeVisible({ timeout: 1_500 });

    const cartTrigger = page.getByRole("button", { name: "Open cart" });
    await expect(cartTrigger).toBeVisible({ timeout: 1_500 });

    await cartTrigger.click();
    await expect(page.getByRole("heading", { name: "Your Cart" })).toBeVisible({ timeout: 1_500 });
  });
});
