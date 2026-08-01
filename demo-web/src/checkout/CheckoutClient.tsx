"use client";

import { useEffect, useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import { Link } from "@tanstack/react-router";

import { toast } from "sonner";
import { ArrowLeft, Loader2 } from "lucide-react";

import { useCartStore, useAuthStore } from "@/lib/store";
import { createOrderServer } from "@/lib/api";
import { fetchLiveCatalogServer } from "@/lib/products";
import { PatternToggle } from "@/components/PatternToggle";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import type { CartItem, CatalogProduct, Pattern } from "@/types";
import { formatPrice } from "@/lib/currency";

const defaultForm = {
  customerId: "",
  firstName: "Alex",
  lastName: "Johnson",
  address: "Jl. Ketintang Wiyata, Ketintang, Kec. Gayungan, Surabaya, Jawa Timur 60231",
  email: "alex@example.com",
};

function getStockValidationError(items: CartItem[], catalog: CatalogProduct[], pattern: Pattern): string | null {
  const liveProducts = new Map(catalog.map((product) => [product.productId, product]));
  const patternLabel = pattern === "choreography" ? "choreography" : "orchestration";

  for (const item of items) {
    const liveProduct = liveProducts.get(item.product.productId);
    if (!liveProduct) {
      return `${item.product.name} is no longer available in ${patternLabel} inventory.`;
    }

    if (liveProduct.stock <= 0) {
      return `${liveProduct.name} is out of stock in ${patternLabel} inventory.`;
    }

    if (item.quantity > liveProduct.stock) {
      return `Only ${liveProduct.stock} ${liveProduct.name} available in ${patternLabel} inventory. Update your cart before checkout.`;
    }
  }

  return null;
}

interface CheckoutClientProps {
  pattern?: Pattern;
}

export default function CheckoutClient({ pattern: routePattern }: CheckoutClientProps) {
  const router = useNavigate();
  const {
    items,
    pattern,
    setPattern,
    reconcileWithCatalog,
    clearCart,
    hasHydrated,
  } = useCartStore();
  const user = useAuthStore((s) => s.user);

  const activePattern = routePattern ?? pattern;
  const total = items.reduce((sum, item) => sum + item.product.price * item.quantity, 0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [formOverrides, setFormOverrides] = useState<Partial<typeof defaultForm>>({});

  useEffect(() => {
    if (routePattern && routePattern !== pattern) {
      setPattern(routePattern);
    }
  }, [pattern, routePattern, setPattern]);

  const form = {
    ...defaultForm,
    customerId: user?.username || "CUST-GUEST",
    ...formOverrides,
  };

  const update = (field: keyof typeof defaultForm, value: string) =>
    setFormOverrides((prev) => ({ ...prev, [field]: value }));

  const handlePlaceOrder = async () => {
    if (items.length === 0) return;
    setLoading(true);
    setError(null);

    try {
      const currentItems = useCartStore.getState().items;
      const liveProducts = await fetchLiveCatalogServer({ data: activePattern });
      const validationError = getStockValidationError(currentItems, liveProducts, activePattern);
      reconcileWithCatalog(liveProducts);

      if (validationError) {
        setError(validationError);
        toast.error(validationError);
        return;
      }

      const validatedItems = useCartStore.getState().items;
      const validatedTotal = validatedItems.reduce((sum, item) => sum + item.product.price * item.quantity, 0);

      const order = await createOrderServer({
        data: {
          pattern: activePattern,
          payload: {
            customerId: form.customerId,
            shippingAddress: form.address,
            items: validatedItems.map((item) => ({
              productId: item.product.productId,
              productName: item.product.name,
              quantity: item.quantity,
              price: item.product.price,
            })),
            totalAmount: validatedTotal,
          },
        },
      });

      const orderId = order.id || order.orderId;
      clearCart();
      toast.success("Order placed — watching the saga…");
      router({ to: `/orders/${orderId}?pattern=${activePattern}` });
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Failed to place order";
      setError(msg);
      toast.error(msg);
    } finally {
      setLoading(false);
    }
  };

  if (!hasHydrated) {
    return (
      <div className="mx-auto flex max-w-7xl flex-col items-center justify-center gap-3 rounded-md border border-dashed border-border bg-card py-24 text-center" aria-live="polite">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        <div>
          <h1 className="text-base font-semibold tracking-tight text-foreground">Checking cart</h1>
          <p className="mx-auto mt-1 max-w-sm text-sm text-muted-foreground">
            Loading saved items before checkout.
          </p>
        </div>
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className="mx-auto flex max-w-7xl flex-col items-center justify-center gap-3 rounded-md border border-dashed border-border bg-card py-24 text-center">
        <div>
          <h1 className="text-base font-semibold tracking-tight text-foreground">Cart is empty</h1>
          <p className="mx-auto mt-1 max-w-sm text-sm text-muted-foreground">
            Add items from the catalog before checkout.
          </p>
        </div>
        <Link to="/">
          <Button size="sm" variant="outline" className="mt-1 rounded-md">
            Back to shop
          </Button>
        </Link>
      </div>
    );
  }

  return (
    <div className="mx-auto w-full max-w-7xl pb-12">
      <div className="mb-8 mt-2 flex items-center gap-3 border-b border-border pb-5">
        <Link to="/">
          <Button
            variant="outline"
            size="icon"
            className="h-9 w-9 shrink-0 rounded-md border-border"
          >
            <ArrowLeft className="h-4 w-4 text-muted-foreground" />
          </Button>
        </Link>
        <div>
          <h1 className="text-xl font-semibold tracking-tight text-foreground">Checkout</h1>
          <p className="mt-0.5 text-sm text-muted-foreground">
            Confirm shipping details and place your order.
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-10 lg:grid-cols-5 lg:gap-12">
        <div className="space-y-8 lg:col-span-3">
          <section aria-labelledby="section-pattern">
            <div className="space-y-3 rounded-md border border-border bg-card p-4">
              <div>
                <h2 id="section-pattern" className="text-sm font-medium tracking-tight text-foreground">
                  Processing pattern
                </h2>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  Choose Choreography or Orchestration for this order.
                </p>
              </div>
              <PatternToggle />
            </div>
          </section>

          <section aria-labelledby="section-shipping" className="space-y-4">
            <h2 id="section-shipping" className="text-base font-semibold tracking-tight text-foreground">
              Shipping & contact
            </h2>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-1.5">
                <Label htmlFor="checkout-firstname" className="text-sm font-medium text-foreground">First name</Label>
                <Input id="checkout-firstname" value={form.firstName} onChange={(e) => update("firstName", e.target.value)} className="h-10 bg-card" />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="checkout-lastname" className="text-sm font-medium text-foreground">Last name</Label>
                <Input id="checkout-lastname" value={form.lastName} onChange={(e) => update("lastName", e.target.value)} className="h-10 bg-card" />
              </div>
              <div className="col-span-2 space-y-1.5">
                <Label htmlFor="checkout-email" className="text-sm font-medium text-foreground">Email address</Label>
                <Input id="checkout-email" type="email" value={form.email} onChange={(e) => update("email", e.target.value)} className="h-10 bg-card" />
              </div>
              <div className="col-span-2 space-y-1.5">
                <Label htmlFor="checkout-address" className="text-sm font-medium text-foreground">Delivery address</Label>
                <Input id="checkout-address" value={form.address} onChange={(e) => update("address", e.target.value)} className="h-10 bg-card" />
              </div>
              <div className="col-span-2 space-y-1.5 border-t border-border pt-4">
                <Label htmlFor="checkout-customerid" className="flex items-center gap-2 text-sm font-medium text-foreground">
                  Customer ID
                  <span className="rounded-sm bg-muted px-1.5 py-0.5 text-[10px] font-normal text-muted-foreground">Optional</span>
                </Label>
                <Input id="checkout-customerid" value={form.customerId} onChange={(e) => update("customerId", e.target.value)} className="h-10 bg-muted/30 font-mono text-sm" />
              </div>
            </div>
          </section>
        </div>

        <aside className="lg:col-span-2">
          <div className="sticky top-24 space-y-4 rounded-md border border-border bg-card p-5">
            <h2 className="text-base font-semibold tracking-tight text-foreground">Order summary</h2>

            <div className="max-h-[22rem] space-y-3 overflow-y-auto pr-1">
              {items.map((item) => (
                <div key={item.product.id} className="flex items-center gap-3">
                  <div className="relative h-12 w-12 shrink-0 overflow-hidden rounded-md border border-border bg-muted/30">
                    <img src={item.product.image} alt={item.product.name} className="h-full w-full object-contain p-1" />
                  </div>
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-medium leading-tight text-foreground">{item.product.name}</p>
                    <p className="mt-0.5 text-xs text-muted-foreground">Qty {item.quantity}</p>
                  </div>
                  <p className="shrink-0 text-sm font-medium tabular-nums text-foreground">
                    {formatPrice(item.product.price * item.quantity)}
                  </p>
                </div>
              ))}
            </div>

            <Separator />

            <div className="space-y-2 text-sm">
              <div className="flex justify-between text-muted-foreground"><span>Subtotal</span><span>{formatPrice(total)}</span></div>
              <div className="flex justify-between text-muted-foreground"><span>Shipping</span><span className="text-emerald-700">Free</span></div>
              <div className="flex justify-between border-t border-border pt-3 text-base font-semibold text-foreground"><span>Total</span><span className="tabular-nums">{formatPrice(total)}</span></div>
            </div>

            {error && (
              <p className="rounded-md border border-destructive/20 bg-destructive/10 px-3 py-2.5 text-xs font-medium text-destructive">
                {error}
              </p>
            )}

            <div className="pt-1">
              <Button
                id="place-order-button"
                onClick={handlePlaceOrder}
                disabled={loading}
                className="h-10 w-full rounded-md bg-primary text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              >
                {loading ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Placing order…
                  </>
                ) : (
                  "Place order"
                )}
              </Button>
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}
