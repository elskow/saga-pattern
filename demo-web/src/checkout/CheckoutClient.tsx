"use client";

import { useEffect, useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import { Link } from "@tanstack/react-router";

import { toast } from "sonner";
import { ArrowLeft, ArrowRight, Loader2, ShoppingBag, ShieldCheck } from "lucide-react";

import { useCartStore } from "@/lib/store";
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
  customerId: "CUST-001",
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
      <div className="flex flex-col items-center justify-center py-32 gap-5 text-center rounded-[2rem] border border-dashed border-border/60 bg-card/30 max-w-7xl mx-auto" aria-live="polite">
        <div className="h-16 w-16 rounded-full border border-border/60 bg-muted/30 flex items-center justify-center mb-2 shadow-sm">
          <Loader2 className="h-7 w-7 animate-spin text-muted-foreground/50" />
        </div>
        <div>
          <h1 className="text-xl font-bold tracking-tight text-foreground">Checking your cart</h1>
          <p className="text-sm text-muted-foreground mt-1 max-w-sm mx-auto">
            Loading your saved cart before checkout.
          </p>
        </div>
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-32 gap-5 text-center rounded-[2rem] border border-dashed border-border/60 bg-card/30 max-w-7xl mx-auto">
        <div className="h-16 w-16 rounded-full border border-border/60 bg-muted/30 flex items-center justify-center mb-2 shadow-sm">
          <ShoppingBag className="h-7 w-7 text-muted-foreground/50" />
        </div>
        <div>
          <h1 className="text-xl font-bold tracking-tight text-foreground">Your cart is empty</h1>
          <p className="text-sm text-muted-foreground mt-1 max-w-sm mx-auto">
            Add some premium tech to your cart before proceeding to checkout.
          </p>
        </div>
        <Link to="/">
          <Button size="sm" className="mt-2 rounded-full gap-2 shadow-sm group">
            <ArrowLeft className="h-4 w-4 transition-transform group-hover:-translate-x-1" />
            Return to Shop
          </Button>
        </Link>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto w-full pb-12">
      <div className="flex items-center gap-4 border-b border-border/60 pb-6 mb-8 mt-2">
        <Link to="/">
          <Button
            variant="outline"
            size="icon"
            className="h-10 w-10 rounded-full border-border/60 hover:bg-muted/50 transition-colors shrink-0"
          >
            <ArrowLeft className="h-4 w-4 text-muted-foreground" />
          </Button>
        </Link>
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-foreground">Secure Checkout</h1>
          <p className="text-sm text-muted-foreground font-medium mt-1">
            Place your order and monitor the microservice saga execution.
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-12 lg:gap-16">
        <div className="lg:col-span-3 space-y-10">
          <section aria-labelledby="section-pattern">
            <div className="rounded-2xl border border-border/60 bg-muted/20 p-6 space-y-4">
              <div>
                <h2 id="section-pattern" className="text-sm font-bold uppercase tracking-wider text-foreground">
                  Order Processing
                </h2>
                <p className="text-xs text-muted-foreground mt-1 font-medium">
                  Select your preferred processing method.
                </p>
              </div>
              <div className="flex flex-col sm:flex-row sm:items-center gap-4 bg-background border border-border/50 p-2.5 rounded-xl">
                <PatternToggle />
                <p className="text-xs text-muted-foreground font-mono bg-muted px-2.5 py-1.5 rounded-md">
                  {activePattern === "choreography"
                    ? "Standard Processing"
                    : "Priority Processing"}
                </p>
              </div>
            </div>
          </section>

          <section aria-labelledby="section-shipping" className="space-y-6">
            <h2 id="section-shipping" className="text-lg font-bold tracking-tight text-foreground flex items-center gap-2">
              Shipping & Contact
              <ShieldCheck className="h-4 w-4 text-green-500" />
            </h2>

            <div className="grid grid-cols-2 gap-5">
              <div className="space-y-2">
                <Label htmlFor="checkout-firstname" className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">First name</Label>
                <Input id="checkout-firstname" value={form.firstName} onChange={(e) => update("firstName", e.target.value)} className="bg-card rounded-xl border-border/60 h-11 focus-visible:ring-1" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="checkout-lastname" className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Last name</Label>
                <Input id="checkout-lastname" value={form.lastName} onChange={(e) => update("lastName", e.target.value)} className="bg-card rounded-xl border-border/60 h-11 focus-visible:ring-1" />
              </div>
              <div className="space-y-2 col-span-2">
                <Label htmlFor="checkout-email" className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Email address</Label>
                <Input id="checkout-email" type="email" value={form.email} onChange={(e) => update("email", e.target.value)} className="bg-card rounded-xl border-border/60 h-11 focus-visible:ring-1" />
              </div>
              <div className="space-y-2 col-span-2">
                <Label htmlFor="checkout-address" className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Delivery address</Label>
                <Input id="checkout-address" value={form.address} onChange={(e) => update("address", e.target.value)} className="bg-card rounded-xl border-border/60 h-11 focus-visible:ring-1" />
              </div>
              <div className="space-y-2 col-span-2 pt-2 border-t border-dashed border-border/60">
                <Label htmlFor="checkout-customerid" className="text-xs font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-2">
                  Customer ID
                  <span className="text-[10px] bg-muted px-1.5 py-0.5 rounded text-muted-foreground normal-case tracking-normal">Optional</span>
                </Label>
                <Input id="checkout-customerid" value={form.customerId} onChange={(e) => update("customerId", e.target.value)} className="bg-muted/30 rounded-xl border-border/60 h-11 font-mono text-sm" />
              </div>
            </div>
          </section>
        </div>

        <aside className="lg:col-span-2">
          <div className="sticky top-32 rounded-[1.5rem] border border-border/50 bg-card/40 backdrop-blur-xl p-7 shadow-xl shadow-muted/20 space-y-6">
            <h2 className="text-lg font-bold tracking-tight text-foreground">Order Summary</h2>

            <div className="space-y-4 max-h-[22rem] overflow-y-auto pr-2 custom-scrollbar">
              {items.map((item) => (
                <div key={item.product.id} className="flex items-center gap-4 group">
                  <div className="relative h-14 w-14 shrink-0 overflow-hidden rounded-xl border border-border/50 bg-gradient-to-b from-muted/30 to-transparent">
                    <img src={item.product.image} alt={item.product.name} className="w-full h-full object-contain p-1.5 transition-transform group-hover:scale-105" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-semibold leading-tight text-foreground truncate">{item.product.name}</p>
                    <p className="text-xs font-medium text-muted-foreground mt-0.5">Qty {item.quantity}</p>
                  </div>
                  <p className="text-sm font-bold shrink-0 tabular-nums text-foreground">
                    {formatPrice(item.product.price * item.quantity)}
                  </p>
                </div>
              ))}
            </div>

            <Separator className="bg-border/60" />

            <div className="space-y-2.5 text-sm">
              <div className="flex justify-between text-muted-foreground font-medium"><span>Subtotal</span><span>{formatPrice(total)}</span></div>
              <div className="flex justify-between text-muted-foreground font-medium"><span>Shipping</span><span className="text-green-600 dark:text-green-400">Complimentary</span></div>
              <div className="flex justify-between font-bold text-foreground text-lg pt-3 border-t border-border/60"><span>Total</span><span>{formatPrice(total)}</span></div>
            </div>

            {error && (
              <p className="text-xs font-medium text-destructive rounded-lg border border-destructive/20 bg-destructive/10 px-4 py-3">
                {error}
              </p>
            )}

            <div className="pt-2">
              <Button
                  id="place-order-button"
                  onClick={handlePlaceOrder}
                  disabled={loading}
                  className="w-full h-12 rounded-full gap-2 bg-foreground text-background hover:bg-foreground/90 font-bold text-sm shadow-md transition-all hover:scale-[1.02] disabled:hover:scale-100 disabled:opacity-50"
                >
                  {loading ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Executing Saga…
                    </>
                  ) : (
                    <>
                      Complete Purchase
                      <ArrowRight className="h-4 w-4" />
                    </>
                  )}
                </Button>
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}
