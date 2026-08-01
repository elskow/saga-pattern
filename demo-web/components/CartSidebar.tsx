"use client";

import { memo, useCallback, useMemo } from "react";
import { useNavigate } from "@tanstack/react-router";
import { formatPrice } from "@/lib/currency";
import { Minus, Plus, ShoppingCart, Trash2 } from "lucide-react";
import { useCartStore } from "@/lib/store";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import type { CartItem } from "@/types";

const CartRow = memo(function CartRow({
  item,
  onRemove,
  onIncrement,
  onDecrement,
}: {
  item: CartItem;
  onRemove: (productId: string) => void;
  onIncrement: (productId: string) => void;
  onDecrement: (productId: string) => void;
}) {
  const { product, quantity } = item;

  return (
    <div className="flex items-center gap-3 border-b border-border py-3 last:border-b-0">
      <div className="relative h-14 w-14 shrink-0 overflow-hidden rounded-md bg-muted/40">
        <img
          src={product.image}
          alt={product.name}
          loading="lazy"
          decoding="async"
          className="h-full w-full object-contain p-1.5"
        />
      </div>

      <div className="flex min-w-0 flex-1 flex-col justify-center gap-1.5">
        <p className="truncate text-sm font-medium text-foreground">
          {product.name}
        </p>
        <div className="flex items-center gap-1.5">
          <button
            type="button"
            onClick={() => onDecrement(product.id)}
            className="flex h-7 w-7 items-center justify-center rounded-md border border-border bg-card transition-colors hover:bg-muted"
            aria-label="Decrease quantity"
          >
            <Minus className="h-3 w-3" />
          </button>
          <span className="w-6 text-center text-sm tabular-nums">{quantity}</span>
          <button
            type="button"
            onClick={() => onIncrement(product.id)}
            disabled={quantity >= product.stock}
            className="flex h-7 w-7 items-center justify-center rounded-md border border-border bg-card transition-colors hover:bg-muted disabled:cursor-not-allowed disabled:opacity-40"
            aria-label="Increase quantity"
          >
            <Plus className="h-3 w-3" />
          </button>
        </div>
      </div>

      <div className="flex shrink-0 flex-col items-end gap-1">
        <p className="text-sm font-medium tabular-nums tracking-tight">
          {formatPrice(product.price * quantity)}
        </p>
        <Button
          variant="ghost"
          size="icon"
          className="h-7 w-7 rounded-md text-muted-foreground transition-colors hover:bg-destructive/10 hover:text-destructive"
          onClick={() => onRemove(product.id)}
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </div>
    </div>
  );
});

export function CartSidebar() {
  const navigate = useNavigate();
  const items = useCartStore((state) => state.items);
  const removeItem = useCartStore((state) => state.removeItem);
  const updateQuantity = useCartStore((state) => state.updateQuantity);
  const count = useMemo(() => items.reduce((sum, item) => sum + item.quantity, 0), [items]);
  const total = useMemo(
    () => items.reduce((sum, item) => sum + item.product.price * item.quantity, 0),
    [items]
  );
  const handleRemove = useCallback((productId: string) => removeItem(productId), [removeItem]);
  const handleIncrement = useCallback((productId: string) => {
    const item = useCartStore.getState().items.find((i) => i.product.id === productId);
    if (item) updateQuantity(productId, item.quantity + 1);
  }, [updateQuantity]);
  const handleDecrement = useCallback((productId: string) => {
    const item = useCartStore.getState().items.find((i) => i.product.id === productId);
    if (item) updateQuantity(productId, item.quantity - 1);
  }, [updateQuantity]);
  const handleCheckout = useCallback(() => {
    void navigate({ to: "/checkout" });
  }, [navigate]);

  return (
    <Sheet>
      <SheetTrigger
        render={
          <Button aria-label="Open cart" variant="ghost" size="icon" className="relative rounded-md transition-colors hover:bg-muted">
            <ShoppingCart className="h-5 w-5" />
            {count > 0 && (
              <span className="absolute -right-0.5 -top-0.5 inline-flex h-4 min-w-[16px] items-center justify-center rounded-sm bg-primary px-1 text-[10px] font-medium text-primary-foreground">
                {count}
              </span>
            )}
          </Button>
        }
      >
        <span className="sr-only">Open cart</span>
      </SheetTrigger>

      <SheetContent keepMounted className="flex w-full flex-col border-l border-border bg-card p-0 sm:max-w-md">
        <SheetHeader className="space-y-1 border-b border-border p-4 text-left">
          <SheetTitle className="text-base font-semibold tracking-tight text-foreground">
            Your cart
          </SheetTitle>
          <SheetDescription className="text-sm text-muted-foreground">
            Review items before checkout.
          </SheetDescription>
        </SheetHeader>

        <div className="flex-1 overflow-y-auto p-4">
          {items.length === 0 ? (
            <div className="flex h-full flex-col items-center justify-center gap-3 pb-8 text-center">
              <div className="space-y-1">
                <p className="text-sm font-medium text-foreground">Cart is empty</p>
                <p className="text-sm text-muted-foreground">
                  Add products from the shop to get started.
                </p>
              </div>
              <SheetClose
                render={
                  <Button variant="outline" className="mt-1 rounded-md px-4">
                    Continue shopping
                  </Button>
                }
              />
            </div>
          ) : (
            <div>
              {items.map((item) => (
                <CartRow
                  key={item.product.id}
                  item={item}
                  onRemove={handleRemove}
                  onIncrement={handleIncrement}
                  onDecrement={handleDecrement}
                />
              ))}
            </div>
          )}
        </div>

        {items.length > 0 && (
          <div className="space-y-3 border-t border-border bg-card p-4">
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Subtotal</span>
              <span className="text-base font-semibold tabular-nums tracking-tight">
                {formatPrice(total)}
              </span>
            </div>
            <SheetClose
              render={
                <Button
                  className="h-10 w-full rounded-md text-sm font-medium"
                  onClick={handleCheckout}
                >
                  Go to checkout
                </Button>
              }
            />
          </div>
        )}
      </SheetContent>
    </Sheet>
  );
}
