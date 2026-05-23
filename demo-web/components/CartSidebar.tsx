"use client";

import { memo, useCallback, useMemo } from "react";
import { useNavigate } from "@tanstack/react-router";
import { formatPrice } from "@/lib/currency";

import { ShoppingCart, Trash2, ArrowRight } from "lucide-react";

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
}: {
  item: CartItem;
  onRemove: (productId: string) => void;
}) {
  const { product, quantity } = item;

  return (
    <div className="group flex items-center gap-4 rounded-[1.25rem] border border-border/50 bg-card/40 p-3 transition-colors duration-200 hover:bg-card hover:shadow-md hover:shadow-muted/50">
      <div className="relative h-20 w-20 shrink-0 overflow-hidden rounded-xl bg-gradient-to-b from-muted/40 to-transparent">
        <img
          src={product.image}
          alt={product.name}
          loading="lazy"
          decoding="async"
          className="w-full h-full object-contain p-2 transition-transform duration-300 group-hover:scale-105"
        />
      </div>

      <div className="flex flex-1 flex-col justify-center min-w-0">
        <p className="truncate text-base font-semibold text-foreground">
          {product.name}
        </p>
        <p className="mt-0.5 text-sm font-medium text-muted-foreground">
          Qty: {quantity}
        </p>
      </div>

      <div className="flex shrink-0 flex-col items-end gap-2">
        <p className="text-base font-bold tabular-nums tracking-tight">
          {formatPrice(product.price * quantity)}
        </p>
        <Button
          variant="ghost"
          size="icon"
          className="h-8 w-8 rounded-full text-muted-foreground transition-colors hover:bg-red-500/10 hover:text-red-500"
          onClick={() => onRemove(product.id)}
        >
          <Trash2 className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
});

export function CartSidebar() {
  const navigate = useNavigate();
  const items = useCartStore((state) => state.items);
  const removeItem = useCartStore((state) => state.removeItem);
  const count = useMemo(() => items.reduce((sum, item) => sum + item.quantity, 0), [items]);
  const total = useMemo(
    () => items.reduce((sum, item) => sum + item.product.price * item.quantity, 0),
    [items]
  );
  const handleRemove = useCallback((productId: string) => removeItem(productId), [removeItem]);
  const handleCheckout = useCallback(() => {
    void navigate({ to: "/checkout" });
  }, [navigate]);

  return (
    <Sheet>
      <SheetTrigger
        render={
          <Button aria-label="Open cart" variant="ghost" size="icon" className="relative rounded-full hover:bg-muted/50 transition-colors">
            <ShoppingCart className="h-5 w-5" />
            {count > 0 && (
              <span className="absolute -right-1 -top-1 inline-flex h-5 min-w-[20px] items-center justify-center rounded-full bg-foreground px-1.5 text-[10px] font-bold text-background shadow-sm animate-in zoom-in">
                {count}
              </span>
            )}
          </Button>
        }
      >
        <span className="sr-only">Open cart</span>
      </SheetTrigger>

      
      <SheetContent keepMounted className="flex w-full flex-col border-l border-border/60 bg-background p-0 sm:max-w-lg shadow-2xl">
        
        <SheetHeader className="border-b border-border/50 p-6 text-left space-y-1">
          <SheetTitle className="text-2xl font-bold tracking-tight text-foreground">
            Your Cart
          </SheetTitle>
          <SheetDescription className="text-sm font-medium text-muted-foreground">
            Review your items before checkout.
          </SheetDescription>
        </SheetHeader>

        <div className="flex-1 overflow-y-auto p-6">
          {items.length === 0 ? (
            <div className="flex h-full flex-col items-center justify-center gap-4 text-center pb-8 md:pb-16">
              <div className="flex h-20 w-20 items-center justify-center rounded-full border border-dashed border-border bg-muted/30">
                <ShoppingCart className="h-8 w-8 text-muted-foreground/40" />
              </div>
              <div className="space-y-1">
                <p className="text-lg font-semibold tracking-tight text-foreground">Cart is empty</p>
                <p className="text-sm font-medium text-muted-foreground">
                  Looks like you haven't added anything yet.
                </p>
              </div>
              <SheetClose
                render={
                  <Button variant="outline" className="mt-4 rounded-full px-6 shadow-sm">
                    Continue Shopping
                  </Button>
                }
              />
            </div>
          ) : (
            <div className="space-y-4">
              {items.map((item) => (
                <CartRow key={item.product.id} item={item} onRemove={handleRemove} />
              ))}
            </div>
          )}
        </div>

        
        {items.length > 0 && (
          <div className="border-t border-border/50 bg-background p-6 space-y-5">
            <div className="flex items-center justify-between">
              <span className="text-base font-medium text-muted-foreground">Subtotal</span>
              <span className="text-2xl font-bold tracking-tight tabular-nums">
                {formatPrice(total)}
              </span>
            </div>
            
            <SheetClose
              render={
                <Button
                  className="group w-full h-12 rounded-full text-base font-semibold shadow-md transition-all hover:scale-[1.02]"
                  onClick={handleCheckout}
                >
                  Go to checkout
                  <ArrowRight className="ml-2 h-4 w-4 transition-transform group-hover:translate-x-1" />
                </Button>
              }
            />
          </div>
        )}
      </SheetContent>
    </Sheet>
  );
}
