"use client";

import { useState } from "react";
import { useCartStore } from "@/lib/store";
import { CatalogProduct } from "@/types";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { formatPrice } from "@/lib/currency";
import { Check, RotateCcw, ShieldCheck, ShoppingCart, Truck } from "lucide-react";
import { toast } from "sonner";
import { cn } from "@/lib/utils";

interface ProductDetailViewProps {
  product: CatalogProduct;
  /** "modal" = compact scale inside the dialog, "page" = full PDP scale for deep links */
  variant?: "modal" | "page";
}

export function ProductDetailView({ product, variant = "page" }: ProductDetailViewProps) {
  const addItem = useCartStore((s) => s.addItem);
  const quantityInCart = useCartStore(
    (s) => s.items.find((item) => item.product.id === product.id)?.quantity ?? 0
  );
  const [added, setAdded] = useState(false);

  const outOfStock = product.stock <= 0;
  const lowStock = !outOfStock && product.stock < 10;
  const isModal = variant === "modal";

  const handleAdd = () => {
    if (outOfStock) {
      toast.error(`${product.name} is out of stock`);
      return;
    }

    const result = addItem({ product, quantity: 1 });
    if (result.reason === "limit") {
      toast.error(`Only ${result.stock} ${product.name} available`);
      return;
    }

    if (result.reason === "out-of-stock") {
      toast.error(`${product.name} is out of stock`);
      return;
    }

    setAdded(true);
    toast.success(`${product.name} added to cart`);
    setTimeout(() => setAdded(false), 2000);
  };

  return (
    <div
      className={cn(
        "grid grid-cols-1 gap-6 sm:grid-cols-2",
        isModal ? "items-start sm:gap-8" : "items-center gap-12 md:grid-cols-[1.05fr_1fr] lg:gap-16"
      )}
    >
      {/* Image panel */}
      <div
        className={cn(
          "relative flex aspect-square w-full items-center justify-center rounded-2xl border border-border bg-muted/40",
          isModal ? "p-6" : "p-8 md:p-12"
        )}
      >
        <img
          src={product.image}
          alt={product.name}
          className="mx-auto max-h-full w-auto max-w-full object-contain mix-blend-multiply transition-transform duration-300 hover:scale-[1.02]"
        />
        {outOfStock && (
          <Badge variant="destructive" className="absolute left-4 top-4 h-6 px-2.5">
            Out of stock
          </Badge>
        )}
      </div>

      {/* Buy panel */}
      <div className="flex flex-col">
        {product.category && (
          <p className="text-xs font-medium uppercase tracking-wider text-primary">
            {product.category}
          </p>
        )}

        <h1
          className={cn(
            "mt-2 text-balance font-semibold tracking-tight text-foreground",
            isModal ? "text-xl md:text-2xl" : "text-3xl md:text-4xl"
          )}
        >
          {product.name}
        </h1>

        <p
          className={cn(
            "mt-3 font-bold tabular-nums tracking-tight text-primary",
            isModal ? "text-xl md:text-2xl" : "text-2xl md:text-3xl"
          )}
        >
          {formatPrice(product.price)}
        </p>

        <div className="mt-3 flex flex-wrap items-center gap-2">
          {outOfStock ? (
            <Badge variant="destructive" className="h-6 px-2.5">Out of stock</Badge>
          ) : (
            <Badge
              variant="secondary"
              className="h-6 border border-emerald-200 bg-emerald-50 px-2.5 text-emerald-700"
            >
              In stock
            </Badge>
          )}
          {lowStock && (
            <span className="text-xs font-medium text-amber-600">
              Only {product.stock} left
            </span>
          )}
        </div>

        <Separator className="my-6" />

        <p className="max-w-prose text-[15px] leading-relaxed text-muted-foreground">
          {product.description}
        </p>

        {quantityInCart > 0 && (
          <div className="mt-5 flex items-center gap-2.5 text-sm">
            <ShoppingCart className="h-4 w-4 shrink-0 text-muted-foreground" />
            <span className="text-muted-foreground">
              You have <span className="font-semibold text-foreground">{quantityInCart}</span> in your cart
            </span>
          </div>
        )}

        <div className="mt-6">
          <Button
            onClick={handleAdd}
            disabled={outOfStock}
            size="lg"
            className={cn(
              "h-11 w-full rounded-md text-sm font-medium transition-colors sm:w-auto sm:min-w-[240px]",
              added
                ? "bg-emerald-600 text-white hover:bg-emerald-700"
                : "bg-primary text-primary-foreground hover:bg-primary/90"
            )}
          >
            {added ? (
              <span className="inline-flex items-center gap-2">
                <Check className="h-4 w-4" />
                Added to cart
              </span>
            ) : (
              "Add to cart"
            )}
          </Button>
        </div>

        <div className="mt-8 space-y-3">
          <div className="flex items-center gap-3 text-sm text-muted-foreground">
            <Truck className="h-4 w-4 shrink-0" />
            Free shipping on all orders
          </div>
          <div className="flex items-center gap-3 text-sm text-muted-foreground">
            <ShieldCheck className="h-4 w-4 shrink-0" />
            1-year warranty included
          </div>
          <div className="flex items-center gap-3 text-sm text-muted-foreground">
            <RotateCcw className="h-4 w-4 shrink-0" />
            30-day hassle-free returns
          </div>
        </div>
      </div>
    </div>
  );
}
