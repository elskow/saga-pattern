"use client";

import { Product } from "@/types";
import { formatPrice } from "@/lib/currency";
import { useCartStore } from "@/lib/store";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";
import { useState } from "react";
import { cn } from "@/lib/utils";
import { Link } from "@tanstack/react-router";

interface ProductCardProps {
    product: Product;
    onSelect?: () => void;
}

export function ProductCard({ product, onSelect }: ProductCardProps) {
    const addItem = useCartStore((s) => s.addItem);
    const quantityInCart = useCartStore((s) => s.items.find((item) => item.product.id === product.id)?.quantity ?? 0);
    const [added, setAdded] = useState(false);
    const outOfStock = product.stock <= 0;
    const atLimit = !outOfStock && quantityInCart >= product.stock;

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
        <div className="group relative flex h-full flex-col overflow-hidden rounded-2xl border border-border bg-card transition-colors hover:border-border/80">
            {onSelect ? (
                <button
                    type="button"
                    onClick={onSelect}
                    className="relative block aspect-[4/3] w-full cursor-pointer bg-muted/40"
                >
                    <img
                        src={product.image}
                        alt={product.name}
                        className="h-full w-full object-contain p-6 mix-blend-multiply transition-transform duration-300 group-hover:scale-[1.03]"
                        sizes="(max-width: 640px) 100vw, (max-width: 1024px) 50vw, 33vw"
                    />

                    {product.stock <= 0 ? (
                        <span className="absolute left-2.5 top-2.5 rounded-sm border border-border bg-card px-1.5 py-0.5 text-[11px] text-muted-foreground">
                            Out of stock
                        </span>
                    ) : product.stock <= 10 && (
                        <span className="absolute left-2.5 top-2.5 rounded-sm border border-border bg-card px-1.5 py-0.5 text-[11px] text-muted-foreground">
                            Low stock
                        </span>
                    )}
                </button>
            ) : (
                <Link
                    to="/products/$productId"
                    params={{ productId: product.id }}
                    className="relative block aspect-[4/3] w-full cursor-pointer bg-muted/40"
                >
                    <img
                        src={product.image}
                        alt={product.name}
                        className="h-full w-full object-contain p-6 mix-blend-multiply transition-transform duration-300 group-hover:scale-[1.03]"
                        sizes="(max-width: 640px) 100vw, (max-width: 1024px) 50vw, 33vw"
                    />

                    {product.stock <= 0 ? (
                        <span className="absolute left-2.5 top-2.5 rounded-sm border border-border bg-card px-1.5 py-0.5 text-[11px] text-muted-foreground">
                            Out of stock
                        </span>
                    ) : product.stock <= 10 && (
                        <span className="absolute left-2.5 top-2.5 rounded-sm border border-border bg-card px-1.5 py-0.5 text-[11px] text-muted-foreground">
                            Low stock
                        </span>
                    )}
                </Link>
            )}

            <div className="flex flex-1 flex-col space-y-3 p-4">
                <div className="flex-1 space-y-1">
                    <p className="text-xs font-medium uppercase tracking-wider text-primary">
                        {product.category}
                    </p>
                    {onSelect ? (
                        <button type="button" onClick={onSelect} className="block w-full text-left">
                            <h3 className="line-clamp-1 text-sm font-medium tracking-tight text-foreground hover:text-primary cursor-pointer">
                                {product.name}
                            </h3>
                        </button>
                    ) : (
                        <Link to="/products/$productId" params={{ productId: product.id }}>
                            <h3 className="line-clamp-1 text-sm font-medium tracking-tight text-foreground hover:text-primary cursor-pointer">
                                {product.name}
                            </h3>
                        </Link>
                    )}
                    <p className="line-clamp-2 text-sm leading-relaxed text-muted-foreground">
                        {product.description}
                    </p>
                </div>

                <div className="flex items-center justify-between gap-3 pt-0.5">
                    <p className="text-sm font-bold tabular-nums tracking-tight text-primary">
                        {formatPrice(product.price)}
                    </p>
                    <Button
                        size="sm"
                        onClick={handleAdd}
                        disabled={outOfStock || atLimit}
                        className={cn(
                            "h-8 min-w-[4.5rem] rounded-md text-xs font-medium",
                            outOfStock || atLimit
                                ? "opacity-50"
                                : added
                                    ? "bg-muted text-foreground hover:bg-muted"
                                    : "",
                        )}
                    >
                        {outOfStock ? "Out" : atLimit ? "Limit" : added ? "Added" : "Add"}
                    </Button>
                </div>
            </div>
        </div>
    );
}
