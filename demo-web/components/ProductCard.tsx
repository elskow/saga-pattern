"use client";

import { Product } from "@/types";
import { formatPrice } from "@/lib/currency";
import { useCartStore } from "@/lib/store";
import { Button } from "@/components/ui/button";

import { ShoppingCart, Check } from "lucide-react";
import { toast } from "sonner";
import { useState } from "react";
import { cn } from "@/lib/utils";

interface ProductCardProps {
    product: Product;
}

export function ProductCard({ product }: ProductCardProps) {
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
        <div className="group relative flex h-full flex-col overflow-hidden rounded-[1.5rem] border border-border/60 bg-card transition-all duration-300 ease-out hover:-translate-y-1 hover:shadow-xl hover:shadow-muted/40">
            
            <div className="relative aspect-square w-full bg-gradient-to-b from-muted/30 to-transparent">
                <img
                    src={product.image}
                    alt={product.name}
                    className="w-full h-full object-contain p-8 transition-transform duration-300 ease-out group-hover:scale-105"
                    sizes="(max-width: 640px) 100vw, (max-width: 1024px) 50vw, 33vw"
                />

                {product.stock <= 0 ? (
                    <span className="absolute left-4 top-4 rounded-full border border-destructive/20 bg-destructive/10 px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-wider text-destructive">
                        Out of stock
                    </span>
                ) : product.stock <= 10 && (
                    <span className="absolute left-4 top-4 rounded-full border border-orange-500/20 bg-orange-500/10 px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-wider text-orange-600 dark:text-orange-400">
                        Low stock
                    </span>
                )}
            </div>

            <div className="flex flex-1 flex-col space-y-4 p-6 pt-2">
                <div className="flex-1 space-y-2">
                    <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-[0.25em]">
                        {product.category}
                    </p>
                    <h3 className="line-clamp-1 text-base font-bold tracking-tight text-foreground">
                        {product.name}
                    </h3>
                    <p className="line-clamp-2 text-sm text-muted-foreground leading-relaxed">
                        {product.description}
                    </p>
                </div>

                <div className="flex items-center justify-between pt-2">
                    <div className="space-y-0.5">
                        <p className="text-lg font-bold tracking-tight text-foreground tabular-nums">
                        {formatPrice(product.price)}
                        </p>
                        <p className="text-xs font-medium text-muted-foreground">
                            {outOfStock ? "Out of stock" : `${product.stock} available`}
                        </p>
                    </div>

                    <Button
                        onClick={handleAdd}
                        disabled={outOfStock}
                        size="sm"
                        className={cn(
                            "relative h-9 rounded-full px-4 text-xs font-semibold transition-all duration-300 shadow-sm",
                            added
                                ? "bg-green-500 text-white hover:bg-green-600 shadow-green-500/20"
                                : "bg-foreground text-background hover:bg-foreground/90 hover:shadow-md hover:scale-105",
                        )}
                    >
                        {outOfStock ? (
                            "Out"
                        ) : atLimit ? (
                            "Limit"
                        ) : added ? (
                            <>
                                <Check className="h-3.5 w-3.5 mr-1.5" />
                                Added
                            </>
                        ) : (
                            <>
                                <ShoppingCart className="h-3.5 w-3.5 mr-1.5" />
                                Add
                            </>
                        )}
                    </Button>
                </div>
            </div>
        </div>
    );
}
