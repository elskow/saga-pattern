"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { fetchLiveCatalogServer } from "@/lib/products";
import { ProductCard } from "@/components/ProductCard";
import { ProductDetailModal } from "@/components/ProductDetailModal";
import { HomeHero } from "@/components/HomeHero";
import { CatalogProduct, Pattern } from "@/types";
import { Button } from "@/components/ui/button";
import { RefreshCw } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { useCartStore } from "@/lib/store";

interface HomePageClientProps {
  initialProducts: CatalogProduct[];
  initialError: string | null;
  initialPattern: Pattern;
}

function ProductCardSkeleton() {
  return (
    <div className="flex h-full flex-col overflow-hidden rounded-md border border-border bg-card">
      <Skeleton className="aspect-[4/3] w-full rounded-none bg-muted/60" />
      <div className="flex flex-1 flex-col space-y-3 p-3.5">
        <div className="flex-1 space-y-2">
          <Skeleton className="h-3 w-16 rounded-sm" />
          <Skeleton className="h-4 w-3/4 rounded-sm" />
          <Skeleton className="h-3.5 w-full rounded-sm" />
        </div>
        <div className="flex items-center justify-between gap-3 pt-0.5">
          <Skeleton className="h-4 w-16 rounded-sm" />
          <Skeleton className="h-8 w-[4.5rem] rounded-md" />
        </div>
      </div>
    </div>
  );
}

export default function HomePageClient({ initialProducts, initialError, initialPattern }: HomePageClientProps) {
  const pattern = useCartStore((state) => state.pattern);
  const reconcileWithCatalog = useCartStore((state) => state.reconcileWithCatalog);
  const initialCatalogLoadStarted = useRef(false);
  const [products, setProducts] = useState<CatalogProduct[]>(initialProducts);
  const [loading, setLoading] = useState(initialProducts.length === 0 && initialError === null);
  const [error, setError] = useState<string | null>(initialError);
  const [catalogPattern, setCatalogPattern] = useState<Pattern>(initialPattern);
  const [selectedProduct, setSelectedProduct] = useState<CatalogProduct | null>(null);

  const load = useCallback(async (nextPattern: Pattern = pattern) => {
    setLoading(true);
    try {
      const liveProducts = await fetchLiveCatalogServer({ data: nextPattern });
      setProducts(liveProducts);
      setCatalogPattern(nextPattern);
      reconcileWithCatalog(liveProducts);
      setError(null);
    } catch (err) {
      setProducts([]);
      setError(err instanceof Error ? err.message : "Live catalog data unavailable");
    } finally {
      setLoading(false);
    }
  }, [pattern, reconcileWithCatalog]);

  useEffect(() => {
    if (pattern !== catalogPattern) {
      void load(pattern);
    }
  }, [catalogPattern, load, pattern]);

  useEffect(() => {
    if (initialProducts.length > 0 || initialError !== null || initialCatalogLoadStarted.current) {
      return;
    }

    initialCatalogLoadStarted.current = true;
    void load(initialPattern);
  }, [initialError, initialPattern, initialProducts.length, load]);

  return (
    <div className="w-full space-y-6">
      <div className="mb-6 md:mb-8">
        <HomeHero />
      </div>

      <section className="border-b border-border pb-5">
        <h1 className="text-xl font-semibold tracking-tight text-foreground md:text-2xl">
          Catalog
        </h1>
        <p className="mt-1 max-w-xl text-sm text-muted-foreground">
          Laptops, phones, and peripherals. Live stock for the selected processing path.
        </p>
      </section>

      <section id="products">
        {loading ? (
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4" aria-label="Loading catalog">
            {Array.from({ length: 8 }).map((_, index) => (
              <ProductCardSkeleton key={index} />
            ))}
          </div>
        ) : error ? (
          <div className="flex flex-col items-start gap-3 rounded-md border border-border bg-card px-5 py-10">
            <div>
              <p className="text-sm font-medium text-foreground">Catalog unavailable</p>
              <p className="mt-1 max-w-md text-sm text-muted-foreground">{error}</p>
            </div>
            <Button
              onClick={() => void load(pattern)}
              variant="outline"
              className="h-9 rounded-md gap-2"
            >
              <RefreshCw className="h-3.5 w-3.5" />
              Retry
            </Button>
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            {products.map((product) => (
              <ProductCard key={product.id} product={product} onSelect={() => setSelectedProduct(product)} />
            ))}
          </div>
        )}
      </section>

      <ProductDetailModal
        product={selectedProduct}
        open={selectedProduct !== null}
        onOpenChange={(open) => {
          if (!open) setSelectedProduct(null);
        }}
      />
    </div>
  );
}
