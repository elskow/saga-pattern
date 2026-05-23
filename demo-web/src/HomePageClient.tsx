"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { fetchLiveCatalogServer } from "@/lib/products";
import { ProductCard } from "@/components/ProductCard";
import { CatalogProduct, Pattern } from "@/types";
import { Button } from "@/components/ui/button";
import { RefreshCw, ShoppingBag, ArrowRight } from "lucide-react";
import { PatternToggle } from "@/components/PatternToggle";
import { useCartStore } from "@/lib/store";

interface HomePageClientProps {
  initialProducts: CatalogProduct[];
  initialError: string | null;
  initialPattern: Pattern;
}

export default function HomePageClient({ initialProducts, initialError, initialPattern }: HomePageClientProps) {
  const pattern = useCartStore((state) => state.pattern);
  const reconcileWithCatalog = useCartStore((state) => state.reconcileWithCatalog);
  const initialCatalogLoadStarted = useRef(false);
  const [products, setProducts] = useState<CatalogProduct[]>(initialProducts);
  const [loading, setLoading] = useState(initialProducts.length === 0 && initialError === null);
  const [error, setError] = useState<string | null>(initialError);
  const [catalogPattern, setCatalogPattern] = useState<Pattern>(initialPattern);

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

  const scrollToProducts = () => {
    document.getElementById("products")?.scrollIntoView({ behavior: "smooth" });
  };

  return (
    <div className="space-y-12 pb-12 w-full max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <section className="relative overflow-hidden px-6 py-20 md:px-12 md:py-32 flex flex-col items-center text-center">
        <div className="max-w-4xl space-y-6 relative z-10 flex flex-col items-center">
          <h1 className="text-4xl md:text-6xl lg:text-7xl font-bold tracking-tighter text-foreground text-balance leading-[1.1]">
            Next-generation tech for <br className="hidden md:block" /> the modern workflow.
          </h1>
          <p className="text-base md:text-lg text-muted-foreground max-w-2xl leading-relaxed text-balance">
            Discover our curated collection of premium gadgets. From high-fidelity audio to ultra-fast laptops, upgrade your setup today.
          </p>

          <Button
            onClick={scrollToProducts}
            size="lg"
            className="mt-4 rounded-full px-8 gap-2 shadow-md group"
          >
            Shop Collection
            <ArrowRight className="h-4 w-4 transition-transform group-hover:translate-x-1" />
          </Button>
        </div>
      </section>

      <section id="products" className="space-y-6 pt-4">
        <div className="flex flex-col gap-4 border-b border-border/60 pb-5 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <h2 className="text-2xl md:text-3xl font-semibold tracking-tight text-foreground">
              New Arrivals
            </h2>
            <p className="text-sm text-muted-foreground mt-1.5 font-medium">
              Available in {catalogPattern} stock and ready to ship.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <PatternToggle />
            <p className="text-xs font-semibold text-muted-foreground rounded-full border border-border/60 bg-muted/30 px-3.5 py-1.5">
              {loading ? "Loading…" : `${products.length} items`}
            </p>
            <Button
              variant="outline"
              size="sm"
              onClick={() => void load(pattern)}
              className="rounded-full gap-2 text-xs font-medium shadow-sm hover:bg-muted/50 transition-colors"
            >
              <RefreshCw className="h-3.5 w-3.5" />
              Refresh
            </Button>
          </div>
        </div>

        {loading ? (
          <div className="flex flex-col items-center justify-center py-32 gap-5 text-center rounded-2xl border border-dashed border-border bg-card/50">
            <ShoppingBag className="h-12 w-12 text-muted-foreground/20 animate-pulse" />
            <div>
              <p className="text-lg font-semibold tracking-tight text-foreground">Loading catalog</p>
              <p className="text-sm text-muted-foreground mt-1">Fetching live inventory...</p>
            </div>
          </div>
        ) : error ? (
          <div className="flex flex-col items-center justify-center py-32 gap-5 text-center rounded-2xl border border-dashed border-red-200 bg-red-50/30">
            <div className="h-12 w-12 rounded-full bg-red-100 flex items-center justify-center">
              <span className="text-red-600 font-bold text-xl">!</span>
            </div>
            <div>
              <p className="text-lg font-semibold tracking-tight text-red-900">Catalog unavailable</p>
              <p className="text-sm text-red-600/80 mt-1 max-w-md mx-auto">{error}</p>
            </div>
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            {products.map((product) => (
              <ProductCard key={product.id} product={product} />
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
