"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { fetchLiveCatalogServer } from "@/lib/products";
import { ProductCard } from "@/components/ProductCard";
import { CatalogProduct, Pattern } from "@/types";
import { Button } from "@/components/ui/button";
import { ArrowRight } from "lucide-react";
import { PatternToggle } from "@/components/PatternToggle";
import { Skeleton } from "@/components/ui/skeleton";
import { useCartStore } from "@/lib/store";

interface HomePageClientProps {
  initialProducts: CatalogProduct[];
  initialError: string | null;
  initialPattern: Pattern;
}

function ProductCardSkeleton() {
  return (
    <div className="flex h-full flex-col overflow-hidden rounded-[1.5rem] border border-border/60 bg-card">
      <Skeleton className="aspect-[4/3] w-full rounded-none bg-muted/60" />
      <div className="flex flex-1 flex-col space-y-4 p-5 pt-4">
        <div className="flex-1 space-y-2">
          <Skeleton className="h-3 w-20 rounded-full" />
          <Skeleton className="h-5 w-3/4 rounded-full" />
          <div className="space-y-1.5">
            <Skeleton className="h-4 w-full rounded-full" />
            <Skeleton className="h-4 w-5/6 rounded-full" />
          </div>
        </div>
        <div className="flex items-center justify-between gap-3 pt-2">
          <Skeleton className="h-6 w-20 rounded-full" />
          <Skeleton className="h-9 w-20 rounded-full" />
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

      <section id="products" className="space-y-10 pt-8">
        <div className="flex flex-col gap-6 border-b border-border/40 pb-8 lg:flex-row lg:items-end lg:justify-between">
          <div className="max-w-2xl">
            <h2 className="text-4xl font-black tracking-tight text-foreground">
              New Arrivals
            </h2>
            <p className="text-base font-medium text-muted-foreground mt-3 leading-relaxed">
              Premium gadgets and technology for the modern professional. Upgrade your workflow today.
            </p>
          </div>
          <div className="w-full rounded-[1.25rem] border border-border/60 bg-card/70 p-4 shadow-sm lg:w-auto lg:min-w-[24rem]">
            <div className="mb-3 flex items-center justify-between gap-4">
              <div>
                <p className="text-xs font-bold uppercase tracking-widest text-muted-foreground">
                  Processing backend
                </p>
                <p className="mt-1 text-xs font-medium text-muted-foreground/80">
                  Choose which service set powers the live catalog.
                </p>
              </div>
            </div>
            <PatternToggle />
          </div>
        </div>

        {loading ? (
          <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4" aria-label="Loading catalog">
            {Array.from({ length: 8 }).map((_, index) => (
              <ProductCardSkeleton key={index} />
            ))}
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
