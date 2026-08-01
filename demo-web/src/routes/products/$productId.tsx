import { createFileRoute, Link, useParams } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { useCartStore } from "@/lib/store";
import { fetchLiveCatalogServer } from "@/lib/products";
import { CatalogProduct } from "@/types";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ProductDetailView } from "@/components/ProductDetailView";
import { ArrowLeft, Package } from "lucide-react";

export const Route = createFileRoute("/products/$productId")({
  component: ProductDetailPage,
});

function ProductDetailSkeleton() {
  return (
    <div className="mx-auto max-w-7xl pb-16">
      <Skeleton className="mb-6 h-4 w-24 rounded-sm" />
      <div className="grid grid-cols-1 items-center gap-12 md:grid-cols-[1.05fr_1fr] lg:gap-16">
        <Skeleton className="aspect-square w-full rounded-2xl" />
        <div className="flex flex-col gap-6">
          <div className="space-y-3">
            <Skeleton className="h-3 w-20 rounded-sm" />
            <Skeleton className="h-9 w-3/4 rounded-sm" />
            <Skeleton className="h-8 w-28 rounded-sm" />
            <Skeleton className="h-5 w-24 rounded-full" />
          </div>
          <Skeleton className="h-px w-full" />
          <div className="space-y-2">
            <Skeleton className="h-3.5 w-full rounded-sm" />
            <Skeleton className="h-3.5 w-full rounded-sm" />
            <Skeleton className="h-3.5 w-2/3 rounded-sm" />
          </div>
          <Skeleton className="h-11 w-full rounded-md sm:w-[240px]" />
          <div className="space-y-2.5 pt-2">
            <Skeleton className="h-3.5 w-40 rounded-sm" />
            <Skeleton className="h-3.5 w-36 rounded-sm" />
            <Skeleton className="h-3.5 w-36 rounded-sm" />
          </div>
        </div>
      </div>
    </div>
  );
}

function ProductDetailPage() {
  const { productId } = useParams({ from: "/products/$productId" });
  const [product, setProduct] = useState<CatalogProduct | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const pattern = useCartStore((s) => s.pattern);

  useEffect(() => {
    const loadProduct = async () => {
      setLoading(true);
      try {
        const liveProducts = await fetchLiveCatalogServer({ data: pattern });
        const found = liveProducts.find((p) => p.id === productId);
        if (found) {
          setProduct(found);
        } else {
          setError("Product not found");
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load product details");
      } finally {
        setLoading(false);
      }
    };

    void loadProduct();
  }, [productId, pattern]);

  if (loading) {
    return <ProductDetailSkeleton />;
  }

  if (error || !product) {
    return (
      <div className="mx-auto flex min-h-[60vh] max-w-7xl items-center justify-center pb-16">
        <div className="flex w-full max-w-md flex-col items-center gap-4 rounded-2xl border border-border bg-card px-8 py-12 text-center">
          <div className="flex h-11 w-11 items-center justify-center rounded-full bg-muted">
            <Package className="h-5 w-5 text-muted-foreground" />
          </div>
          <div className="space-y-1.5">
            <h1 className="text-lg font-semibold tracking-tight text-foreground">Product unavailable</h1>
            <p className="text-sm leading-relaxed text-muted-foreground">
              {error || "This product is missing or no longer listed."}
            </p>
          </div>
          <Link to="/">
            <Button variant="outline" className="mt-1 gap-2 rounded-md">
              <ArrowLeft className="h-4 w-4" />
              Back to shop
            </Button>
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-7xl pb-16">
      <div className="mb-6">
        <Link
          to="/"
          className="inline-flex items-center gap-1.5 text-sm text-muted-foreground transition-colors hover:text-primary"
        >
          <ArrowLeft className="h-3.5 w-3.5" />
          Back to shop
        </Link>
      </div>

      <ProductDetailView product={product} variant="page" />
    </div>
  );
}
