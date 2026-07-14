import { createFileRoute, Link, useParams } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { useCartStore } from "@/lib/store";
import { fetchLiveCatalogServer } from "@/lib/products";
import { CatalogProduct, Pattern } from "@/types";
import { Button } from "@/components/ui/button";
import { formatPrice } from "@/lib/currency";
import { ArrowLeft, Check, PackageOpen, ShoppingCart } from "lucide-react";
import { toast } from "sonner";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/products/$productId")({
  component: ProductDetailPage,
});

function ProductDetailPage() {
  const { productId } = useParams({ from: "/products/$productId" });
  const [product, setProduct] = useState<CatalogProduct | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const pattern = useCartStore((s) => s.pattern);
  const addItem = useCartStore((s) => s.addItem);
  const cartItems = useCartStore((s) => s.items);

  const quantityInCart = cartItems.find((item) => item.product.id === productId)?.quantity ?? 0;
  const [added, setAdded] = useState(false);

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

  const handleAdd = () => {
    if (!product) return;

    if (product.stock <= 0) {
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

  if (loading) {
    return (
      <div className="max-w-7xl mx-auto px-4 py-20 flex items-center justify-center min-h-[60vh]">
        <div className="flex flex-col items-center gap-4 animate-pulse">
          <div className="h-12 w-12 rounded-full bg-muted/50 flex items-center justify-center">
            <PackageOpen className="h-6 w-6 text-muted-foreground/40" />
          </div>
          <p className="text-muted-foreground font-medium">Loading product details...</p>
        </div>
      </div>
    );
  }

  if (error || !product) {
    return (
      <div className="max-w-7xl mx-auto px-4 py-20 flex items-center justify-center min-h-[60vh]">
        <div className="flex flex-col items-center justify-center text-center gap-4 max-w-md">
          <div className="h-16 w-16 rounded-full bg-red-100 flex items-center justify-center">
            <span className="text-red-600 font-bold text-2xl">!</span>
          </div>
          <h1 className="text-2xl font-bold tracking-tight text-foreground">Product Unavailable</h1>
          <p className="text-muted-foreground">{error || "The product you're looking for doesn't exist or is currently unavailable."}</p>
          <Link to="/">
            <Button className="mt-4 rounded-full shadow-sm">
              <ArrowLeft className="h-4 w-4 mr-2" />
              Back to Shop
            </Button>
          </Link>
        </div>
      </div>
    );
  }

  const outOfStock = product.stock <= 0;

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-10 min-h-[80vh]">
      <div className="mb-8">
        <Link to="/">
          <Button variant="ghost" size="sm" className="rounded-full gap-2 text-muted-foreground hover:text-foreground hover:bg-muted/50 -ml-3">
            <ArrowLeft className="h-4 w-4" />
            Back to Shop
          </Button>
        </Link>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-12 lg:gap-20 items-start">
        <div className="relative aspect-square w-full rounded-[2rem] overflow-hidden bg-card border border-border/50 p-4 sm:p-6 flex items-center justify-center">
          <img
            src={product.image}
            alt={product.name}
            className="w-full h-full object-contain"
          />
          {outOfStock && (
            <div className="absolute top-6 left-6">
              <span className="rounded-full border border-destructive/20 bg-destructive/10 px-3 py-1 text-xs font-bold uppercase tracking-wider text-destructive backdrop-blur-md">
                Out of stock
              </span>
            </div>
          )}
        </div>

        <div className="flex flex-col space-y-8 pt-4">
          <div className="space-y-4">
            <div className="flex items-center gap-3">
              <span className="px-3 py-1 text-[11px] font-bold text-muted-foreground uppercase tracking-[0.25em] bg-muted/50 rounded-full border border-border/50">
                {product.category}
              </span>
              <span className="text-xs font-mono text-muted-foreground">
                SKU: {product.productId}
              </span>
            </div>

            <h1 className="text-4xl md:text-5xl font-bold tracking-tight text-foreground text-balance">
              {product.name}
            </h1>

            <p className="text-4xl font-bold tracking-tight text-foreground tabular-nums pt-2">
              {formatPrice(product.price)}
            </p>
          </div>

          <div className="space-y-6">
            <p className="text-base leading-relaxed text-muted-foreground">
              {product.description}
            </p>

            <div className="flex flex-col gap-5 py-6 border-y border-border/50 sm:flex-row sm:items-center sm:gap-8">
              <div className="flex flex-col">
                <span className="text-sm font-semibold text-foreground">Availability</span>
                <span className={cn("text-sm font-medium", outOfStock ? "text-destructive" : "text-green-600")}>
                  {outOfStock ? "Out of stock" : `${product.stock} units available in ${pattern}`}
                </span>
              </div>
              {quantityInCart > 0 && (
                <div className="flex flex-col sm:border-l sm:border-border sm:pl-8">
                  <span className="text-sm font-semibold text-foreground">In Cart</span>
                  <span className="text-sm font-medium text-muted-foreground">
                    {quantityInCart} selected
                  </span>
                </div>
              )}
            </div>
          </div>

          <div className="pt-2">
            <Button
              onClick={handleAdd}
              disabled={outOfStock}
              size="lg"
              className={cn(
                "w-full sm:w-auto min-w-[240px] h-14 rounded-full text-base font-semibold shadow-xl transition-all duration-300",
                added
                  ? "bg-green-500 hover:bg-green-600 text-white shadow-green-500/25"
                  : "bg-foreground hover:bg-foreground/90 hover:-translate-y-0.5 hover:shadow-2xl"
              )}
            >
              {added ? (
                <>
                  <Check className="h-5 w-5 mr-2" />
                  Added to Cart
                </>
              ) : (
                <>
                  <ShoppingCart className="h-5 w-5 mr-2" />
                  Add to Cart
                </>
              )}
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
