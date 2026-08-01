"use client";

import { CatalogProduct } from "@/types";
import { Dialog, DialogContent } from "@/components/ui/dialog";
import { ProductDetailView } from "@/components/ProductDetailView";

interface ProductDetailModalProps {
  product: CatalogProduct | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function ProductDetailModal({ product, open, onOpenChange }: ProductDetailModalProps) {
  if (!product) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="p-6 md:p-8">
        <ProductDetailView product={product} variant="modal" />
      </DialogContent>
    </Dialog>
  );
}
