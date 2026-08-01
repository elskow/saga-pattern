import { createFileRoute } from '@tanstack/react-router'
import { Suspense, useCallback, useEffect, useRef, useState } from "react";
import {
  fetchInventoryServer,
  updateInventoryStockServer,
  updateInventoryVisibilityServer,
  createProductServer,
  updateProductMetaServer,
  deleteProductServer,
  type CreateProductPayload,
  type UpdateProductMetaPayload,
} from "@/lib/admin";
import { uploadImageServer } from "@/lib/upload";
import { InventoryItem, Pattern } from "@/types";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  AlertTriangle, Check, Minus, Plus, RefreshCw, X,
  PackagePlus, Pencil, Trash2, ImageIcon, Upload,
} from "lucide-react";
import { toast } from "sonner";
import { cn } from "@/lib/utils";
import { formatCurrencyInput, normalizeCurrencyInput } from "@/lib/currency";

export const Route = createFileRoute('/admin/inventory')({
  component: InventoryPage,
})

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function StockBar({ value, max, reserved }: { value: number; max: number; reserved: number }) {
  const availPct = max > 0 ? (value / max) * 100 : 0;
  const resPct = max > 0 ? (reserved / max) * 100 : 0;
  return (
    <div className="flex items-center gap-2">
      <div className="flex h-1.5 w-24 overflow-hidden rounded-full bg-muted">
        <div className="h-full rounded-full bg-primary" style={{ width: `${availPct}%` }} />
        <div className="h-full bg-amber-400" style={{ width: `${resPct}%` }} />
      </div>
      <span className="text-xs text-muted-foreground">{value} avail</span>
    </div>
  );
}

function validateDraftStock(value: string, reserved: number): string | null {
  const trimmed = value.trim();
  if (trimmed.length === 0) return "Enter a total stock value.";
  if (!/^-?\d+$/.test(trimmed)) return "Total stock must be a whole number.";
  const parsed = Number(trimmed);
  if (!Number.isSafeInteger(parsed)) return "Total stock must be a whole number.";
  if (parsed < 0) return "Total stock cannot be negative.";
  if (parsed < reserved) return `Total stock must be at least reserved (${reserved}).`;
  return null;
}

function parseDraftStock(value: string): number | null {
  const trimmed = value.trim();
  if (!/^-?\d+$/.test(trimmed)) return null;
  const parsed = Number(trimmed);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

// ---------------------------------------------------------------------------
// Image upload helper (client → server fn via base64)
// ---------------------------------------------------------------------------

async function uploadFile(file: File): Promise<string> {
  const reader = new FileReader();
  const base64 = await new Promise<string>((resolve, reject) => {
    reader.onload = () => resolve((reader.result as string).split(",")[1]);
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
  const { url } = await uploadImageServer({
    data: { base64, filename: file.name, mimeType: file.type },
  });
  return url;
}

// ---------------------------------------------------------------------------
// Image picker sub-component
// ---------------------------------------------------------------------------

function ImagePicker({
  value,
  onChange,
  disabled,
}: {
  value: string;
  onChange: (url: string) => void;
  disabled?: boolean;
}) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [uploading, setUploading] = useState(false);

  const handleFile = async (file: File) => {
    if (!file.type.startsWith("image/")) {
      toast.error("Please select an image file.");
      return;
    }
    setUploading(true);
    try {
      const url = await uploadFile(file);
      onChange(url);
      toast.success("Image uploaded.");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Upload failed.");
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className="space-y-2">
      <div
        className={cn(
          "relative flex flex-col items-center justify-center gap-2 rounded-md border-2 border-dashed border-border bg-muted/30 transition-colors cursor-pointer hover:bg-muted/50",
          value ? "h-28" : "h-20",
          disabled && "pointer-events-none opacity-50"
        )}
        onClick={() => inputRef.current?.click()}
        onDragOver={(e) => e.preventDefault()}
        onDrop={(e) => {
          e.preventDefault();
          const file = e.dataTransfer.files[0];
          if (file) void handleFile(file);
        }}
      >
        {value ? (
          <img
            src={value}
            alt="Product"
            className="h-full w-full rounded-md object-cover"
          />
        ) : (
          <>
            <ImageIcon className="h-6 w-6 text-muted-foreground" />
            <p className="text-xs text-muted-foreground">Click or drag & drop</p>
          </>
        )}
        {uploading && (
          <div className="absolute inset-0 flex items-center justify-center rounded-md bg-background/70 backdrop-blur-sm">
            <RefreshCw className="h-5 w-5 animate-spin text-muted-foreground" />
          </div>
        )}
      </div>
      <Input
        type="text"
        placeholder="Or paste image URL…"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled || uploading}
        className="text-xs"
      />
      <input
        ref={inputRef}
        type="file"
        accept="image/*"
        className="hidden"
        onChange={(e) => {
          const file = e.target.files?.[0];
          if (file) void handleFile(file);
          e.target.value = "";
        }}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Add / Edit product dialog (inline modal)
// ---------------------------------------------------------------------------

interface ProductFormData {
  name: string;
  description: string;
  price: string;
  category: string;
  image: string;
  choreographyStock: number;
  orchestrationStock: number;
}

const EMPTY_FORM: ProductFormData = {
  name: "", description: "", price: "", category: "", image: "",
  choreographyStock: 0, orchestrationStock: 0,
};

function ProductDialog({
  mode,
  initial,
  onClose,
  onCreated,
  onUpdated,
}: {
  mode: "add" | "edit";
  initial?: Partial<ProductFormData> & { productId?: string };
  onClose: () => void;
  onCreated?: (items: { choreography: InventoryItem; orchestration: InventoryItem }) => void;
  onUpdated?: (items: { choreography: InventoryItem; orchestration: InventoryItem }) => void;
}) {
  const [form, setForm] = useState<ProductFormData>({
    ...EMPTY_FORM,
    ...initial,
  });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const set = (key: keyof ProductFormData, value: string | number) =>
    setForm((prev) => ({ ...prev, [key]: value }));

  const validate = (): string | null => {
    if (!form.name.trim()) return "Product name is required.";
    if (!normalizeCurrencyInput(form.price) || isNaN(Number(normalizeCurrencyInput(form.price)))) return "Valid price is required.";
    if (mode === "add" && form.choreographyStock < 0) return "Choreography stock cannot be negative.";
    if (mode === "add" && form.orchestrationStock < 0) return "Orchestration stock cannot be negative.";
    return null;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const validationErr = validate();
    if (validationErr) { setError(validationErr); return; }
    setError(null);
    setSaving(true);
    try {
      if (mode === "add") {
        const payload: CreateProductPayload = {
          ...form,
          price: normalizeCurrencyInput(form.price),
        };
        const result = await createProductServer({ data: payload });
        toast.success(`Product "${form.name}" added to both services.`);
        onCreated?.(result);
        onClose();
      } else {
        const payload: UpdateProductMetaPayload = {
          productId: initial?.productId ?? "",
          name: form.name,
          description: form.description,
          price: normalizeCurrencyInput(form.price),
          category: form.category,
          image: form.image,
        };
        const result = await updateProductMetaServer({ data: payload });
        toast.success(`Product "${form.name}" updated.`);
        onUpdated?.(result);
        onClose();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Operation failed.");
    } finally {
      setSaving(false);
    }
  };

  return (
    // Backdrop
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/40 backdrop-blur-sm">
      <div className="w-full max-w-lg overflow-hidden rounded-md border border-border bg-card">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-border px-6 py-4">
          <h2 className="text-base font-semibold tracking-tight">
            {mode === "add" ? "Add product" : "Edit product"}
          </h2>
          <button
            onClick={onClose}
            disabled={saving}
            className="text-muted-foreground hover:text-foreground transition-colors"
            aria-label="Close"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* Body */}
        <form onSubmit={handleSubmit}>
          <div className="px-6 py-5 space-y-4 max-h-[65vh] overflow-y-auto">
            {/* Name */}
            <div className="space-y-1.5">
              <Label htmlFor="pf-name">Product name *</Label>
              <Input id="pf-name" value={form.name} onChange={(e) => set("name", e.target.value)} disabled={saving} placeholder="e.g. Wireless Earbuds Pro" />
            </div>

            {/* Description */}
            <div className="space-y-1.5">
              <Label htmlFor="pf-desc">Description</Label>
              <textarea
                id="pf-desc"
                value={form.description}
                onChange={(e) => set("description", e.target.value)}
                disabled={saving}
                placeholder="Short product description…"
                rows={2}
                className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring resize-none disabled:opacity-50"
              />
            </div>

            {/* Price + Category row */}
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-1.5">
                <Label htmlFor="pf-price">Price (IDR) *</Label>
                <Input
                  id="pf-price"
                  type="text"
                  inputMode="numeric"
                  value={form.price}
                  onChange={(e) => set("price", formatCurrencyInput(e.target.value))}
                  disabled={saving}
                  placeholder="19.999.000"
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="pf-category">Category</Label>
                <Input id="pf-category" value={form.category} onChange={(e) => set("category", e.target.value)} disabled={saving} placeholder="Laptops" />
              </div>
            </div>

            {/* Image */}
            <div className="space-y-1.5">
              <Label>Product image</Label>
              <ImagePicker value={form.image} onChange={(url) => set("image", url)} disabled={saving} />
            </div>

            {/* Stock — only shown on add */}
            {mode === "add" && (
              <div className="grid grid-cols-2 gap-3 rounded-md border border-border bg-muted/20 p-4">
                <div className="space-y-1.5">
                  <Label htmlFor="pf-c-stock" className="text-xs">
                    Choreography stock
                  </Label>
                  <Input
                    id="pf-c-stock"
                    type="number"
                    min="0"
                    value={form.choreographyStock}
                    onChange={(e) => set("choreographyStock", Number(e.target.value))}
                    disabled={saving}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="pf-o-stock" className="text-xs">
                    Orchestration stock
                  </Label>
                  <Input
                    id="pf-o-stock"
                    type="number"
                    min="0"
                    value={form.orchestrationStock}
                    onChange={(e) => set("orchestrationStock", Number(e.target.value))}
                    disabled={saving}
                  />
                </div>
              </div>
            )}

            {error && <p className="text-sm text-destructive font-medium">{error}</p>}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end gap-2 border-t border-border px-6 py-4">
            <Button type="button" variant="outline" size="sm" className="rounded-md" onClick={onClose} disabled={saving}>
              Cancel
            </Button>
            <Button type="submit" size="sm" className="gap-1.5 rounded-md" disabled={saving}>
              {saving ? <RefreshCw className="h-3.5 w-3.5 animate-spin" /> : <Check className="h-3.5 w-3.5" />}
              {saving ? "Saving…" : mode === "add" ? "Add product" : "Save changes"}
            </Button>
          </div>
        </form>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Delete confirmation dialog
// ---------------------------------------------------------------------------

function DeleteDialog({
  productName,
  productId,
  onClose,
  onDeleted,
}: {
  productName: string;
  productId: string;
  onClose: () => void;
  onDeleted: (productId: string) => void;
}) {
  const [deleting, setDeleting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleDelete = async () => {
    setDeleting(true);
    setError(null);
    try {
      await deleteProductServer({ data: { productId } });
      toast.success(`"${productName}" has been removed.`);
      onDeleted(productId);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Delete failed.");
      setDeleting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/40 backdrop-blur-sm">
      <div className="w-full max-w-sm space-y-4 rounded-md border border-border bg-card p-5">
        <div>
          <h2 className="text-sm font-semibold text-foreground">Delete product</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Remove <span className="font-medium text-foreground">"{productName}"</span> from both inventory services? This cannot be undone.
          </p>
        </div>
        {error && <p className="text-sm text-destructive font-medium">{error}</p>}
        <div className="flex justify-end gap-2">
          <Button variant="outline" size="sm" className="rounded-md" onClick={onClose} disabled={deleting}>
            Cancel
          </Button>
          <Button
            size="sm"
            className="gap-1.5 rounded-md bg-red-600 text-white hover:bg-red-700"
            onClick={handleDelete}
            disabled={deleting}
          >
            {deleting ? <RefreshCw className="h-3.5 w-3.5 animate-spin" /> : <Trash2 className="h-3.5 w-3.5" />}
            {deleting ? "Deleting…" : "Delete"}
          </Button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Inventory table row (existing stock/visibility controls, + new edit/delete)
// ---------------------------------------------------------------------------

const DEFAULT_PATTERN: Pattern = "orchestration";

function InventoryTableRow({
  item,
  pattern,
  onItemUpdated,
  onEditClick,
  onDeleteClick,
  globalIsSaving,
  setGlobalIsSaving,
}: {
  item: InventoryItem;
  pattern: Pattern;
  onItemUpdated: (item: InventoryItem) => void;
  onEditClick: (item: InventoryItem) => void;
  onDeleteClick: (item: InventoryItem) => void;
  globalIsSaving: boolean;
  setGlobalIsSaving: (saving: boolean) => void;
}) {
  const [draftValue, setDraftValue] = useState<string | null>(null);
  const [serverError, setServerError] = useState<string | null>(null);
  const [isSavingStock, setIsSavingStock] = useState(false);
  const [isSavingVisibility, setIsSavingVisibility] = useState(false);

  const currentDraft = draftValue ?? item.totalStock.toString();
  const parsedDraft = parseDraftStock(currentDraft);
  const validationError = validateDraftStock(currentDraft, item.reserved);
  const rowError = validationError ?? serverError;
  const hasChanges = parsedDraft !== null ? parsedDraft !== item.totalStock : draftValue !== null;
  const lowStock = item.available <= item.reorderPoint;

  const decrementDisabled = isSavingStock || (parsedDraft !== null && parsedDraft <= item.reserved);
  const isAnotherRowSaving = globalIsSaving && !isSavingStock && !isSavingVisibility;

  const handleAdjustDraft = (delta: number) => {
    const baseValue = parsedDraft ?? item.totalStock;
    const nextValue = Math.max(item.reserved, baseValue + delta);
    setDraftValue(nextValue.toString());
    setServerError(null);
  };

  const clearRowDraft = () => { setDraftValue(null); setServerError(null); };

  const handleSaveStock = async () => {
    if (validationError || !hasChanges) return;
    const nextTotalStock = Number(currentDraft.trim());
    if (nextTotalStock === item.totalStock) { clearRowDraft(); return; }
    setServerError(null);
    setIsSavingStock(true);
    setGlobalIsSaving(true);
    try {
      const updatedItem = await updateInventoryStockServer({ data: { pattern, productId: item.productId, totalStock: nextTotalStock } });
      onItemUpdated(updatedItem);
      clearRowDraft();
      toast.success(`Updated ${updatedItem.productName} stock to ${updatedItem.totalStock}.`);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Stock update failed";
      setServerError(message);
      toast.error(message);
    } finally {
      setIsSavingStock(false);
      setGlobalIsSaving(false);
    }
  };

  const handleToggleVisibility = async () => {
    const nextVisible = !(item.visible ?? true);
    setIsSavingVisibility(true);
    setGlobalIsSaving(true);
    try {
      const updatedItem = await updateInventoryVisibilityServer({ data: { pattern, productId: item.productId, visible: nextVisible } });
      onItemUpdated(updatedItem);
      toast.success(nextVisible ? `${updatedItem.productName} is now visible.` : `${updatedItem.productName} is now hidden.`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Visibility update failed");
    } finally {
      setIsSavingVisibility(false);
      setGlobalIsSaving(false);
    }
  };

  return (
    <TableRow className={cn(hasChanges && "bg-amber-50/60")}>
      {/* Image */}
      <TableCell>
        {item.visible !== false && (
          <div className="h-10 w-10 overflow-hidden rounded-md border border-border bg-muted/40 shrink-0">
            {item.image ? (
              <img src={item.image} alt={item.productName} className="h-full w-full object-cover" />
            ) : (
              <div className="flex h-full w-full items-center justify-center">
                <ImageIcon className="h-4 w-4 text-muted-foreground/40" />
              </div>
            )}
          </div>
        )}
      </TableCell>

      {/* Name */}
      <TableCell className="font-medium">
        <div className="flex items-center gap-2">
          {lowStock && <AlertTriangle className="h-3.5 w-3.5 shrink-0 text-red-500" />}
          <span>{item.productName}</span>
          {hasChanges && (
            <span className="rounded-md border border-amber-200/80 bg-amber-50 px-2 py-0.5 text-[10px] font-medium text-amber-800">
              Unsaved
            </span>
          )}
        </div>
      </TableCell>

      {/* SKU */}
      <TableCell className="font-mono text-xs text-muted-foreground">{item.sku}</TableCell>

      {/* Visibility */}
      <TableCell className="align-top">
        <Button
          type="button"
          variant={item.visible === false ? "outline" : "secondary"}
          size="sm"
          className="h-7 rounded-md px-2.5 text-xs"
          disabled={isSavingVisibility || isSavingStock}
          onClick={handleToggleVisibility}
        >
          {isSavingVisibility ? "Updating…" : item.visible === false ? "Hidden" : "Visible"}
        </Button>
      </TableCell>

      {/* Stock editor */}
      <TableCell className="min-w-[220px] text-right align-top">
        <div className="flex flex-col items-end gap-1">
          <div className="flex items-center gap-1.5">
            <Button variant="outline" size="icon-sm" className="rounded-md" disabled={decrementDisabled} onClick={() => handleAdjustDraft(-1)} aria-label={`Decrease ${item.productName} stock`}>
              <Minus className="h-3.5 w-3.5" />
            </Button>
            <Input
              type="text"
              inputMode="numeric"
              value={currentDraft}
              disabled={isSavingStock}
              aria-invalid={Boolean(rowError)}
              aria-label={`${item.productName} total stock`}
              className="h-8 w-20 text-right font-mono"
              onChange={(e) => { setDraftValue(e.target.value); setServerError(null); }}
            />
            <Button variant="outline" size="icon-sm" className="rounded-md" disabled={isSavingStock} onClick={() => handleAdjustDraft(1)} aria-label={`Increase ${item.productName} stock`}>
              <Plus className="h-3.5 w-3.5" />
            </Button>
          </div>
          {rowError && <p className="max-w-[220px] whitespace-normal text-right text-xs text-destructive">{rowError}</p>}
        </div>
      </TableCell>

      <TableCell className="text-right">
        <span className={cn("text-sm", item.reserved > 0 ? "font-medium text-amber-700" : "text-muted-foreground")}>{item.reserved}</span>
      </TableCell>
      <TableCell className="text-right">
        <span className={cn("font-semibold", lowStock ? "text-red-600" : "text-foreground")}>{item.available}</span>
      </TableCell>
      <TableCell>
        <StockBar value={item.available} max={item.totalStock} reserved={item.reserved} />
      </TableCell>
      <TableCell className="text-xs text-muted-foreground">
        {new Date(item.lastRestocked).toLocaleDateString(undefined, { month: "short", day: "numeric" })}
      </TableCell>

      {/* Actions */}
      <TableCell className="align-top">
        <div className="flex items-center justify-end gap-1.5">
          <Button variant="outline" size="sm" className="h-7 rounded-md px-2.5 text-xs" disabled={isSavingStock || (draftValue === null && !serverError)} onClick={clearRowDraft}>
            <X className="h-3 w-3" /> Cancel
          </Button>
          <Button size="sm" className="h-7 rounded-md px-2.5 text-xs" disabled={isSavingStock || isAnotherRowSaving || Boolean(validationError) || !hasChanges} onClick={handleSaveStock}>
            {isSavingStock ? <RefreshCw className="h-3 w-3 animate-spin" /> : <Check className="h-3 w-3" />}
            {isSavingStock ? "Saving…" : "Save"}
          </Button>
          <Button
            variant="outline"
            size="icon-sm"
            className="h-7 w-7 rounded-md"
            onClick={() => onEditClick(item)}
            disabled={isSavingStock || isSavingVisibility}
            aria-label={`Edit ${item.productName}`}
          >
            <Pencil className="h-3 w-3" />
          </Button>
          <Button
            variant="outline"
            size="icon-sm"
            className="h-7 w-7 rounded-md text-red-500 hover:border-red-300 hover:text-red-600"
            onClick={() => onDeleteClick(item)}
            disabled={isSavingStock || isSavingVisibility}
            aria-label={`Delete ${item.productName}`}
          >
            <Trash2 className="h-3 w-3" />
          </Button>
        </div>
      </TableCell>
    </TableRow>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

function InventoryPageInner() {
  const [inventory, setInventory] = useState<InventoryItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pattern, setPattern] = useState<Pattern>(DEFAULT_PATTERN);
  const [globalIsSaving, setGlobalIsSaving] = useState(false);
  const latestLoadRequestId = useRef(0);

  // Dialog state
  const [addOpen, setAddOpen] = useState(false);
  const [editTarget, setEditTarget] = useState<InventoryItem | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<InventoryItem | null>(null);

  const load = useCallback(async (activePattern: Pattern) => {
    const requestId = ++latestLoadRequestId.current;
    try {
      const items = await fetchInventoryServer({ data: activePattern });
      if (requestId !== latestLoadRequestId.current) return;
      setInventory(items);
      setError(null);
    } catch (err) {
      if (requestId !== latestLoadRequestId.current) return;
      setInventory([]);
      setError(err instanceof Error ? err.message : "Live inventory data unavailable");
    } finally {
      if (requestId === latestLoadRequestId.current) setLoading(false);
    }
  }, []);

  useEffect(() => { void load(DEFAULT_PATTERN); }, [load]);

  const handleRefresh = () => { setLoading(true); void load(pattern); };
  const handlePatternChange = (next: Pattern) => {
    if (next === pattern) return;
    setPattern(next);
    setLoading(true);
    void load(next);
  };
  const handleItemUpdated = useCallback((updated: InventoryItem) => {
    setInventory((prev) => prev.map((cur) => cur.productId === updated.productId ? updated : cur));
  }, []);

  const handleCreated = (result: { choreography: InventoryItem; orchestration: InventoryItem }) => {
    const item = pattern === "choreography" ? result.choreography : result.orchestration;
    setInventory((prev) => [...prev, item]);
  };

  const handleUpdated = (result: { choreography: InventoryItem; orchestration: InventoryItem }) => {
    const item = pattern === "choreography" ? result.choreography : result.orchestration;
    handleItemUpdated(item);
  };

  const handleDeleted = (productId: string) => {
    setInventory((prev) => prev.filter((i) => i.productId !== productId));
  };

  return (
    <div className="max-w-5xl space-y-6">
      {/* Header */}
      <div className="flex flex-col gap-3 border-b border-border pb-5 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-foreground">Inventory</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Live stock levels from the {pattern} service. Add, edit, or remove products across both services simultaneously.
          </p>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <Button
            variant="outline"
            size="sm"
            onClick={handleRefresh}
            disabled={globalIsSaving}
            className="gap-1.5 rounded-md text-xs"
          >
            <RefreshCw className={cn("h-3.5 w-3.5", loading && "animate-spin")} />
            Refresh
          </Button>
          <Button
            size="sm"
            className="gap-1.5 rounded-md text-xs"
            onClick={() => setAddOpen(true)}
          >
            <PackagePlus className="h-3.5 w-3.5" />
            Add product
          </Button>
        </div>
      </div>

      {error && (
        <div className="rounded-md border border-amber-200/80 bg-amber-50 p-3 text-sm text-amber-800">{error}</div>
      )}

      {/* Pattern tabs */}
      <div className="inline-flex items-center gap-0.5 rounded-md border border-border bg-card p-0.5">
        {(["orchestration", "choreography"] as Pattern[]).map((value) => (
          <button
            key={value}
            type="button"
            disabled={globalIsSaving}
            onClick={() => handlePatternChange(value)}
            className={cn(
              "rounded-md px-3 py-1 text-xs font-medium capitalize transition-colors disabled:opacity-50",
              pattern === value
                ? "bg-secondary text-primary"
                : "text-muted-foreground hover:text-foreground"
            )}
          >
            {value}
          </button>
        ))}
      </div>

      {/* Table */}
      <div className="overflow-hidden rounded-md border border-border bg-card">
        {loading ? (
          <div className="space-y-3 p-6">
            {[1, 2, 3, 4, 5].map((i) => <Skeleton key={i} className="h-10 w-full rounded-md" />)}
          </div>
        ) : error ? (
          <div className="py-12 text-center text-sm text-muted-foreground">Live inventory data is unavailable.</div>
        ) : inventory.length === 0 ? (
          <div className="flex flex-col items-center justify-center gap-3 py-12">
            <div className="text-center">
              <p className="text-sm font-medium text-foreground">No products found</p>
              <p className="mt-1 text-xs text-muted-foreground">Add your first product to get started.</p>
            </div>
            <Button size="sm" className="gap-1.5 rounded-md" onClick={() => setAddOpen(true)}>
              <PackagePlus className="h-3.5 w-3.5" /> Add product
            </Button>
          </div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="w-14 px-4 text-xs font-medium text-muted-foreground">Image</TableHead>
                <TableHead className="px-4 text-xs font-medium text-muted-foreground">Product</TableHead>
                <TableHead className="px-4 text-xs font-medium text-muted-foreground">SKU</TableHead>
                <TableHead className="px-4 text-xs font-medium text-muted-foreground">Storefront</TableHead>
                <TableHead className="px-4 text-right text-xs font-medium text-muted-foreground">Total</TableHead>
                <TableHead className="px-4 text-right text-xs font-medium text-muted-foreground">Reserved</TableHead>
                <TableHead className="px-4 text-right text-xs font-medium text-muted-foreground">Available</TableHead>
                <TableHead className="px-4 text-xs font-medium text-muted-foreground">Stock level</TableHead>
                <TableHead className="px-4 text-xs font-medium text-muted-foreground">Last restocked</TableHead>
                <TableHead className="px-4 text-xs font-medium text-muted-foreground">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {inventory.map((item) => (
                <InventoryTableRow
                  key={item.productId}
                  item={item}
                  pattern={pattern}
                  onItemUpdated={handleItemUpdated}
                  onEditClick={setEditTarget}
                  onDeleteClick={setDeleteTarget}
                  globalIsSaving={globalIsSaving}
                  setGlobalIsSaving={setGlobalIsSaving}
                />
              ))}
            </TableBody>
          </Table>
        )}
      </div>

      {/* Add dialog */}
      {addOpen && (
        <ProductDialog
          mode="add"
          onClose={() => setAddOpen(false)}
          onCreated={handleCreated}
        />
      )}

      {/* Edit dialog */}
      {editTarget && (
        <ProductDialog
          mode="edit"
          initial={{
            productId: editTarget.productId,
            name: editTarget.productName,
            description: editTarget.description ?? "",
            price: formatCurrencyInput(editTarget.price),
            category: editTarget.category ?? "",
            image: editTarget.image ?? "",
          }}
          onClose={() => setEditTarget(null)}
          onUpdated={handleUpdated}
        />
      )}

      {/* Delete dialog */}
      {deleteTarget && (
        <DeleteDialog
          productName={deleteTarget.productName}
          productId={deleteTarget.productId}
          onClose={() => setDeleteTarget(null)}
          onDeleted={handleDeleted}
        />
      )}
    </div>
  );
}

function InventoryPage() {
  return (
    <Suspense fallback={<div className="max-w-5xl space-y-6" />}>
      <InventoryPageInner />
    </Suspense>
  );
}
