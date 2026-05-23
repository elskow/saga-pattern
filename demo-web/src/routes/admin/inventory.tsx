import { createFileRoute } from '@tanstack/react-router'
import { Suspense, useCallback, useEffect, useRef, useState } from "react";
import { fetchInventoryServer, updateInventoryStockServer, updateInventoryVisibilityServer } from "@/lib/admin";
import { InventoryItem, Pattern } from "@/types";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { AlertTriangle, Check, Minus, Plus, RefreshCw, X } from "lucide-react";
import { toast } from "sonner";
import { cn } from "@/lib/utils";

export const Route = createFileRoute('/admin/inventory')({
  component: InventoryPage,
})

function StockBar({ value, max, reserved }: { value: number; max: number; reserved: number }) {
  const availPct = max > 0 ? (value / max) * 100 : 0;
  const resPct = max > 0 ? (reserved / max) * 100 : 0;
  return (
    <div className="flex items-center gap-2">
      <div className="flex h-1.5 w-24 overflow-hidden rounded-full bg-muted">
        <div
          className="h-full rounded-full bg-foreground"
          style={{ width: `${availPct}%` }}
        />
        <div
          className="h-full bg-amber-400"
          style={{ width: `${resPct}%` }}
        />
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

const DEFAULT_PATTERN: Pattern = "orchestration";

function InventoryTableRow({
  item,
  pattern,
  onItemUpdated,
  globalIsSaving,
  setGlobalIsSaving
}: {
  item: InventoryItem;
  pattern: Pattern;
  onItemUpdated: (item: InventoryItem) => void;
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

  const clearRowDraft = () => {
    setDraftValue(null);
    setServerError(null);
  };

  const handleSaveStock = async () => {
    if (validationError || !hasChanges) return;

    const nextTotalStock = Number(currentDraft.trim());
    if (nextTotalStock === item.totalStock) {
      clearRowDraft();
      return;
    }

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
      toast.success(
        nextVisible
          ? `${updatedItem.productName} is now visible on the storefront.`
          : `${updatedItem.productName} is now hidden from the storefront.`
      );
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Visibility update failed");
    } finally {
      setIsSavingVisibility(false);
      setGlobalIsSaving(false);
    }
  };

  return (
    <TableRow className={cn(hasChanges && "bg-amber-50/60 dark:bg-amber-950/20")}>
      <TableCell className="font-medium">
        <div className="flex items-center gap-2">
          {lowStock && <AlertTriangle className="h-3.5 w-3.5 shrink-0 text-red-500" />}
          <span>{item.productName}</span>
          {hasChanges && (
            <span className="rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-amber-800 dark:bg-amber-500/15 dark:text-amber-200">
              Unsaved
            </span>
          )}
        </div>
      </TableCell>
      <TableCell className="font-mono text-xs text-muted-foreground">
        {item.sku}
      </TableCell>
      <TableCell className="align-top">
        <Button
          type="button"
          variant={item.visible === false ? "outline" : "default"}
          size="sm"
          className="h-7 rounded-full px-2.5 text-xs"
          disabled={isSavingVisibility || isSavingStock}
          onClick={handleToggleVisibility}
        >
          {isSavingVisibility ? "Updating…" : item.visible === false ? "Hidden" : "Visible"}
        </Button>
      </TableCell>
      <TableCell className="min-w-[220px] text-right align-top">
        <div className="flex flex-col items-end gap-1">
          <div className="flex items-center gap-1.5">
            <Button
              variant="outline"
              size="icon-sm"
              className="rounded-full"
              disabled={decrementDisabled}
              onClick={() => handleAdjustDraft(-1)}
              aria-label={`Decrease ${item.productName} total stock`}
            >
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
              onChange={(e) => {
                setDraftValue(e.target.value);
                setServerError(null);
              }}
            />
            <Button
              variant="outline"
              size="icon-sm"
              className="rounded-full"
              disabled={isSavingStock}
              onClick={() => handleAdjustDraft(1)}
              aria-label={`Increase ${item.productName} total stock`}
            >
              <Plus className="h-3.5 w-3.5" />
            </Button>
          </div>
          {rowError && (
            <p className="max-w-[220px] whitespace-normal text-right text-xs text-destructive">
              {rowError}
            </p>
          )}
        </div>
      </TableCell>
      <TableCell className="text-right">
        <span className={cn("text-sm", item.reserved > 0 ? "font-medium text-amber-700" : "text-muted-foreground")}>
          {item.reserved}
        </span>
      </TableCell>
      <TableCell className="text-right">
        <span className={cn("font-semibold", lowStock ? "text-red-600" : "text-foreground")}>
          {item.available}
        </span>
      </TableCell>
      <TableCell>
        <StockBar value={item.available} max={item.totalStock} reserved={item.reserved} />
      </TableCell>
      <TableCell className="text-xs text-muted-foreground">
        {new Date(item.lastRestocked).toLocaleDateString(undefined, {
          month: "short",
          day: "numeric",
        })}
      </TableCell>
      <TableCell className="align-top">
        <div className="flex items-center justify-end gap-1.5">
          <Button
            variant="outline"
            size="sm"
            className="h-7 rounded-full px-2.5 text-xs"
            disabled={isSavingStock || (draftValue === null && !serverError)}
            onClick={clearRowDraft}
          >
            <X className="h-3 w-3" />
            Cancel
          </Button>
          <Button
            size="sm"
            className="h-7 rounded-full px-2.5 text-xs"
            disabled={isSavingStock || isAnotherRowSaving || Boolean(validationError) || !hasChanges}
            onClick={handleSaveStock}
          >
            {isSavingStock ? <RefreshCw className="h-3 w-3 animate-spin" /> : <Check className="h-3 w-3" />}
            {isSavingStock ? "Saving…" : "Save"}
          </Button>
        </div>
      </TableCell>
    </TableRow>
  );
}

function InventoryPageInner() {
  const [inventory, setInventory] = useState<InventoryItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pattern, setPattern] = useState<Pattern>(DEFAULT_PATTERN);
  const [globalIsSaving, setGlobalIsSaving] = useState(false);
  const latestLoadRequestId = useRef(0);

  const load = useCallback(async (activePattern: Pattern) => {
    const requestId = ++latestLoadRequestId.current;

    try {
      const items = await fetchInventoryServer({ data: activePattern });

      if (requestId !== latestLoadRequestId.current) return;

      setInventory(items);
      setError(null);
    } catch (err) {
      if (requestId !== latestLoadRequestId.current) return;

      console.error("[Inventory] Fetch failed:", err);
      setInventory([]);
      setError(err instanceof Error ? err.message : "Live inventory data unavailable");
    } finally {
      if (requestId === latestLoadRequestId.current) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void load(DEFAULT_PATTERN);
  }, [load]);

  const handleRefresh = () => {
    setLoading(true);
    void load(pattern);
  };

  const handlePatternChange = (nextPattern: Pattern) => {
    if (nextPattern === pattern) return;
    setPattern(nextPattern);
    setLoading(true);
    void load(nextPattern);
  };

  const handleItemUpdated = useCallback((updatedItem: InventoryItem) => {
    setInventory((prev) =>
      prev.map((current) =>
        current.productId === updatedItem.productId ? updatedItem : current
      )
    );
  }, []);

  return (
    <div className="max-w-5xl space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Inventory</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Live stock levels from the {pattern} inventory service. Reserved units reflect active sagas and stay read-only.
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            Adjust total stock inline for each product, and change whether each item is shown on the storefront. Inventory always shows the full catalog, including hidden internal items.
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={handleRefresh}
          disabled={globalIsSaving}
          className="gap-1.5 rounded-full text-xs"
        >
          <RefreshCw className={cn("h-3.5 w-3.5", loading && "animate-spin")} />
          Refresh
        </Button>
      </div>

      {error && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
          {error}
        </div>
      )}
      <div className="flex items-center gap-2">
        {(["orchestration", "choreography"] as Pattern[]).map((value) => (
          <Button
            key={value}
            variant={pattern === value ? "default" : "outline"}
            size="sm"
            className="rounded-full text-xs capitalize"
            disabled={globalIsSaving}
            onClick={() => handlePatternChange(value)}
          >
            {value}
          </Button>
        ))}
      </div>

      <div className="overflow-hidden rounded-xl border border-border bg-card">
        {loading ? (
          <div className="space-y-3 p-6">
            {[1, 2, 3, 4, 5, 6].map((i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        ) : error ? (
          <div className="py-20 text-center text-sm text-muted-foreground">
            Live inventory data is unavailable.
          </div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Product</TableHead>
                <TableHead>SKU</TableHead>
                <TableHead>Storefront</TableHead>
                <TableHead className="text-right">Total</TableHead>
                <TableHead className="text-right">Reserved</TableHead>
                <TableHead className="text-right">Available</TableHead>
                <TableHead>Stock level</TableHead>
                <TableHead>Last restocked</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {inventory.map((item) => (
                <InventoryTableRow
                  key={item.productId}
                  item={item}
                  pattern={pattern}
                  onItemUpdated={handleItemUpdated}
                  globalIsSaving={globalIsSaving}
                  setGlobalIsSaving={setGlobalIsSaving}
                />
              ))}
            </TableBody>
          </Table>
        )}
      </div>
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
