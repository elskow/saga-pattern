"use client";

import { useEffect, useState, useCallback } from "react";
import { getOrderServer, isTerminalStatus } from "@/lib/api";
import { cancelOrderServer } from "@/lib/admin";
import { Order, Pattern } from "@/types";
import { formatPrice } from "@/lib/currency";
import { SagaTimeline } from "@/components/SagaTimeline";
import { OrderStatusBadge } from "@/components/OrderStatusBadge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Separator } from "@/components/ui/separator";
import { ArrowLeft, Loader2 } from "lucide-react";
import { Link } from "@tanstack/react-router";
import { toast } from "sonner";

interface OrderTrackingPageClientProps {
  orderId: string;
  pattern: Pattern;
  initialOrder: Order | null;
  initialError: string | null;
}

function isNotFoundError(message: string): boolean {
  const normalized = message.toLowerCase();
  return normalized.includes("http 404") || normalized.includes("(404)") || normalized.includes("404 - not found") || normalized.includes("order not found");
}

export default function OrderTrackingPageClient({
  orderId,
  pattern,
  initialOrder,
  initialError,
}: OrderTrackingPageClientProps) {
  const initialAwaitingProjection = pattern === "orchestration" && initialOrder == null && initialError != null && isNotFoundError(initialError);
  const [order, setOrder] = useState<Order | null>(initialOrder);
  const [loading, setLoading] = useState(initialOrder == null && initialError == null);
  const [error, setError] = useState<string | null>(initialAwaitingProjection ? null : initialError);
  const [polling, setPolling] = useState(false);
  const [pollCount, setPollCount] = useState(0);
  const [awaitingProjection, setAwaitingProjection] = useState(initialAwaitingProjection);
  const [cancelling, setCancelling] = useState(false);

  const fetchOrder = useCallback(async () => {
    try {
      const data = await getOrderServer({ data: { pattern, orderId } });
      setOrder(data);
      setError(null);
      setAwaitingProjection(false);
      return data;
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to fetch order";
      if (pattern === "orchestration" && isNotFoundError(message)) {
        setAwaitingProjection(true);
        setError(null);
        return null;
      }
      setError(message);
      setAwaitingProjection(false);
      return null;
    }
  }, [pattern, orderId]);

  useEffect(() => {
    if (initialOrder || initialError) {
      return;
    }
    setLoading(true);
    fetchOrder().finally(() => setLoading(false));
  }, [fetchOrder, initialError, initialOrder]);

  const handleCancel = useCallback(async () => {
    setCancelling(true);
    try {
      await cancelOrderServer({ data: { pattern, orderId } });
      toast.success("Order cancelled successfully");
      await fetchOrder();
    } catch (err) {
      toast.error("Failed to cancel order: " + (err instanceof Error ? err.message : String(err)));
    } finally {
      setCancelling(false);
    }
  }, [fetchOrder, orderId, pattern]);

  useEffect(() => {
    if ((!order && !awaitingProjection) || (order && isTerminalStatus(order.status))) return;

    let active = true;
    let inFlight = false;
    setPolling(true);

    const interval = setInterval(async () => {
      if (!active || inFlight) {
        return;
      }

      inFlight = true;
      setPollCount((c) => c + 1);
      try {
        const updated = await fetchOrder();
        if (updated && isTerminalStatus(updated.status)) {
          clearInterval(interval);
          setPolling(false);
        } else if (updated) {
          setAwaitingProjection(false);
        }
      } finally {
        inFlight = false;
      }
    }, 1500);

    return () => {
      active = false;
      clearInterval(interval);
      setPolling(false);
    };
  }, [order?.status, awaitingProjection, fetchOrder]);

  if (loading) {
    return (
      <div className="mx-auto w-full max-w-7xl space-y-8">
        <Skeleton className="h-9 w-56 rounded-md" />
        <div className="grid grid-cols-1 gap-8 lg:grid-cols-5">
          <div className="space-y-3 lg:col-span-3">
            {[1, 2, 3, 4].map((i) => <Skeleton key={i} className="h-16 rounded-md" />)}
          </div>
          <div className="lg:col-span-2">
            <Skeleton className="h-64 rounded-md" />
          </div>
        </div>
      </div>
    );
  }

  if (error || (!order && !awaitingProjection)) {
    return (
      <div className="mx-auto flex max-w-7xl flex-col items-center justify-center gap-3 rounded-md border border-dashed border-destructive/30 bg-destructive/5 py-24 text-center">
        <div>
          <h1 className="text-base font-semibold tracking-tight text-foreground">Order not found</h1>
          <p className="mx-auto mt-1 max-w-sm text-sm text-muted-foreground">
            {error ?? "Order missing or backend unreachable."}
          </p>
        </div>
        <div className="mt-1 flex gap-2">
          <Button
            variant="outline"
            className="rounded-md"
            onClick={() => { setLoading(true); fetchOrder().finally(() => setLoading(false)); }}
          >
            Retry
          </Button>
          <Link to="/orders">
            <Button className="rounded-md">
              Back to orders
            </Button>
          </Link>
        </div>
      </div>
    );
  }

  if (!order && awaitingProjection) {
    return (
      <div className="mx-auto w-full max-w-7xl space-y-8">
        <div className="mb-6 mt-2 flex items-center gap-3 border-b border-border pb-5">
          <Link to="/orders">
            <Button variant="outline" size="icon" className="h-9 w-9 shrink-0 rounded-md border-border">
              <ArrowLeft className="h-4 w-4 text-muted-foreground" />
            </Button>
          </Link>
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <h1 className="text-xl font-semibold tracking-tight text-foreground">Order tracking</h1>
              <span className="inline-flex items-center gap-1.5 rounded-sm border border-amber-200 bg-amber-50 px-2 py-0.5 text-xs text-amber-800">
                <Loader2 className="h-3 w-3 animate-spin" />
                Awaiting projection
              </span>
            </div>
            <p className="mt-0.5 font-mono text-sm text-muted-foreground">#{orderId}</p>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-8 lg:grid-cols-5">
          <div className="space-y-3 lg:col-span-3">
            {[1, 2, 3, 4].map((i) => <Skeleton key={i} className="h-16 rounded-md" />)}
          </div>
          <div className="lg:col-span-2">
            <Skeleton className="h-64 rounded-md" />
          </div>
        </div>
      </div>
    );
  }

  const resolvedOrder = order!;
  const isComplete = isTerminalStatus(resolvedOrder.status);
  const canCancel = !isComplete && !resolvedOrder.trackingNumber;

  return (
    <div className="mx-auto w-full max-w-7xl space-y-8 pb-12">
      <div className="mb-6 mt-2 flex flex-col justify-between gap-4 border-b border-border pb-5 sm:flex-row sm:items-end">
        <div className="flex items-start gap-3 sm:items-center">
          <Link to="/orders">
            <Button
              variant="outline"
              size="icon"
              className="mt-0.5 h-9 w-9 shrink-0 rounded-md border-border sm:mt-0"
            >
              <ArrowLeft className="h-4 w-4 text-muted-foreground" />
            </Button>
          </Link>
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <h1 className="text-xl font-semibold tracking-tight text-foreground">Order tracking</h1>
              <OrderStatusBadge status={resolvedOrder.status} />
              {polling && (
                <span className="inline-flex items-center gap-1.5 rounded-sm border border-border bg-muted/40 px-2 py-0.5 text-[11px] text-muted-foreground">
                  <Loader2 className="h-3 w-3 animate-spin text-primary" />
                  Live ({pollCount})
                </span>
              )}
            </div>
            <p className="mt-0.5 font-mono text-sm text-muted-foreground">
              #{resolvedOrder.id || resolvedOrder.orderId}
            </p>
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          {canCancel && (
            <Button
              variant="destructive"
              onClick={handleCancel}
              disabled={cancelling}
              className="h-9 rounded-md text-xs font-medium"
            >
              {cancelling ? (
                <>
                  <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
                  Cancelling…
                </>
              ) : (
                "Cancel order"
              )}
            </Button>
          )}
          <Button
            variant="outline"
            onClick={() => fetchOrder()}
            className="h-9 rounded-md text-xs font-medium"
          >
            Refresh
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-8 lg:grid-cols-5 lg:gap-10">
        <div className="lg:col-span-3">
          <div className="rounded-md border border-border bg-card p-5 sm:p-6">
            <h2 className="mb-6 text-base font-semibold tracking-tight text-foreground">Fulfillment progress</h2>
            <SagaTimeline order={resolvedOrder} />
          </div>
        </div>

        <aside className="space-y-4 lg:col-span-2">
          <div className="space-y-4 rounded-md border border-border bg-card p-5">
            <h2 className="text-base font-semibold tracking-tight text-foreground">Order details</h2>

            <div className="space-y-3 text-sm">
              {[
                { label: "Customer", value: resolvedOrder.customerId, mono: true },
                { label: "Shipping address", value: resolvedOrder.shippingAddress },
                ...(resolvedOrder.totalAmount
                  ? [{ label: "Total amount", value: formatPrice(resolvedOrder.totalAmount) }]
                  : []),
                ...(resolvedOrder.paymentId
                  ? [{ label: "Payment ID", value: resolvedOrder.paymentId, mono: true }]
                  : []),
                ...(resolvedOrder.reservationId
                  ? [{ label: "Inventory reservation", value: resolvedOrder.reservationId, mono: true }]
                  : []),
                ...(resolvedOrder.trackingNumber
                  ? [{ label: "Tracking number", value: resolvedOrder.trackingNumber, mono: true }]
                  : resolvedOrder.shippingId
                    ? [{ label: "Shipment ID", value: resolvedOrder.shippingId, mono: true }]
                  : []),
              ].map(({ label, value, mono }) => (
                <div key={label}>
                  <p className="text-xs text-muted-foreground">{label}</p>
                  <p className={`mt-0.5 break-all text-foreground ${mono ? "inline-block font-mono text-sm" : "text-sm font-medium"}`}>
                    {value}
                  </p>
                </div>
              ))}
            </div>

            {resolvedOrder.items && resolvedOrder.items.length > 0 && (
              <>
                <Separator />
                <div className="space-y-2">
                  <p className="text-xs text-muted-foreground">Purchased items</p>
                  {resolvedOrder.items.map((item, i) => (
                    <div key={i} className="flex items-center justify-between text-sm">
                      <span className="font-medium text-foreground">{item.productName} <span className="ml-1 text-muted-foreground">× {item.quantity}</span></span>
                      <span className="tabular-nums font-medium">{formatPrice(item.price * item.quantity)}</span>
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>

          <div className="space-y-2 rounded-md border border-border bg-muted/30 p-4">
            <h3 className="text-xs font-medium text-muted-foreground">
              Order processing
            </h3>
            <p className="text-sm leading-relaxed text-muted-foreground">
              {pattern === "choreography" ? (
                <><strong className="font-medium text-foreground">Choreography</strong> — services react to events in parallel for faster fulfillment.</>
              ) : (
                <><strong className="font-medium text-foreground">Orchestration</strong> — a central controller drives each step for tighter control.</>
              )}
            </p>
          </div>

          {isComplete && resolvedOrder.status === "COMPLETED" && (
            <div className="space-y-2 pt-1">
              {(resolvedOrder.trackingNumber || resolvedOrder.shippingId) && (
                <Link
                  to="/tracking/$trackingId"
                  params={{ trackingId: resolvedOrder.trackingNumber || resolvedOrder.shippingId || "" }}
                  search={{ pattern: resolvedOrder.pattern, orderId: resolvedOrder.id || resolvedOrder.orderId, shipmentId: resolvedOrder.shippingId ?? null }}
                >
                  <Button variant="outline" className="mb-2 h-10 w-full rounded-md text-sm font-medium">
                    Track shipment
                  </Button>
                </Link>
              )}
              <Link to="/">
                <Button className="h-10 w-full rounded-md bg-primary text-sm font-medium text-primary-foreground hover:bg-primary/90">
                  Continue shopping
                </Button>
              </Link>
            </div>
          )}
        </aside>
      </div>
    </div>
  );
}
