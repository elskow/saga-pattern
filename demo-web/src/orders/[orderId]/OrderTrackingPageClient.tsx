"use client";

import { useEffect, useState, useCallback } from "react";
import { getOrderServer, isTerminalStatus } from "@/lib/api";
import { Order, Pattern } from "@/types";
import { formatPrice } from "@/lib/currency";
import { SagaTimeline } from "@/components/SagaTimeline";
import { OrderStatusBadge } from "@/components/OrderStatusBadge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Separator } from "@/components/ui/separator";
import { ArrowLeft, ArrowRight, RefreshCw, Loader2, Info } from "lucide-react";
import { Link } from "@tanstack/react-router";

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
      <div className="max-w-7xl mx-auto space-y-8 w-full">
        <Skeleton className="h-10 w-64 rounded-xl" />
        <div className="grid grid-cols-1 lg:grid-cols-5 gap-10">
          <div className="lg:col-span-3 space-y-4">
            {[1, 2, 3, 4].map((i) => <Skeleton key={i} className="h-20 rounded-[1.5rem]" />)}
          </div>
          <div className="lg:col-span-2">
            <Skeleton className="h-72 rounded-[1.5rem]" />
          </div>
        </div>
      </div>
    );
  }

  if (error || (!order && !awaitingProjection)) {
    return (
      <div className="flex flex-col items-center justify-center py-32 gap-5 text-center rounded-[2rem] border border-dashed border-red-200 bg-red-50/30 max-w-7xl mx-auto">
        <div className="h-16 w-16 rounded-full bg-red-100 flex items-center justify-center mb-2 shadow-sm">
          <span className="text-red-600 font-bold text-3xl">!</span>
        </div>
        <div>
          <h1 className="text-xl font-bold tracking-tight text-red-900">Order not found</h1>
          <p className="text-sm text-red-600/80 mt-1 max-w-sm mx-auto">
            {error ?? "The backend may not be running on the expected port or the order doesn't exist."}
          </p>
        </div>
        <div className="flex gap-3 mt-2">
          <Button
            variant="outline"
            className="rounded-full gap-2 shadow-sm border-red-200 text-red-700 hover:bg-red-50"
            onClick={() => { setLoading(true); fetchOrder().finally(() => setLoading(false)); }}
          >
            <RefreshCw className="h-4 w-4" />
            Retry Connection
          </Button>
          <Link to="/orders">
            <Button className="rounded-full gap-2 shadow-sm">
              <ArrowLeft className="h-4 w-4" />
              Return to Orders
            </Button>
          </Link>
        </div>
      </div>
    );
  }

  if (!order && awaitingProjection) {
    return (
      <div className="max-w-7xl mx-auto space-y-8 w-full">
        <div className="flex items-center gap-4 border-b border-border/60 pb-6 mb-8 mt-2">
          <Link to="/orders">
            <Button variant="outline" size="icon" className="h-10 w-10 rounded-full border-border/60 hover:bg-muted/50 transition-colors shrink-0">
              <ArrowLeft className="h-4 w-4 text-muted-foreground" />
            </Button>
          </Link>
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-3xl font-bold tracking-tight text-foreground">Order Tracking</h1>
              <span className="flex items-center gap-1.5 text-xs font-semibold bg-amber-500/10 text-amber-600 dark:text-amber-400 border border-amber-500/20 px-2.5 py-1 rounded-full">
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                Awaiting final projection
              </span>
            </div>
            <p className="text-sm text-muted-foreground font-mono mt-1">#{orderId}</p>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-5 gap-10">
          <div className="lg:col-span-3 space-y-4">
            {[1, 2, 3, 4].map((i) => <Skeleton key={i} className="h-20 rounded-[1.5rem]" />)}
          </div>
          <div className="lg:col-span-2">
            <Skeleton className="h-72 rounded-[1.5rem]" />
          </div>
        </div>
      </div>
    );
  }

  const resolvedOrder = order!;
  const isComplete = isTerminalStatus(resolvedOrder.status);

  return (
    <div className="max-w-7xl mx-auto space-y-8 w-full pb-12">
      <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4 border-b border-border/60 pb-6 mb-8 mt-2">
        <div className="flex items-start sm:items-center gap-4">
          <Link to="/orders">
            <Button
              variant="outline"
              size="icon"
              className="h-10 w-10 mt-1 sm:mt-0 rounded-full border-border/60 hover:bg-muted/50 transition-colors shrink-0"
            >
              <ArrowLeft className="h-4 w-4 text-muted-foreground" />
            </Button>
          </Link>
          <div>
            <div className="flex items-center gap-3 flex-wrap">
              <h1 className="text-3xl font-bold tracking-tight text-foreground">Order Tracking</h1>
              <OrderStatusBadge status={resolvedOrder.status} />
              {polling && (
                <span className="flex items-center gap-1.5 text-[11px] font-bold uppercase tracking-wider text-muted-foreground bg-muted/50 border border-border/50 px-2.5 py-1 rounded-full">
                  <Loader2 className="h-3 w-3 animate-spin text-primary" />
                  Live Polling ({pollCount})
                </span>
              )}
            </div>
            <p className="text-sm text-muted-foreground font-mono mt-1.5">
              #{resolvedOrder.id || resolvedOrder.orderId}
            </p>
          </div>
        </div>

        <Button
          variant="outline"
          onClick={() => fetchOrder()}
          className="gap-2 rounded-full text-xs font-semibold shadow-sm hover:bg-muted/50"
        >
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh Status
        </Button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-10 lg:gap-12">
        <div className="lg:col-span-3">
          <div className="rounded-[1.5rem] border border-border/50 bg-card/40 backdrop-blur-xl p-7 sm:p-9 shadow-xl shadow-muted/20">
            <div className="flex items-center gap-2 mb-8">
              <h2 className="text-lg font-bold tracking-tight text-foreground">Saga Execution</h2>
            </div>
            <SagaTimeline order={resolvedOrder} />
          </div>
        </div>

        <aside className="lg:col-span-2 space-y-6">
          <div className="rounded-[1.5rem] border border-border/50 bg-card/40 backdrop-blur-xl p-7 shadow-xl shadow-muted/20 space-y-5">
            <h2 className="text-lg font-bold tracking-tight text-foreground">Order Details</h2>

            <div className="space-y-4 text-sm">
              {[
                { label: "Customer", value: resolvedOrder.customerId, mono: true },
                { label: "Shipping Address", value: resolvedOrder.shippingAddress },
                ...(resolvedOrder.totalAmount
                  ? [{ label: "Total Amount", value: formatPrice(resolvedOrder.totalAmount) }]
                  : []),
                ...(resolvedOrder.paymentId
                  ? [{ label: "Payment ID", value: resolvedOrder.paymentId, mono: true }]
                  : []),
                ...(resolvedOrder.reservationId
                  ? [{ label: "Inventory Reservation", value: resolvedOrder.reservationId, mono: true }]
                  : []),
                ...(resolvedOrder.trackingNumber
                  ? [{ label: "Tracking Number", value: resolvedOrder.trackingNumber, mono: true }]
                  : resolvedOrder.shippingId
                    ? [{ label: "Shipment ID", value: resolvedOrder.shippingId, mono: true }]
                  : []),
              ].map(({ label, value, mono }) => (
                <div key={label}>
                  <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">{label}</p>
                  <p className={`mt-1 text-foreground break-all ${mono ? "font-mono text-sm font-medium bg-muted/40 p-1.5 rounded-md inline-block border border-border/50" : "text-sm font-medium"}`}>
                    {value}
                  </p>
                </div>
              ))}
            </div>

            {resolvedOrder.items && resolvedOrder.items.length > 0 && (
              <>
                <Separator className="bg-border/60" />
                <div className="space-y-2.5">
                  <p className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest mb-1">Purchased Items</p>
                  {resolvedOrder.items.map((item, i) => (
                    <div key={i} className="flex justify-between items-center text-sm">
                      <span className="font-medium text-foreground">{item.productName} <span className="text-muted-foreground ml-1">× {item.quantity}</span></span>
                      <span className="font-bold tabular-nums">{formatPrice(item.price * item.quantity)}</span>
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>

          <div className="rounded-2xl border border-border/60 bg-muted/20 p-6 space-y-2.5">
            <h3 className="flex items-center gap-2 text-[10px] font-bold text-muted-foreground uppercase tracking-widest">
              <Info className="h-3.5 w-3.5" />
              Order Processing System
            </h3>
            <p className="text-sm text-muted-foreground leading-relaxed">
              {pattern === "choreography" ? (
                <><strong className="font-bold text-foreground">Choreography</strong> — Our distributed
                  warehouse systems process your order in parallel to ensure lightning-fast fulfillment.</>
              ) : (
                <><strong className="font-bold text-foreground">Orchestration</strong> — A central
                  dispatch controller oversees your entire order journey for maximum precision and reliability.</>
              )}
            </p>
          </div>

          {isComplete && resolvedOrder.status === "COMPLETED" && (
            <div className="space-y-3 pt-2">
              {(resolvedOrder.trackingNumber || resolvedOrder.shippingId) && (
                <Link to={`/tracking/${resolvedOrder.trackingNumber || resolvedOrder.shippingId}?pattern=${resolvedOrder.pattern}&orderId=${resolvedOrder.id || resolvedOrder.orderId}${resolvedOrder.shippingId ? `&shipmentId=${resolvedOrder.shippingId}` : ""}`}>
                  <Button variant="outline" className="w-full h-11 rounded-full gap-2 text-sm font-semibold shadow-sm hover:bg-muted/50 transition-all mb-2">
                    Track Shipment Transit
                    <ArrowRight className="h-4 w-4" />
                  </Button>
                </Link>
              )}
              <Link to="/">
                <Button className="w-full h-12 rounded-full gap-2 bg-foreground text-background hover:bg-foreground/90 text-sm font-bold shadow-md transition-all hover:scale-[1.02]">
                  Continue Shopping
                  <ArrowRight className="h-4 w-4" />
                </Button>
              </Link>
            </div>
          )}
        </aside>
      </div>
    </div>
  );
}
