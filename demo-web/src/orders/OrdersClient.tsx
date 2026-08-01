"use client";

import { useState, useEffect } from "react";
import { fetchAllOrdersServer } from "@/lib/admin";
import { Order } from "@/types";
import { formatPrice } from "@/lib/currency";
import { isTerminalStatus } from "@/lib/api";
import { OrderStatusBadge } from "@/components/OrderStatusBadge";
import { Button } from "@/components/ui/button";
import { Link } from "@tanstack/react-router";
import { useAuthStore } from "@/lib/store";
import { cancelOrderServer } from "@/lib/admin";
import { toast } from "sonner";
import { cn } from "@/lib/utils";
import type { Pattern } from "@/types";

interface OrdersClientProps {
  initialOrders: Order[];
  initialError: string | null;
}

export default function OrdersClient({ initialOrders, initialError }: OrdersClientProps) {
  const user = useAuthStore((s) => s.user);
  
  const [orders, setOrders] = useState<Order[]>(() => 
    user ? initialOrders.filter(o => o.customerId === user.username) : []
  );
  const [error, setError] = useState<string | null>(initialError);
  const [loading, setLoading] = useState(false);
  const [cancellingOrderId, setCancellingOrderId] = useState<string | null>(null);

  // Refilter if user changes (e.g. hydration or logout)
  useEffect(() => {
    if (initialOrders.length > 0) {
      setOrders(user ? initialOrders.filter(o => o.customerId === user.username) : []);
    }
  }, [user, initialOrders]);

  const load = async () => {
    setLoading(true);
    try {
      const data = await fetchAllOrdersServer();
      const filtered = user ? data.filter(o => o.customerId === user.username) : [];
      setOrders(filtered);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Live order listing is unavailable");
    } finally {
      setLoading(false);
    }
  };

  const handleCancel = async (orderId: string, pattern: Pattern) => {
    setCancellingOrderId(orderId);
    try {
      await cancelOrderServer({ data: { pattern, orderId } });
      toast.success("Order cancelled successfully");
      await load();
    } catch (err) {
      toast.error("Failed to cancel order: " + (err instanceof Error ? err.message : String(err)));
    } finally {
      setCancellingOrderId(null);
    }
  };

  return (
    <div className="mx-auto w-full max-w-7xl space-y-8 py-8">
      <div className="flex flex-col justify-between gap-4 border-b border-border pb-5 sm:flex-row sm:items-end">
        <div>
          <h1 className="text-xl font-semibold tracking-tight text-foreground">Orders</h1>
          <p className="mt-0.5 text-sm text-muted-foreground">Track purchases and open order detail.</p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={load}
          className="rounded-md text-xs font-medium"
        >
          Refresh
        </Button>
      </div>

      {loading ? (
        <div className="flex flex-col items-center justify-center gap-2 rounded-md border border-dashed border-border bg-card py-24 text-center">
          <p className="text-sm font-medium text-foreground">Loading orders…</p>
        </div>
      ) : error ? (
        <div className="flex flex-col items-center justify-center gap-3 rounded-md border border-dashed border-red-200 bg-red-50/40 py-24 text-center">
          <div>
            <p className="text-sm font-medium text-red-900">Order listing unavailable</p>
            <p className="mx-auto mt-1 max-w-md text-sm text-red-700/80">{error}</p>
          </div>
          <Link to="/">
            <Button size="sm" variant="outline" className="mt-1 rounded-md">
              Return to shop
            </Button>
          </Link>
        </div>
      ) : !user ? (
        <div className="flex flex-col items-center justify-center gap-3 rounded-md border border-dashed border-border bg-card py-24 text-center">
          <div>
            <p className="text-sm font-medium text-foreground">Sign in to view orders</p>
            <p className="mx-auto mt-1 max-w-sm text-sm text-muted-foreground">
              Order history is tied to your account.
            </p>
          </div>
          <Link to="/login">
            <Button size="sm" className="mt-1 rounded-md">
              Sign in
            </Button>
          </Link>
        </div>
      ) : orders.length === 0 ? (
        <div className="flex flex-col items-center justify-center gap-3 rounded-md border border-dashed border-border bg-card py-24 text-center">
          <div>
            <p className="text-sm font-medium text-foreground">No orders yet</p>
            <p className="mx-auto mt-1 max-w-sm text-sm text-muted-foreground">
              Once you place an order, it will appear here.
            </p>
          </div>
          <Link to="/">
            <Button size="sm" variant="outline" className="mt-1 rounded-md">
              Browse shop
            </Button>
          </Link>
        </div>
      ) : (
        <div className="divide-y divide-border overflow-hidden rounded-md border border-border bg-card">
          {[...orders].reverse().map((order) => {
            const id = order.id || order.orderId || "unknown";
            const itemCount = order.items?.reduce((s, i) => s + i.quantity, 0) ?? 0;
            const total = order.totalAmount ?? order.items?.reduce((s, i) => s + i.price * i.quantity, 0) ?? 0;
            const canCancel = !isTerminalStatus(order.status) && !order.trackingNumber;
            const isCancelling = cancellingOrderId === id;

            return (
              <div
                key={id}
                className="flex items-center gap-4 px-4 py-4 transition-colors hover:bg-muted/30 sm:px-5"
              >
                <div className="min-w-0 flex-1">
                  <div className="mb-1 flex flex-wrap items-center gap-2">
                    <p className="truncate font-mono text-sm font-medium text-foreground">#{id}</p>
                    <OrderStatusBadge status={order.status} />
                    <span className={cn(
                      "rounded-sm border px-1.5 py-0.5 text-[11px]",
                      order.pattern === "choreography"
                        ? "border-primary/20 bg-secondary text-primary"
                        : "border-border bg-muted text-foreground"
                    )}>
                      {order.pattern === "choreography" ? "Choreography" : "Orchestration"}
                    </span>
                  </div>
                  <p className="text-sm text-muted-foreground">
                    {itemCount} item{itemCount !== 1 ? "s" : ""} · {formatPrice(total)} ·{" "}
                    {new Date(order.createdAt).toLocaleDateString(undefined, {
                      month: "short",
                      day: "numeric",
                      hour: "2-digit",
                      minute: "2-digit",
                    })}
                  </p>
                  <div className="mt-1.5 flex items-center gap-1">
                    {canCancel && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => handleCancel(id, order.pattern)}
                        disabled={isCancelling}
                        className="h-8 rounded-md px-2 text-xs font-medium text-destructive hover:bg-destructive/10"
                      >
                        {isCancelling ? "Cancelling…" : "Cancel"}
                      </Button>
                    )}
                    <Link to="/orders/$orderId" params={{ orderId: id }} search={{ pattern: order.pattern }}>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-8 rounded-md px-2 text-xs font-medium text-muted-foreground hover:bg-muted/50 hover:text-foreground"
                      >
                        View order
                      </Button>
                    </Link>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
