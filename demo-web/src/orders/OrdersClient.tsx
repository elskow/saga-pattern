"use client";

import { useState } from "react";
import { fetchAllOrdersServer } from "@/lib/admin";
import { Order } from "@/types";
import { formatPrice } from "@/lib/currency";
import { OrderStatusBadge } from "@/components/OrderStatusBadge";
import { Button } from "@/components/ui/button";
import { Package, ArrowRight, ShoppingBag, RefreshCw } from "lucide-react";
import { Link } from "@tanstack/react-router";

interface OrdersClientProps {
  initialOrders: Order[];
  initialError: string | null;
}

export default function OrdersClient({ initialOrders, initialError }: OrdersClientProps) {
  const [orders, setOrders] = useState<Order[]>(initialOrders);
  const [error, setError] = useState<string | null>(initialError);
  const [loading, setLoading] = useState(false);

  const load = async () => {
    setLoading(true);
    try {
      const data = await fetchAllOrdersServer();
      setOrders(data);
      setError(null);
    } catch (err) {
      setOrders([]);
      setError(err instanceof Error ? err.message : "Live order listing is unavailable");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-7xl mx-auto space-y-8 w-full py-10">
      <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4 border-b border-border/60 pb-5">
        <h1 className="text-3xl font-bold tracking-tight text-foreground">Order History</h1>
        <Button
          variant="outline"
          size="sm"
          onClick={load}
          className="gap-2 text-xs rounded-full font-medium shadow-sm hover:bg-muted/50"
        >
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh Data
        </Button>
      </div>

      {loading ? (
        <div className="flex flex-col items-center justify-center py-32 gap-5 text-center rounded-2xl border border-dashed border-border bg-card/50">
          <div className="h-14 w-14 rounded-full bg-muted/50 flex items-center justify-center mb-2 animate-pulse">
            <ShoppingBag className="h-6 w-6 text-muted-foreground/40" />
          </div>
          <div>
            <p className="text-lg font-semibold tracking-tight text-foreground">Loading orders...</p>
          </div>
        </div>
      ) : error ? (
        <div className="flex flex-col items-center justify-center py-32 gap-5 text-center rounded-2xl border border-dashed border-red-200 bg-red-50/30">
          <div className="h-14 w-14 rounded-full bg-red-100 flex items-center justify-center mb-2">
            <span className="text-red-600 font-bold text-2xl">!</span>
          </div>
          <div>
            <p className="text-lg font-semibold tracking-tight text-red-900">Order listing unavailable</p>
            <p className="text-sm text-red-600/80 mt-1 max-w-md mx-auto">{error}</p>
          </div>
          <Link to="/">
            <Button size="sm" className="mt-2 gap-2 rounded-full shadow-sm">
              Return to shop
              <ArrowRight className="h-3.5 w-3.5" />
            </Button>
          </Link>
        </div>
      ) : orders.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-32 gap-5 text-center rounded-2xl border border-dashed border-border bg-card/50">
          <div className="h-16 w-16 rounded-full border border-border/60 bg-muted/30 flex items-center justify-center mb-2 shadow-sm">
            <ShoppingBag className="h-7 w-7 text-muted-foreground/50" />
          </div>
          <div>
            <p className="text-lg font-semibold tracking-tight text-foreground">No orders placed yet</p>
            <p className="text-sm text-muted-foreground mt-1 max-w-sm mx-auto">
              Once you place an order using the orchestration backend, it will appear here.
            </p>
          </div>
          <Link to="/">
            <Button size="sm" className="mt-2 gap-2 rounded-full shadow-sm group">
              Browse collection
              <ArrowRight className="h-4 w-4 transition-transform group-hover:translate-x-1" />
            </Button>
          </Link>
        </div>
      ) : (
        <div className="rounded-2xl border border-border bg-card overflow-hidden divide-y divide-border shadow-sm">
          {[...orders].reverse().map((order) => {
            const id = order.id || order.orderId || "unknown";
            const itemCount = order.items?.reduce((s, i) => s + i.quantity, 0) ?? 0;
            const total = order.totalAmount ?? order.items?.reduce((s, i) => s + i.price * i.quantity, 0) ?? 0;

            return (
              <div
                key={id}
                className="flex items-center gap-5 px-6 py-5 hover:bg-muted/30 transition-colors"
              >
                <div className="hidden sm:flex h-12 w-12 shrink-0 items-center justify-center rounded-full border border-border/60 bg-muted/30">
                  <Package className="h-5 w-5 text-muted-foreground" />
                </div>

                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-3 flex-wrap mb-1.5">
                    <p className="text-sm font-semibold font-mono text-foreground truncate">#{id}</p>
                    <OrderStatusBadge status={order.status} />
                    <span className="text-[10px] font-bold text-muted-foreground uppercase tracking-wider border border-border bg-muted/40 rounded-md px-2 py-0.5">
                      {order.pattern}
                    </span>
                  </div>
                  <p className="text-sm text-muted-foreground font-medium">
                    {itemCount} item{itemCount !== 1 ? "s" : ""} · {formatPrice(total)} ·{" "}
                    {new Date(order.createdAt).toLocaleDateString(undefined, {
                      month: "short",
                      day: "numeric",
                      hour: "2-digit",
                      minute: "2-digit",
                    })}
                  </p>
                </div>

                <Link to={`/orders/${id}?pattern=${order.pattern}`}>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="gap-1.5 text-xs rounded-full font-medium text-muted-foreground hover:text-foreground hover:bg-muted/50"
                  >
                    View saga
                    <ArrowRight className="h-3.5 w-3.5" />
                  </Button>
                </Link>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
