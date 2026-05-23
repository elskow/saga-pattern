"use client";

import { useState } from "react";
import { fetchAllOrdersServer } from "@/lib/admin";
import { Order, Pattern } from "@/types";
import { formatPrice } from "@/lib/currency";
import { OrderStatusBadge } from "@/components/OrderStatusBadge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { RefreshCw, ArrowRight, AlertCircle } from "lucide-react";
import { Link } from "@tanstack/react-router";
import { cn } from "@/lib/utils";

const PATTERN_FILTERS: (Pattern | "all")[] = ["all", "choreography", "orchestration"];
const STATUS_GROUPS = {
  all: null,
  active: ["PENDING", "SAGA_STARTED", "PAYMENT_PROCESSING", "INVENTORY_RESERVING", "SHIPPING_SCHEDULING"],
  completed: ["COMPLETED"],
  failed: ["FAILED", "PAYMENT_FAILED", "INVENTORY_FAILED", "SHIPPING_FAILED", "COMPENSATED", "COMPENSATING", "CANCELLED"],
};

interface AdminOrdersClientProps {
  initialOrders: Order[];
  initialError: string | null;
}

export default function AdminOrdersClient({ initialOrders, initialError }: AdminOrdersClientProps) {
  const [orders, setOrders] = useState<Order[]>(initialOrders);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(initialError);
  const [patternFilter, setPatternFilter] = useState<Pattern | "all">("all");
  const [statusFilter, setStatusFilter] = useState<keyof typeof STATUS_GROUPS>("all");

  const load = async () => {
    setLoading(true);
    try {
      const data = await fetchAllOrdersServer();
      setOrders(data);
      setError(null);
    } catch (err) {
      setOrders([]);
      setError(err instanceof Error ? err.message : "Live order listing unavailable");
    } finally {
      setLoading(false);
    }
  };

  const filtered = orders.filter((o) => {
    if (patternFilter !== "all" && o.pattern !== patternFilter) return false;
    const statuses = STATUS_GROUPS[statusFilter];
    if (statuses && !statuses.includes(o.status)) return false;
    return true;
  });

  return (
    <div className="space-y-6 max-w-6xl">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Orders</h1>
          <p className="text-sm text-muted-foreground mt-1">Live order listing across choreography and orchestration</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={load} className="gap-1.5 rounded-full text-xs">
            <RefreshCw className="h-3.5 w-3.5" />
            Refresh
          </Button>
        </div>
      </div>

      {error && (
        <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
          <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-0.5 rounded-full border border-border p-0.5">
          {PATTERN_FILTERS.map((p) => (
            <button
              key={p}
              onClick={() => setPatternFilter(p)}
              className={cn(
                "rounded-full px-3 py-1 text-xs font-medium transition-colors capitalize",
                patternFilter === p
                  ? "bg-foreground text-background"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              {p}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-0.5 rounded-full border border-border p-0.5">
          {(Object.keys(STATUS_GROUPS) as (keyof typeof STATUS_GROUPS)[]).map((s) => (
            <button
              key={s}
              onClick={() => setStatusFilter(s)}
              className={cn(
                "rounded-full px-3 py-1 text-xs font-medium transition-colors capitalize",
                statusFilter === s
                  ? "bg-foreground text-background"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              {s}
            </button>
          ))}
        </div>

        <span className="text-xs text-muted-foreground ml-auto">
          {filtered.length} order{filtered.length !== 1 ? "s" : ""}
        </span>
      </div>

      <div className="rounded-xl border border-border bg-card overflow-hidden">
        {loading ? (
          <div className="p-6 space-y-3">
            {[1, 2, 3, 4].map((i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : filtered.length === 0 ? (
          <div className="py-16 text-center text-sm text-muted-foreground">
            No orders match the selected filters.
          </div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Order ID</TableHead>
                <TableHead>Customer</TableHead>
                <TableHead>Pattern</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Total</TableHead>
                <TableHead>Payment</TableHead>
                <TableHead>Tracking</TableHead>
                <TableHead />
              </TableRow>
            </TableHeader>
            <TableBody>
              {[...filtered].reverse().map((order) => {
                const id = order.id ?? order.orderId;
                return (
                  <TableRow key={id}>
                    <TableCell className="font-mono text-xs">{id}</TableCell>
                    <TableCell className="text-sm">{order.customerId}</TableCell>
                    <TableCell>
                      <span className="capitalize text-xs border border-border rounded-full px-2 py-0.5 text-muted-foreground">
                        {order.pattern}
                      </span>
                    </TableCell>
                    <TableCell>
                      <OrderStatusBadge status={order.status} />
                    </TableCell>
                    <TableCell className="text-right text-sm">
                      {order.totalAmount ? formatPrice(order.totalAmount) : "—"}
                    </TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">
                      {order.paymentId ? order.paymentId.slice(0, 8) + "…" : "—"}
                    </TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">
                      {order.shippingId ? order.shippingId.slice(0, 8) + "…" : "—"}
                    </TableCell>
                    <TableCell>
                      <Link to={`/orders/${id}?pattern=${order.pattern}`}>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="rounded-full h-7 px-2.5 gap-1 text-xs"
                        >
                          Saga <ArrowRight className="h-3 w-3" />
                        </Button>
                      </Link>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        )}
      </div>
    </div>
  );
}
