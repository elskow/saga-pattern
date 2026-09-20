"use client";

import { useMemo, useState } from "react";
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
const DISPLAY_LIMIT = 300;

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
      const data = await fetchAllOrdersServer({ data: { force: true } });
      setOrders(data);
      setError(null);
    } catch (err) {
      setOrders([]);
      setError(err instanceof Error ? err.message : "Live order listing unavailable");
    } finally {
      setLoading(false);
    }
  };

  const { displayOrders, filteredCount } = useMemo(() => {
    const filtered = orders.filter((o) => {
      if (patternFilter !== "all" && o.pattern !== patternFilter) return false;
      const statuses = STATUS_GROUPS[statusFilter];
      if (statuses && !statuses.includes(o.status)) return false;
      return true;
    });
    const newestFirst = filtered.length > 0 ? [...filtered].reverse() : filtered;
    return {
      filteredCount: filtered.length,
      displayOrders: newestFirst.slice(0, DISPLAY_LIMIT),
    };
  }, [orders, patternFilter, statusFilter]);

  return (
    <div className="max-w-6xl space-y-6">
      <div className="flex flex-col gap-3 border-b border-border pb-5 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-foreground">Orders</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Live order listing across Choreography and Orchestration
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={load} className="gap-1.5 text-xs">
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh
        </Button>
      </div>

      {error && (
        <div className="flex items-start gap-2 rounded-md border border-amber-200/80 bg-amber-50 p-3 text-sm text-amber-800">
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-0.5 rounded-md border border-border bg-card p-0.5">
          {PATTERN_FILTERS.map((p) => (
            <button
              key={p}
              onClick={() => setPatternFilter(p)}
              className={cn(
                "rounded-sm px-3 py-1 text-xs font-medium capitalize transition-colors",
                patternFilter === p
                  ? "bg-secondary text-primary"
                  : "text-muted-foreground hover:text-foreground"
              )}
            >
              {p}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-0.5 rounded-md border border-border bg-card p-0.5">
          {(Object.keys(STATUS_GROUPS) as (keyof typeof STATUS_GROUPS)[]).map((s) => (
            <button
              key={s}
              onClick={() => setStatusFilter(s)}
              className={cn(
                "rounded-sm px-3 py-1 text-xs font-medium capitalize transition-colors",
                statusFilter === s
                  ? "bg-secondary text-primary"
                  : "text-muted-foreground hover:text-foreground"
              )}
            >
              {s}
            </button>
          ))}
        </div>

        <span className="ml-auto text-xs text-muted-foreground">
          {filteredCount > displayOrders.length
            ? `Showing ${displayOrders.length} of ${filteredCount} orders`
            : `${filteredCount} order${filteredCount !== 1 ? "s" : ""}`}
        </span>
      </div>

      <div className="overflow-hidden rounded-md border border-border bg-card">
        {loading ? (
          <div className="space-y-3 p-6">
            {[1, 2, 3, 4].map((i) => (
              <Skeleton key={i} className="h-12 w-full rounded-md" />
            ))}
          </div>
        ) : displayOrders.length === 0 ? (
          <div className="py-12 text-center text-sm text-muted-foreground">
            No orders match the selected filters.
          </div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Order ID
                </TableHead>
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Customer
                </TableHead>
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Pattern
                </TableHead>
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Status
                </TableHead>
                <TableHead className="h-10 px-4 text-right text-xs font-medium text-muted-foreground">
                  Total
                </TableHead>
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Payment
                </TableHead>
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Tracking
                </TableHead>
                <TableHead className="h-10 px-4" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {displayOrders.map((order) => {
                const id = order.id ?? order.orderId;
                return (
                  <TableRow key={id}>
                    <TableCell className="px-4 py-3 font-mono text-xs">{id}</TableCell>
                    <TableCell className="px-4 py-3 text-sm">{order.customerId}</TableCell>
                    <TableCell className="px-4 py-3">
                      <span className="rounded-sm border border-primary/15 bg-secondary px-1.5 py-0.5 text-[11px] font-medium capitalize text-primary">
                        {order.pattern}
                      </span>
                    </TableCell>
                    <TableCell className="px-4 py-3">
                      <OrderStatusBadge status={order.status} />
                    </TableCell>
                    <TableCell className="px-4 py-3 text-right text-sm tabular-nums">
                      {order.totalAmount ? formatPrice(order.totalAmount) : "—"}
                    </TableCell>
                    <TableCell className="px-4 py-3 font-mono text-xs text-muted-foreground">
                      {order.paymentId ? order.paymentId.slice(0, 8) + "…" : "—"}
                    </TableCell>
                    <TableCell className="px-4 py-3 font-mono text-xs text-muted-foreground">
                      {order.shippingId ? order.shippingId.slice(0, 8) + "…" : "—"}
                    </TableCell>
                    <TableCell className="px-4 py-3">
                      <Link to="/orders/$orderId" params={{ orderId: id }} search={{ pattern: order.pattern }}>
                        <Button variant="ghost" size="sm" className="h-7 gap-1 px-2.5 text-xs">
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
