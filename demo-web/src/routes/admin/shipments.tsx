import { createFileRoute } from '@tanstack/react-router'
import { Suspense, useMemo, useState } from "react";
import { fetchShipmentsServer, updateShipmentStatusServer } from "@/lib/admin";
import { Shipment, ShipmentStatus } from "@/types";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { RefreshCw } from "lucide-react";
import { Link } from "@tanstack/react-router";
import { cn } from "@/lib/utils";

export const Route = createFileRoute('/admin/shipments')({
  component: AdminShipmentsPage,
  loader: async () => {
    try {
      const initialShipments = await fetchShipmentsServer();
      return { initialShipments, initialError: null };
    } catch (error) {
      return {
        initialShipments: [],
        initialError: error instanceof Error ? error.message : "Live shipment data unavailable",
      };
    }
  },
})

const STATUS_OPTIONS: Array<ShipmentStatus | "all"> = [
  "all",
  "LABEL_CREATED",
  "PICKED_UP",
  "IN_TRANSIT",
  "OUT_FOR_DELIVERY",
  "DELIVERED",
  "FAILED",
];

function ShipmentStatusSelect({ shipment, onStatusChange }: { shipment: Shipment, onStatusChange: (s: ShipmentStatus) => void }) {
  const [loading, setLoading] = useState(false);

  const handleChange = async (e: React.ChangeEvent<HTMLSelectElement>) => {
    const newStatus = e.target.value as ShipmentStatus;
    setLoading(true);
    try {
      await updateShipmentStatusServer({ data: { pattern: shipment.pattern, shippingId: shipment.shipmentId, status: newStatus } });
      onStatusChange(newStatus);
    } catch (err) {
      console.error("Failed to update status", err);
      alert("Failed to update status");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="relative inline-block min-w-[180px]">
      <select
        value={shipment.status}
        onChange={handleChange}
        disabled={loading}
        className={cn(
          "h-8 min-h-8 w-full py-1 text-xs",
          shipment.status === "FAILED"
            ? "border-destructive/20 bg-destructive/10 text-destructive"
            : "bg-card text-foreground",
          loading && "opacity-70"
        )}
      >
        {STATUS_OPTIONS.filter(s => s !== "all").map(s => (
          <option key={s} value={s}>{s}</option>
        ))}
      </select>
      {loading ? (
        <RefreshCw className="absolute right-3 top-2.5 h-3 w-3 animate-spin text-muted-foreground" />
      ) : null}
    </div>
  );
}

function AdminShipmentsPageInner({
  initialShipments,
  initialError,
}: {
  initialShipments: Shipment[];
  initialError: string | null;
}) {
  const [shipments, setShipments] = useState<Shipment[]>(initialShipments);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(initialError);
  const [filter, setFilter] = useState<ShipmentStatus | "all">("all");

  const load = async () => {
    setLoading(true);
    try {
      const data = await fetchShipmentsServer({ data: { force: true } });
      setShipments(data);
      setError(null);
    } catch (err) {
      setShipments([]);
      setError(err instanceof Error ? err.message : "Live shipment data unavailable");
    } finally {
      setLoading(false);
    }
  };

  const DISPLAY_LIMIT = 300;

  const { displayShipments, filteredCount } = useMemo(() => {
    const filtered =
      filter === "all" ? shipments : shipments.filter((shipment) => shipment.status === filter);
    const ordered = filtered.some((s) => s.createdAt)
      ? [...filtered].sort(
          (a, b) => Date.parse(b.createdAt ?? "0") - Date.parse(a.createdAt ?? "0")
        )
      : [...filtered].reverse();
    return {
      filteredCount: filtered.length,
      displayShipments: ordered.slice(0, DISPLAY_LIMIT),
    };
  }, [filter, shipments]);

  const handleStatusChange = (shipmentId: string, newStatus: ShipmentStatus) => {
    setShipments(prev => prev.map(s => s.shipmentId === shipmentId ? { ...s, status: newStatus } : s));
  };

  return (
    <div className="max-w-6xl space-y-6">
      <div className="flex flex-col gap-3 border-b border-border pb-5 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-foreground">Shipments</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Live shipment records from shipping services
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={load} className="gap-1.5 text-xs">
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh
        </Button>
      </div>

      {error && (
        <div className="rounded-md border border-amber-200/80 bg-amber-50 p-3 text-sm text-amber-800">
          {error}
        </div>
      )}

      <div className="flex flex-wrap items-center gap-2">
        <div className="flex flex-wrap items-center gap-0.5 rounded-md border border-border bg-card p-0.5">
          {STATUS_OPTIONS.map((status) => (
            <button
              key={status}
              type="button"
              onClick={() => setFilter(status)}
              className={cn(
                "rounded-sm px-3 py-1 text-xs font-medium transition-colors",
                filter === status
                  ? "bg-secondary text-primary"
                  : "text-muted-foreground hover:text-foreground"
              )}
            >
              {status}
            </button>
          ))}
        </div>
        <span className="ml-auto text-xs text-muted-foreground">
          {filteredCount > displayShipments.length
            ? `Showing ${displayShipments.length} of ${filteredCount} shipments`
            : `${filteredCount} shipment${filteredCount !== 1 ? "s" : ""}`}
        </span>
      </div>

      <div className="overflow-hidden rounded-md border border-border bg-card">
        {loading ? (
          <div className="space-y-3 p-6">
            {[1, 2, 3, 4, 5].map((i) => (
              <Skeleton key={i} className="h-10 w-full rounded-md" />
            ))}
          </div>
        ) : error ? (
          <div className="py-12 text-center text-sm text-muted-foreground">
            Live shipment data is unavailable.
          </div>
        ) : displayShipments.length === 0 ? (
          <div className="py-12 text-center text-sm text-muted-foreground">
            No shipment records match the current filter.
          </div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Shipment
                </TableHead>
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Order
                </TableHead>
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Pattern
                </TableHead>
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Status
                </TableHead>
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Carrier
                </TableHead>
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  Tracking
                </TableHead>
                <TableHead className="h-10 px-4 text-xs font-medium text-muted-foreground">
                  ETA
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {displayShipments.map((shipment) => (
                <TableRow key={shipment.shipmentId}>
                  <TableCell className="px-4 py-3 font-medium">
                    <span className="max-w-[120px] truncate font-mono text-xs" title={shipment.shipmentId}>
                      {shipment.shipmentId}
                    </span>
                  </TableCell>
                  <TableCell className="max-w-[120px] truncate px-4 py-3 font-mono text-xs" title={shipment.orderId}>
                    {shipment.orderId}
                  </TableCell>
                  <TableCell className="px-4 py-3">
                    <span className="rounded-sm border border-primary/15 bg-secondary px-1.5 py-0.5 text-[11px] font-medium capitalize text-primary">
                      {shipment.pattern}
                    </span>
                  </TableCell>
                  <TableCell className="px-4 py-3">
                    <ShipmentStatusSelect
                      shipment={shipment}
                      onStatusChange={(newStatus) => handleStatusChange(shipment.shipmentId, newStatus)}
                    />
                  </TableCell>
                  <TableCell className="px-4 py-3 text-sm">{shipment.carrier}</TableCell>
                  <TableCell className="px-4 py-3 font-mono text-xs">
                    {shipment.trackingNumber ? (
                      <Link
                        to="/tracking/$trackingId"
                        params={{ trackingId: shipment.trackingNumber }}
                        search={{ pattern: shipment.pattern, orderId: shipment.orderId, shipmentId: shipment.shipmentId }}
                        className="text-primary hover:underline"
                      >
                        {shipment.trackingNumber}
                      </Link>
                    ) : (
                      "—"
                    )}
                  </TableCell>
                  <TableCell className="px-4 py-3 text-xs text-muted-foreground">
                    {new Date(shipment.estimatedDelivery).toLocaleDateString(undefined, {
                      month: "short",
                      day: "numeric",
                    })}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </div>
    </div>
  );
}

function AdminShipmentsPage() {
  const data = Route.useLoaderData();

  return (
    <Suspense fallback={<div className="max-w-6xl space-y-6" />}>
      <AdminShipmentsPageInner initialShipments={data.initialShipments} initialError={data.initialError} />
    </Suspense>
  );
}
