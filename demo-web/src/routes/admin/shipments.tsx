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
import { Badge } from "@/components/ui/badge";
import { RefreshCw, Truck } from "lucide-react";
import { Link } from "@tanstack/react-router";

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
    <div className="relative inline-block min-w-[150px]">
      <select 
        value={shipment.status} 
        onChange={handleChange} 
        disabled={loading}
        className={`w-full appearance-none border border-border text-xs rounded-full px-2.5 py-1 pr-7 outline-none focus:ring-1 focus:ring-ring transition-colors ${
          shipment.status === "FAILED" ? "bg-destructive/10 text-destructive border-destructive/20" : "bg-secondary text-secondary-foreground"
        } ${loading ? "opacity-70" : ""}`}
      >
        {STATUS_OPTIONS.filter(s => s !== "all").map(s => (
          <option key={s} value={s}>{s}</option>
        ))}
      </select>
      {loading ? (
        <RefreshCw className="h-3 w-3 absolute right-2.5 top-1.5 animate-spin text-muted-foreground" />
      ) : (
        <div className="pointer-events-none absolute right-2.5 top-1.5 text-muted-foreground">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="m6 9 6 6 6-6"/></svg>
        </div>
      )}
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
      const data = await fetchShipmentsServer();
      setShipments(data);
      setError(null);
    } catch (err) {
      setShipments([]);
      setError(err instanceof Error ? err.message : "Live shipment data unavailable");
    } finally {
      setLoading(false);
    }
  };

  const filtered = useMemo(() => {
    return filter === "all" ? shipments : shipments.filter((shipment) => shipment.status === filter);
  }, [filter, shipments]);

  const handleStatusChange = (shipmentId: string, newStatus: ShipmentStatus) => {
    setShipments(prev => prev.map(s => s.shipmentId === shipmentId ? { ...s, status: newStatus } : s));
  };

  return (
    <div className="space-y-6 max-w-6xl">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Shipments</h1>
          <p className="text-sm text-muted-foreground mt-1">Live shipment records from the shipping services.</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={load} className="gap-1.5 rounded-full text-xs">
            <RefreshCw className="h-3.5 w-3.5" />
            Refresh
          </Button>
        </div>
      </div>

      {error && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
          {error}
        </div>
      )}

      <div className="flex flex-wrap gap-2">
        {STATUS_OPTIONS.map((status) => (
          <Button
            key={status}
            variant={filter === status ? "default" : "outline"}
            size="sm"
            className="rounded-full text-xs"
            onClick={() => setFilter(status)}
          >
            {status}
          </Button>
        ))}
      </div>

      <div className="rounded-xl border border-border bg-card overflow-hidden">
        {loading ? (
          <div className="p-6 space-y-3">
            {[1, 2, 3, 4, 5].map((i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        ) : error ? (
          <div className="py-20 text-center text-sm text-muted-foreground">
            Live shipment data is unavailable.
          </div>
        ) : filtered.length === 0 ? (
          <div className="py-20 text-center text-sm text-muted-foreground">
            No shipment records match the current filter.
          </div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Shipment</TableHead>
                <TableHead>Order</TableHead>
                <TableHead>Pattern</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Carrier</TableHead>
                <TableHead>Tracking</TableHead>
                <TableHead>ETA</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.map((shipment) => (
                <TableRow key={shipment.shipmentId}>
                  <TableCell className="font-medium">
                    <div className="flex items-center gap-2">
                      <Truck className="h-4 w-4 shrink-0 text-muted-foreground" />
                      <span className="truncate max-w-[120px]" title={shipment.shipmentId}>
                        {shipment.shipmentId}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell className="font-mono text-xs max-w-[120px] truncate" title={shipment.orderId}>
                    {shipment.orderId}
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline" className="capitalize">{shipment.pattern}</Badge>
                  </TableCell>
                  <TableCell>
                    <ShipmentStatusSelect shipment={shipment} onStatusChange={(newStatus) => handleStatusChange(shipment.shipmentId, newStatus)} />
                  </TableCell>
                  <TableCell>{shipment.carrier}</TableCell>
                  <TableCell className="font-mono text-xs">
                    {shipment.trackingNumber ? (
                      <Link to={`/tracking/${shipment.trackingNumber}`} className="text-blue-500 hover:underline">
                        {shipment.trackingNumber}
                      </Link>
                    ) : (
                      "—"
                    )}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
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
    <Suspense fallback={<div className="space-y-6 max-w-6xl" />}>
      <AdminShipmentsPageInner initialShipments={data.initialShipments} initialError={data.initialError} />
    </Suspense>
  );
}
