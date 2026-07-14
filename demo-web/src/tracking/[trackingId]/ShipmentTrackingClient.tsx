"use client";

import { useEffect, useState } from "react";
import { useParams, useSearch } from "@tanstack/react-router";
import { Link } from "@tanstack/react-router";
import { ArrowLeft, MapPin, Package, RefreshCw, Truck } from "lucide-react";

import { findShipmentByTrackingServer } from "@/lib/admin";
import { Pattern, Shipment } from "@/types";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";

type TrackingParams = {
  trackingId: string;
};

type TrackingSearch = {
  shipmentId?: string | null;
  orderId?: string | null;
  pattern?: Pattern | null;
};

const toPattern = (value: unknown): Pattern | null =>
  value === "choreography" || value === "orchestration" ? value : null;

const toOptionalString = (value: unknown): string | null =>
  typeof value === "string" && value.length > 0 ? value : null;

interface ShipmentTrackingClientProps {
  initialShipment?: Shipment | null;
  initialError?: string | null;
}

export default function ShipmentTrackingClient({
  initialShipment = null,
  initialError = null,
}: ShipmentTrackingClientProps) {
  const params = useParams({ strict: false }) as TrackingParams;
  const searchParams = useSearch({ strict: false }) as TrackingSearch;

  const trackingId = params.trackingId;
  const shipmentId = toOptionalString(searchParams.shipmentId);
  const orderId = toOptionalString(searchParams.orderId);
  const pattern = toPattern(searchParams.pattern);

  const [shipment, setShipment] = useState<Shipment | null>(initialShipment);
  const [loading, setLoading] = useState(initialShipment == null && initialError == null);
  const [error, setError] = useState<string | null>(initialError);

  const load = async () => {
    setLoading(true);
    try {
      const found = await findShipmentByTrackingServer({
        data: { trackingId, shipmentId, orderId, pattern },
      });
      setShipment(found ?? null);
      setError(null);
    } catch (err) {
      setShipment(null);
      setError(err instanceof Error ? err.message : "Live shipment tracking unavailable");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (initialShipment || initialError) {
      return;
    }
    load();
  }, [trackingId, shipmentId, orderId, pattern, initialShipment, initialError]);

  if (loading) {
    return (
      <div className="max-w-7xl mx-auto space-y-8 w-full py-10">
        <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4 border-b border-border/60 pb-5">
          <Skeleton className="h-10 w-64" />
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          <Skeleton className="lg:col-span-2 h-96 rounded-2xl" />
          <Skeleton className="lg:col-span-1 h-64 rounded-2xl" />
        </div>
      </div>
    );
  }

  if (error || !shipment) {
    return (
      <div className="flex flex-col items-center justify-center py-24 gap-4 text-center">
        <Package className="h-12 w-12 text-muted-foreground/30" />
        <h1 className="text-xl font-semibold">Shipment not found</h1>
        <p className="text-sm text-muted-foreground max-w-md">
          {error ?? `Could not find tracking info for ${trackingId}. The shipment may not exist yet.`}
        </p>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" className="rounded-full gap-2" onClick={load}>
            <RefreshCw className="h-3.5 w-3.5" />
            Retry
          </Button>
          <Link to="/admin/shipments">
            <Button variant="outline" size="sm" className="rounded-full gap-2">
              <ArrowLeft className="h-3.5 w-3.5" />
              Shipments
            </Button>
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto space-y-8 w-full py-10 px-4 sm:px-6 lg:px-8">
      <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4 border-b border-border/60 pb-5">
        <div>
          <div className="flex items-center gap-3">
            <h1 className="text-3xl font-bold tracking-tight text-foreground">Shipment Tracking</h1>
            <Badge variant={shipment.status === "FAILED" ? "destructive" : "secondary"} className="rounded-full font-medium">
              {shipment.status}
            </Badge>
          </div>
          <p className="text-sm text-muted-foreground mt-2 font-mono tracking-wide">{shipment.trackingNumber}</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" className="gap-2 text-xs rounded-full font-medium shadow-sm hover:bg-muted/50" onClick={load}>
            <RefreshCw className="h-3.5 w-3.5" />
            Refresh Data
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        <div className="lg:col-span-2 rounded-2xl border border-border bg-card p-8 shadow-sm space-y-6">
          <h2 className="text-lg font-semibold tracking-tight">Tracking Timeline</h2>
          <div className="space-y-6">
            {shipment.events.map((event, index) => (
              <div key={`${event.status}-${index}`} className="flex gap-4 relative">
                {index < shipment.events.length - 1 && (
                  <div className="absolute left-5 top-10 bottom-[-16px] w-[2px] bg-border/50" />
                )}
                <div className="mt-0.5 flex h-10 w-10 shrink-0 items-center justify-center rounded-full border border-border/60 bg-muted/30 shadow-sm text-muted-foreground z-10">
                  <Truck className="h-4 w-4" />
                </div>
                <div className="space-y-1.5 pb-4">
                  <div className="flex flex-col sm:flex-row sm:items-center gap-1 sm:gap-3">
                    <p className="text-base font-semibold tracking-tight text-foreground">{event.status}</p>
                    <span className="text-xs text-muted-foreground font-medium">{new Date(event.timestamp).toLocaleString()}</span>
                  </div>
                  <p className="text-sm text-muted-foreground/80">{event.description}</p>
                  <p className="text-sm text-muted-foreground flex items-center gap-1.5 pt-1">
                    <MapPin className="h-3.5 w-3.5" />
                    {event.location}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>

        <aside className="lg:col-span-1 space-y-4">
          <div className="rounded-2xl border border-border bg-card p-6 shadow-sm space-y-5">
            <h2 className="text-lg font-semibold tracking-tight">Shipment Details</h2>
            <div className="space-y-4">
              <div>
                <p className="text-xs text-muted-foreground uppercase tracking-wider font-semibold mb-1">Shipment ID</p>
                <p className="font-mono text-sm break-all">{shipment.shipmentId}</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground uppercase tracking-wider font-semibold mb-1">Order ID</p>
                <p className="font-mono text-sm break-all">{shipment.orderId}</p>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider font-semibold mb-1">Carrier</p>
                  <p className="text-sm font-medium">{shipment.carrier}</p>
                </div>
                <div>
                  <p className="text-xs text-muted-foreground uppercase tracking-wider font-semibold mb-1">ETA</p>
                  <p className="text-sm font-medium">{new Date(shipment.estimatedDelivery).toLocaleDateString()}</p>
                </div>
              </div>
              <div>
                <p className="text-xs text-muted-foreground uppercase tracking-wider font-semibold mb-1">Shipping Address</p>
                <p className="text-sm text-muted-foreground leading-relaxed">{shipment.shippingAddress}</p>
              </div>
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}
