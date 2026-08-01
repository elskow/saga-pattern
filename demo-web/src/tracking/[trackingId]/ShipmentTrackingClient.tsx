"use client";

import { useEffect, useState } from "react";
import { useParams, useSearch } from "@tanstack/react-router";
import { Link } from "@tanstack/react-router";

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
      <div className="mx-auto w-full max-w-7xl space-y-8 py-8">
        <div className="flex flex-col justify-between gap-4 border-b border-border pb-5 sm:flex-row sm:items-end">
          <Skeleton className="h-9 w-56 rounded-md" />
        </div>
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
          <Skeleton className="h-80 rounded-md lg:col-span-2" />
          <Skeleton className="h-56 rounded-md lg:col-span-1" />
        </div>
      </div>
    );
  }

  if (error || !shipment) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-24 text-center">
        <h1 className="text-base font-semibold tracking-tight text-foreground">Shipment not found</h1>
        <p className="max-w-md text-sm text-muted-foreground">
          {error ?? `Could not find tracking info for ${trackingId}. The shipment may not exist yet.`}
        </p>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" className="rounded-md" onClick={load}>
            Retry
          </Button>
          <Link to="/admin/shipments">
            <Button variant="outline" size="sm" className="rounded-md">
              Shipments
            </Button>
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="mx-auto w-full max-w-7xl space-y-8 px-4 py-8 sm:px-6 lg:px-8">
      <div className="flex flex-col justify-between gap-4 border-b border-border pb-5 sm:flex-row sm:items-end">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="text-xl font-semibold tracking-tight text-foreground">Shipment tracking</h1>
            <Badge variant={shipment.status === "FAILED" ? "destructive" : "secondary"} className="rounded-sm font-medium">
              {shipment.status}
            </Badge>
          </div>
          <p className="mt-1 font-mono text-sm text-muted-foreground">{shipment.trackingNumber}</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" className="rounded-md text-xs font-medium" onClick={load}>
            Refresh
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <div className="space-y-5 rounded-md border border-border bg-card p-5 lg:col-span-2 sm:p-6">
          <h2 className="text-base font-semibold tracking-tight">Tracking timeline</h2>
          <div className="divide-y divide-border">
            {shipment.events.map((event, index) => (
              <div key={`${event.status}-${index}`} className="flex gap-3 py-3 first:pt-0 last:pb-0">
                <div className="mt-1.5 h-2 w-2 shrink-0 rounded-full bg-primary" />
                <div className="min-w-0 space-y-1">
                  <div className="flex flex-col gap-0.5 sm:flex-row sm:items-center sm:gap-3">
                    <p className="text-sm font-medium tracking-tight text-foreground">{event.status}</p>
                    <span className="text-xs text-muted-foreground">{new Date(event.timestamp).toLocaleString()}</span>
                  </div>
                  <p className="text-sm text-muted-foreground">{event.description}</p>
                  <p className="text-xs text-muted-foreground">{event.location}</p>
                </div>
              </div>
            ))}
          </div>
        </div>

        <aside className="space-y-4 lg:col-span-1">
          <div className="space-y-4 rounded-md border border-border bg-card p-5">
            <h2 className="text-base font-semibold tracking-tight">Shipment details</h2>
            <div className="space-y-3">
              <div>
                <p className="mb-0.5 text-xs text-muted-foreground">Shipment ID</p>
                <p className="break-all font-mono text-sm">{shipment.shipmentId}</p>
              </div>
              <div>
                <p className="mb-0.5 text-xs text-muted-foreground">Order ID</p>
                <p className="break-all font-mono text-sm">{shipment.orderId}</p>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <p className="mb-0.5 text-xs text-muted-foreground">Carrier</p>
                  <p className="text-sm font-medium">{shipment.carrier}</p>
                </div>
                <div>
                  <p className="mb-0.5 text-xs text-muted-foreground">ETA</p>
                  <p className="text-sm font-medium">{new Date(shipment.estimatedDelivery).toLocaleDateString()}</p>
                </div>
              </div>
              <div>
                <p className="mb-0.5 text-xs text-muted-foreground">Shipping address</p>
                <p className="text-sm leading-relaxed text-muted-foreground">{shipment.shippingAddress}</p>
              </div>
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}
