import { createFileRoute } from '@tanstack/react-router'
import { Suspense } from "react";
import ShipmentTrackingClient from '../../tracking/[trackingId]/ShipmentTrackingClient';
import { Skeleton } from "@/components/ui/skeleton";
import { findShipmentByTrackingServer } from "@/lib/admin";
import { Pattern } from "@/types";

type TrackingSearch = {
  shipmentId?: string | null;
  orderId?: string | null;
  pattern?: Pattern | null;
};

const toPattern = (value: unknown): Pattern | null =>
  value === "choreography" || value === "orchestration" ? value : null;

const toOptionalString = (value: unknown): string | null =>
  typeof value === "string" && value.length > 0 ? value : null;

export const Route = createFileRoute('/tracking/$trackingId')({
  component: ShipmentTrackingPage,
  loader: async ({ params, location }) => {
    const searchParams = location.search as TrackingSearch;
    let initialShipment = null;
    let initialError: string | null = null;

    try {
      initialShipment = await findShipmentByTrackingServer({
        data: {
          trackingId: params.trackingId,
          shipmentId: toOptionalString(searchParams.shipmentId),
          orderId: toOptionalString(searchParams.orderId),
          pattern: toPattern(searchParams.pattern),
        }
      });
    } catch (error) {
      initialError = error instanceof Error ? error.message : "Live shipment tracking unavailable";
    }

    return { initialShipment, initialError };
  }
})

function ShipmentTrackingPage() {
  const data = Route.useLoaderData();
  
  return (
    <Suspense
      fallback={
        <div className="max-w-2xl mx-auto space-y-6">
          <Skeleton className="h-7 w-48" />
          <Skeleton className="h-48 rounded-xl" />
          <Skeleton className="h-64 rounded-xl" />
        </div>
      }
    >
      <ShipmentTrackingClient initialShipment={data.initialShipment} initialError={data.initialError} />
    </Suspense>
  );
}
