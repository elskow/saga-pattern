import { createFileRoute } from '@tanstack/react-router'
import { Suspense } from "react";
import ShipmentTrackingClient from '../../tracking/[trackingId]/ShipmentTrackingClient';
import { Skeleton } from "@/components/ui/skeleton";
import { findShipmentByTrackingServer } from "@/lib/admin";
import { Pattern } from "@/types";

export const Route = createFileRoute('/tracking/$trackingId')({
  component: ShipmentTrackingPage,
  loader: async ({ params, location }) => {
    const searchParams = location.search as any;
    let initialShipment = null;
    let initialError: string | null = null;

    try {
      initialShipment = await findShipmentByTrackingServer({
        data: {
          trackingId: params.trackingId,
          shipmentId: searchParams.shipmentId ?? null,
          orderId: searchParams.orderId ?? null,
          pattern: searchParams.pattern ?? null,
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
