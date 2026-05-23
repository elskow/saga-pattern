import { createFileRoute } from '@tanstack/react-router'
import OrderTrackingPageClient from '../../orders/[orderId]/OrderTrackingPageClient';
import { getOrderServer } from "@/lib/api";
import { Pattern } from "@/types";

export const Route = createFileRoute('/orders/$orderId')({
  component: OrderTrackingPage,
  loader: async ({ params, location }) => {
    const pattern = (location.search as any)?.pattern as Pattern ?? "choreography";

    try {
      const initialOrder = await getOrderServer({ data: { pattern, orderId: params.orderId } });
      return { orderId: params.orderId, pattern, initialOrder, initialError: null };
    } catch (error) {
      return { 
        orderId: params.orderId, 
        pattern, 
        initialOrder: null, 
        initialError: error instanceof Error ? error.message : "Failed to fetch order" 
      };
    }
  }
})

function OrderTrackingPage() {
  const data = Route.useLoaderData();
  return <OrderTrackingPageClient orderId={data.orderId} pattern={data.pattern} initialOrder={data.initialOrder} initialError={data.initialError} />;
}
