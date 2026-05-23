import { createFileRoute } from '@tanstack/react-router'
import OrdersClient from '../../orders/OrdersClient';
import { fetchAllOrdersServer } from "@/lib/admin";

export const Route = createFileRoute('/orders/')({
  component: OrdersPage,
  loader: async () => {
    try {
      const initialOrders = await fetchAllOrdersServer();
      return { initialOrders, initialError: null };
    } catch (error) {
      return { 
        initialOrders: [], 
        initialError: error instanceof Error ? error.message : "Live order listing is unavailable" 
      };
    }
  }
})

function OrdersPage() {
  const data = Route.useLoaderData();
  return <OrdersClient initialOrders={data.initialOrders} initialError={data.initialError} />;
}
