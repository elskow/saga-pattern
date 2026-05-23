import { createFileRoute } from '@tanstack/react-router'
import AdminOrdersClient from '../../admin/orders/AdminOrdersClient';
import { fetchAllOrdersServer } from "@/lib/admin";

export const Route = createFileRoute('/admin/orders')({
  component: AdminOrdersPage,
  loader: async () => {
    try {
      const initialOrders = await fetchAllOrdersServer();
      return { initialOrders, initialError: null };
    } catch (error) {
      return { 
        initialOrders: [], 
        initialError: error instanceof Error ? error.message : "Live order listing unavailable" 
      };
    }
  }
})

function AdminOrdersPage() {
  const data = Route.useLoaderData();
  return <AdminOrdersClient initialOrders={data.initialOrders} initialError={data.initialError} />;
}
