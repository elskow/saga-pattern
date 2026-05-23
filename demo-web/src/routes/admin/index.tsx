import { createFileRoute } from '@tanstack/react-router'
import AdminDashboardClient from '../../admin/AdminDashboardClient';
import { fetchAllOrdersServer, fetchAllServiceHealthServer } from "@/lib/admin";

export const Route = createFileRoute('/admin/')({
  component: AdminDashboard,
  loader: async () => {
    const [ordersResult, healthResult] = await Promise.allSettled([
      fetchAllOrdersServer(),
      fetchAllServiceHealthServer(),
    ]);

    const initialOrders = ordersResult.status === "fulfilled" ? ordersResult.value : [];
    const initialHealth = healthResult.status === "fulfilled" ? healthResult.value : [];
    const initialError = ordersResult.status === "fulfilled"
      ? null
      : ordersResult.reason instanceof Error
        ? ordersResult.reason.message
        : "Live order listing unavailable";

    return { initialOrders, initialHealth, initialError };
  }
})

function AdminDashboard() {
  const data = Route.useLoaderData()
  return <AdminDashboardClient initialOrders={data.initialOrders} initialHealth={data.initialHealth} initialError={data.initialError} />;
}
