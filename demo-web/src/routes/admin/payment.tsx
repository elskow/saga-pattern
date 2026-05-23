import { createFileRoute } from '@tanstack/react-router'
import AdminPaymentClient from '../../admin/payment/AdminPaymentClient';
import { fetchDepositBalancesServer } from "@/lib/admin";

export const Route = createFileRoute('/admin/payment')({
  component: AdminPaymentPage,
  loader: async () => {
    try {
      const initialBalances = await fetchDepositBalancesServer();
      return { initialBalances };
    } catch (error) {
      return { initialBalances: {} };
    }
  }
})

function AdminPaymentPage() {
  const data = Route.useLoaderData();
  return <AdminPaymentClient initialBalances={data.initialBalances} />;
}
