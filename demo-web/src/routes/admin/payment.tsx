import { createFileRoute } from '@tanstack/react-router'
import AdminPaymentClient from '../../admin/payment/AdminPaymentClient';
import { fetchDepositBalancesServer } from "@/lib/admin";

export const Route = createFileRoute('/admin/payment')({
  component: AdminPaymentPage,
  loader: async () => {
    try {
      const initialBalances = await fetchDepositBalancesServer();
      return { initialBalances, initialError: null };
    } catch (error) {
      return {
        initialBalances: {},
        initialError: error instanceof Error ? error.message : "Live payment balances unavailable",
      };
    }
  }
})

function AdminPaymentPage() {
  const data = Route.useLoaderData();
  return <AdminPaymentClient initialBalances={data.initialBalances} initialError={data.initialError} />;
}
