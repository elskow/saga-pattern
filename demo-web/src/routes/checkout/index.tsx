import { createFileRoute } from '@tanstack/react-router'
import CheckoutClient from '../../checkout/CheckoutClient';

export const Route = createFileRoute('/checkout/')({
  component: CheckoutPage,
})

function CheckoutPage() {
  return <CheckoutClient />;
}
