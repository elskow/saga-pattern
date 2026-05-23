"use client";

import { OrderStatus } from "@/types";
import { cn } from "@/lib/utils";

interface OrderStatusBadgeProps {
  status: OrderStatus | string;
}

const statusConfig: Record<
  string,
  { label: string; className: string }
> = {
  PENDING:              { label: "Pending",              className: "bg-muted text-muted-foreground" },
  SAGA_STARTED:         { label: "Processing",           className: "bg-muted text-foreground" },
  PAYMENT_PROCESSING:   { label: "Processing Payment",   className: "bg-amber-50 text-amber-800 border-amber-200" },
  PAYMENT_COMPLETED:    { label: "Payment Done",         className: "bg-green-50 text-green-800 border-green-200" },
  PAYMENT_FAILED:       { label: "Payment Failed",       className: "bg-red-50 text-red-800 border-red-200" },
  INVENTORY_RESERVING:  { label: "Reserving Stock",      className: "bg-amber-50 text-amber-800 border-amber-200" },
  INVENTORY_RESERVED:   { label: "Stock Reserved",       className: "bg-green-50 text-green-800 border-green-200" },
  INVENTORY_FAILED:     { label: "Stock Failed",         className: "bg-red-50 text-red-800 border-red-200" },
  SHIPPING_SCHEDULING:  { label: "Scheduling Delivery",  className: "bg-amber-50 text-amber-800 border-amber-200" },
  SHIPPING_SCHEDULED:   { label: "Delivery Scheduled",   className: "bg-green-50 text-green-800 border-green-200" },
  SHIPPING_FAILED:      { label: "Delivery Failed",      className: "bg-red-50 text-red-800 border-red-200" },
  COMPLETED:            { label: "Completed",            className: "bg-green-50 text-green-800 border-green-200" },
  FAILED:               { label: "Failed",               className: "bg-red-50 text-red-800 border-red-200" },
  COMPENSATING:         { label: "Rolling Back",         className: "bg-orange-50 text-orange-800 border-orange-200" },
  COMPENSATED:         { label: "Rolled Back",         className: "bg-orange-50 text-orange-800 border-orange-200" },
  CANCELLED:           { label: "Cancelled",           className: "bg-red-50 text-red-800 border-red-200" },
};

export function OrderStatusBadge({ status }: OrderStatusBadgeProps) {
  const cfg = statusConfig[status] ?? {
    label: status,
    className: "bg-muted text-muted-foreground",
  };

  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border px-2.5 py-0.5 text-[11px] font-medium",
        cfg.className
      )}
    >
      {cfg.label}
    </span>
  );
}
