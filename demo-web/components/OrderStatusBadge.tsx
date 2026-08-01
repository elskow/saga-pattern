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
  PENDING:              { label: "Pending",              className: "bg-muted text-muted-foreground border-border" },
  SAGA_STARTED:         { label: "Processing",           className: "bg-secondary text-primary border-primary/15" },
  PAYMENT_PROCESSING:   { label: "Processing payment",   className: "bg-amber-50 text-amber-800 border-amber-200/80" },
  PAYMENT_COMPLETED:    { label: "Payment done",         className: "bg-emerald-50 text-emerald-800 border-emerald-200/80" },
  PAYMENT_FAILED:       { label: "Payment failed",       className: "bg-red-50 text-red-800 border-red-200/80" },
  INVENTORY_RESERVING:  { label: "Reserving stock",      className: "bg-amber-50 text-amber-800 border-amber-200/80" },
  INVENTORY_RESERVED:   { label: "Stock reserved",       className: "bg-emerald-50 text-emerald-800 border-emerald-200/80" },
  INVENTORY_FAILED:     { label: "Stock failed",         className: "bg-red-50 text-red-800 border-red-200/80" },
  SHIPPING_SCHEDULING:  { label: "Scheduling delivery",  className: "bg-amber-50 text-amber-800 border-amber-200/80" },
  SHIPPING_SCHEDULED:   { label: "Delivery scheduled",   className: "bg-emerald-50 text-emerald-800 border-emerald-200/80" },
  SHIPPING_FAILED:      { label: "Delivery failed",      className: "bg-red-50 text-red-800 border-red-200/80" },
  COMPLETED:            { label: "Completed",            className: "bg-emerald-50 text-emerald-800 border-emerald-200/80" },
  FAILED:               { label: "Failed",               className: "bg-red-50 text-red-800 border-red-200/80" },
  COMPENSATING:         { label: "Rolling back",         className: "bg-orange-50 text-orange-800 border-orange-200/80" },
  COMPENSATED:          { label: "Rolled back",          className: "bg-orange-50 text-orange-800 border-orange-200/80" },
  CANCELLED:            { label: "Cancelled",            className: "bg-red-50 text-red-800 border-red-200/80" },
};

export function OrderStatusBadge({ status }: OrderStatusBadgeProps) {
  const cfg = statusConfig[status] ?? {
    label: status,
    className: "bg-muted text-muted-foreground border-border",
  };

  return (
    <span
      className={cn(
        "inline-flex items-center rounded-sm border px-1.5 py-0.5 text-[11px] font-medium",
        cfg.className
      )}
    >
      {cfg.label}
    </span>
  );
}
