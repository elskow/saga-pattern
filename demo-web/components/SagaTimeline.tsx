"use client";

import { Order } from "@/types";
import { deriveSteps } from "@/lib/api";
import {
  ShoppingCart,
  CreditCard,
  Package,
  Truck,
  Check,
  X,
  Loader2,
  Clock,
  RotateCcw,
} from "lucide-react";
import { cn } from "@/lib/utils";

const iconMap: Record<string, React.ElementType> = {
  ShoppingCart,
  CreditCard,
  Package,
  Truck,
};

type StepStatus =
  | "PENDING"
  | "IN_PROGRESS"
  | "COMPLETED"
  | "FAILED"
  | "COMPENSATING"
  | "COMPENSATED";

const statusConfig: Record<
  StepStatus,
  {
    dot: string;
    label: string;
    labelClass: string;
    spin?: boolean;
    icon?: React.ElementType;
  }
> = {
  PENDING: {
    dot: "bg-muted-foreground/20 border border-border",
    label: "Pending",
    labelClass: "text-muted-foreground",
    icon: Clock,
  },
  IN_PROGRESS: {
    dot: "bg-amber-400",
    label: "In progress",
    labelClass: "text-amber-700",
    spin: true,
    icon: Loader2,
  },
  COMPLETED: {
    dot: "bg-green-500",
    label: "Done",
    labelClass: "text-green-700",
    icon: Check,
  },
  FAILED: {
    dot: "bg-red-500",
    label: "Failed",
    labelClass: "text-red-700",
    icon: X,
  },
  COMPENSATING: {
    dot: "bg-orange-400",
    label: "Compensating",
    labelClass: "text-orange-700",
    icon: RotateCcw,
  },
  COMPENSATED: {
    dot: "bg-orange-300",
    label: "Compensated",
    labelClass: "text-orange-700",
    icon: RotateCcw,
  },
};

interface SagaTimelineProps {
  order: Order;
}

export function SagaTimeline({ order }: SagaTimelineProps) {
  const steps = deriveSteps(order);
  const isCompensating =
    ["FAILED", "COMPENSATING", "COMPENSATED"].includes(order.status) ||
    (order.status === "CANCELLED" && Boolean(order.failureStep || order.compensatedSteps?.length));

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2">
        <span className="rounded-full border border-border px-3 py-0.5 text-xs font-medium text-foreground capitalize">
          {order.pattern}
        </span>
        <span className="text-xs text-muted-foreground">
          {order.pattern === "choreography"
            ? "Event-driven via Kafka"
            : "Orchestrated via coordinator"}
        </span>
      </div>

      <ol className="relative space-y-0" aria-label="Saga steps">
        {steps.map((step, idx) => {
          const StepIcon = iconMap[step.icon] || ShoppingCart;
          const cfg =
            statusConfig[step.status as StepStatus] ?? statusConfig.PENDING;
          const StatusIcon = cfg.icon ?? Clock;
          const isLast = idx === steps.length - 1;

          return (
            <li
              key={step.id}
              id={`saga-step-${step.id}`}
              className="flex gap-4"
            >
              <div className="flex flex-col items-center">
                <div
                  className={cn(
                    "mt-1 h-4 w-4 rounded-full shrink-0 flex items-center justify-center",
                    cfg.dot
                  )}
                >
                  {step.status === "COMPLETED" && (
                    <Check className="h-2.5 w-2.5 text-white" strokeWidth={3} />
                  )}
                  {step.status === "FAILED" && (
                    <X className="h-2.5 w-2.5 text-white" strokeWidth={3} />
                  )}
                </div>
                {!isLast && (
                  <div className="mt-1 w-px flex-1 bg-border min-h-[2rem]" />
                )}
              </div>

              <div className={cn("pb-6 flex-1 min-w-0", isLast && "pb-0")}>
                <div className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-2">
                    <StepIcon className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                    <span className="text-sm font-medium text-foreground">
                      {step.name}
                    </span>
                  </div>
                  <div className={cn("flex items-center gap-1 text-xs", cfg.labelClass)}>
                    <StatusIcon
                      className={cn("h-3 w-3", cfg.spin && "animate-spin")}
                    />
                    {cfg.label}
                  </div>
                </div>
                <p className="mt-0.5 ml-[22px] text-xs text-muted-foreground">
                  {step.description}
                </p>
                {isCompensating && step.status === "COMPENSATED" && (
                  <p className="mt-1 ml-[22px] text-xs text-orange-700 flex items-center gap-1">
                    <RotateCcw className="h-3 w-3" />
                    Changes rolled back
                  </p>
                )}
              </div>
            </li>
          );
        })}
      </ol>

      {isCompensating && (
        <div className="rounded-lg border border-border bg-muted/50 p-4 text-sm space-y-1">
          <p className="font-medium text-foreground">Saga compensation triggered</p>
          <p className="text-xs text-muted-foreground leading-relaxed">
            {order.pattern === "choreography"
              ? "Each service listens for failure events and issues its own compensating action."
              : "The central orchestrator sends compensating commands in reverse order."}
          </p>
          {order.failureReason && (
            <p className="text-xs text-destructive font-mono">
              {order.failureReason}
            </p>
          )}
        </div>
      )}
    </div>
  );
}
