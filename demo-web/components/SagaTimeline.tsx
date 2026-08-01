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
    dot: "bg-muted border border-border",
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
    dot: "bg-emerald-600",
    label: "Done",
    labelClass: "text-emerald-700",
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

  const patternLabel = order.pattern === "choreography" ? "Choreography" : "Orchestration";

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center gap-2">
        <span
          className={cn(
            "rounded-sm border px-1.5 py-0.5 text-[11px] font-medium",
            order.pattern === "choreography"
              ? "border-primary/20 bg-secondary text-primary"
              : "border-border bg-muted text-foreground"
          )}
        >
          {patternLabel}
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
              className="flex gap-3.5"
            >
              <div className="flex flex-col items-center">
                <div
                  className={cn(
                    "mt-1 flex h-4 w-4 shrink-0 items-center justify-center rounded-full",
                    cfg.dot
                  )}
                >
                  {step.status === "COMPLETED" && (
                    <Check className="h-2.5 w-2.5 text-white" strokeWidth={3} />
                  )}
                  {step.status === "FAILED" && (
                    <X className="h-2.5 w-2.5 text-white" strokeWidth={3} />
                  )}
                  {step.status === "IN_PROGRESS" && (
                    <Loader2 className="h-2.5 w-2.5 animate-spin text-white" />
                  )}
                </div>
                {!isLast && (
                  <div
                    className={cn(
                      "mt-1 min-h-[2rem] w-px flex-1",
                      step.status === "IN_PROGRESS" ? "animate-pulse bg-amber-400/50" : "bg-border"
                    )}
                  />
                )}
              </div>

              <div className={cn("min-w-0 flex-1", isLast ? "pb-0" : "pb-5")}>
                <div className="flex items-start justify-between gap-3">
                  <div className="flex min-w-0 items-center gap-2">
                    <StepIcon className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                    <p className="text-sm font-medium text-foreground">{step.name}</p>
                  </div>
                  <span className={cn("inline-flex shrink-0 items-center gap-1 text-[11px] font-medium", cfg.labelClass)}>
                    <StatusIcon className={cn("h-3 w-3", cfg.spin && "animate-spin")} />
                    {cfg.label}
                  </span>
                </div>
                {step.description && (
                  <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                    {step.description}
                  </p>
                )}
              </div>
            </li>
          );
        })}
      </ol>

      {isCompensating && (
        <div className="space-y-1 rounded-md border border-border bg-muted/40 p-3.5 text-sm">
          <p className="font-medium text-foreground">Fulfillment rollback started</p>
          <p className="text-xs leading-relaxed text-muted-foreground">
            {order.pattern === "choreography" ? (
              <>
                <span className="font-semibold text-foreground">Choreography</span> lets each service listen for failure events and issue its own compensating action.
              </>
            ) : (
              <>
                <span className="font-semibold text-foreground">Orchestration</span> sends compensating commands from the central coordinator in reverse order.
              </>
            )}
          </p>
          {order.failureReason && (
            <p className="font-mono text-xs text-destructive">
              {order.failureReason}
            </p>
          )}
        </div>
      )}
    </div>
  );
}
