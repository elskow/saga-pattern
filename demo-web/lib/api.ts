import { CreateOrderPayload, Order, Pattern } from "@/types";
import { Effect } from "effect";
import { requestJson, getServiceUrl } from "./effect-services";
import { createServerFn } from "@tanstack/react-start";

export async function healthCheck(pattern: Pattern): Promise<boolean> {
  const program = Effect.gen(function* () {
    const url = `${getServiceUrl("order", pattern, false)}/actuator/health`;
    const res = yield* Effect.tryPromise({
      try: () => fetch(url, { signal: AbortSignal.timeout(5000) }),
      catch: () => new Error("Health check failed"),
    });
    return res.ok;
  }).pipe(Effect.catchAll(() => Effect.succeed(false)));

  return Effect.runPromise(program);
}

export async function createOrder(
  pattern: Pattern,
  payload: CreateOrderPayload
): Promise<Order> {
  const body: CreateOrderPayload = {
    customerId: payload.customerId,
    shippingAddress: payload.shippingAddress,
    items: payload.items,
    ...(pattern === "orchestration" && { totalAmount: payload.totalAmount }),
  };

  const program = Effect.gen(function* () {
    const url = `${getServiceUrl("order", pattern, false)}/api/orders`;
    const data = yield* requestJson<Order>(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).pipe(Effect.timeout("15 seconds"));
    return { ...data, pattern };
  });

  return Effect.runPromise(program);
}

export const createOrderServer = createServerFn({ method: "POST" })
  .inputValidator((data: { pattern: Pattern; payload: CreateOrderPayload }) => data)
  .handler(async ({ data }: { data: { pattern: Pattern; payload: CreateOrderPayload } }): Promise<Order> => {
    const { pattern, payload } = data;
    const body: CreateOrderPayload = {
      customerId: payload.customerId,
      shippingAddress: payload.shippingAddress,
      items: payload.items,
      ...(pattern === "orchestration" && { totalAmount: payload.totalAmount }),
    };

    const program = Effect.gen(function* () {
      const url = `${getServiceUrl("order", pattern, true)}/api/orders`;
      const order = yield* requestJson<Order>(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }).pipe(Effect.timeout("15 seconds"));
      return { ...order, pattern };
    });

    return Effect.runPromise(program);
  });

export async function getOrder(
  pattern: Pattern,
  orderId: string
): Promise<Order> {
  const program = Effect.gen(function* () {
    const url = `${getServiceUrl("order", pattern, false)}/api/orders/${orderId}`;
    const data = yield* requestJson<Order>(url, { cache: "no-store" }).pipe(
      Effect.timeout("5 seconds")
    );
    return { ...data, pattern };
  });

  return Effect.runPromise(program);
}

export const getOrderServer = createServerFn({ method: "POST" })
  .inputValidator((data: { pattern: Pattern; orderId: string }) => data)
  .handler(async ({ data }: { data: { pattern: Pattern; orderId: string } }): Promise<Order> => {
    const program = Effect.gen(function* () {
      const { pattern, orderId } = data;
      const url = `${getServiceUrl("order", pattern, true)}/api/orders/${orderId}`;
      const res = yield* requestJson<Order>(url, { cache: "no-store" }).pipe(
        Effect.timeout("8 seconds")
      );
      return { ...res, pattern };
    });
    return Effect.runPromise(program);
  });

// Derive saga steps from order status for visualization
export function deriveSteps(order: Order) {
  const status = order.status;
  const compensatedSteps = new Set(order.compensatedSteps ?? []);
  const failureStep = order.failureStep ?? inferFailureStep(order);

  const steps = [
    {
      id: "order",
      name: "Order Created",
      icon: "ShoppingCart",
      description: "Order validated and saga initiated",
    },
    {
      id: "payment",
      name: "Payment",
      icon: "CreditCard",
      description: "Payment processed and authorized",
    },
    {
      id: "inventory",
      name: "Inventory",
      icon: "Package",
      description: "Items reserved from warehouse",
    },
    {
      id: "shipping",
      name: "Shipping",
      icon: "Truck",
      description: "Delivery scheduled and tracking assigned",
    },
  ];

  const isCompensating = ["FAILED", "COMPENSATING", "COMPENSATED"].includes(status);

  const getStepStatus = (stepId: string) => {
    if (status === "PENDING" || status === "SAGA_STARTED") {
      if (stepId === "order") return "COMPLETED";
      return "PENDING";
    }

    if (status === "CANCELLED") {
      if (stepId === "order") return "COMPLETED";
      if (compensatedSteps.has(stepId as "payment" | "inventory" | "shipping")) return "COMPENSATED";
      if (failureStep === stepId) return "FAILED";
      if (failureStep === "inventory" && stepId === "payment" && order.paymentId) return "COMPLETED";
      if (failureStep === "shipping" && ["payment", "inventory"].includes(stepId)) return "COMPLETED";
      return "PENDING";
    }

    switch (stepId) {
      case "order":
        return "COMPLETED";
      case "payment":
        if (["PAYMENT_PROCESSING"].includes(status)) return "IN_PROGRESS";
        if (["PAYMENT_FAILED"].includes(status)) return "FAILED";
        if (isCompensating && order.paymentId) return "COMPENSATED";
        if (
          [
            "PAYMENT_COMPLETED",
            "INVENTORY_RESERVING",
            "INVENTORY_RESERVED",
            "INVENTORY_FAILED",
            "SHIPPING_SCHEDULING",
            "SHIPPING_SCHEDULED",
            "SHIPPING_FAILED",
            "COMPLETED",
          ].includes(status)
        )
          return "COMPLETED";
        return "PENDING";
      case "inventory":
        if (["INVENTORY_RESERVING"].includes(status)) return "IN_PROGRESS";
        if (["INVENTORY_FAILED"].includes(status)) return "FAILED";
        if (isCompensating && order.reservationId) return "COMPENSATED";
        if (
          [
            "INVENTORY_RESERVED",
            "SHIPPING_SCHEDULING",
            "SHIPPING_SCHEDULED",
            "SHIPPING_FAILED",
            "COMPLETED",
          ].includes(status)
        )
          return "COMPLETED";
        return "PENDING";
      case "shipping":
        if (["SHIPPING_SCHEDULING"].includes(status)) return "IN_PROGRESS";
        if (["SHIPPING_FAILED"].includes(status)) return "FAILED";
        if (["SHIPPING_SCHEDULED", "COMPLETED"].includes(status))
          return "COMPLETED";
        return "PENDING";
      default:
        return "PENDING";
    }
  };

  return steps.map((step) => ({
    ...step,
    status: getStepStatus(step.id),
  }));
}

function inferFailureStep(order: Order): "payment" | "inventory" | "shipping" | undefined {
  const reason = order.failureReason?.toLowerCase() ?? "";
  if (reason.includes("payment")) return "payment";
  if (reason.includes("inventory")) return "inventory";
  if (reason.includes("shipping")) return "shipping";
  return undefined;
}

export function isTerminalStatus(status: string): boolean {
  return ["COMPLETED", "FAILED", "COMPENSATED", "CANCELLED"].includes(status);
}
