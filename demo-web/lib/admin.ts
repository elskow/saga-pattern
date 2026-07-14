import {
  AdminMetrics,
  DepositBalanceStatus,
  DepositServiceKey,
  FailureModeStatus,
  FailureServiceKey,
  InventoryItem,
  Order,
  Pattern,
  ServiceHealth,
  Shipment,
  ShipmentEvent,
  ShipmentStatus,
} from "@/types";
import { createServerFn } from "@tanstack/react-start";
import { Effect, Either } from "effect";
import { requestJson, getServiceUrl } from "./effect-services";

const SERVICE_PORTS: { name: "order" | "payment" | "inventory" | "shipping"; pattern: Pattern; port: number }[] = [
  { name: "order", pattern: "choreography", port: 8081 },
  { name: "payment", pattern: "choreography", port: 8082 },
  { name: "inventory", pattern: "choreography", port: 8083 },
  { name: "shipping", pattern: "choreography", port: 8084 },
  { name: "order", pattern: "orchestration", port: 8091 },
  { name: "payment", pattern: "orchestration", port: 8092 },
  { name: "inventory", pattern: "orchestration", port: 8093 },
  { name: "shipping", pattern: "orchestration", port: 8094 },
];

const ORDERS_CACHE_TTL_MS = 5_000;
const SERVICE_HEALTH_CACHE_TTL_MS = 5_000;

let cachedOrders: { value: Order[]; expiresAt: number } | null = null;
let inFlightOrders: Promise<Order[]> | null = null;

let cachedServiceHealth: { value: ServiceHealth[]; expiresAt: number } | null = null;
let inFlightServiceHealth: Promise<ServiceHealth[]> | null = null;

const fetchOrdersFromSources = (serverSide: boolean) =>
  Effect.gen(function* () {
    const sources = ["orchestration", "choreography"] as const;

    const results = yield* Effect.all(
      sources.map((pattern) => {
        const url = `${getServiceUrl("order", pattern, serverSide)}/api/orders`;
        return requestJson<Order[]>(url, { cache: "no-store" }).pipe(
          Effect.timeout("3 seconds"),
          Effect.map((data) => {
            if (!Array.isArray(data)) throw new Error("Invalid payload");
            return data.map((o) => ({ ...o, pattern }));
          })
        );
      }),
      { mode: "either" }
    );

    const errors: string[] = [];
    const merged: Order[] = [];
    let successfulSources = 0;

    results.forEach((result) => {
      if (Either.isLeft(result)) {
        errors.push(result.left.message);
        return;
      }

      successfulSources++;
      for (const order of result.right) {
        const orderId = order.id ?? order.orderId;
        const existingIndex = merged.findIndex((o) => (o.id ?? o.orderId) === orderId);
        if (existingIndex >= 0) {
          merged[existingIndex] = { ...merged[existingIndex], ...order };
        } else {
          merged.push(order);
        }
      }
    });

    if (merged.length > 0) return merged;
    if (successfulSources > 0) return [];

    return yield* Effect.fail(new Error(`Live order listing unavailable. ${errors.join("; ")}`));
  });

export const fetchAllOrdersServer = createServerFn({ method: "GET" })
  .handler(async (): Promise<Order[]> => {
    return Effect.runPromise(fetchOrdersFromSources(true));
  });

export async function fetchAllServiceHealth(): Promise<ServiceHealth[]> {
  const now = Date.now();
  if (cachedServiceHealth && cachedServiceHealth.expiresAt > now) {
    return cachedServiceHealth.value;
  }
  if (inFlightServiceHealth) {
    return inFlightServiceHealth;
  }

  inFlightServiceHealth = Effect.runPromise(
    Effect.gen(function* () {
      const results = yield* Effect.all(
        SERVICE_PORTS.map((svc) => {
          const url = `${getServiceUrl(svc.name, svc.pattern, false)}/actuator/health`;
          return Effect.tryPromise({
            try: () => fetch(url, { signal: AbortSignal.timeout(5000) }),
            catch: () => false,
          }).pipe(
            Effect.map((res) => ({ name: svc.name.charAt(0).toUpperCase() + svc.name.slice(1), port: svc.port, pattern: svc.pattern, healthy: typeof res === "boolean" ? false : res.ok })),
            Effect.catchAll(() => Effect.succeed({ name: svc.name.charAt(0).toUpperCase() + svc.name.slice(1), port: svc.port, pattern: svc.pattern, healthy: false }))
          );
        }),
        { concurrency: "unbounded" }
      );

      cachedServiceHealth = {
        value: results,
        expiresAt: Date.now() + SERVICE_HEALTH_CACHE_TTL_MS,
      };

      return results;
    })
  ).finally(() => {
    inFlightServiceHealth = null;
  });

  return inFlightServiceHealth;
}

export const fetchAllServiceHealthServer = createServerFn({ method: "GET" })
  .handler(async (): Promise<ServiceHealth[]> => {
    const program = Effect.all(
      SERVICE_PORTS.map((svc) => {
        const url = `${getServiceUrl(svc.name, svc.pattern, true)}/actuator/health`;
        return Effect.tryPromise({
          try: () => fetch(url, { signal: AbortSignal.timeout(2000), cache: "no-store" }),
          catch: () => false,
        }).pipe(
          Effect.map((res) => ({ name: svc.name.charAt(0).toUpperCase() + svc.name.slice(1), port: svc.port, pattern: svc.pattern, healthy: typeof res === "boolean" ? false : res.ok })),
          Effect.catchAll(() => Effect.succeed({ name: svc.name.charAt(0).toUpperCase() + svc.name.slice(1), port: svc.port, pattern: svc.pattern, healthy: false }))
        );
      }),
      { concurrency: "unbounded" }
    );
    return Effect.runPromise(program);
  });

export async function fetchAllOrders(): Promise<Order[]> {
  const now = Date.now();
  if (cachedOrders && cachedOrders.expiresAt > now) {
    return cachedOrders.value;
  }
  if (inFlightOrders) {
    return inFlightOrders;
  }

  inFlightOrders = Effect.runPromise(fetchOrdersFromSources(false)).then((merged) => {
    cachedOrders = {
      value: merged,
      expiresAt: Date.now() + ORDERS_CACHE_TTL_MS,
    };
    return merged;
  }).finally(() => {
    inFlightOrders = null;
  });

  return inFlightOrders;
}

export function computeMetrics(orders: Order[]): AdminMetrics {
  const total = orders.length;
  const terminalStatuses = ["COMPLETED", "FAILED", "COMPENSATED", "CANCELLED"];
  const completed = orders.filter((o) => o.status === "COMPLETED").length;
  const failed = orders.filter((o) =>
    ["FAILED", "PAYMENT_FAILED", "INVENTORY_FAILED", "SHIPPING_FAILED", "CANCELLED"].includes(o.status)
  ).length;
  const compensated = orders.filter((o) =>
    o.status === "COMPENSATED" || (o.compensatedSteps?.length ?? 0) > 0
  ).length;
  const pending = orders.filter((o) =>
    !terminalStatuses.includes(o.status)
  ).length;

  return {
    totalOrders: total,
    completedOrders: completed,
    failedOrders: failed,
    compensatedOrders: compensated,
    pendingOrders: pending,
    successRate: total > 0 ? Math.round((completed / total) * 100) : 0,
  };
}

interface RawProductChoreo {
  ProductID: string;
  ProductName: string;
  Description?: string;
  Price?: number | string;
  Category?: string;
  Image?: string;
  QuantityAvailable: number;
  QuantityReserved: number;
  Visible?: boolean;
  LastRestockedAt?: string;
  productId?: string;
  name?: string;
  description?: string;
  price?: number | string;
  category?: string;
  image?: string;
  stock?: number;
  reserved?: number;
  available?: number;
}
interface RawProductOrch {
  ProductID: string;
  ProductName: string;
  Description?: string;
  Price?: number | string;
  Category?: string;
  Image?: string;
  Quantity: number;
  ReservedQuantity: number;
  Visible?: boolean;
  LastRestockedAt?: string;
  productId?: string;
  name?: string;
  description?: string;
  price?: number | string;
  category?: string;
  image?: string;
  stock?: number;
  reserved?: number;
  available?: number;
}

function mapInventoryItem(p: RawProductChoreo & RawProductOrch): InventoryItem {
  const total = p.Quantity ?? p.stock ?? ((p.QuantityAvailable ?? 0) + (p.QuantityReserved ?? 0));
  const reserved = p.ReservedQuantity ?? p.QuantityReserved ?? p.reserved ?? 0;
  const available = p.QuantityAvailable ?? p.available ?? (total - reserved);
  const lastRestocked = p.LastRestockedAt && !Number.isNaN(Date.parse(p.LastRestockedAt))
    ? new Date(p.LastRestockedAt).toISOString()
    : new Date(0).toISOString();
  const productId = p.ProductID ?? p.productId;
  const price = p.Price ?? p.price;

  return {
    productId,
    productName: p.ProductName ?? p.name ?? productId,
    sku: `SKU-${productId}`,
    description: p.Description ?? p.description,
    price: typeof price === "string" ? Number(price) : price,
    category: p.Category ?? p.category,
    image: p.Image ?? p.image,
    totalStock: total,
    reserved,
    available,
    reorderPoint: Math.max(1, Math.floor(total * 0.2)),
    lastRestocked,
    visible: p.Visible ?? true,
  };
}

const fetchRealInventory = (pattern: Pattern, serverSide: boolean) =>
  Effect.gen(function* () {
    const url = `${getServiceUrl("inventory", pattern, serverSide)}/api/products`;
    const data = yield* requestJson<any[]>(url, { cache: "no-store" }).pipe(
      Effect.timeout("8 seconds")
    );
    if (!Array.isArray(data)) {
      return yield* Effect.fail(new Error("Invalid payload"));
    }
    return data.map(mapInventoryItem);
  });

export const fetchInventoryServer = createServerFn({ method: "POST" })
  .inputValidator((data: Pattern) => data)
  .handler(async ({ data }: { data: Pattern }): Promise<InventoryItem[]> => {
    return Effect.runPromise(fetchRealInventory(data, true));
  });

export async function fetchInventory(pattern: Pattern): Promise<InventoryItem[]> {
  return Effect.runPromise(fetchRealInventory(pattern, false));
}

export const updateInventoryStockServer = createServerFn({ method: "POST" })
  .inputValidator((data: { pattern: Pattern, productId: string, totalStock: number }) => data)
  .handler(async ({ data }: { data: { pattern: Pattern, productId: string, totalStock: number } }): Promise<InventoryItem> => {
    const program = Effect.gen(function* () {
      const url = `${getServiceUrl("inventory", data.pattern, true)}/api/products/${data.productId}/stock`;
      const res = yield* requestJson<any>(url, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ totalStock: data.totalStock }),
      }).pipe(Effect.timeout("5 seconds"));
      return mapInventoryItem(res);
    });
    return Effect.runPromise(program);
  });

export const updateInventoryVisibilityServer = createServerFn({ method: "POST" })
  .inputValidator((data: { pattern: Pattern, productId: string, visible: boolean }) => data)
  .handler(async ({ data }: { data: { pattern: Pattern, productId: string, visible: boolean } }): Promise<InventoryItem> => {
    const program = Effect.gen(function* () {
      const url = `${getServiceUrl("inventory", data.pattern, true)}/api/products/${data.productId}/visibility`;
      const res = yield* requestJson<any>(url, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ visible: data.visible }),
      }).pipe(Effect.timeout("5 seconds"));
      return mapInventoryItem(res);
    });
    return Effect.runPromise(program);
  });

export async function updateInventoryStock(pattern: Pattern, productId: string, totalStock: number): Promise<InventoryItem> {
  const program = Effect.gen(function* () {
    const url = `${getServiceUrl("inventory", pattern, false)}/api/products/${productId}/stock`;
    const res = yield* requestJson<any>(url, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ totalStock }),
    }).pipe(Effect.timeout("5 seconds"));
    return mapInventoryItem(res);
  });
  return Effect.runPromise(program);
}

export async function updateInventoryVisibility(pattern: Pattern, productId: string, visible: boolean): Promise<InventoryItem> {
  const program = Effect.gen(function* () {
    const url = `${getServiceUrl("inventory", pattern, false)}/api/products/${productId}/visibility`;
    const res = yield* requestJson<any>(url, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ visible }),
    }).pipe(Effect.timeout("5 seconds"));
    return mapInventoryItem(res);
  });
  return Effect.runPromise(program);
}


interface RawShipmentChoreo {
  ShippingID: string;
  OrderID: string;
  TrackingNumber: string;
  ShippingAddress: string;
  Status: string;
  EstimatedDelivery: string;
  CreatedAt: string;
}

interface RawShipmentOrch {
  ShippingID: string;
  OrderID: string;
  ShippingAddress: string;
  Status: string;
  CreatedAt: string;
  ScheduledAt?: string;
}

const CARRIERS = ["FedEx", "UPS", "DHL", "USPS"];
const CITIES = [
  "San Francisco Hub", "Los Angeles Depot", "Chicago Gateway",
  "New York Facility", "Dallas Distribution Center", "Seattle Warehouse",
];

function progressForStatus(status: ShipmentStatus): ShipmentEvent[] {
  const all: { status: ShipmentStatus; desc: string }[] = [
    { status: "LABEL_CREATED",    desc: "Shipping label created" },
    { status: "PICKED_UP",        desc: "Package picked up by carrier" },
    { status: "IN_TRANSIT",       desc: "In transit to destination" },
    { status: "OUT_FOR_DELIVERY", desc: "Out for delivery" },
    { status: "DELIVERED",        desc: "Delivered to recipient" },
  ];
  const idx = all.findIndex((e) => e.status === status);
  const active = idx === -1 ? all.slice(0, 1) : all.slice(0, idx + 1);
  const now = Date.now();
  return active.map((e, i) => ({
    status: e.status,
    location: CITIES[i % CITIES.length],
    description: e.desc,
    timestamp: new Date(now - (active.length - 1 - i) * 6 * 3600000).toISOString(),
  }));
}

function mapBackendStatusToFrontend(backendStatus: string): ShipmentStatus {
  const map: Record<string, ShipmentStatus> = {
    PENDING: "LABEL_CREATED",
    SCHEDULED: "PICKED_UP",
    CANCELLED: "FAILED",
    FAILED: "FAILED",
    LABEL_CREATED: "LABEL_CREATED",
    PICKED_UP: "PICKED_UP",
    IN_TRANSIT: "IN_TRANSIT",
    OUT_FOR_DELIVERY: "OUT_FOR_DELIVERY",
    DELIVERED: "DELIVERED",
  };
  return map[backendStatus] ?? "LABEL_CREATED";
}

const fetchRealShipments = (pattern: Pattern, serverSide: boolean) =>
  Effect.gen(function* () {
    const url = `${getServiceUrl("shipping", pattern, serverSide)}/api/shipments`;
    const data = yield* requestJson<any[] | null>(url, { cache: "no-store" }).pipe(
      Effect.timeout("8 seconds")
    );
    if (data === null) {
      return [];
    }
    if (!Array.isArray(data)) {
      return yield* Effect.fail(new Error("Invalid payload"));
    }
    return data.map((s: RawShipmentChoreo & RawShipmentOrch): Shipment => {
      const feStatus = mapBackendStatusToFrontend(s.Status);
      const carrierSeed = parseInt((s.ShippingID ?? "0000").slice(-4), 16);
      const carrier = CARRIERS[carrierSeed % CARRIERS.length];
      const dayOffset = feStatus === "DELIVERED" ? -1 : 2;
      return {
        shipmentId: s.ShippingID,
        orderId: s.OrderID,
        customerId: "unknown",
        pattern,
        status: feStatus,
        carrier,
        trackingNumber: s.TrackingNumber ?? `${carrier.toUpperCase().slice(0, 3)}${s.ShippingID.slice(0, 8).toUpperCase()}`,
        estimatedDelivery: new Date(Date.now() + dayOffset * 86400000).toISOString(),
        shippingAddress: s.ShippingAddress,
        items: [],
        events: progressForStatus(feStatus),
        createdAt: s.CreatedAt,
      };
    });
  });

export const fetchRealShipmentsServer = createServerFn({ method: "POST" })
  .inputValidator((data: Pattern) => data)
  .handler(async ({ data }: { data: Pattern }): Promise<Shipment[]> => {
    return Effect.runPromise(fetchRealShipments(data, true));
  });

export const fetchShipmentsServer = createServerFn({ method: "GET" })
  .handler(async (): Promise<Shipment[]> => {
    const program = Effect.gen(function* () {
      const patterns: Pattern[] = ["orchestration", "choreography"];
      const results = yield* Effect.all(
        patterns.map((p) => fetchRealShipments(p, true)),
        { mode: "either" }
      );

      const errors: string[] = [];
      const shipments: Shipment[] = [];
      let successfulSources = 0;

      results.forEach((result) => {
        if (Either.isRight(result)) {
          successfulSources++;
          shipments.push(...result.right);
        } else {
          errors.push(result.left.message);
        }
      });

      if (shipments.length > 0) return shipments;
      if (successfulSources > 0) return [];
      return yield* Effect.fail(new Error(`Shipment data unavailable. ${errors.join("; ")}`));
    });

    return Effect.runPromise(program);
  });

export async function fetchShipments(): Promise<Shipment[]> {
  const program = Effect.gen(function* () {
    const patterns: Pattern[] = ["orchestration", "choreography"];
    const results = yield* Effect.all(
      patterns.map((p) => fetchRealShipments(p, false)),
      { mode: "either" }
    );
    
    const errors: string[] = [];
    const shipments: Shipment[] = [];
    let successfulSources = 0;
    
    results.forEach((result) => {
      if (Either.isRight(result)) {
        successfulSources++;
        shipments.push(...result.right);
      } else {
        errors.push(result.left.message);
      }
    });

    if (shipments.length > 0) return shipments;
    if (successfulSources > 0) return [];
    return yield* Effect.fail(new Error(`Shipment data unavailable. ${errors.join("; ")}`));
  });

  return Effect.runPromise(program);
}

export const updateShipmentStatusServer = createServerFn({ method: "POST" })
  .inputValidator((data: { pattern: Pattern, shippingId: string, status: string }) => data)
  .handler(async ({ data }: { data: { pattern: Pattern, shippingId: string, status: string } }): Promise<void> => {
    const program = Effect.gen(function* () {
      const url = `${getServiceUrl("shipping", data.pattern, true)}/api/shipments/${data.shippingId}/status`;
      yield* requestJson<any>(url, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: data.status }),
      }).pipe(Effect.timeout("5 seconds"));
    });
    return Effect.runPromise(program);
  });

export async function updateShipmentStatus(pattern: Pattern, shippingId: string, status: string): Promise<void> {
  const program = Effect.gen(function* () {
    const url = `${getServiceUrl("shipping", pattern, false)}/api/shipments/${shippingId}/status`;
    yield* requestJson<any>(url, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status }),
    }).pipe(Effect.timeout("5 seconds"));
  });
  return Effect.runPromise(program);
}


export async function findShipmentByTracking(
  trackingId: string,
  shipmentId: string | null,
  orderId: string | null,
  pattern: Pattern | null
): Promise<Shipment | null> {
  const shipments = await fetchShipments();
  const scoped = pattern ? shipments.filter((shipment) => shipment.pattern === pattern) : shipments;
  return (
    scoped.find((s) => s.trackingNumber === trackingId) ??
    scoped.find((s) => shipmentId != null && s.shipmentId === shipmentId) ??
    scoped.find((s) => orderId != null && s.orderId === orderId) ??
    null
  );
}

export const findShipmentByTrackingServer = createServerFn({ method: "POST" })
  .inputValidator((data: { trackingId: string, shipmentId: string | null, orderId: string | null, pattern: Pattern | null }) => data)
  .handler(async ({ data }: { data: { trackingId: string, shipmentId: string | null, orderId: string | null, pattern: Pattern | null } }): Promise<Shipment | null> => {
    const program = Effect.gen(function* () {
      const patterns: Pattern[] = ["orchestration", "choreography"];
      const results = yield* Effect.all(
        patterns.map((p) => fetchRealShipments(p, true)),
        { mode: "either" }
      );
      
      const shipments: Shipment[] = [];
      results.forEach((result) => {
        if (Either.isRight(result)) shipments.push(...result.right);
      });

      const scoped = data.pattern ? shipments.filter((shipment) => shipment.pattern === data.pattern) : shipments;
      return (
        scoped.find((s) => s.trackingNumber === data.trackingId) ??
        scoped.find((s) => data.shipmentId != null && s.shipmentId === data.shipmentId) ??
        scoped.find((s) => data.orderId != null && s.orderId === data.orderId) ??
        null
      );
    });

    return Effect.runPromise(program);
  });

const FAILURE_SERVICE_PROXY_PATHS: Record<FailureServiceKey, string> = {
  "choreography-inventory": "inventory",
  "choreography-shipping": "shipping",
  "orchestration-inventory": "inventory",
  "orchestration-shipping": "shipping",
};

const FAILURE_SERVICE_LABELS: Record<FailureServiceKey, string> = {
  "choreography-inventory": "Inventory (Choreography)",
  "choreography-shipping": "Shipping (Choreography)",
  "orchestration-inventory": "Inventory (Orchestration)",
  "orchestration-shipping": "Shipping (Orchestration)",
};

export async function fetchFailureMode(
  key: FailureServiceKey
): Promise<FailureModeStatus> {
  const program = Effect.gen(function* () {
    const pattern = key.startsWith("choreography") ? "choreography" : "orchestration";
    const svc = FAILURE_SERVICE_PROXY_PATHS[key] as any;
    const url = `${getServiceUrl(svc, pattern, false)}/api/admin/failure-mode`;
    return yield* requestJson<FailureModeStatus>(url, { cache: "no-store" }).pipe(
      Effect.timeout("5 seconds")
    );
  }).pipe(Effect.catchAll(() => Effect.succeed({ enabled: false })));

  return Effect.runPromise(program);
}

export const fetchFailureModeServer = createServerFn({ method: "POST" })
  .inputValidator((data: FailureServiceKey) => data)
  .handler(async ({ data }: { data: FailureServiceKey }): Promise<FailureModeStatus> => {
    const program = Effect.gen(function* () {
      const pattern = data.startsWith("choreography") ? "choreography" : "orchestration";
      const svc = FAILURE_SERVICE_PROXY_PATHS[data] as any;
      const url = `${getServiceUrl(svc, pattern, true)}/api/admin/failure-mode`;
      return yield* requestJson<FailureModeStatus>(url, { cache: "no-store" }).pipe(
        Effect.timeout("5 seconds")
      );
    }).pipe(Effect.catchAll(() => Effect.succeed({ enabled: false })));

    return Effect.runPromise(program);
  });

export async function setFailureMode(
  key: FailureServiceKey,
  enabled: boolean
): Promise<FailureModeStatus> {
  const program = Effect.gen(function* () {
    const pattern = key.startsWith("choreography") ? "choreography" : "orchestration";
    const svc = FAILURE_SERVICE_PROXY_PATHS[key] as any;
    const url = `${getServiceUrl(svc, pattern, false)}/api/admin/failure-mode`;
    return yield* requestJson<FailureModeStatus>(url, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled }),
    }).pipe(Effect.timeout("5 seconds"));
  });

  return Effect.runPromise(program);
}

export const setFailureModeServer = createServerFn({ method: "POST" })
  .inputValidator((data: { key: FailureServiceKey; enabled: boolean }) => data)
  .handler(async ({ data }: { data: { key: FailureServiceKey; enabled: boolean } }): Promise<FailureModeStatus> => {
    const program = Effect.gen(function* () {
      const pattern = data.key.startsWith("choreography") ? "choreography" : "orchestration";
      const svc = FAILURE_SERVICE_PROXY_PATHS[data.key] as any;
      const url = `${getServiceUrl(svc, pattern, true)}/api/admin/failure-mode`;
      return yield* requestJson<FailureModeStatus>(url, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: data.enabled }),
      }).pipe(Effect.timeout("5 seconds"));
    });

    return Effect.runPromise(program);
  });

export const FAILURE_SERVICE_LABELS_MAP: Record<FailureServiceKey, string> = FAILURE_SERVICE_LABELS;
export const FAILURE_SERVICE_KEYS: FailureServiceKey[] = Object.keys(FAILURE_SERVICE_LABELS) as FailureServiceKey[];

const DEPOSIT_SERVICE_LABELS: Record<DepositServiceKey, string> = {
  "choreography-payment": "Payment (Choreography)",
  "orchestration-payment": "Payment (Orchestration)",
};

export const fetchDepositBalancesServer = createServerFn({ method: "GET" })
  .handler(async (): Promise<Record<string, DepositBalanceStatus>> => {
    const program = Effect.gen(function* () {
      const results = yield* Effect.all(
        DEPOSIT_SERVICE_KEYS.map((key) => {
          const pattern = key.startsWith("choreography") ? "choreography" : "orchestration";
          const url = `${getServiceUrl("payment", pattern, true)}/api/admin/deposit-balance`;
          return requestJson<DepositBalanceStatus>(url, { cache: "no-store" }).pipe(
            Effect.timeout("2 seconds"),
            Effect.map((status) => ({ key, status })),
            Effect.catchAll(() => Effect.succeed({ key, status: { balance: "", unlimited: true } }))
          );
        }),
        { concurrency: "unbounded" }
      );

      const balances: Record<string, DepositBalanceStatus> = {};
      for (const res of results) {
        balances[res.key] = res.status;
      }
      return balances;
    });

    return Effect.runPromise(program);
  });

export const cancelOrderServer = createServerFn({ method: "POST" })
  .inputValidator((data: { pattern: Pattern; orderId: string }) => data)
  .handler(async ({ data }: { data: { pattern: Pattern; orderId: string } }): Promise<void> => {
    const program = Effect.gen(function* () {
      const url = `${getServiceUrl("order", data.pattern, true)}/api/orders/${data.orderId}/cancel`;
      yield* requestJson<any>(url, {
        method: "POST",
      }).pipe(Effect.timeout("5 seconds"));
    });
    return Effect.runPromise(program);
  });

export async function fetchDepositBalance(
  key: DepositServiceKey
): Promise<DepositBalanceStatus> {
  const program = Effect.gen(function* () {
    const pattern = key.startsWith("choreography") ? "choreography" : "orchestration";
    const url = `${getServiceUrl("payment", pattern, false)}/api/admin/deposit-balance`;
    return yield* requestJson<DepositBalanceStatus>(url, { cache: "no-store" }).pipe(
      Effect.timeout("5 seconds")
    );
  }).pipe(Effect.catchAll(() => Effect.succeed({ balance: "", unlimited: true })));

  return Effect.runPromise(program);
}

export const fetchDepositBalanceServer = createServerFn({ method: "POST" })
  .inputValidator((data: DepositServiceKey) => data)
  .handler(async ({ data }: { data: DepositServiceKey }): Promise<DepositBalanceStatus> => {
    const program = Effect.gen(function* () {
      const pattern = data.startsWith("choreography") ? "choreography" : "orchestration";
      const url = `${getServiceUrl("payment", pattern, true)}/api/admin/deposit-balance`;
      return yield* requestJson<DepositBalanceStatus>(url, { cache: "no-store" }).pipe(
        Effect.timeout("5 seconds")
      );
    }).pipe(Effect.catchAll(() => Effect.succeed({ balance: "", unlimited: true })));

    return Effect.runPromise(program);
  });

export async function setDepositBalance(
  key: DepositServiceKey,
  balance: string | null
): Promise<DepositBalanceStatus> {
  const program = Effect.gen(function* () {
    const pattern = key.startsWith("choreography") ? "choreography" : "orchestration";
    const url = `${getServiceUrl("payment", pattern, false)}/api/admin/deposit-balance`;
    const body = balance === null || balance === "" ? { balance: null } : { balance };
    return yield* requestJson<DepositBalanceStatus>(url, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).pipe(Effect.timeout("5 seconds"));
  });

  return Effect.runPromise(program);
}

export const setDepositBalanceServer = createServerFn({ method: "POST" })
  .inputValidator((data: { key: DepositServiceKey; balance: string | null }) => data)
  .handler(async ({ data }: { data: { key: DepositServiceKey; balance: string | null } }): Promise<DepositBalanceStatus> => {
    const program = Effect.gen(function* () {
      const pattern = data.key.startsWith("choreography") ? "choreography" : "orchestration";
      const url = `${getServiceUrl("payment", pattern, true)}/api/admin/deposit-balance`;
      const body = data.balance === null || data.balance === "" ? { balance: null } : { balance: data.balance };
      return yield* requestJson<DepositBalanceStatus>(url, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }).pipe(Effect.timeout("5 seconds"));
    });

    return Effect.runPromise(program);
  });

export const DEPOSIT_SERVICE_LABELS_MAP: Record<DepositServiceKey, string> = DEPOSIT_SERVICE_LABELS;
export const DEPOSIT_SERVICE_KEYS: DepositServiceKey[] = Object.keys(DEPOSIT_SERVICE_LABELS) as DepositServiceKey[];

export interface CreateProductPayload {
  name: string;
  description: string;
  price: string; // numeric string e.g. "19999000"
  category: string;
  image: string;
  choreographyStock: number;
  orchestrationStock: number;
}

export interface UpdateProductMetaPayload {
  productId: string;
  name: string;
  description: string;
  price: string;
  category: string;
  image: string;
}

export const createProductServer = createServerFn({ method: "POST" })
  .inputValidator((data: CreateProductPayload) => data)
  .handler(async ({ data }: { data: CreateProductPayload }): Promise<{ choreography: InventoryItem; orchestration: InventoryItem }> => {
    const program = Effect.gen(function* () {
      const [choreoResult, orchResult] = yield* Effect.all(
        [
          requestJson<any>(
            `${getServiceUrl("inventory", "choreography", true)}/api/products`,
            {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                name: data.name,
                description: data.description,
                price: data.price,
                category: data.category,
                image: data.image,
                stock: data.choreographyStock,
              }),
            }
          ).pipe(Effect.timeout("8 seconds")),
          requestJson<any>(
            `${getServiceUrl("inventory", "orchestration", true)}/api/products`,
            {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                name: data.name,
                description: data.description,
                price: data.price,
                category: data.category,
                image: data.image,
                stock: data.orchestrationStock,
              }),
            }
          ).pipe(Effect.timeout("8 seconds")),
        ],
        { concurrency: 2 }
      );
      return {
        choreography: mapInventoryItem(choreoResult as any),
        orchestration: mapInventoryItem(orchResult as any),
      };
    });
    return Effect.runPromise(program);
  });

export const updateProductMetaServer = createServerFn({ method: "POST" })
  .inputValidator((data: UpdateProductMetaPayload) => data)
  .handler(async ({ data }: { data: UpdateProductMetaPayload }): Promise<{ choreography: InventoryItem; orchestration: InventoryItem }> => {
    const body = JSON.stringify({
      name: data.name,
      description: data.description,
      price: data.price,
      category: data.category,
      image: data.image,
    });
    const program = Effect.gen(function* () {
      const [choreoResult, orchResult] = yield* Effect.all(
        [
          requestJson<any>(
            `${getServiceUrl("inventory", "choreography", true)}/api/products/${data.productId}`,
            { method: "PATCH", headers: { "Content-Type": "application/json" }, body }
          ).pipe(Effect.timeout("8 seconds")),
          requestJson<any>(
            `${getServiceUrl("inventory", "orchestration", true)}/api/products/${data.productId}`,
            { method: "PATCH", headers: { "Content-Type": "application/json" }, body }
          ).pipe(Effect.timeout("8 seconds")),
        ],
        { concurrency: 2 }
      );
      return {
        choreography: mapInventoryItem(choreoResult as any),
        orchestration: mapInventoryItem(orchResult as any),
      };
    });
    return Effect.runPromise(program);
  });

export const deleteProductServer = createServerFn({ method: "POST" })
  .inputValidator((data: { productId: string }) => data)
  .handler(async ({ data }: { data: { productId: string } }): Promise<void> => {
    const program = Effect.gen(function* () {
      yield* Effect.all(
        [
          requestJson<any>(
            `${getServiceUrl("inventory", "choreography", true)}/api/products/${data.productId}`,
            { method: "DELETE" }
          ).pipe(Effect.timeout("8 seconds")),
          requestJson<any>(
            `${getServiceUrl("inventory", "orchestration", true)}/api/products/${data.productId}`,
            { method: "DELETE" }
          ).pipe(Effect.timeout("8 seconds")),
        ],
        { concurrency: 2 }
      );
    });
    return Effect.runPromise(program);
  });
