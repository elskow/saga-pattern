export type Pattern = "choreography" | "orchestration";

export type FailureServiceKey =
  | "choreography-inventory"
  | "choreography-shipping"
  | "orchestration-inventory"
  | "orchestration-shipping";

export interface FailureModeStatus {
  enabled: boolean;
}

export interface InventoryItem {
  productId: string;
  productName: string;
  sku: string;
  description?: string;
  price?: number;
  category?: string;
  image?: string;
  totalStock: number;
  reserved: number;
  available: number;
  reorderPoint: number;
  lastRestocked: string;
  visible?: boolean;
}

export type ShipmentStatus =
  | "LABEL_CREATED"
  | "PICKED_UP"
  | "IN_TRANSIT"
  | "OUT_FOR_DELIVERY"
  | "DELIVERED"
  | "FAILED";

export interface Shipment {
  shipmentId: string;
  orderId: string;
  customerId: string;
  pattern: Pattern;
  status: ShipmentStatus;
  carrier: string;
  trackingNumber: string;
  estimatedDelivery: string;
  shippingAddress: string;
  items: { productName: string; quantity: number }[];
  events: ShipmentEvent[];
  createdAt: string;
}

export interface ShipmentEvent {
  status: ShipmentStatus;
  location: string;
  description: string;
  timestamp: string;
}

export interface ServiceHealth {
  name: string;
  port: number;
  pattern: Pattern;
  healthy: boolean;
}

export interface AdminMetrics {
  totalOrders: number;
  completedOrders: number;
  failedOrders: number;
  compensatedOrders: number;
  pendingOrders: number;
  successRate: number;
}

export interface Product {
  id: string;
  name: string;
  description: string;
  price: number;
  image: string;
  category: string;
  stock: number;
  productId: string; // matches backend PROD-xxx format
}

export interface CatalogProduct extends Product {
  reserved: number;
  available: number;
}

export interface CartItem {
  product: Product;
  quantity: number;
}

export interface OrderItem {
  productId: string;
  productName: string;
  quantity: number;
  price: number;
}

export interface CreateOrderPayload {
  customerId: string;
  shippingAddress: string;
  items: OrderItem[];
  totalAmount?: number; // required for orchestration, omitted for choreography
}

export type SagaStepStatus = "PENDING" | "IN_PROGRESS" | "COMPLETED" | "FAILED" | "COMPENSATING" | "COMPENSATED";

export type OrderStatus =
  | "PENDING"
  | "SAGA_STARTED"
  | "PAYMENT_PROCESSING"
  | "PAYMENT_COMPLETED"
  | "PAYMENT_FAILED"
  | "INVENTORY_RESERVING"
  | "INVENTORY_RESERVED"
  | "INVENTORY_FAILED"
  | "SHIPPING_SCHEDULING"
  | "SHIPPING_SCHEDULED"
  | "SHIPPING_FAILED"
  | "COMPLETED"
  | "FAILED"
  | "COMPENSATING"
  | "COMPENSATED"
  | "CANCELLED";

export interface SagaStep {
  id: string;
  name: string;
  status: SagaStepStatus;
  startedAt?: string;
  completedAt?: string;
  error?: string;
}

export interface Order {
  id: string;
  orderId: string;
  customerId: string;
  status: OrderStatus;
  totalAmount?: number;
  shippingAddress: string;
  items: OrderItem[];
  createdAt: string;
  updatedAt?: string;
  paymentId?: string;
  reservationId?: string;
  shippingId?: string;
  trackingNumber?: string;
  failureReason?: string;
  failureStep?: "payment" | "inventory" | "shipping";
  compensatedSteps?: Array<"payment" | "inventory" | "shipping">;
  pattern: Pattern;
}

export interface BackendStatus {
  choreography: boolean;
  orchestration: boolean;
}

export interface DepositBalanceStatus {
  balance: string;
  unlimited: boolean;
}

export type DepositServiceKey =
  | "choreography-payment"
  | "orchestration-payment";
