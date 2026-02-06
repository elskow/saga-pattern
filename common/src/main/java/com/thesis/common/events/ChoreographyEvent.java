package com.thesis.common.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = OrderCreatedEvent.class, name = "ORDER_CREATED"),
    @JsonSubTypes.Type(value = OrderCompletedEvent.class, name = "ORDER_COMPLETED"),
    @JsonSubTypes.Type(value = OrderCancelledEvent.class, name = "ORDER_CANCELLED"),
    @JsonSubTypes.Type(value = PaymentCompletedEvent.class, name = "PAYMENT_COMPLETED"),
    @JsonSubTypes.Type(value = PaymentFailedEvent.class, name = "PAYMENT_FAILED"),
    @JsonSubTypes.Type(value = PaymentRefundedEvent.class, name = "PAYMENT_REFUNDED"),
    @JsonSubTypes.Type(value = InventoryReservedEvent.class, name = "INVENTORY_RESERVED"),
    @JsonSubTypes.Type(value = InventoryReservationFailedEvent.class, name = "INVENTORY_RESERVATION_FAILED"),
    @JsonSubTypes.Type(value = InventoryReleasedEvent.class, name = "INVENTORY_RELEASED"),
    @JsonSubTypes.Type(value = ShippingScheduledEvent.class, name = "SHIPPING_SCHEDULED"),
    @JsonSubTypes.Type(value = ShippingFailedEvent.class, name = "SHIPPING_FAILED"),
    @JsonSubTypes.Type(value = ShippingCancelledEvent.class, name = "SHIPPING_CANCELLED")
})
public sealed interface ChoreographyEvent permits
    OrderCreatedEvent, OrderCompletedEvent, OrderCancelledEvent,
    PaymentCompletedEvent, PaymentFailedEvent, PaymentRefundedEvent,
    InventoryReservedEvent, InventoryReservationFailedEvent, InventoryReleasedEvent,
    ShippingScheduledEvent, ShippingFailedEvent, ShippingCancelledEvent {

    String orderId();
    String correlationId();
    Instant createdAt();
}
