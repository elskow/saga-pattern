package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record ShippingCancelledEvent(
    @NotBlank(message = "Shipping ID cannot be blank")
    String shippingId,

    @NotBlank(message = "Order ID cannot be blank")
    String orderId,

    @NotNull(message = "Cancelled at cannot be null")
    Instant cancelledAt,

    @NotBlank(message = "Correlation ID cannot be blank")
    String correlationId,

    Instant createdAt
) implements ChoreographyEvent {
    public ShippingCancelledEvent {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public static ShippingCancelledEvent of(String shippingId, String orderId, Instant cancelledAt, String correlationId) {
        return new ShippingCancelledEvent(shippingId, orderId, cancelledAt, correlationId, Instant.now());
    }
}
