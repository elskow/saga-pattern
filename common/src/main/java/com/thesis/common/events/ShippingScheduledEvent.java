package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record ShippingScheduledEvent(
    @NotBlank(message = "Shipping ID cannot be blank")
    String shippingId,

    @NotBlank(message = "Order ID cannot be blank")
    String orderId,

    @NotBlank(message = "Tracking number cannot be blank")
    String trackingNumber,

    @NotBlank(message = "Address cannot be blank")
    String address,

    @NotNull(message = "Estimated delivery cannot be null")
    Instant estimatedDelivery,

    @NotNull(message = "Scheduled at cannot be null")
    Instant scheduledAt,

    @NotBlank(message = "Correlation ID cannot be blank")
    String correlationId,

    Instant createdAt
) implements ChoreographyEvent {
    public ShippingScheduledEvent {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public static ShippingScheduledEvent of(String shippingId, String orderId, String trackingNumber,
                                            String address, Instant estimatedDelivery, Instant scheduledAt,
                                            String correlationId) {
        return new ShippingScheduledEvent(shippingId, orderId, trackingNumber, address, estimatedDelivery,
                                          scheduledAt, correlationId, Instant.now());
    }
}
