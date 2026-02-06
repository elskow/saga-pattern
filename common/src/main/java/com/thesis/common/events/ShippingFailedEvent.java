package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record ShippingFailedEvent(
    @NotBlank(message = "Order ID cannot be blank")
    String orderId,

    @NotBlank(message = "Reason cannot be blank")
    String reason,

    @NotNull(message = "Failed at cannot be null")
    Instant failedAt,

    @NotBlank(message = "Correlation ID cannot be blank")
    String correlationId,

    Instant createdAt
) implements ChoreographyEvent {
    public ShippingFailedEvent {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public static ShippingFailedEvent of(String orderId, String reason, Instant failedAt, String correlationId) {
        return new ShippingFailedEvent(orderId, reason, failedAt, correlationId, Instant.now());
    }
}
