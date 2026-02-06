package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record InventoryReservationFailedEvent(
    @NotBlank(message = "Order ID cannot be blank")
    String orderId,

    String productId,

    @NotBlank(message = "Reason cannot be blank")
    String reason,

    @NotNull(message = "Failed at cannot be null")
    Instant failedAt,

    @NotBlank(message = "Correlation ID cannot be blank")
    String correlationId,

    Instant createdAt
) implements ChoreographyEvent {
    public InventoryReservationFailedEvent {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public static InventoryReservationFailedEvent of(String orderId, String productId, String reason,
                                                     Instant failedAt, String correlationId) {
        return new InventoryReservationFailedEvent(orderId, productId, reason, failedAt, correlationId, Instant.now());
    }
}
