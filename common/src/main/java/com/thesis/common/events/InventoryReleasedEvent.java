package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record InventoryReleasedEvent(
    @NotBlank(message = "Reservation ID cannot be blank")
    String reservationId,

    @NotBlank(message = "Order ID cannot be blank")
    String orderId,

    @NotNull(message = "Released at cannot be null")
    Instant releasedAt,

    @NotBlank(message = "Correlation ID cannot be blank")
    String correlationId,

    Instant createdAt
) implements ChoreographyEvent {
    public InventoryReleasedEvent {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public static InventoryReleasedEvent of(String reservationId, String orderId, Instant releasedAt, String correlationId) {
        return new InventoryReleasedEvent(reservationId, orderId, releasedAt, correlationId, Instant.now());
    }
}
