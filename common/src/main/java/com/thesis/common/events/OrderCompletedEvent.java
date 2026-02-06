package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record OrderCompletedEvent(
    @NotBlank(message = "Order ID cannot be blank")
    String orderId,

    @NotNull(message = "Completed at cannot be null")
    Instant completedAt,

    @NotBlank(message = "Correlation ID cannot be blank")
    String correlationId
) implements ChoreographyEvent {
    @Override
    public Instant createdAt() {
        return completedAt;
    }

    public static OrderCompletedEvent of(String orderId, Instant completedAt, String correlationId) {
        return new OrderCompletedEvent(orderId, completedAt, correlationId);
    }
}
