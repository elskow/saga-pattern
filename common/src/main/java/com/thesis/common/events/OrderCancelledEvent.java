package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record OrderCancelledEvent(
    @NotBlank(message = "Order ID cannot be blank")
    String orderId,

    @NotBlank(message = "Reason cannot be blank")
    String reason,

    @NotNull(message = "Cancelled at cannot be null")
    Instant cancelledAt,

    @NotBlank(message = "Correlation ID cannot be blank")
    String correlationId
) implements ChoreographyEvent {
    @Override
    public Instant createdAt() {
        return cancelledAt;
    }

    public static OrderCancelledEvent of(String orderId, String reason, Instant cancelledAt, String correlationId) {
        return new OrderCancelledEvent(orderId, reason, cancelledAt, correlationId);
    }
}
