package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record PaymentFailedEvent(
    String paymentId,

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
    public PaymentFailedEvent {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public static PaymentFailedEvent of(String paymentId, String orderId, String reason,
                                        Instant failedAt, String correlationId) {
        return new PaymentFailedEvent(paymentId, orderId, reason, failedAt, correlationId, Instant.now());
    }
}
