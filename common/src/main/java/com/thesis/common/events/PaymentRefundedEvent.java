package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentRefundedEvent(
    @NotBlank(message = "Payment ID cannot be blank")
    String paymentId,

    @NotBlank(message = "Order ID cannot be blank")
    String orderId,

    @NotNull(message = "Refund amount cannot be null")
    @Positive(message = "Refund amount must be positive")
    BigDecimal refundAmount,

    @NotNull(message = "Refunded at cannot be null")
    Instant refundedAt,

    @NotBlank(message = "Correlation ID cannot be blank")
    String correlationId,

    Instant createdAt
) implements ChoreographyEvent {
    public PaymentRefundedEvent {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public static PaymentRefundedEvent of(String paymentId, String orderId, BigDecimal refundAmount,
                                          Instant refundedAt, String correlationId) {
        return new PaymentRefundedEvent(paymentId, orderId, refundAmount, refundedAt, correlationId, Instant.now());
    }
}
