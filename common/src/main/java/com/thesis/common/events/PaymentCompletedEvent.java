package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentCompletedEvent(
    @NotBlank(message = "Payment ID cannot be blank")
    String paymentId,

    @NotBlank(message = "Order ID cannot be blank")
    String orderId,

    @NotNull(message = "Amount cannot be null")
    @Positive(message = "Amount must be positive")
    BigDecimal amount,

    @NotBlank(message = "Transaction ID cannot be blank")
    String transactionId,

    @NotNull(message = "Completed at cannot be null")
    Instant completedAt,

    @NotBlank(message = "Correlation ID cannot be blank")
    String correlationId,

    Instant createdAt
) implements ChoreographyEvent {
    public PaymentCompletedEvent {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public static PaymentCompletedEvent of(String paymentId, String orderId, BigDecimal amount,
                                           String transactionId, Instant completedAt, String correlationId) {
        return new PaymentCompletedEvent(paymentId, orderId, amount, transactionId, completedAt, correlationId, Instant.now());
    }
}
