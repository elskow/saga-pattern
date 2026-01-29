package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentCompletedEvent {
    @NotBlank(message = "Payment ID cannot be blank")
    private String paymentId;

    @NotBlank(message = "Order ID cannot be blank")
    private String orderId;

    @NotNull(message = "Amount cannot be null")
    @Positive(message = "Amount must be positive")
    private BigDecimal amount;

    @NotBlank(message = "Transaction ID cannot be blank")
    private String transactionId;

    @NotNull(message = "Completed at cannot be null")
    private Instant completedAt;

    @NotBlank(message = "Correlation ID cannot be blank")
    private String correlationId;

    @Builder.Default
    private Instant createdAt = Instant.now();
}
