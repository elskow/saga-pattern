package com.thesis.common.command;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record ProcessPaymentCommand(
    @NotBlank(message = "Command type cannot be blank")
    String commandType,

    @NotNull(message = "Payment ID cannot be null")
    String paymentId,

    @NotNull(message = "Order ID cannot be null")
    String orderId,

    @NotNull(message = "Customer ID cannot be null")
    String customerId,

    @NotNull(message = "Amount cannot be null")
    @Positive(message = "Amount must be positive")
    BigDecimal amount
) {
    public static ProcessPaymentCommand of(String commandType, String paymentId, String orderId,
                                           String customerId, BigDecimal amount) {
        return new ProcessPaymentCommand(commandType, paymentId, orderId, customerId, amount);
    }
}
