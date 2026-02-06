package com.thesis.common.command;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record RefundPaymentCommand(
    @NotBlank(message = "Command type cannot be blank")
    String commandType,

    @NotNull(message = "Payment ID cannot be null")
    String paymentId,

    @NotNull(message = "Order ID cannot be null")
    String orderId
) {
    public static RefundPaymentCommand of(String commandType, String paymentId, String orderId) {
        return new RefundPaymentCommand(commandType, paymentId, orderId);
    }
}
