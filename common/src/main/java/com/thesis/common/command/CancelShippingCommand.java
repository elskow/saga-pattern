package com.thesis.common.command;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CancelShippingCommand(
    @NotBlank(message = "Command type cannot be blank")
    String commandType,

    @NotNull(message = "Shipping ID cannot be null")
    String shippingId,

    @NotNull(message = "Order ID cannot be null")
    String orderId
) {
    public static CancelShippingCommand of(String commandType, String shippingId, String orderId) {
        return new CancelShippingCommand(commandType, shippingId, orderId);
    }
}
