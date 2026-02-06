package com.thesis.common.command;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record ScheduleShippingCommand(
    @NotBlank(message = "Command type cannot be blank")
    String commandType,

    @NotNull(message = "Shipping ID cannot be null")
    String shippingId,

    @NotNull(message = "Order ID cannot be null")
    String orderId,

    @NotNull(message = "Shipping address cannot be null")
    @NotBlank(message = "Shipping address cannot be blank")
    String shippingAddress
) {
    public static ScheduleShippingCommand of(String commandType, String shippingId,
                                              String orderId, String shippingAddress) {
        return new ScheduleShippingCommand(commandType, shippingId, orderId, shippingAddress);
    }
}
