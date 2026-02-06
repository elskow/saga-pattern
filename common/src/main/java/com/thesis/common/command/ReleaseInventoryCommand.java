package com.thesis.common.command;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record ReleaseInventoryCommand(
    @NotBlank(message = "Command type cannot be blank")
    String commandType,

    @NotNull(message = "Reservation ID cannot be null")
    String reservationId,

    @NotNull(message = "Order ID cannot be null")
    String orderId
) {
    public static ReleaseInventoryCommand of(String commandType, String reservationId, String orderId) {
        return new ReleaseInventoryCommand(commandType, reservationId, orderId);
    }
}
