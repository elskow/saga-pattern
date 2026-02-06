package com.thesis.common.command;

import com.thesis.common.events.OrderCreatedEvent;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record ReserveInventoryCommand(
    @NotBlank(message = "Command type cannot be blank")
    String commandType,

    @NotNull(message = "Reservation ID cannot be null")
    String reservationId,

    @NotNull(message = "Order ID cannot be null")
    String orderId,

    @NotNull(message = "Items cannot be null")
    @NotEmpty(message = "Items cannot be empty")
    List<OrderCreatedEvent.OrderItemEvent> items
) {
    public static ReserveInventoryCommand of(String commandType, String reservationId,
                                              String orderId, List<OrderCreatedEvent.OrderItemEvent> items) {
        return new ReserveInventoryCommand(commandType, reservationId, orderId, items);
    }
}
