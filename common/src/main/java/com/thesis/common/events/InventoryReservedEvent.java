package com.thesis.common.events;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.Instant;
import java.util.List;

public record InventoryReservedEvent(
    @NotBlank(message = "Reservation ID cannot be blank")
    String reservationId,

    @NotBlank(message = "Order ID cannot be blank")
    String orderId,

    @NotEmpty(message = "Reserved items list cannot be empty")
    @Valid
    List<ReservedItem> reservedItems,

    @NotNull(message = "Reserved at cannot be null")
    Instant reservedAt,

    @NotBlank(message = "Correlation ID cannot be blank")
    String correlationId,

    Instant createdAt
) implements ChoreographyEvent {
    public InventoryReservedEvent {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public record ReservedItem(
        @NotBlank(message = "Product ID cannot be blank")
        String productId,

        @Positive(message = "Quantity must be positive")
        int quantity
    ) {
        public static ReservedItem of(String productId, int quantity) {
            return new ReservedItem(productId, quantity);
        }
    }

    public static InventoryReservedEvent of(String reservationId, String orderId, List<ReservedItem> reservedItems,
                                            Instant reservedAt, String correlationId) {
        return new InventoryReservedEvent(reservationId, orderId, reservedItems, reservedAt, correlationId, Instant.now());
    }
}
