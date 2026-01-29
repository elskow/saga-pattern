package com.thesis.common.events;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReservedEvent {
    @NotBlank(message = "Reservation ID cannot be blank")
    private String reservationId;

    @NotBlank(message = "Order ID cannot be blank")
    private String orderId;

    @NotEmpty(message = "Reserved items list cannot be empty")
    @Valid
    private List<ReservedItem> reservedItems;

    @NotNull(message = "Reserved at cannot be null")
    private Instant reservedAt;

    @NotBlank(message = "Correlation ID cannot be blank")
    private String correlationId;

    @Builder.Default
    private Instant createdAt = Instant.now();

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReservedItem {
        @NotBlank(message = "Product ID cannot be blank")
        private String productId;

        @Positive(message = "Quantity must be positive")
        private int quantity;
    }
}
