package com.thesis.common.events;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderCreatedEvent(
    @NotBlank(message = "Order ID cannot be blank")
    String orderId,

    @NotBlank(message = "Customer ID cannot be blank")
    String customerId,

    @NotBlank(message = "Shipping address cannot be blank")
    String shippingAddress,

    @NotBlank(message = "Correlation ID cannot be blank")
    String correlationId,

    @NotEmpty(message = "Items list cannot be empty")
    @Valid
    List<OrderItemEvent> items,

    @NotNull(message = "Total amount cannot be null")
    @Positive(message = "Total amount must be positive")
    BigDecimal totalAmount,

    Instant createdAt
) implements ChoreographyEvent {

    public record OrderItemEvent(
        @NotBlank(message = "Product ID cannot be blank")
        String productId,

        @NotBlank(message = "Product name cannot be blank")
        String productName,

        @Positive(message = "Quantity must be positive")
        int quantity,

        @NotNull(message = "Price cannot be null")
        @Positive(message = "Price must be positive")
        BigDecimal price
    ) {
        public static OrderItemEvent of(String productId, String productName, int quantity, BigDecimal price) {
            return new OrderItemEvent(productId, productName, quantity, price);
        }
    }

    public static OrderCreatedEvent of(String orderId, String customerId, String shippingAddress,
                                       String correlationId, List<OrderItemEvent> items,
                                       BigDecimal totalAmount, Instant createdAt) {
        return new OrderCreatedEvent(orderId, customerId, shippingAddress, correlationId, items, totalAmount, createdAt);
    }
}
