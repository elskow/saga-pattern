package com.thesis.common.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.List;

public record CreateOrderRequest(
    @NotBlank(message = "Customer ID cannot be blank")
    String customerId,

    @NotBlank(message = "Shipping address cannot be blank")
    String shippingAddress,

    @NotEmpty(message = "Items list cannot be empty")
    @Valid
    List<OrderItemRequest> items
) {

    public record OrderItemRequest(
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
        public static OrderItemRequest of(String productId, String productName, int quantity, BigDecimal price) {
            return new OrderItemRequest(productId, productName, quantity, price);
        }
    }

    public static CreateOrderRequest of(String customerId, String shippingAddress, List<OrderItemRequest> items) {
        return new CreateOrderRequest(customerId, shippingAddress, items);
    }
}
