package com.thesis.orchestration.order.dto;

import com.thesis.common.events.OrderCreatedEvent;
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

    @NotNull(message = "Total amount cannot be null")
    @Positive(message = "Total amount must be positive")
    BigDecimal totalAmount,

    @NotBlank(message = "Shipping address cannot be blank")
    String shippingAddress,

    @NotNull(message = "Items cannot be null")
    @NotEmpty(message = "Items cannot be empty")
    @Valid
    List<OrderCreatedEvent.OrderItemEvent> items
) {
    public static CreateOrderRequest of(String customerId, BigDecimal totalAmount,
                                        String shippingAddress, List<OrderCreatedEvent.OrderItemEvent> items) {
        return new CreateOrderRequest(customerId, totalAmount, shippingAddress, items);
    }
}
