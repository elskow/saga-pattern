package com.thesis.orchestration.order.dto;

import com.thesis.common.events.OrderCreatedEvent;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderRequest {

    @NotBlank(message = "Customer ID is required")
    private String customerId;

    @NotNull(message = "Total amount is required")
    @Positive(message = "Total amount must be positive")
    private BigDecimal totalAmount;

    @NotBlank(message = "Shipping address is required")
    private String shippingAddress;

    @NotNull(message = "Items are required")
    @NotEmpty(message = "Items list cannot be empty")
    @Valid
    private List<OrderCreatedEvent.OrderItemEvent> items;
}
