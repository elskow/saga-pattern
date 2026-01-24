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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {
    @NotBlank(message = "Order ID cannot be blank")
    private String orderId;
    
    @NotBlank(message = "Customer ID cannot be blank")
    private String customerId;
    
    @NotBlank(message = "Shipping address cannot be blank")
    private String shippingAddress;
    
    @NotBlank(message = "Correlation ID cannot be blank")
    private String correlationId;
    
    @NotEmpty(message = "Items list cannot be empty")
    @Valid
    private List<OrderItemEvent> items;
    
    @NotNull(message = "Total amount cannot be null")
    @Positive(message = "Total amount must be positive")
    private BigDecimal totalAmount;
    
    private Instant createdAt;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItemEvent {
        @NotBlank(message = "Product ID cannot be blank")
        private String productId;
        
        @NotBlank(message = "Product name cannot be blank")
        private String productName;
        
        @Positive(message = "Quantity must be positive")
        private int quantity;
        
        @NotNull(message = "Price cannot be null")
        @Positive(message = "Price must be positive")
        private BigDecimal price;
    }
}
