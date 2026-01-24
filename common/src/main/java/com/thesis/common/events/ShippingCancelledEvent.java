package com.thesis.common.events;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShippingCancelledEvent {
    @NotBlank(message = "Shipping ID cannot be blank")
    private String shippingId;
    
    @NotBlank(message = "Order ID cannot be blank")
    private String orderId;
    
    @NotNull(message = "Cancelled at cannot be null")
    private Instant cancelledAt;
    
    @NotBlank(message = "Correlation ID cannot be blank")
    private String correlationId;
    
    @Builder.Default
    private Instant createdAt = Instant.now();
}
