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
public class ShippingScheduledEvent {
    @NotBlank(message = "Shipping ID cannot be blank")
    private String shippingId;
    
    @NotBlank(message = "Order ID cannot be blank")
    private String orderId;
    
    @NotBlank(message = "Tracking number cannot be blank")
    private String trackingNumber;
    
    @NotBlank(message = "Address cannot be blank")
    private String address;
    
    @NotNull(message = "Estimated delivery cannot be null")
    private Instant estimatedDelivery;
    
    @NotNull(message = "Scheduled at cannot be null")
    private Instant scheduledAt;
    
    @NotBlank(message = "Correlation ID cannot be blank")
    private String correlationId;
    
    @Builder.Default
    private Instant createdAt = Instant.now();
}
