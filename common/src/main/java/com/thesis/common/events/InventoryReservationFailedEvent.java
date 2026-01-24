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
public class InventoryReservationFailedEvent {
    @NotBlank(message = "Order ID cannot be blank")
    private String orderId;
    
    private String productId;
    
    @NotBlank(message = "Reason cannot be blank")
    private String reason;
    
    @NotNull(message = "Failed at cannot be null")
    private Instant failedAt;
    
    @NotBlank(message = "Correlation ID cannot be blank")
    private String correlationId;
    
    @Builder.Default
    private Instant createdAt = Instant.now();
}
