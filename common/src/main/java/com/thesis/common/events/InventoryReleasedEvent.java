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
public class InventoryReleasedEvent {
    private String reservationId;
    
    @NotBlank(message = "Order ID cannot be blank")
    private String orderId;
    
    @NotNull(message = "Released at cannot be null")
    private Instant releasedAt;
    
    @NotBlank(message = "Correlation ID cannot be blank")
    private String correlationId;
    
    @Builder.Default
    private Instant createdAt = Instant.now();
}
