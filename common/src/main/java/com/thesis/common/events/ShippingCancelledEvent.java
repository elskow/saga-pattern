package com.thesis.common.events;

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
    private String shippingId;
    private String orderId;
    private Instant cancelledAt;
    @Builder.Default
    private Instant createdAt = Instant.now();
}
