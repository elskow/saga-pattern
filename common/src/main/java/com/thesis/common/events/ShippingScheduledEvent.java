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
public class ShippingScheduledEvent {
    private String shippingId;
    private String orderId;
    private String trackingNumber;
    private String address;
    private Instant estimatedDelivery;
    private Instant scheduledAt;
}
