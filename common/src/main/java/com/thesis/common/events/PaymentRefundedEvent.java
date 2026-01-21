package com.thesis.common.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentRefundedEvent {
    private String paymentId;
    private String orderId;
    private BigDecimal refundAmount;
    private Instant refundedAt;
    @Builder.Default
    private Instant createdAt = Instant.now();
}
