package com.thesis.common.replies;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Reply sent by shipping service after processing a shipping cancellation compensation.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShippingCancelledReply implements SagaReply {
    private String shipmentId;
    private String orderId;
    private boolean success;
    private String reason;
}
