package com.thesis.common.replies;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Reply sent by payment service after processing a refund compensation.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentRefundedReply implements SagaReply {
    private String paymentId;
    private String orderId;
    private boolean success;
    private String reason;
}
