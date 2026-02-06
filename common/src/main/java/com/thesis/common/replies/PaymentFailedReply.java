package com.thesis.common.replies;

public record PaymentFailedReply(
    String paymentId,
    String orderId,
    String reason
) implements SagaReply {

    public static PaymentFailedReply of(String paymentId, String orderId, String reason) {
        return new PaymentFailedReply(paymentId, orderId, reason);
    }
}
