package com.thesis.common.replies;

public record PaymentCompletedReply(
    String paymentId,
    String orderId
) implements SagaReply {

    public static PaymentCompletedReply of(String paymentId, String orderId) {
        return new PaymentCompletedReply(paymentId, orderId);
    }
}
