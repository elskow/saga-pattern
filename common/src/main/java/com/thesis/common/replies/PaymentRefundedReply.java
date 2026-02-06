package com.thesis.common.replies;

public record PaymentRefundedReply(
    String paymentId,
    String orderId,
    boolean success,
    String reason
) implements SagaReply {

    public static PaymentRefundedReply success(String paymentId, String orderId) {
        return new PaymentRefundedReply(paymentId, orderId, true, null);
    }

    public static PaymentRefundedReply failure(String paymentId, String orderId, String reason) {
        return new PaymentRefundedReply(paymentId, orderId, false, reason);
    }
}
