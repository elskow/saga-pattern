package com.thesis.common.replies;

public record ShippingFailedReply(
    String shippingId,
    String orderId,
    String reason
) implements SagaReply {

    public static ShippingFailedReply of(String shippingId, String orderId, String reason) {
        return new ShippingFailedReply(shippingId, orderId, reason);
    }
}
