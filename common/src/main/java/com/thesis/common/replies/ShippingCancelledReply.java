package com.thesis.common.replies;

public record ShippingCancelledReply(
    String shippingId,
    String orderId,
    boolean success,
    String reason
) implements SagaReply {

    public static ShippingCancelledReply success(String shippingId, String orderId) {
        return new ShippingCancelledReply(shippingId, orderId, true, null);
    }

    public static ShippingCancelledReply failure(String shippingId, String orderId, String reason) {
        return new ShippingCancelledReply(shippingId, orderId, false, reason);
    }
}
