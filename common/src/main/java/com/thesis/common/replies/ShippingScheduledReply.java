package com.thesis.common.replies;

public record ShippingScheduledReply(
    String shippingId,
    String orderId
) implements SagaReply {

    public static ShippingScheduledReply of(String shippingId, String orderId) {
        return new ShippingScheduledReply(shippingId, orderId);
    }
}
