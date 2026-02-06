package com.thesis.common.replies;

public record InventoryFailedReply(
    String reservationId,
    String orderId,
    String reason
) implements SagaReply {

    public static InventoryFailedReply of(String reservationId, String orderId, String reason) {
        return new InventoryFailedReply(reservationId, orderId, reason);
    }
}
