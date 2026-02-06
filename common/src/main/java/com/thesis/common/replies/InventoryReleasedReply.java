package com.thesis.common.replies;

public record InventoryReleasedReply(
    String reservationId,
    String orderId,
    boolean success,
    String reason
) implements SagaReply {

    public static InventoryReleasedReply success(String reservationId, String orderId) {
        return new InventoryReleasedReply(reservationId, orderId, true, null);
    }

    public static InventoryReleasedReply failure(String reservationId, String orderId, String reason) {
        return new InventoryReleasedReply(reservationId, orderId, false, reason);
    }
}
