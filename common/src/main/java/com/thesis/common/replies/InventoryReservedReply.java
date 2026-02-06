package com.thesis.common.replies;

public record InventoryReservedReply(
    String reservationId,
    String orderId
) implements SagaReply {

    public static InventoryReservedReply of(String reservationId, String orderId) {
        return new InventoryReservedReply(reservationId, orderId);
    }
}
