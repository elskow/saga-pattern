package com.thesis.common.replies;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Reply sent by inventory service after processing an inventory release compensation.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReleasedReply implements SagaReply {
    private String reservationId;
    private String orderId;
    private boolean success;
    private String reason;
}
