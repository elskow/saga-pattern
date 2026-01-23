package com.thesis.common.replies;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryFailedReply implements SagaReply {
    private String reservationId;
    private String orderId;
    private String reason;
}
