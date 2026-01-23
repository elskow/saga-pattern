package com.thesis.common.replies;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShippingScheduledReply implements SagaReply {
    private String shipmentId;
    private String orderId;
}
