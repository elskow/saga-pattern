package com.thesis.common.command;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScheduleShippingCommand {
    private String shipmentId;
    private String orderId;
    private String shippingAddress;
}
