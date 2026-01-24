package com.thesis.common.command;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScheduleShippingCommand {
    @NotBlank(message = "commandType must not be blank")
    private String commandType;
    
    @NotNull(message = "shipmentId must not be null")
    private String shipmentId;
    
    @NotNull(message = "orderId must not be null")
    private String orderId;
    
    @NotNull(message = "shippingAddress must not be null")
    @NotBlank(message = "shippingAddress must not be blank")
    private String shippingAddress;
}
