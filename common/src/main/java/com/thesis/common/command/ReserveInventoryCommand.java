package com.thesis.common.command;

import com.thesis.common.events.OrderCreatedEvent;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReserveInventoryCommand {
    @NotBlank(message = "commandType must not be blank")
    private String commandType;
    
    @NotNull(message = "reservationId must not be null")
    private String reservationId;
    
    @NotNull(message = "orderId must not be null")
    private String orderId;
    
    @NotNull(message = "items must not be null")
    @NotEmpty(message = "items must not be empty")
    private List<OrderCreatedEvent.OrderItemEvent> items;
}
