package com.thesis.common.command;

import com.thesis.common.events.OrderCreatedEvent;
import io.eventuate.tram.commands.common.Command;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReserveInventoryCommand implements Command {
    private String reservationId;
    private String orderId;
    private List<OrderCreatedEvent.OrderItemEvent> items;
}
