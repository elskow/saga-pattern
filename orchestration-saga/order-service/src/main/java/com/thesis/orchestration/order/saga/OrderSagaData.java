package com.thesis.orchestration.order.saga;

import com.thesis.common.events.OrderCreatedEvent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderSagaData {
    private String orderId;
    private String customerId;
    private String paymentId;
    private String reservationId;
    private String shipmentId;
    private BigDecimal totalAmount;
    private String shippingAddress;
    private List<OrderCreatedEvent.OrderItemEvent> items;
    
    // Flags to track progress
    private boolean paymentCompleted;
    private boolean inventoryReserved;
    private boolean shippingScheduled;
}
