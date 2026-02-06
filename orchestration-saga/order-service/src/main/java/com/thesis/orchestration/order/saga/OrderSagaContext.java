package com.thesis.orchestration.order.saga;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.saga.core.AbstractSagaContext;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class OrderSagaContext extends AbstractSagaContext {

    private String orderId;
    private String customerId;
    private String paymentId;
    private String reservationId;
    private String shipmentId;
    private BigDecimal totalAmount;
    private String shippingAddress;
    private List<OrderCreatedEvent.OrderItemEvent> items;

    private boolean paymentCompleted;
    private boolean inventoryReserved;
    private boolean shippingScheduled;

    private boolean paymentRefunded;
    private boolean inventoryReleased;
    private boolean shippingCancelled;

    public static OrderSagaContext create(
        String orderId,
        String customerId,
        BigDecimal totalAmount,
        String shippingAddress,
        List<OrderCreatedEvent.OrderItemEvent> items,
        String paymentId,
        String reservationId,
        String shipmentId) {

        return OrderSagaContext.builder()
            .orderId(orderId)
            .customerId(customerId)
            .totalAmount(totalAmount)
            .shippingAddress(shippingAddress)
            .items(items)
            .paymentId(paymentId)
            .reservationId(reservationId)
            .shipmentId(shipmentId)
            .createdAt(Instant.now())
            .lastUpdatedAt(Instant.now())
            .paymentCompleted(false)
            .inventoryReserved(false)
            .shippingScheduled(false)
            .paymentRefunded(false)
            .inventoryReleased(false)
            .shippingCancelled(false)
            .expectedCompensations(0)
            .completedCompensations(0)
            .build();
    }

    @Override
    public String getSagaId() {
        return orderId;
    }

    @Override
    @JsonIgnore
    public AbstractSagaContext withTouch() {
        OrderSagaContext copy = this.toBuilder().build();
        copy.touch();
        return copy;
    }
}
