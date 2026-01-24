package com.thesis.orchestration.order.statemachine;

import com.thesis.common.events.OrderCreatedEvent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Holds the context data for an order saga execution.
 * Tracks the order details and compensation state.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaData {
    private String orderId;
    private String customerId;
    private String paymentId;
    private String reservationId;
    private String shipmentId;
    private BigDecimal totalAmount;
    private String shippingAddress;
    private List<OrderCreatedEvent.OrderItemEvent> items;
    private Instant createdAt;
    private Instant lastUpdatedAt;
    private SagaStep currentStep;
    private boolean paymentCompleted;
    private boolean inventoryReserved;
    private boolean shippingScheduled;

    // Compensation tracking fields
    private boolean paymentRefunded;
    private boolean inventoryReleased;
    private boolean shippingCancelled;
    private int expectedCompensations;
    private int completedCompensations;

    /**
     * Saga execution steps for tracking compensation needs.
     */
    public enum SagaStep {
        PAYMENT,
        INVENTORY,
        SHIPPING,
        COMPLETED,
        COMPENSATING
    }
}
