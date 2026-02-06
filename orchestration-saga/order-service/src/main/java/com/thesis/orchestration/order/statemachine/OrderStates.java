package com.thesis.orchestration.order.statemachine;

public enum OrderStates {
    CREATED,
    PAYMENT_PENDING,
    INVENTORY_PENDING,
    SHIPPING_PENDING,
    COMPLETED,
    COMPENSATING,
    CANCELLED
}
