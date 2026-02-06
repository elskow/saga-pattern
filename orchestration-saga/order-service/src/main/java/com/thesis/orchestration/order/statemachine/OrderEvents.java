package com.thesis.orchestration.order.statemachine;

public enum OrderEvents {
    START_SAGA,
    PAYMENT_SUCCESS,
    PAYMENT_FAILED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    SHIPPING_SCHEDULED,
    SHIPPING_FAILED,
    COMPENSATION_COMPLETE
}
