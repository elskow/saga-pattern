package com.thesis.orchestration.order.statemachine;

/**
 * Events for the Order Saga State Machine.
 * Represents all possible events that can trigger state transitions.
 */
public enum OrderEvents {
    /** Initiates the saga (CREATED -> PAYMENT_PENDING) */
    START_SAGA,

    /** Payment processed successfully (PAYMENT_PENDING -> INVENTORY_PENDING) */
    PAYMENT_SUCCESS,

    /** Payment failed (PAYMENT_PENDING -> CANCELLED) */
    PAYMENT_FAILED,

    /** Inventory reserved successfully (INVENTORY_PENDING -> SHIPPING_PENDING) */
    INVENTORY_RESERVED,

    /** Inventory reservation failed (INVENTORY_PENDING -> COMPENSATING) */
    INVENTORY_FAILED,

    /** Shipping scheduled successfully (SHIPPING_PENDING -> COMPLETED) */
    SHIPPING_SCHEDULED,

    /** Shipping failed (SHIPPING_PENDING -> COMPENSATING) */
    SHIPPING_FAILED,

    /** Compensation completed (COMPENSATING -> CANCELLED) */
    COMPENSATION_COMPLETE
}
