package com.thesis.orchestration.order.statemachine;

/**
 * States for the Order Saga State Machine.
 * Represents all possible states during the order processing saga.
 */
public enum OrderStates {
    /** Initial state when order is created */
    CREATED,

    /** Waiting for payment to be processed */
    PAYMENT_PENDING,

    /** Waiting for inventory to be reserved */
    INVENTORY_PENDING,

    /** Waiting for shipping to be scheduled */
    SHIPPING_PENDING,

    /** Order completed successfully */
    COMPLETED,

    /** Running compensation actions */
    COMPENSATING,

    /** Order cancelled (after failure or compensation) */
    CANCELLED
}
