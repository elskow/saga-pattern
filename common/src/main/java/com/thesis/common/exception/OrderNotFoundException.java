package com.thesis.common.exception;

/**
 * Exception thrown when an order cannot be found.
 */
public class OrderNotFoundException extends ResourceNotFoundException {

    public OrderNotFoundException(String orderId) {
        super("Order", orderId);
    }

    public OrderNotFoundException(String orderId, Throwable cause) {
        super("Order", orderId, cause);
    }
}
