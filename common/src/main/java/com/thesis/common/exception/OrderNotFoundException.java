package com.thesis.common.exception;

public class OrderNotFoundException extends ResourceNotFoundException {

    public OrderNotFoundException(String orderId) {
        super("Order", orderId);
    }

    public OrderNotFoundException(String orderId, Throwable cause) {
        super("Order", orderId, cause);
    }
}
