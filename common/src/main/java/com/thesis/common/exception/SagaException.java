package com.thesis.common.exception;

public class SagaException extends RuntimeException {

    private final String orderId;
    private final String step;

    public SagaException(String message, String orderId, String step) {
        super(message);
        this.orderId = orderId;
        this.step = step;
    }

    public SagaException(String message, String orderId, String step, Throwable cause) {
        super(message, cause);
        this.orderId = orderId;
        this.step = step;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getStep() {
        return step;
    }
}
