package com.thesis.common.exception;

public class SagaSerializationException extends SagaException {

    public SagaSerializationException(String message, String orderId, String step) {
        super(message, orderId, step);
    }

    public SagaSerializationException(String message, String orderId, String step, Throwable cause) {
        super(message, orderId, step, cause);
    }

    public SagaSerializationException(String message, Throwable cause) {
        super(message, null, null, cause);
    }
}
