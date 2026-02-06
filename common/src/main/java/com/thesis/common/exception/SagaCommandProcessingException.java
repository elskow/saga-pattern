package com.thesis.common.exception;

public class SagaCommandProcessingException extends SagaException {

    public SagaCommandProcessingException(String message, String orderId, String step) {
        super(message, orderId, step);
    }

    public SagaCommandProcessingException(String message, String orderId, String step, Throwable cause) {
        super(message, orderId, step, cause);
    }

    public SagaCommandProcessingException(String message, Throwable cause) {
        super(message, null, null, cause);
    }
}
