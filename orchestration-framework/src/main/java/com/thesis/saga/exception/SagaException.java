package com.thesis.saga.exception;

public class SagaException extends RuntimeException {

    private final String sagaId;
    private final String sagaType;

    public SagaException(String message) {
        super(message);
        this.sagaId = null;
        this.sagaType = null;
    }

    public SagaException(String message, String sagaId, String sagaType) {
        super(message);
        this.sagaId = sagaId;
        this.sagaType = sagaType;
    }

    public SagaException(String message, Throwable cause) {
        super(message, cause);
        this.sagaId = null;
        this.sagaType = null;
    }

    public SagaException(String message, String sagaId, String sagaType, Throwable cause) {
        super(message, cause);
        this.sagaId = sagaId;
        this.sagaType = sagaType;
    }

    public String getSagaId() {
        return sagaId;
    }

    public String getSagaType() {
        return sagaType;
    }
}
