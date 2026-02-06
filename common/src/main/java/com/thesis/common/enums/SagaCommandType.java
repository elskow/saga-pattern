package com.thesis.common.enums;

public enum SagaCommandType {
    PROCESS_PAYMENT("PROCESS_PAYMENT"),
    REFUND_PAYMENT("REFUND_PAYMENT"),
    RESERVE_INVENTORY("RESERVE_INVENTORY"),
    RELEASE_INVENTORY("RELEASE_INVENTORY"),
    SCHEDULE_SHIPPING("SCHEDULE_SHIPPING"),
    CANCEL_SHIPPING("CANCEL_SHIPPING");

    private final String value;

    SagaCommandType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static SagaCommandType fromValue(String value) {
        for (SagaCommandType type : values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown command type: %s".formatted(value));
    }

    @Override
    public String toString() {
        return value;
    }
}
