package com.thesis.common.enums;

public enum CommandType {

    PROCESS_PAYMENT("PROCESS_PAYMENT"),
    REFUND_PAYMENT("REFUND_PAYMENT"),
    RESERVE_INVENTORY("RESERVE_INVENTORY"),
    RELEASE_INVENTORY("RELEASE_INVENTORY"),
    SCHEDULE_SHIPPING("SCHEDULE_SHIPPING"),
    CANCEL_SHIPPING("CANCEL_SHIPPING");

    private final String value;

    CommandType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static CommandType fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (CommandType type : CommandType.values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown CommandType: " + value);
    }
}
