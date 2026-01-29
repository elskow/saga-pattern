package com.thesis.common.enums;

public enum CommandStatus {

    PENDING,
    SENT,
    SKIPPED;

    public static CommandStatus fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (CommandStatus status : CommandStatus.values()) {
            if (status.name().equals(value)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown CommandStatus: " + value);
    }

    public String getValue() {
        return name();
    }
}
