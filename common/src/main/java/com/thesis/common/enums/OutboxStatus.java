package com.thesis.common.enums;

public enum OutboxStatus {

    PENDING,
    SENT,
    FAILED;

    public String getValue() {
        return name();
    }

    public static OutboxStatus fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (OutboxStatus status : OutboxStatus.values()) {
            if (status.name().equals(value)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown OutboxStatus: " + value);
    }
}
