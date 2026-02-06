package com.thesis.common.enums;

import java.util.Arrays;

public enum OutboxStatus {

    PENDING, SENT, FAILED;

    public static OutboxStatus fromValue(String value) {
        return value == null ? null : Arrays.stream(values())
            .filter(s -> s.name().equals(value))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown OutboxStatus: %s".formatted(value)));
    }

    public String getValue() {
        return name();
    }
}
