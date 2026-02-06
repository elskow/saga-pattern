package com.thesis.common.enums;

import java.util.Arrays;

public enum CommandStatus {

    PENDING, SENT, SKIPPED;

    public static CommandStatus fromValue(String value) {
        return value == null ? null : Arrays.stream(values())
            .filter(s -> s.name().equals(value))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown CommandStatus: %s".formatted(value)));
    }

    public String getValue() {
        return name();
    }
}
