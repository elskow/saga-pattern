package com.thesis.common.util;

import java.util.Collection;

public final class Validators {

    private Validators() {}

    public static <T> T requireNonNull(T value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " cannot be null");
        }
        return value;
    }

    public static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be null or blank");
        }
        return value;
    }

    public static <T extends Collection<?>> T requireNotEmpty(T collection, String fieldName) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " cannot be null or empty");
        }
        return collection;
    }
}
