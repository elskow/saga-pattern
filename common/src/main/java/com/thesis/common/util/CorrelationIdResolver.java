package com.thesis.common.util;

import org.slf4j.MDC;

import java.util.Optional;
import java.util.UUID;

public final class CorrelationIdResolver {

    private CorrelationIdResolver() {}

    public static String resolve(String eventCorrelationId) {
        return Optional.ofNullable(MDC.get("correlationId"))
            .filter(s -> !s.isBlank())
            .or(() -> Optional.ofNullable(eventCorrelationId).filter(s -> !s.isBlank()))
            .orElseGet(() -> UUID.randomUUID().toString());
    }
}
