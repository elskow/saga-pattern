package com.thesis.common.util;

import com.thesis.common.events.ChoreographyEvent;
import org.slf4j.MDC;

import java.util.Objects;

public final class MdcUtils {

    private MdcUtils() {}

    public static void setupEventMdc(String orderId, String correlationId) {
        MDC.put("orderId", Objects.requireNonNullElse(orderId, "unknown"));
        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }
    }

    public static void setupEventMdc(ChoreographyEvent event, String defaultCorrelationId) {
        String orderId = event != null ? event.orderId() : null;
        String correlationId = event != null && event.correlationId() != null 
            ? event.correlationId() 
            : defaultCorrelationId;
        setupEventMdc(orderId, correlationId);
    }

    public static void clearEventMdc() {
        MDC.remove("orderId");
        MDC.remove("correlationId");
    }

    public static String getOrderId(Object event) {
        return event instanceof ChoreographyEvent ce ? ce.orderId() : "unknown";
    }

    public static String getCorrelationId(Object event, String defaultCorrelationId) {
        if (event instanceof ChoreographyEvent ce && ce.correlationId() != null) {
            return ce.correlationId();
        }
        return defaultCorrelationId;
    }
}
