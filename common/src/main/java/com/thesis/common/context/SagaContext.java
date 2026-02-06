package com.thesis.common.context;

import org.slf4j.MDC;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;

public final class SagaContext {

    public record ContextData(
        String orderId,
        String correlationId,
        String sagaId,
        String sagaType
    ) {
        public ContextData {
            orderId = Objects.requireNonNullElse(orderId, "unknown");
            correlationId = Objects.requireNonNullElse(correlationId, UUID.randomUUID().toString());
        }

        public static ContextData forChoreography(String orderId, String correlationId) {
            return new ContextData(orderId, correlationId, null, null);
        }

        public static ContextData forOrchestration(String orderId, String correlationId, 
                                                    String sagaId, String sagaType) {
            return new ContextData(orderId, correlationId, sagaId, sagaType);
        }

        public static ContextData withNewCorrelationId(String orderId) {
            return new ContextData(orderId, UUID.randomUUID().toString(), null, null);
        }
    }

    @SuppressWarnings("preview")
    private static final ScopedValue<ContextData> CONTEXT = ScopedValue.newInstance();

    private SagaContext() {}

    @SuppressWarnings("preview")
    public static void run(ContextData data, Runnable action) {
        ScopedValue.where(CONTEXT, data).run(() -> {
            syncToMdc(data);
            try {
                action.run();
            } finally {
                clearMdc();
            }
        });
    }

    @SuppressWarnings("preview")
    public static <T> T call(ContextData data, Callable<T> callable) throws Exception {
        return ScopedValue.where(CONTEXT, data).call(() -> {
            syncToMdc(data);
            try {
                return callable.call();
            } finally {
                clearMdc();
            }
        });
    }

    @SuppressWarnings("preview")
    public static ContextData current() {
        return CONTEXT.isBound() ? CONTEXT.get() : null;
    }

    @SuppressWarnings("preview")
    public static String orderId() {
        return CONTEXT.isBound() ? CONTEXT.get().orderId() : "unknown";
    }

    @SuppressWarnings("preview")
    public static String correlationId() {
        return CONTEXT.isBound() ? CONTEXT.get().correlationId() : UUID.randomUUID().toString();
    }

    @SuppressWarnings("preview")
    public static String sagaId() {
        return CONTEXT.isBound() ? CONTEXT.get().sagaId() : null;
    }

    @SuppressWarnings("preview")
    public static String sagaType() {
        return CONTEXT.isBound() ? CONTEXT.get().sagaType() : null;
    }

    @SuppressWarnings("preview")
    public static boolean isBound() {
        return CONTEXT.isBound();
    }

    private static void syncToMdc(ContextData data) {
        MDC.put("orderId", data.orderId());
        MDC.put("correlationId", data.correlationId());
        if (data.sagaId() != null) {
            MDC.put("sagaId", data.sagaId());
        }
        if (data.sagaType() != null) {
            MDC.put("sagaType", data.sagaType());
        }
    }

    private static void clearMdc() {
        MDC.remove("orderId");
        MDC.remove("correlationId");
        MDC.remove("sagaId");
        MDC.remove("sagaType");
    }
}
