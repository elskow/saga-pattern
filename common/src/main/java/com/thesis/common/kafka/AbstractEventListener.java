package com.thesis.common.kafka;

import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;

@Slf4j
public abstract class AbstractEventListener<T> {

    private final Validator validator;
    private final String serviceName;
    private final Counter eventProcessedCounter;
    private final Counter eventProcessingFailedCounter;

    protected AbstractEventListener(Validator validator, MeterRegistry meterRegistry, String serviceName) {
        this.validator = validator;
        this.serviceName = serviceName;
        this.eventProcessedCounter = meterRegistry.counter(
            SagaMetrics.SAGA_MESSAGES_TOTAL,
            "service", serviceName,
            "direction", SagaMetrics.DIRECTION_RECEIVED,
            "type", SagaMetrics.TYPE_EVENT,
            "outcome", SagaMetrics.OUTCOME_SUCCESS
        );
        this.eventProcessingFailedCounter = meterRegistry.counter(
            SagaMetrics.SAGA_MESSAGES_TOTAL,
            "service", serviceName,
            "direction", SagaMetrics.DIRECTION_RECEIVED,
            "type", SagaMetrics.TYPE_EVENT,
            "outcome", SagaMetrics.OUTCOME_FAILURE
        );
    }

    protected abstract String getEventId(ConsumerRecord<String, Object> record, T event);

    protected abstract String getEventType();

    protected abstract String getOrderId(T event);

    protected abstract String getCorrelationId(T event);

    protected abstract void processEvent(T event);

    protected boolean validateEvent(T event) {
        Set<ConstraintViolation<T>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            log.error("Invalid {} received. Violations: {}", getEventType(), violations);
            return false;
        }
        return true;
    }

    protected void handleEvent(ConsumerRecord<String, Object> record, T event,
                               BiConsumer<String, String> mdcSetup,
                               Runnable idempotencyCheck,
                               Runnable eventProcessor) {
        String correlationId = Objects.requireNonNullElse(getCorrelationId(event), UUID.randomUUID().toString());

        try {
            mdcSetup.accept(getOrderId(event), correlationId);

            if (!validateEvent(event)) {
                eventProcessingFailedCounter.increment();
                return;
            }

            String eventId = getEventId(record, event);
            if (eventId != null) {
                idempotencyCheck.run();
            }

            log.debug("Received {} for order: {}", getEventType(), getOrderId(event));
            eventProcessor.run();
            eventProcessedCounter.increment();

        } catch (Exception e) {
            log.error("Error processing {} for order: {}", getEventType(), getOrderId(event), e);
            eventProcessingFailedCounter.increment();
            throw e;
        } finally {
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }

    protected void setupMdc(String orderId, String correlationId) {
        MDC.put("orderId", Objects.requireNonNullElse(orderId, "unknown"));
        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }
    }
}
