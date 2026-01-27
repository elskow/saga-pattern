package com.thesis.common.kafka;

import com.thesis.common.config.KafkaTopicsConfig;
import com.thesis.common.metrics.SagaMetrics;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsConfig topicsConfig;
    private final MeterRegistry meterRegistry;
    private final Map<String, Counter> successCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> failureCounters = new ConcurrentHashMap<>();

    protected AbstractEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                   KafkaTopicsConfig topicsConfig,
                                   MeterRegistry meterRegistry,
                                   String serviceName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicsConfig = topicsConfig;
        this.meterRegistry = meterRegistry;
    }

    protected abstract String getTopic();

    protected abstract String getServiceName();

    @CircuitBreaker(name = "kafka-publisher", fallbackMethod = "publishFallback")
    public void publishEvent(String eventType, Object event, String orderId) {
        try {
            MDC.put("orderId", orderId != null ? orderId : "unknown");
            log.info("Publishing {} event for order: {}", eventType, orderId);
            sendEventSafely(getTopic(), orderId, event, eventType, orderId);
        } finally {
            MDC.clear();
        }
    }

    protected void publishFallback(String eventType, Object event, String orderId, Exception ex) {
        log.error("Circuit breaker open or retry exhausted for {} event order: {}. Error: {}",
                eventType, orderId, ex.getMessage());
        getFailureCounter(eventType).increment();
    }

    protected void sendEventSafely(String topic, String key, Object event, String eventType, String orderId) {
        try {
            kafkaTemplate.send(topic, key, event)
                .thenAccept(result -> {
                    log.debug("Event {} sent successfully to topic {} for order {}", eventType, topic, orderId);
                    getSuccessCounter(eventType).increment();
                })
                .exceptionally(ex -> {
                    log.error("Failed to send event {} to topic {} for order {}: {}",
                            eventType, topic, orderId, ex.getMessage(), ex);
                    getFailureCounter(eventType).increment();
                    return null;
                });
        } catch (Exception e) {
            log.error("Exception sending event {} to topic {} for order {}: {}",
                    eventType, topic, orderId, e.getMessage(), e);
            getFailureCounter(eventType).increment();
        }
    }

    protected Counter getSuccessCounter(String eventType) {
        return successCounters.computeIfAbsent(eventType, type ->
            meterRegistry.counter(
                SagaMetrics.KAFKA_EVENT_SEND_SUCCESS,
                Tags.of(
                    SagaMetrics.TAG_SERVICE, getServiceName(),
                    SagaMetrics.TAG_EVENT_TYPE, type
                )
            )
        );
    }

    protected Counter getFailureCounter(String eventType) {
        return failureCounters.computeIfAbsent(eventType, type ->
            meterRegistry.counter(
                SagaMetrics.KAFKA_EVENT_SEND_FAILURE,
                Tags.of(
                    SagaMetrics.TAG_SERVICE, getServiceName(),
                    SagaMetrics.TAG_EVENT_TYPE, type
                )
            )
        );
    }
}
