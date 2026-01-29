package com.thesis.common.kafka;

import com.thesis.common.config.KafkaTopicsConfig;
import com.thesis.common.metrics.SagaMetrics;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsConfig topicsConfig;
    private final MeterRegistry meterRegistry;
    private final Map<String, Counter> successCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> failureCounters = new ConcurrentHashMap<>();
    private final Map<String, Timer> publishLatencyTimers = new ConcurrentHashMap<>();

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
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            kafkaTemplate.send(topic, key, event)
                .thenAccept(result -> {
                    // Record successful publish latency
                    sample.stop(getPublishLatencyTimer(eventType));
                    log.debug("Event {} sent successfully to topic {} for order {} in {}ms",
                        eventType, topic, orderId,
                        result.getRecordMetadata() != null ?
                            System.currentTimeMillis() - result.getRecordMetadata().timestamp() : "N/A");
                    getSuccessCounter(eventType).increment();
                })
                .exceptionally(ex -> {
                    // Record failed publish latency (still useful to know how long before failure)
                    sample.stop(getPublishLatencyTimer(eventType));
                    log.error("Failed to send event {} to topic {} for order {}: {}",
                        eventType, topic, orderId, ex.getMessage(), ex);
                    getFailureCounter(eventType).increment();
                    return null;
                });
        } catch (Exception e) {
            // Record latency even on synchronous exception
            sample.stop(getPublishLatencyTimer(eventType));
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

    /**
     * Gets or creates a Timer for tracking Kafka publish latency by event type.
     * This measures the time from when send() is called until the callback is received.
     */
    protected Timer getPublishLatencyTimer(String eventType) {
        return publishLatencyTimers.computeIfAbsent(eventType, type ->
            meterRegistry.timer(
                SagaMetrics.KAFKA_PUBLISH_LATENCY,
                Tags.of(
                    SagaMetrics.TAG_SERVICE, getServiceName(),
                    SagaMetrics.TAG_EVENT_TYPE, type
                )
            )
        );
    }
}
