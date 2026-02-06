package com.thesis.common.kafka;

import com.thesis.common.config.KafkaTopicsProperties;
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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final KafkaTopicsProperties topics;
    private final Map<String, Counter> successCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> failureCounters = new ConcurrentHashMap<>();
    private final Map<String, Timer> publishLatencyTimers = new ConcurrentHashMap<>();

    protected AbstractEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                     KafkaTopicsProperties topics,
                                     MeterRegistry meterRegistry) {
        this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate required");
        this.topics = Objects.requireNonNull(topics, "topics required");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry required");
    }

    protected abstract String getTopic();

    protected abstract String getServiceName();

    protected KafkaTopicsProperties topics() {
        return topics;
    }

    @CircuitBreaker(name = "kafka-publisher", fallbackMethod = "publishFallback")
    public void publishEvent(String eventType, Object event, String orderId) {
        try {
            MDC.put("orderId", Objects.requireNonNullElse(orderId, "unknown"));
            log.debug("Publishing {} event for order: {}", eventType, orderId);
            sendEventSafely(getTopic(), orderId, event, eventType, orderId);
        } finally {
            MDC.remove("orderId");
        }
    }

    protected void publishFallback(String eventType, Object event, String orderId, Exception ex) {
        log.error("Circuit breaker open for {} event order: {}. Error: {}", eventType, orderId, ex.getMessage());
        getFailureCounter(eventType).increment();
    }

    protected void sendEventSafely(String topic, String key, Object event, String eventType, String orderId) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            kafkaTemplate.send(topic, key, event)
                .thenAccept(result -> {
                    sample.stop(getPublishLatencyTimer(eventType));
                    log.debug("Event {} sent to topic {} for order {}", eventType, topic, orderId);
                    getSuccessCounter(eventType).increment();
                })
                .exceptionally(ex -> {
                    sample.stop(getPublishLatencyTimer(eventType));
                    log.error("Failed to send event {} to topic {} for order {}: {}", eventType, topic, orderId, ex.getMessage(), ex);
                    getFailureCounter(eventType).increment();
                    return null;
                });
        } catch (Exception e) {
            sample.stop(getPublishLatencyTimer(eventType));
            log.error("Exception sending event {} to topic {} for order {}: {}", eventType, topic, orderId, e.getMessage(), e);
            getFailureCounter(eventType).increment();
        }
    }

    protected Counter getSuccessCounter(String eventType) {
        return successCounters.computeIfAbsent(eventType, type ->
            meterRegistry.counter(
                SagaMetrics.KAFKA_EVENT_SEND_SUCCESS,
                Tags.of(SagaMetrics.TAG_SERVICE, getServiceName(), SagaMetrics.TAG_EVENT_TYPE, type)
            )
        );
    }

    protected Counter getFailureCounter(String eventType) {
        return failureCounters.computeIfAbsent(eventType, type ->
            meterRegistry.counter(
                SagaMetrics.KAFKA_EVENT_SEND_FAILURE,
                Tags.of(SagaMetrics.TAG_SERVICE, getServiceName(), SagaMetrics.TAG_EVENT_TYPE, type)
            )
        );
    }

    protected Timer getPublishLatencyTimer(String eventType) {
        return publishLatencyTimers.computeIfAbsent(eventType, type ->
            meterRegistry.timer(
                SagaMetrics.KAFKA_PUBLISH_LATENCY,
                Tags.of(SagaMetrics.TAG_SERVICE, getServiceName(), SagaMetrics.TAG_EVENT_TYPE, type)
            )
        );
    }
}
