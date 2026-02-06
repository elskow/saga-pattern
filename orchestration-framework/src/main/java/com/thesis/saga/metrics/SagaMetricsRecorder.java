package com.thesis.saga.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
public class SagaMetricsRecorder {

    private static final String PREFIX = "saga.framework";

    private final MeterRegistry meterRegistry;
    private final Map<String, Counter> counterCache = new ConcurrentHashMap<>();
    private final Map<String, Timer> timerCache = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> gaugeValues = new ConcurrentHashMap<>();


    public void recordSagaStarted(String sagaType) {
        getCounter("%s.started".formatted(PREFIX), "type", sagaType).increment();
    }

    public void recordSagaCompleted(String sagaType, boolean success) {
        String outcome = success ? "success" : "failure";
        getCounter("%s.completed".formatted(PREFIX), "type", sagaType, "outcome", outcome).increment();
    }

    public void recordCompensationStarted(String sagaType) {
        getCounter("%s.compensation.started".formatted(PREFIX), "type", sagaType).increment();
        getCounter("saga.compensations.total", "service", "orchestration").increment();
    }

    public void recordCompensationCompleted(String sagaType) {
        getCounter("%s.compensation.completed".formatted(PREFIX), "type", sagaType).increment();
    }

    public void recordSagaTimeout(String sagaType) {
        getCounter("%s.timeout".formatted(PREFIX), "type", sagaType).increment();
    }

    public void recordSagaRecovered(String sagaType) {
        getCounter("%s.recovered".formatted(PREFIX), "type", sagaType).increment();
    }


    public void recordCommandSent(String sagaType, String stepName) {
        getCounter("%s.command.sent".formatted(PREFIX), "type", sagaType, "step", stepName).increment();
    }

    public void recordReplyReceived(String sagaType, String stepName, boolean success) {
        String outcome = success ? "success" : "failure";
        getCounter("%s.reply.received".formatted(PREFIX), "type", sagaType, "step", stepName, "outcome", outcome).increment();
    }

    public void recordCompensationCommandSent(String sagaType, String stepName) {
        getCounter("%s.compensation.command.sent".formatted(PREFIX), "type", sagaType, "step", stepName).increment();
        recordCompensationStep(stepName);
    }

    public void recordCompensationReplyReceived(String sagaType, String stepName) {
        getCounter("%s.compensation.reply.received".formatted(PREFIX), "type", sagaType, "step", stepName).increment();
        recordCompensationStep(stepName);
    }


    public void recordOutboxPublishAttempt(String sagaType) {
        getCounter("%s.outbox.attempt".formatted(PREFIX), "type", sagaType).increment();
    }

    public void recordOutboxPublishSuccess(String sagaType) {
        getCounter("%s.outbox.success".formatted(PREFIX), "type", sagaType).increment();
    }

    public void recordOutboxPublishFailure(String sagaType) {
        getCounter("%s.outbox.failure".formatted(PREFIX), "type", sagaType).increment();
    }


    public void recordDuplicateReply(String sagaType, String replyType) {
        getCounter("%s.duplicate.reply".formatted(PREFIX), "type", sagaType, "replyType", replyType).increment();
    }

    public void recordDuplicateCommand(String sagaType, String commandType) {
        getCounter("%s.duplicate.command".formatted(PREFIX), "type", sagaType, "commandType", commandType).increment();
    }


    public void recordSagaDuration(String sagaType, Duration duration) {
        getTimer("%s.duration".formatted(PREFIX), "type", sagaType).record(duration);
    }

    public void recordStepDuration(String sagaType, String stepName, Duration duration) {
        getTimer("%s.step.duration".formatted(PREFIX), "type", sagaType, "step", stepName).record(duration);
        String stepMetric = switch (stepName.toLowerCase()) {
            case "payment" -> "saga.step.payment.duration";
            case "inventory" -> "saga.step.inventory.duration";
            case "shipping" -> "saga.step.shipping.duration";
            default -> null;
        };
        if (stepMetric != null) {
            getTimer(stepMetric, "service", "orchestration").record(duration);
        }
    }


    public void recordInMemorySagaCount(String sagaType, int count) {
        String key = "%s.inmemory.count:%s".formatted(PREFIX, sagaType);
        AtomicInteger gauge = gaugeValues.computeIfAbsent(key, k -> {
            AtomicInteger value = new AtomicInteger(0);
            Gauge.builder("%s.inmemory.count".formatted(PREFIX), value, AtomicInteger::get)
                .tag("type", sagaType)
                .description("Number of sagas held in memory")
                .register(meterRegistry);
            return value;
        });
        gauge.set(count);
    }


    private Counter getCounter(String name, String... tags) {
        String key = name + String.join(":", tags);
        return counterCache.computeIfAbsent(key, k ->
            Counter.builder(name)
                .tags(tags)
                .register(meterRegistry));
    }

    private Timer getTimer(String name, String... tags) {
        String key = name + String.join(":", tags);
        return timerCache.computeIfAbsent(key, k ->
            Timer.builder(name)
                .tags(tags)
                .register(meterRegistry));
    }

    private void recordCompensationStep(String stepName) {
        String metric = switch (stepName.toLowerCase()) {
            case "payment" -> "saga.compensations.payment";
            case "inventory" -> "saga.compensations.inventory";
            case "shipping" -> "saga.compensations.shipping";
            default -> null;
        };
        if (metric != null) {
            getCounter(metric, "service", "orchestration").increment();
        }
    }
}
