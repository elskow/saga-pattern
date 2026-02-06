package com.thesis.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SagaMetricsHelper {

    private final MeterRegistry meterRegistry;
    private final String serviceType;

    private final Counter messagesSentCounter;
    private final Counter messagesReceivedCounter;
    private final Counter dbInsertsCounter;
    private final Counter dbUpdatesCounter;

    private final DistributionSummary messagesPerSuccessfulSaga;
    private final DistributionSummary messagesPerFailedSaga;
    private final DistributionSummary dbWritesPerSuccessfulSaga;
    private final DistributionSummary dbWritesPerFailedSaga;

    private final ConcurrentHashMap<String, AtomicInteger> messageCountByOrder = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> dbWriteCountByOrder = new ConcurrentHashMap<>();

    public SagaMetricsHelper(MeterRegistry meterRegistry, String serviceType) {
        this.meterRegistry = meterRegistry;
        this.serviceType = serviceType;

        this.messagesSentCounter = Counter.builder(SagaMetrics.SAGA_MESSAGES_TOTAL)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_SENT)
            .register(meterRegistry);

        this.messagesReceivedCounter = Counter.builder(SagaMetrics.SAGA_MESSAGES_TOTAL)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_RECEIVED)
            .register(meterRegistry);

        this.dbInsertsCounter = Counter.builder(SagaMetrics.SAGA_DB_WRITES_TOTAL)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_OPERATION, SagaMetrics.OPERATION_INSERT)
            .register(meterRegistry);

        this.dbUpdatesCounter = Counter.builder(SagaMetrics.SAGA_DB_WRITES_TOTAL)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_OPERATION, SagaMetrics.OPERATION_UPDATE)
            .register(meterRegistry);

        this.messagesPerSuccessfulSaga = DistributionSummary.builder(SagaMetrics.SAGA_MESSAGES_PER_TRANSACTION)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_OUTCOME, SagaMetrics.OUTCOME_SUCCESS)
            .register(meterRegistry);

        this.messagesPerFailedSaga = DistributionSummary.builder(SagaMetrics.SAGA_MESSAGES_PER_TRANSACTION)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_OUTCOME, SagaMetrics.OUTCOME_FAILURE)
            .register(meterRegistry);

        this.dbWritesPerSuccessfulSaga = DistributionSummary.builder(SagaMetrics.SAGA_DB_WRITES_PER_TRANSACTION)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_OUTCOME, SagaMetrics.OUTCOME_SUCCESS)
            .register(meterRegistry);

        this.dbWritesPerFailedSaga = DistributionSummary.builder(SagaMetrics.SAGA_DB_WRITES_PER_TRANSACTION)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_OUTCOME, SagaMetrics.OUTCOME_FAILURE)
            .register(meterRegistry);
    }

    public void recordMessageSent(String orderId, String messageType) {
        messagesSentCounter.increment();
        messageCountByOrder.computeIfAbsent(orderId, k -> new AtomicInteger(0)).incrementAndGet();

        Counter.builder(SagaMetrics.SAGA_MESSAGES_TOTAL)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_SENT)
            .tag(SagaMetrics.TAG_MESSAGE_TYPE, messageType)
            .register(meterRegistry)
            .increment();
    }

    public void recordMessageReceived(String orderId, String messageType) {
        messagesReceivedCounter.increment();
        messageCountByOrder.computeIfAbsent(orderId, k -> new AtomicInteger(0)).incrementAndGet();

        Counter.builder(SagaMetrics.SAGA_MESSAGES_TOTAL)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_RECEIVED)
            .tag(SagaMetrics.TAG_MESSAGE_TYPE, messageType)
            .register(meterRegistry)
            .increment();
    }

    public void recordDbInsert(String orderId, String entity) {
        dbInsertsCounter.increment();
        dbWriteCountByOrder.computeIfAbsent(orderId, k -> new AtomicInteger(0)).incrementAndGet();

        Counter.builder(SagaMetrics.SAGA_DB_WRITES_TOTAL)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_OPERATION, SagaMetrics.OPERATION_INSERT)
            .tag(SagaMetrics.TAG_ENTITY, entity)
            .register(meterRegistry)
            .increment();
    }

    public void recordDbUpdate(String orderId, String entity) {
        dbUpdatesCounter.increment();
        dbWriteCountByOrder.computeIfAbsent(orderId, k -> new AtomicInteger(0)).incrementAndGet();

        Counter.builder(SagaMetrics.SAGA_DB_WRITES_TOTAL)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_OPERATION, SagaMetrics.OPERATION_UPDATE)
            .tag(SagaMetrics.TAG_ENTITY, entity)
            .register(meterRegistry)
            .increment();
    }

    public void recordMessageLatency(String fromService, String toService, Duration latency) {
        Timer.builder(SagaMetrics.SAGA_MESSAGE_LATENCY)
            .tag(SagaMetrics.TAG_SERVICE, serviceType)
            .tag(SagaMetrics.TAG_FROM_SERVICE, fromService)
            .tag(SagaMetrics.TAG_TO_SERVICE, toService)
            .register(meterRegistry)
            .record(latency);
    }

    public void recordSagaSuccess(String orderId) {
        Optional.ofNullable(messageCountByOrder.remove(orderId))
            .ifPresent(count -> messagesPerSuccessfulSaga.record(count.get()));
        Optional.ofNullable(dbWriteCountByOrder.remove(orderId))
            .ifPresent(count -> dbWritesPerSuccessfulSaga.record(count.get()));
    }

    public void recordSagaFailure(String orderId) {
        Optional.ofNullable(messageCountByOrder.remove(orderId))
            .ifPresent(count -> messagesPerFailedSaga.record(count.get()));
        Optional.ofNullable(dbWriteCountByOrder.remove(orderId))
            .ifPresent(count -> dbWritesPerFailedSaga.record(count.get()));
    }

    public int getMessageCount(String orderId) {
        return Optional.ofNullable(messageCountByOrder.get(orderId))
            .map(AtomicInteger::get)
            .orElse(0);
    }

    public int getDbWriteCount(String orderId) {
        return Optional.ofNullable(dbWriteCountByOrder.get(orderId))
            .map(AtomicInteger::get)
            .orElse(0);
    }
}
