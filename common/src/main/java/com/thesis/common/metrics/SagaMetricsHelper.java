package com.thesis.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class for recording thesis comparison metrics.
 * Provides convenient methods to track messages and DB writes per saga transaction.
 */
public class SagaMetricsHelper {

    private final MeterRegistry meterRegistry;
    private final String serviceType;
    
    // Counters for total metrics
    private final Counter messagesSentCounter;
    private final Counter messagesReceivedCounter;
    private final Counter dbInsertsCounter;
    private final Counter dbUpdatesCounter;
    
    // Distribution summaries for per-transaction metrics
    private final DistributionSummary messagesPerSuccessfulSaga;
    private final DistributionSummary messagesPerFailedSaga;
    private final DistributionSummary dbWritesPerSuccessfulSaga;
    private final DistributionSummary dbWritesPerFailedSaga;
    
    // Per-transaction tracking (orderId -> counts)
    private final ConcurrentHashMap<String, AtomicInteger> messageCountByOrder = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> dbWriteCountByOrder = new ConcurrentHashMap<>();

    public SagaMetricsHelper(MeterRegistry meterRegistry, String serviceType) {
        this.meterRegistry = meterRegistry;
        this.serviceType = serviceType;
        
        // Initialize total counters
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
        
        // Initialize distribution summaries for per-transaction metrics
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

    /**
     * Record a message being sent.
     * @param orderId The order ID for per-transaction tracking
     * @param messageType The type of message (event, command, reply)
     */
    public void recordMessageSent(String orderId, String messageType) {
        messagesSentCounter.increment();
        messageCountByOrder.computeIfAbsent(orderId, k -> new AtomicInteger(0)).incrementAndGet();
        
        // Also record with message type tag for detailed analysis
        Counter.builder(SagaMetrics.SAGA_MESSAGES_TOTAL)
                .tag(SagaMetrics.TAG_SERVICE, serviceType)
                .tag(SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_SENT)
                .tag(SagaMetrics.TAG_MESSAGE_TYPE, messageType)
                .register(meterRegistry)
                .increment();
    }

    /**
     * Record a message being received.
     * @param orderId The order ID for per-transaction tracking
     * @param messageType The type of message (event, command, reply)
     */
    public void recordMessageReceived(String orderId, String messageType) {
        messagesReceivedCounter.increment();
        messageCountByOrder.computeIfAbsent(orderId, k -> new AtomicInteger(0)).incrementAndGet();
        
        // Also record with message type tag for detailed analysis
        Counter.builder(SagaMetrics.SAGA_MESSAGES_TOTAL)
                .tag(SagaMetrics.TAG_SERVICE, serviceType)
                .tag(SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_RECEIVED)
                .tag(SagaMetrics.TAG_MESSAGE_TYPE, messageType)
                .register(meterRegistry)
                .increment();
    }

    /**
     * Record a database insert operation.
     * @param orderId The order ID for per-transaction tracking
     * @param entity The entity type being inserted
     */
    public void recordDbInsert(String orderId, String entity) {
        dbInsertsCounter.increment();
        dbWriteCountByOrder.computeIfAbsent(orderId, k -> new AtomicInteger(0)).incrementAndGet();
        
        // Also record with entity tag for detailed analysis
        Counter.builder(SagaMetrics.SAGA_DB_WRITES_TOTAL)
                .tag(SagaMetrics.TAG_SERVICE, serviceType)
                .tag(SagaMetrics.TAG_OPERATION, SagaMetrics.OPERATION_INSERT)
                .tag(SagaMetrics.TAG_ENTITY, entity)
                .register(meterRegistry)
                .increment();
    }

    /**
     * Record a database update operation.
     * @param orderId The order ID for per-transaction tracking
     * @param entity The entity type being updated
     */
    public void recordDbUpdate(String orderId, String entity) {
        dbUpdatesCounter.increment();
        dbWriteCountByOrder.computeIfAbsent(orderId, k -> new AtomicInteger(0)).incrementAndGet();
        
        // Also record with entity tag for detailed analysis
        Counter.builder(SagaMetrics.SAGA_DB_WRITES_TOTAL)
                .tag(SagaMetrics.TAG_SERVICE, serviceType)
                .tag(SagaMetrics.TAG_OPERATION, SagaMetrics.OPERATION_UPDATE)
                .tag(SagaMetrics.TAG_ENTITY, entity)
                .register(meterRegistry)
                .increment();
    }

    /**
     * Record message latency between services.
     * @param fromService The sending service
     * @param toService The receiving service
     * @param latency The latency duration
     */
    public void recordMessageLatency(String fromService, String toService, Duration latency) {
        Timer.builder(SagaMetrics.SAGA_MESSAGE_LATENCY)
                .tag(SagaMetrics.TAG_SERVICE, serviceType)
                .tag(SagaMetrics.TAG_FROM_SERVICE, fromService)
                .tag(SagaMetrics.TAG_TO_SERVICE, toService)
                .register(meterRegistry)
                .record(latency);
    }

    /**
     * Called when a saga completes successfully.
     * Records per-transaction metrics and cleans up tracking data.
     * @param orderId The order ID
     */
    public void recordSagaSuccess(String orderId) {
        AtomicInteger messageCount = messageCountByOrder.remove(orderId);
        AtomicInteger dbWriteCount = dbWriteCountByOrder.remove(orderId);
        
        if (messageCount != null) {
            messagesPerSuccessfulSaga.record(messageCount.get());
        }
        if (dbWriteCount != null) {
            dbWritesPerSuccessfulSaga.record(dbWriteCount.get());
        }
    }

    /**
     * Called when a saga fails.
     * Records per-transaction metrics and cleans up tracking data.
     * @param orderId The order ID
     */
    public void recordSagaFailure(String orderId) {
        AtomicInteger messageCount = messageCountByOrder.remove(orderId);
        AtomicInteger dbWriteCount = dbWriteCountByOrder.remove(orderId);
        
        if (messageCount != null) {
            messagesPerFailedSaga.record(messageCount.get());
        }
        if (dbWriteCount != null) {
            dbWritesPerFailedSaga.record(dbWriteCount.get());
        }
    }

    /**
     * Get current message count for an order (for debugging/logging).
     */
    public int getMessageCount(String orderId) {
        AtomicInteger count = messageCountByOrder.get(orderId);
        return count != null ? count.get() : 0;
    }

    /**
     * Get current DB write count for an order (for debugging/logging).
     */
    public int getDbWriteCount(String orderId) {
        AtomicInteger count = dbWriteCountByOrder.get(orderId);
        return count != null ? count.get() : 0;
    }
}
