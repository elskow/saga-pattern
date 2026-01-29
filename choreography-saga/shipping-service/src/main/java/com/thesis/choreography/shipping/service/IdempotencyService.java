package com.thesis.choreography.shipping.service;

import com.thesis.choreography.shipping.model.ProcessedEvent;
import com.thesis.choreography.shipping.repository.ProcessedEventRepository;
import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service to ensure idempotent processing of Kafka events.
 * Uses database storage to track processed events and prevent duplicates.
 */
@Service
@Slf4j
public class IdempotencyService {

    private static final int RETENTION_DAYS = 7;
    private static final String SERVICE_NAME = "shipping-choreography";
    private final ProcessedEventRepository processedEventRepository;
    private final Counter idempotencyCheckCounter;
    private final Counter duplicateDetectedCounter;
    private final AtomicLong tableSizeGauge;

    public IdempotencyService(ProcessedEventRepository processedEventRepository, MeterRegistry meterRegistry) {
        this.processedEventRepository = processedEventRepository;

        // Counter for total idempotency checks
        this.idempotencyCheckCounter = meterRegistry.counter(
            SagaMetrics.IDEMPOTENCY_CHECK_COUNT,
            SagaMetrics.TAG_SERVICE, SERVICE_NAME
        );

        // Counter for duplicate events detected
        this.duplicateDetectedCounter = meterRegistry.counter(
            SagaMetrics.IDEMPOTENCY_DUPLICATE_DETECTED,
            SagaMetrics.TAG_SERVICE, SERVICE_NAME
        );

        // Gauge for ProcessedEvent table size
        this.tableSizeGauge = new AtomicLong(0);
        Gauge.builder(SagaMetrics.IDEMPOTENCY_TABLE_SIZE, tableSizeGauge, AtomicLong::get)
            .tag(SagaMetrics.TAG_SERVICE, SERVICE_NAME)
            .register(meterRegistry);
    }

    /**
     * Check if an event has already been processed.
     *
     * @param eventId the unique event identifier
     * @return true if the event was already processed, false otherwise
     */
    public boolean isProcessed(String eventId) {
        idempotencyCheckCounter.increment();
        return processedEventRepository.existsByEventId(eventId);
    }

    /**
     * Mark an event as processed. This should be called within the same transaction
     * as the event processing to ensure atomicity.
     *
     * @param eventId   the unique event identifier
     * @param eventType the type of the event
     * @return true if the event was marked as processed, false if it was already processed
     */
    @Transactional
    public boolean markProcessed(String eventId, String eventType) {
        try {
            if (isProcessed(eventId)) {
                log.debug("Event {} of type {} was already processed", eventId, eventType);
                duplicateDetectedCounter.increment();
                return false;
            }
            ProcessedEvent processedEvent = ProcessedEvent.builder()
                .eventId(eventId)
                .eventType(eventType)
                .build();
            processedEventRepository.save(processedEvent);
            log.debug("Marked event {} of type {} as processed", eventId, eventType);
            return true;
        } catch (DataIntegrityViolationException e) {
            duplicateDetectedCounter.increment();
            log.debug("Event {} was already processed (concurrent processing)", eventId);
            return false;
        }
    }

    /**
     * Scheduled cleanup job to remove processed events older than retention period.
     * Runs daily at 2:15 AM to stagger with other services.
     */
    @Scheduled(cron = "0 15 2 * * *")
    @Transactional
    public void cleanupOldProcessedEvents() {
        Instant cutoff = Instant.now().minus(RETENTION_DAYS, ChronoUnit.DAYS);
        int deletedCount = processedEventRepository.deleteByProcessedAtBefore(cutoff);
        log.info("Cleaned up {} processed events older than {} days", deletedCount, RETENTION_DAYS);
    }

    /**
     * Scheduled job to report ProcessedEvent table size.
     * Runs every minute.
     */
    @Scheduled(fixedRate = 60000)
    public void reportTableSize() {
        try {
            long count = processedEventRepository.count();
            tableSizeGauge.set(count);
            log.debug("ProcessedEvent table size: {}", count);
        } catch (Exception e) {
            log.warn("Failed to report ProcessedEvent table size: {}", e.getMessage());
        }
    }
}
