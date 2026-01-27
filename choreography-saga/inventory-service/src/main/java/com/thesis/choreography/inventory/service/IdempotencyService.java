package com.thesis.choreography.inventory.service;

import com.thesis.choreography.inventory.model.ProcessedEvent;
import com.thesis.choreography.inventory.repository.ProcessedEventRepository;
import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Service to ensure idempotent processing of Kafka events.
 * Uses database storage to track processed events and prevent duplicates.
 */
@Service
@Slf4j
public class IdempotencyService {

    private final ProcessedEventRepository processedEventRepository;
    private final Counter duplicateEventCounter;
    private static final int RETENTION_DAYS = 7;

    public IdempotencyService(ProcessedEventRepository processedEventRepository, MeterRegistry meterRegistry) {
        this.processedEventRepository = processedEventRepository;
        this.duplicateEventCounter = meterRegistry.counter(
                SagaMetrics.SAGA_MESSAGES_TOTAL,
                "service", "inventory-choreography",
                "direction", "received",
                "type", "event",
                "outcome", "duplicate"
        );
    }

    /**
     * Check if an event has already been processed.
     *
     * @param eventId the unique event identifier
     * @return true if the event was already processed, false otherwise
     */
    public boolean isProcessed(String eventId) {
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
                duplicateEventCounter.increment();
                return true;
            }
            ProcessedEvent processedEvent = ProcessedEvent.builder()
                    .eventId(eventId)
                    .eventType(eventType)
                    .build();
            processedEventRepository.save(processedEvent);
            log.debug("Marked event {} of type {} as processed", eventId, eventType);
            return false;
        } catch (DataIntegrityViolationException e) {
            duplicateEventCounter.increment();
            log.debug("Event {} was already processed (concurrent processing)", eventId);
            return true;
        }
    }

    /**
     * Scheduled cleanup job to remove processed events older than retention period.
     * Runs daily at 2:10 AM to stagger with other services.
     */
    @Scheduled(cron = "0 10 2 * * *")
    @Transactional
    public void cleanupOldProcessedEvents() {
        Instant cutoff = Instant.now().minus(RETENTION_DAYS, ChronoUnit.DAYS);
        int deletedCount = processedEventRepository.deleteByProcessedAtBefore(cutoff);
        log.info("Cleaned up {} processed events older than {} days", deletedCount, RETENTION_DAYS);
    }
}
