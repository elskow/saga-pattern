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

        this.idempotencyCheckCounter = meterRegistry.counter(
            SagaMetrics.IDEMPOTENCY_CHECK_COUNT,
            SagaMetrics.TAG_SERVICE, SERVICE_NAME
        );

        this.duplicateDetectedCounter = meterRegistry.counter(
            SagaMetrics.IDEMPOTENCY_DUPLICATE_DETECTED,
            SagaMetrics.TAG_SERVICE, SERVICE_NAME
        );

        this.tableSizeGauge = new AtomicLong(0);
        Gauge.builder(SagaMetrics.IDEMPOTENCY_TABLE_SIZE, tableSizeGauge, AtomicLong::get)
            .tag(SagaMetrics.TAG_SERVICE, SERVICE_NAME)
            .register(meterRegistry);
    }

    public boolean isProcessed(String eventId) {
        idempotencyCheckCounter.increment();
        return processedEventRepository.existsByEventId(eventId);
    }

    @Transactional
    public boolean tryMarkAsProcessed(String eventId, String eventType) {
        try {
            if (isProcessed(eventId)) {
                log.trace("Event {} of type {} was already processed", eventId, eventType);
                duplicateDetectedCounter.increment();
                return false;
            }
            ProcessedEvent processedEvent = ProcessedEvent.builder()
                .eventId(eventId)
                .eventType(eventType)
                .build();
            processedEventRepository.save(processedEvent);
            log.trace("Marked event {} of type {} as processed", eventId, eventType);
            return true;
        } catch (DataIntegrityViolationException e) {
            duplicateDetectedCounter.increment();
            log.trace("Event {} was already processed (concurrent processing)", eventId);
            return false;
        }
    }

    @Scheduled(cron = "0 15 2 * * *")
    @Transactional
    public void cleanupOldProcessedEvents() {
        Instant cutoff = Instant.now().minus(RETENTION_DAYS, ChronoUnit.DAYS);
        int deletedCount = processedEventRepository.deleteByProcessedAtBefore(cutoff);
        log.debug("Cleaned up {} processed events older than {} days", deletedCount, RETENTION_DAYS);
    }

    @Scheduled(fixedRate = 60000)
    public void reportTableSize() {
        try {
            long count = processedEventRepository.count();
            tableSizeGauge.set(count);
            log.trace("ProcessedEvent table size: {}", count);
        } catch (Exception e) {
            log.warn("Failed to report ProcessedEvent table size: {}", e.getMessage());
        }
    }
}
