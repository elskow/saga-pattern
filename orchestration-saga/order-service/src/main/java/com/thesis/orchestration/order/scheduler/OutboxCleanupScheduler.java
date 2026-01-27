package com.thesis.orchestration.order.scheduler;

import com.thesis.common.metrics.SagaMetrics;
import com.thesis.orchestration.order.config.SagaOrchestratorProperties;
import com.thesis.orchestration.order.repository.OutboxCommandRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Periodic cleanup for outbox rows that are already terminal.
 */
@Component
@Slf4j
public class OutboxCleanupScheduler {

    private static final List<String> TERMINAL_STATUSES = List.of("SENT", "FAILED");

    private final OutboxCommandRepository outboxCommandRepository;
    private final SagaOrchestratorProperties sagaProperties;
    private final Counter outboxCleanupCounter;

    public OutboxCleanupScheduler(OutboxCommandRepository outboxCommandRepository,
                                  SagaOrchestratorProperties sagaProperties,
                                  MeterRegistry meterRegistry) {
        this.outboxCommandRepository = outboxCommandRepository;
        this.sagaProperties = sagaProperties;
        this.outboxCleanupCounter = meterRegistry.counter(
                SagaMetrics.OUTBOX_CLEANUP_DELETIONS,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION
        );
    }

    @Scheduled(fixedDelayString = "${saga.orchestrator.outbox-cleanup-interval:5m}")
    @Transactional
    public void cleanupOutbox() {
        Instant cutoff = Instant.now().minus(Duration.ofDays(sagaProperties.getOutboxRetentionDays()));
        LocalDateTime cutoffDateTime = LocalDateTime.ofInstant(cutoff, java.time.ZoneId.systemDefault());
        long deleted = outboxCommandRepository.deleteByStatusInAndCreatedAtBefore(TERMINAL_STATUSES, cutoffDateTime);
        if (deleted > 0) {
            outboxCleanupCounter.increment(deleted);
            log.info("Outbox cleanup deleted {} rows", deleted);
        }
    }
}
