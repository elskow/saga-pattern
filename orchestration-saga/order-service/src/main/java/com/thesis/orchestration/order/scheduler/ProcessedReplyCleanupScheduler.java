package com.thesis.orchestration.order.scheduler;

import com.thesis.common.metrics.SagaMetrics;
import com.thesis.orchestration.order.config.SagaOrchestratorProperties;
import com.thesis.orchestration.order.repository.ProcessedReplyRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Periodic cleanup for processed reply records used for idempotency tracking.
 * Removes records older than the configured retention period to prevent unbounded growth.
 */
@Component
@Slf4j
public class ProcessedReplyCleanupScheduler {

    private final ProcessedReplyRepository processedReplyRepository;
    private final SagaOrchestratorProperties sagaProperties;
    private final Counter replyCleanupCounter;

    public ProcessedReplyCleanupScheduler(ProcessedReplyRepository processedReplyRepository,
                                          SagaOrchestratorProperties sagaProperties,
                                          MeterRegistry meterRegistry) {
        this.processedReplyRepository = processedReplyRepository;
        this.sagaProperties = sagaProperties;
        this.replyCleanupCounter = meterRegistry.counter(
                "saga.processed_reply.cleanup.deletions",
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION
        );
    }

    @Scheduled(fixedDelayString = "${saga.orchestrator.reply-cleanup-interval}")
    @Transactional
    public void cleanupProcessedReplies() { // Force update
        // Use same retention as outbox, or default to 7 days
        long retentionDays = sagaProperties.getOutboxRetentionDays();
        Instant cutoff = Instant.now().minus(Duration.ofDays(retentionDays));
        LocalDateTime cutoffDateTime = LocalDateTime.ofInstant(cutoff, ZoneId.systemDefault());
        
        long deleted = processedReplyRepository.deleteByProcessedAtBefore(cutoffDateTime);
        if (deleted > 0) {
            replyCleanupCounter.increment(deleted);
            log.info("ProcessedReply cleanup deleted {} records older than {} days", deleted, retentionDays);
        }
    }
}
