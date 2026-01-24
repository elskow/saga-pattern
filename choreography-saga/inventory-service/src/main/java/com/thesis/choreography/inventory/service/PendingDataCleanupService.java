package com.thesis.choreography.inventory.service;

import com.thesis.choreography.inventory.repository.PendingOrderItemRepository;
import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Service to cleanup orphaned pending order items.
 * Removes pending items that are older than retention period or belong to orders that will never complete.
 */
@Service
@Slf4j
public class PendingDataCleanupService {

    private final PendingOrderItemRepository pendingOrderItemRepository;
    private static final int RETENTION_DAYS = 7;

    public PendingDataCleanupService(PendingOrderItemRepository pendingOrderItemRepository,
                                     MeterRegistry meterRegistry) {
        this.pendingOrderItemRepository = pendingOrderItemRepository;
        
        // Register gauge metric for pending order items count
        meterRegistry.gauge(
                "saga.pending.order.items.count",
                java.util.List.of(io.micrometer.core.instrument.Tag.of(SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY)),
                pendingOrderItemRepository,
                repo -> repo.count()
        );
    }

    /**
     * Scheduled cleanup job to remove pending order items older than retention period.
     * Runs daily at 3 AM.
     */
    @Scheduled(cron = "0 0 3 * * *")
    @Transactional
    public void cleanupOrphanedPendingItems() {
        try {
            Instant cutoff = Instant.now().minus(RETENTION_DAYS, ChronoUnit.DAYS);
            int deletedCount = pendingOrderItemRepository.deleteByCreatedAtBefore(cutoff);
            log.info("Cleaned up {} pending order items older than {} days", deletedCount, RETENTION_DAYS);
            
            // Additional cleanup: Remove items for orders that are in terminal states
            // This would require integration with order service, but for now TTL cleanup is sufficient
        } catch (Exception e) {
            log.error("Error during pending order items cleanup", e);
        }
    }
}
