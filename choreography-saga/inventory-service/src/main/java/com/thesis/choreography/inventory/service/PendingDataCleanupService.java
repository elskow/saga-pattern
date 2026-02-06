package com.thesis.choreography.inventory.service;

import com.thesis.choreography.inventory.repository.PendingOrderItemRepository;
import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.repository.CrudRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Service
@Slf4j
public class PendingDataCleanupService {

    private static final int RETENTION_DAYS = 7;
    private final PendingOrderItemRepository pendingOrderItemRepository;

    public PendingDataCleanupService(PendingOrderItemRepository pendingOrderItemRepository,
                                     MeterRegistry meterRegistry) {
        this.pendingOrderItemRepository = pendingOrderItemRepository;

        meterRegistry.gauge(
            "saga.pending.order.items.count",
            List.of(io.micrometer.core.instrument.Tag.of(SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY)),
            pendingOrderItemRepository,
            CrudRepository::count
        );
    }

    @Scheduled(cron = "0 0 3 * * *")
    @Transactional
    public void cleanupOrphanedPendingItems() {
        try {
            Instant cutoff = Instant.now().minus(RETENTION_DAYS, ChronoUnit.DAYS);
            int deletedCount = pendingOrderItemRepository.deleteByCreatedAtBefore(cutoff);
            log.debug("Cleaned up {} pending order items older than {} days", deletedCount, RETENTION_DAYS);
        } catch (Exception e) {
            log.error("Error during pending order items cleanup", e);
        }
    }
}
