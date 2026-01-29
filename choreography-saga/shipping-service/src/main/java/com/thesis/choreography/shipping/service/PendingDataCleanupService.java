package com.thesis.choreography.shipping.service;

import com.thesis.choreography.shipping.repository.PendingShippingAddressRepository;
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

/**
 * Service to cleanup orphaned pending shipping addresses.
 * Removes pending addresses that are older than retention period.
 */
@Service
@Slf4j
public class PendingDataCleanupService {

    private static final int RETENTION_DAYS = 7;
    private final PendingShippingAddressRepository pendingShippingAddressRepository;

    public PendingDataCleanupService(PendingShippingAddressRepository pendingShippingAddressRepository,
                                     MeterRegistry meterRegistry) {
        this.pendingShippingAddressRepository = pendingShippingAddressRepository;

        // Register gauge metric for pending shipping addresses count
        meterRegistry.gauge(
            "saga.pending.shipping.addresses.count",
            List.of(io.micrometer.core.instrument.Tag.of(SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY)),
            pendingShippingAddressRepository,
            CrudRepository::count
        );
    }

    /**
     * Scheduled cleanup job to remove pending shipping addresses older than retention period.
     * Runs daily at 3:15 AM (slightly offset from inventory cleanup).
     */
    @Scheduled(cron = "0 15 3 * * *")
    @Transactional
    public void cleanupOrphanedPendingAddresses() {
        try {
            Instant cutoff = Instant.now().minus(RETENTION_DAYS, ChronoUnit.DAYS);
            int deletedCount = pendingShippingAddressRepository.deleteByCreatedAtBefore(cutoff);
            log.info("Cleaned up {} pending shipping addresses older than {} days", deletedCount, RETENTION_DAYS);
        } catch (Exception e) {
            log.error("Error during pending shipping addresses cleanup", e);
        }
    }
}
