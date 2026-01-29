package com.thesis.choreography.inventory.health;

import com.thesis.choreography.inventory.repository.PendingOrderItemRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Health indicator for pending order items.
 * Monitors pending data accumulation to detect stuck sagas or system issues.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PendingDataHealthIndicator implements HealthIndicator {

    private static final long MAX_PENDING_ITEMS = 1000;
    private static final long WARNING_THRESHOLD = 500;

    private final PendingOrderItemRepository pendingOrderItemRepository;

    @Override
    public Health health() {
        try {
            long pendingItems = pendingOrderItemRepository.count();

            Health.Builder builder = Health.up()
                .withDetail("pendingOrderItems", pendingItems)
                .withDetail("maxThreshold", MAX_PENDING_ITEMS)
                .withDetail("warningThreshold", WARNING_THRESHOLD);

            if (pendingItems > MAX_PENDING_ITEMS) {
                log.warn("Pending order items exceeded maximum threshold: {} (max: {})",
                    pendingItems, MAX_PENDING_ITEMS);
                return builder.down()
                    .withDetail("message", "Too many pending order items: " + pendingItems +
                        " (threshold: " + MAX_PENDING_ITEMS + ")")
                    .withDetail("status", "CRITICAL")
                    .build();
            }

            if (pendingItems > WARNING_THRESHOLD) {
                log.warn("Pending order items approaching threshold: {} (warning: {})",
                    pendingItems, WARNING_THRESHOLD);
                return builder.status("WARNING")
                    .withDetail("message", "Pending order items approaching threshold: " + pendingItems)
                    .withDetail("status", "WARNING")
                    .build();
            }

            return builder
                .withDetail("status", "HEALTHY")
                .build();
        } catch (Exception e) {
            log.error("Error checking pending data health", e);
            return Health.down()
                .withDetail("error", e.getMessage())
                .build();
        }
    }
}
