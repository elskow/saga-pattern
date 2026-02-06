package com.thesis.choreography.shipping.health;

import com.thesis.choreography.shipping.repository.PendingShippingAddressRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PendingDataHealthIndicator implements HealthIndicator {
    
    private static final long MAX_PENDING_ADDRESSES = 500;
    private static final long WARNING_THRESHOLD = 250;
    
    private final PendingShippingAddressRepository pendingShippingAddressRepository;
    
    @Override
    public Health health() {
        try {
            long pendingAddresses = pendingShippingAddressRepository.count();
            
            Health.Builder builder = Health.up()
                .withDetail("pendingShippingAddresses", pendingAddresses)
                .withDetail("maxThreshold", MAX_PENDING_ADDRESSES)
                .withDetail("warningThreshold", WARNING_THRESHOLD);
            
            if (pendingAddresses > MAX_PENDING_ADDRESSES) {
                log.warn("Pending shipping addresses exceeded maximum threshold: {} (max: {})", 
                        pendingAddresses, MAX_PENDING_ADDRESSES);
                return builder.down()
                    .withDetail("message", "Too many pending shipping addresses: " + pendingAddresses + 
                            " (threshold: " + MAX_PENDING_ADDRESSES + ")")
                    .withDetail("status", "CRITICAL")
                    .build();
            }
            
            if (pendingAddresses > WARNING_THRESHOLD) {
                log.warn("Pending shipping addresses approaching threshold: {} (warning: {})", 
                        pendingAddresses, WARNING_THRESHOLD);
                return builder.status("WARNING")
                    .withDetail("message", "Pending shipping addresses approaching threshold: " + pendingAddresses)
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
