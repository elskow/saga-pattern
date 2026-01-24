package com.thesis.orchestration.order.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Data
@Component
@ConfigurationProperties(prefix = "saga.orchestrator")
public class SagaOrchestratorProperties {
    private Duration pendingCommandRetryInterval = Duration.ofSeconds(10);
    private Duration pendingCommandRetryDelay = Duration.ofSeconds(10);
    private Duration inMemorySagaTtl = Duration.ofHours(1);
    private Duration inMemoryCleanupInterval = Duration.ofMinutes(1);
    private Duration sagaTimeout = Duration.ofSeconds(30);
    private Duration sagaTimeoutCheckInterval = Duration.ofSeconds(10);
    private Duration outboxPollInterval = Duration.ofSeconds(5);
    private Duration outboxRetryDelay = Duration.ofSeconds(10);
    private int outboxMaxAttempts = 10;
    private Duration outboxCleanupInterval = Duration.ofMinutes(5);
    private Duration outboxRetention = Duration.ofHours(24);
    private long outboxRetentionDays = 7;
    
    // Kafka send retry configuration
    private int kafkaSendMaxRetries = 3;
    private Duration kafkaSendRetryDelay = Duration.ofSeconds(2);
    private Duration kafkaSendTimeout = Duration.ofSeconds(10);
}
