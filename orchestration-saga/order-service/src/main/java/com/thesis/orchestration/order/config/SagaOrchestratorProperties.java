package com.thesis.orchestration.order.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Data
@Component
@ConfigurationProperties(prefix = "saga.orchestrator")
public class SagaOrchestratorProperties {
    private long pendingCommandRetryInterval = 10000L;
    private Duration pendingCommandRetryDelay = Duration.ofSeconds(10);
    private Duration inMemorySagaTtl = Duration.ofHours(1);
    private long inMemoryCleanupInterval = 60000L;
    private Duration sagaTimeout = Duration.ofSeconds(30);
    private long sagaTimeoutCheckInterval = 10000L;
    private long outboxPollInterval = 5000L;
    private Duration outboxRetryDelay = Duration.ofSeconds(10);
    private int outboxMaxAttempts = 10;
    private long outboxCleanupInterval = 300000L;
    private Duration outboxRetention = Duration.ofHours(24);
    private long outboxRetentionDays = 7;
    private long replyCleanupInterval = 3600000L;
    
    // Kafka send retry configuration
    private int kafkaSendMaxRetries = 3;
    private Duration kafkaSendRetryDelay = Duration.ofSeconds(2);
    private Duration kafkaSendTimeout = Duration.ofSeconds(10);
}
