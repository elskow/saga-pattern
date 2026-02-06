package com.thesis.orchestration.order.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties(prefix = "saga.orchestrator")
public record SagaOrchestratorProperties(
    @DefaultValue("10000") long pendingCommandRetryInterval,
    @DefaultValue("PT10S") Duration pendingCommandRetryDelay,
    @DefaultValue("PT1H") Duration inMemorySagaTtl,
    @DefaultValue("60000") long inMemoryCleanupInterval,
    @DefaultValue("PT30S") Duration sagaTimeout,
    @DefaultValue("10000") long sagaTimeoutCheckInterval,
    @DefaultValue("5000") long outboxPollInterval,
    @DefaultValue("PT10S") Duration outboxRetryDelay,
    @DefaultValue("10") int outboxMaxAttempts,
    @DefaultValue("300000") long outboxCleanupInterval,
    @DefaultValue("PT24H") Duration outboxRetention,
    @DefaultValue("7") long outboxRetentionDays,
    @DefaultValue("3600000") long replyCleanupInterval,
    @DefaultValue("3") int kafkaSendMaxRetries,
    @DefaultValue("PT2S") Duration kafkaSendRetryDelay,
    @DefaultValue("PT10S") Duration kafkaSendTimeout
) {
}
