package com.thesis.saga.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

@ConfigurationProperties(prefix = "saga.framework")
public record SagaFrameworkProperties(
    Duration sagaTimeout,
    Duration timeoutCheckInterval,
    Duration outboxPublishInterval,
    Duration outboxRetryInterval,
    int outboxMaxAttempts,
    int outboxBatchSize,
    Duration outboxRetentionPeriod,
    Duration processedMessageRetentionPeriod,
    Duration cleanupInterval,
    Duration inMemoryMaxAge,
    Duration inMemoryCleanupInterval,
    boolean recoveryEnabled,
    boolean outboxPublisherEnabled,
    boolean timeoutSchedulerEnabled,
    boolean cleanupSchedulerEnabled,
    Duration kafkaSendTimeout
) {
    public SagaFrameworkProperties {
        sagaTimeout = Objects.requireNonNullElse(sagaTimeout, Duration.ofMinutes(5));
        timeoutCheckInterval = Objects.requireNonNullElse(timeoutCheckInterval, Duration.ofSeconds(10));
        outboxPublishInterval = Objects.requireNonNullElse(outboxPublishInterval, Duration.ofSeconds(1));
        outboxRetryInterval = Objects.requireNonNullElse(outboxRetryInterval, Duration.ofSeconds(30));
        if (outboxMaxAttempts <= 0) outboxMaxAttempts = 5;
        if (outboxBatchSize <= 0) outboxBatchSize = 100;
        outboxRetentionPeriod = Objects.requireNonNullElse(outboxRetentionPeriod, Duration.ofHours(24));
        processedMessageRetentionPeriod = Objects.requireNonNullElse(processedMessageRetentionPeriod, Duration.ofHours(24));
        cleanupInterval = Objects.requireNonNullElse(cleanupInterval, Duration.ofHours(1));
        inMemoryMaxAge = Objects.requireNonNullElse(inMemoryMaxAge, Duration.ofHours(1));
        inMemoryCleanupInterval = Objects.requireNonNullElse(inMemoryCleanupInterval, Duration.ofMinutes(1));
        kafkaSendTimeout = Objects.requireNonNullElse(kafkaSendTimeout, Duration.ofSeconds(10));
    }

    public SagaFrameworkProperties() {
        this(null, null, null, null, 0, 0, null, null, null, null, null, true, true, true, true, null);
    }
}
