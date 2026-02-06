package com.thesis.common.config;

import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "app.kafka.error-handling")
@Validated
public record KafkaErrorHandlingProperties(
    @Min(100) long backoffIntervalMs,
    @Min(1) int maxRetries,
    @Min(1) int defaultPartitions
) {
    public KafkaErrorHandlingProperties {
        if (backoffIntervalMs <= 0) backoffIntervalMs = 1000L;
        if (maxRetries <= 0) maxRetries = 3;
        if (defaultPartitions <= 0) defaultPartitions = 3;
    }

    public KafkaErrorHandlingProperties() {
        this(1000L, 3, 3);
    }
}
