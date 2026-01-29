package com.thesis.choreography.order.health;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaAdmin kafkaAdmin;
    private final int timeoutMs;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin,
                                @Value("${app.health.kafka.timeout-ms:5000}") int timeoutMs) {
        this.kafkaAdmin = kafkaAdmin;
        this.timeoutMs = timeoutMs;
    }
    
    @Override
    public Health health() {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DescribeClusterOptions options = new DescribeClusterOptions()
                .timeoutMs(timeoutMs);

            String clusterId = adminClient.describeCluster(options)
                .clusterId()
                .get(timeoutMs, TimeUnit.MILLISECONDS);

            int nodeCount = adminClient.describeCluster(options)
                .nodes()
                .get(timeoutMs, TimeUnit.MILLISECONDS)
                .size();

            return Health.up()
                .withDetail("clusterId", clusterId)
                .withDetail("nodeCount", nodeCount)
                .build();
        } catch (Exception e) {
            log.error("Kafka health check failed", e);
            return Health.down()
                .withDetail("error", e.getMessage())
                .build();
        }
    }
}
