package com.thesis.saga.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.thesis.saga.idempotency.IdempotencyService;
import com.thesis.saga.metrics.SagaMetricsRecorder;
import com.thesis.saga.outbox.OutboxService;
import com.thesis.saga.persistence.OutboxRepository;
import com.thesis.saga.persistence.ProcessedMessageRepository;
import com.thesis.saga.persistence.SagaInstanceRepository;
import com.thesis.saga.scheduler.CleanupScheduler;
import com.thesis.saga.scheduler.OutboxPublisherScheduler;
import com.thesis.saga.scheduler.SagaTimeoutScheduler;
import io.micrometer.core.instrument.MeterRegistry;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;
import java.util.concurrent.TimeUnit;

@AutoConfiguration(after = DataSourceAutoConfiguration.class)
@EnableConfigurationProperties(SagaFrameworkProperties.class)
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "10m")
@EnableCaching
@ComponentScan(basePackages = "com.thesis.saga")
public class SagaFrameworkAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(LockProvider.class)
    @ConditionalOnBean(DataSource.class)
    public LockProvider lockProvider(DataSource dataSource) {
        return new JdbcTemplateLockProvider(
                JdbcTemplateLockProvider.Configuration.builder()
                        .withJdbcTemplate(new org.springframework.jdbc.core.JdbcTemplate(dataSource))
                        .usingDbTime()
                        .build()
        );
    }

    @Bean
    @ConditionalOnBean(DataSource.class)
    public InitializingBean shedlockTableInitializer(DataSource dataSource) {
        return () -> {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            jdbcTemplate.execute(
                "CREATE TABLE IF NOT EXISTS shedlock (" +
                "name VARCHAR(64) NOT NULL, " +
                "lock_until TIMESTAMP NOT NULL, " +
                "locked_at TIMESTAMP NOT NULL, " +
                "locked_by VARCHAR(255) NOT NULL, " +
                "PRIMARY KEY (name)" +
                ")"
            );
        };
    }

    @Bean
    @ConditionalOnMissingBean(CacheManager.class)
    public CacheManager cacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();
        cacheManager.setCaffeine(Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .recordStats());
        return cacheManager;
    }

    @Bean
    @ConditionalOnMissingBean(SagaMetricsRecorder.class)
    public SagaMetricsRecorder sagaMetricsRecorder(MeterRegistry meterRegistry) {
        return new SagaMetricsRecorder(meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean(OutboxService.class)
    @ConditionalOnBean(OutboxRepository.class)
    public OutboxService outboxService(OutboxRepository outboxRepository, ObjectMapper objectMapper) {
        return new OutboxService(outboxRepository, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean(IdempotencyService.class)
    @ConditionalOnBean(ProcessedMessageRepository.class)
    public IdempotencyService idempotencyService(ProcessedMessageRepository repository) {
        return new IdempotencyService(repository);
    }

    @Bean
    @ConditionalOnMissingBean(OutboxPublisherScheduler.class)
    @ConditionalOnProperty(prefix = "saga.framework", name = "outbox-publisher-enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnBean({OutboxRepository.class, KafkaTemplate.class})
    public OutboxPublisherScheduler outboxPublisherScheduler(
            OutboxRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            SagaFrameworkProperties properties,
            SagaMetricsRecorder metricsRecorder) {
        return new OutboxPublisherScheduler(outboxRepository, kafkaTemplate, properties, metricsRecorder);
    }

    @Bean
    @ConditionalOnMissingBean(SagaTimeoutScheduler.class)
    @ConditionalOnProperty(prefix = "saga.framework", name = "timeout-scheduler-enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnBean(SagaInstanceRepository.class)
    public SagaTimeoutScheduler sagaTimeoutScheduler(
            SagaInstanceRepository instanceRepository,
            SagaFrameworkProperties properties,
            SagaMetricsRecorder metricsRecorder) {
        return new SagaTimeoutScheduler(instanceRepository, properties, metricsRecorder);
    }

    @Bean
    @ConditionalOnMissingBean(CleanupScheduler.class)
    @ConditionalOnProperty(prefix = "saga.framework", name = "cleanup-scheduler-enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnBean({OutboxService.class, IdempotencyService.class})
    public CleanupScheduler cleanupScheduler(
            OutboxService outboxService,
            IdempotencyService idempotencyService,
            SagaFrameworkProperties properties) {
        return new CleanupScheduler(outboxService, idempotencyService, properties);
    }
}
