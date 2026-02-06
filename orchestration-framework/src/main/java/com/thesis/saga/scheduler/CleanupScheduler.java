package com.thesis.saga.scheduler;

import com.thesis.saga.config.SagaFrameworkProperties;
import com.thesis.saga.idempotency.IdempotencyService;
import com.thesis.saga.outbox.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@Slf4j
@RequiredArgsConstructor
public class CleanupScheduler {

    private final OutboxService outboxService;
    private final IdempotencyService idempotencyService;
    private final SagaFrameworkProperties properties;

    @Scheduled(fixedDelayString = "${saga.framework.cleanup-interval:3600000}")
    @SchedulerLock(name = "sagaCleanup", lockAtLeastFor = "30s", lockAtMostFor = "30m")
    public void cleanupOldEntries() {
        LocalDateTime outboxCutoff = LocalDateTime.now().minus(properties.outboxRetentionPeriod());
        LocalDateTime messageCutoff = LocalDateTime.now().minus(properties.processedMessageRetentionPeriod());

        try {
            int outboxDeleted = outboxService.cleanupOldSent(outboxCutoff);
            int messagesDeleted = idempotencyService.cleanupOldEntries(messageCutoff);

            if (outboxDeleted > 0 || messagesDeleted > 0) {
                log.debug("Cleanup completed: {} outbox entries, {} processed messages deleted",
                        outboxDeleted, messagesDeleted);
            }
        } catch (Exception e) {
            log.error("Cleanup failed: {}", e.getMessage(), e);
        }
    }
}
