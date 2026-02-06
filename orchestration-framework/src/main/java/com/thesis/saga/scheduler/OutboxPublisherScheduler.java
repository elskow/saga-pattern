package com.thesis.saga.scheduler;

import com.thesis.saga.config.SagaFrameworkProperties;
import com.thesis.saga.metrics.SagaMetricsRecorder;
import com.thesis.saga.persistence.OutboxEntity;
import com.thesis.saga.persistence.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;

@Component
@Slf4j
@RequiredArgsConstructor
@SuppressWarnings("preview")
public class OutboxPublisherScheduler {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final SagaFrameworkProperties properties;
    private final SagaMetricsRecorder metricsRecorder;

    @Scheduled(fixedDelayString = "${saga.framework.outbox-publish-interval:1000}")
    @SchedulerLock(name = "outboxPublisher", lockAtLeastFor = "500ms", lockAtMostFor = "5m")
    @Transactional
    public void publishPendingCommands() {
        LocalDateTime cutoff = LocalDateTime.now().minus(properties.outboxRetryInterval());
        int batchSize = properties.outboxBatchSize();

        List<OutboxEntity> pending = outboxRepository.findPendingWithLock(
            OutboxEntity.STATUS_PENDING, cutoff, batchSize);

        if (pending.isEmpty()) {
            return;
        }

        log.trace("Publishing {} pending outbox commands using structured concurrency", pending.size());

        pending.forEach(entry -> {
            entry.recordAttempt();
            metricsRecorder.recordOutboxPublishAttempt(entry.getSagaType());
        });

        try (var scope = StructuredTaskScope.open()) {
            List<Subtask<PublishResult>> subtasks = new ArrayList<>(pending.size());
            for (OutboxEntity entry : pending) {
                subtasks.add(scope.fork(() -> publishCommand(entry)));
            }

            scope.join();

            for (int i = 0; i < subtasks.size(); i++) {
                Subtask<PublishResult> subtask = subtasks.get(i);
                OutboxEntity entry = pending.get(i);
                
                if (subtask.state() == Subtask.State.SUCCESS) {
                    PublishResult result = subtask.get();
                    if (result.success()) {
                        entry.markSent();
                        metricsRecorder.recordOutboxPublishSuccess(entry.getSagaType());
                        log.trace("Published command {} for saga {} to topic {}",
                                entry.getCommandType(), entry.getSagaId(), entry.getTopic());
                    } else {
                        handlePublishFailure(entry, result.errorMessage());
                    }
                } else if (subtask.state() == Subtask.State.FAILED) {
                    String errorMsg = subtask.exception() != null 
                        ? subtask.exception().getMessage() 
                        : "Unknown error";
                    handlePublishFailure(entry, errorMsg);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Outbox publishing interrupted");
        }

        outboxRepository.saveAll(pending);
    }

    private PublishResult publishCommand(OutboxEntity entry) {
        try {
            kafkaTemplate.send(entry.getTopic(), entry.getSagaId(), entry.getPayloadJson()).get();
            return new PublishResult(true, null);
        } catch (Exception ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            log.error("Failed to publish command {} for saga {}: {}",
                    entry.getCommandType(), entry.getSagaId(), cause.getMessage());
            return new PublishResult(false, cause.getMessage());
        }
    }

    private void handlePublishFailure(OutboxEntity entry, String errorMessage) {
        if (entry.getAttempts() >= properties.outboxMaxAttempts()) {
            entry.markFailed(errorMessage);
            log.error("Outbox entry {} exhausted retries, marking as FAILED", entry.getId());
        }
        metricsRecorder.recordOutboxPublishFailure(entry.getSagaType());
    }

    private record PublishResult(boolean success, String errorMessage) {}
}
