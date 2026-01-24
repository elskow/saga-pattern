package com.thesis.orchestration.order.scheduler;

import com.thesis.orchestration.order.config.SagaOrchestratorProperties;
import com.thesis.orchestration.order.model.OutboxCommand;
import com.thesis.orchestration.order.repository.OutboxCommandRepository;
import com.thesis.orchestration.order.statemachine.OrderSagaOrchestrator;
import com.thesis.common.metrics.SagaMetrics;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Publishes pending outbox commands to Kafka.
 */
@Component
@Slf4j
public class OutboxCommandPublisherScheduler {

    private static final String OUTBOX_STATUS_PENDING = "PENDING";
    private static final String OUTBOX_STATUS_SENT = "SENT";
    private static final String OUTBOX_STATUS_FAILED = "FAILED";

    private final OutboxCommandRepository outboxCommandRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final SagaOrchestratorProperties sagaProperties;
    private final OrderSagaOrchestrator orchestrator;
    private final Counter outboxPublishAttemptsCounter;
    private final Counter outboxPublishSuccessCounter;
    private final Counter outboxPublishFailureCounter;
    private final Counter outboxMaxAttemptsExceededCounter;

    public OutboxCommandPublisherScheduler(OutboxCommandRepository outboxCommandRepository,
                                           KafkaTemplate<String, Object> kafkaTemplate,
                                           SagaOrchestratorProperties sagaProperties,
                                           OrderSagaOrchestrator orchestrator,
                                           MeterRegistry meterRegistry) {
        this.outboxCommandRepository = outboxCommandRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.sagaProperties = sagaProperties;
        this.orchestrator = orchestrator;
        this.outboxPublishAttemptsCounter = meterRegistry.counter(
                SagaMetrics.OUTBOX_PUBLISH_ATTEMPTS,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION
        );
        this.outboxPublishSuccessCounter = meterRegistry.counter(
                SagaMetrics.OUTBOX_PUBLISH_SUCCESS,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION
        );
        this.outboxPublishFailureCounter = meterRegistry.counter(
                SagaMetrics.OUTBOX_PUBLISH_FAILURE,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION
        );
        this.outboxMaxAttemptsExceededCounter = meterRegistry.counter(
                SagaMetrics.OUTBOX_MAX_ATTEMPTS_EXCEEDED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION
        );
        meterRegistry.gauge(
                SagaMetrics.OUTBOX_PENDING_COUNT,
                java.util.List.of(io.micrometer.core.instrument.Tag.of(SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION)),
                outboxCommandRepository,
                repo -> repo.findByStatus(OUTBOX_STATUS_PENDING).size()
        );
        meterRegistry.gauge(
                SagaMetrics.OUTBOX_FAILED_COUNT,
                java.util.List.of(io.micrometer.core.instrument.Tag.of(SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION)),
                outboxCommandRepository,
                repo -> repo.findByStatus(OUTBOX_STATUS_FAILED).size()
        );
    }

    @Scheduled(fixedDelayString = "${saga.orchestrator.outbox-poll-interval:5s}")
    public void publishPendingCommands() {
        Duration retryDelay = sagaProperties.getOutboxRetryDelay();
        LocalDateTime cutoff = LocalDateTime.now().minus(retryDelay);
        List<OutboxCommand> pendingCommands = outboxCommandRepository.findByStatus(OUTBOX_STATUS_PENDING);

        for (OutboxCommand outbox : pendingCommands) {
            if (outbox.getLastAttemptAt() != null && outbox.getLastAttemptAt().isAfter(cutoff)) {
                continue;
            }
            if (outbox.getAttempts() >= sagaProperties.getOutboxMaxAttempts()) {
                log.error("Outbox command exceeded max attempts: orderId={}, type={}, attempts={}",
                        outbox.getOrderId(), outbox.getCommandType(), outbox.getAttempts());
                outbox.setStatus(OUTBOX_STATUS_FAILED);
                outboxCommandRepository.save(outbox);
                orchestrator.markCommandFailedForOutbox(outbox.getOrderId(), outbox.getCommandType());
                outboxMaxAttemptsExceededCounter.increment();
                outboxPublishFailureCounter.increment();
                continue;
            }
            try {
                outboxPublishAttemptsCounter.increment();
                CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(
                        outbox.getTopic(), outbox.getOrderId(), outbox.getPayloadJson());

                future.whenComplete((result, ex) -> {
                    if (ex == null) {
                        try {
                            outbox.setStatus(OUTBOX_STATUS_SENT);
                            outbox.setLastAttemptAt(LocalDateTime.now());
                            outbox.setAttempts(outbox.getAttempts() + 1);
                            outboxCommandRepository.save(outbox);
                            orchestrator.markCommandSentForOutbox(outbox.getOrderId(), outbox.getCommandType());
                            outboxPublishSuccessCounter.increment();
                            log.debug("Successfully published outbox command orderId={}, type={}",
                                    outbox.getOrderId(), outbox.getCommandType());
                        } catch (Exception e) {
                            log.error("Failed to update outbox status after successful send orderId={}, type={}: {}",
                                    outbox.getOrderId(), outbox.getCommandType(), e.getMessage(), e);
                        }
                    } else {
                        try {
                            outbox.setLastAttemptAt(LocalDateTime.now());
                            outbox.setAttempts(outbox.getAttempts() + 1);
                            outboxCommandRepository.save(outbox);
                            outboxPublishFailureCounter.increment();
                            log.error("Failed to publish outbox command orderId={}, type={}: {}",
                                    outbox.getOrderId(), outbox.getCommandType(), ex.getMessage(), ex);
                        } catch (Exception e) {
                            log.error("Failed to update outbox status after send failure orderId={}, type={}: {}",
                                    outbox.getOrderId(), outbox.getCommandType(), e.getMessage(), e);
                        }
                    }
                });
            } catch (Exception e) {
                // Handle synchronous exceptions (e.g., serialization errors)
                outbox.setLastAttemptAt(LocalDateTime.now());
                outbox.setAttempts(outbox.getAttempts() + 1);
                outboxCommandRepository.save(outbox);
                outboxPublishFailureCounter.increment();
                log.error("Exception while sending outbox command orderId={}, type={}: {}",
                        outbox.getOrderId(), outbox.getCommandType(), e.getMessage(), e);
            }
        }
    }
}
