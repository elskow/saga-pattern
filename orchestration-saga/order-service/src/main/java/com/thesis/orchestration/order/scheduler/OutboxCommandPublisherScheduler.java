package com.thesis.orchestration.order.scheduler;

import com.thesis.common.metrics.SagaMetrics;
import com.thesis.orchestration.order.config.SagaOrchestratorProperties;
import com.thesis.orchestration.order.model.OutboxCommand;
import com.thesis.orchestration.order.repository.OutboxCommandRepository;
import com.thesis.orchestration.order.statemachine.OrderSagaOrchestrator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes pending outbox commands to Kafka.
 * Uses pessimistic locking to prevent duplicate sends in multi-instance deployments.
 */
@Component
@Slf4j
public class OutboxCommandPublisherScheduler {

    private static final String OUTBOX_STATUS_PENDING = "PENDING";
    private static final String OUTBOX_STATUS_SENT = "SENT";
    private static final String OUTBOX_STATUS_FAILED = "FAILED";
    private static final long KAFKA_SEND_TIMEOUT_SECONDS = 30;

    private final OutboxCommandRepository outboxCommandRepository;
    private final KafkaTemplate<String, String> stringKafkaTemplate;
    private final SagaOrchestratorProperties sagaProperties;
    private final OrderSagaOrchestrator orchestrator;
    private final Counter outboxPublishAttemptsCounter;
    private final Counter outboxPublishSuccessCounter;
    private final Counter outboxPublishFailureCounter;
    private final Counter outboxMaxAttemptsExceededCounter;

    public OutboxCommandPublisherScheduler(OutboxCommandRepository outboxCommandRepository,
                                           KafkaTemplate<String, String> stringKafkaTemplate,
                                           SagaOrchestratorProperties sagaProperties,
                                           OrderSagaOrchestrator orchestrator,
                                           MeterRegistry meterRegistry) {
        this.outboxCommandRepository = outboxCommandRepository;
        this.stringKafkaTemplate = stringKafkaTemplate;
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
            List.of(io.micrometer.core.instrument.Tag.of(SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION)),
            outboxCommandRepository,
            repo -> repo.findByStatus(OUTBOX_STATUS_PENDING).size()
        );
        meterRegistry.gauge(
            SagaMetrics.OUTBOX_FAILED_COUNT,
            List.of(io.micrometer.core.instrument.Tag.of(SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION)),
            outboxCommandRepository,
            repo -> repo.findByStatus(OUTBOX_STATUS_FAILED).size()
        );
    }

    /**
     * Publishes pending outbox commands with pessimistic locking.
     * The @Transactional ensures the pessimistic lock is held during processing.
     * Uses synchronous Kafka send to ensure message is delivered before releasing lock.
     */
    @Scheduled(fixedDelayString = "${saga.orchestrator.outbox-poll-interval:5s}")
    @Transactional
    public void publishPendingCommands() {
        Duration retryDelay = sagaProperties.getOutboxRetryDelay();
        LocalDateTime cutoff = LocalDateTime.now().minus(retryDelay);

        // Use pessimistic locking query to prevent concurrent processing
        List<OutboxCommand> pendingCommands = outboxCommandRepository.findPendingWithLock(OUTBOX_STATUS_PENDING, cutoff);

        for (OutboxCommand outbox : pendingCommands) {
            if (outbox.getAttempts() >= sagaProperties.getOutboxMaxAttempts()) {
                log.error("Outbox command exceeded max attempts: orderId={}, type={}, attempts={}",
                    outbox.getOrderId(), outbox.getCommandType(), outbox.getAttempts());
                outbox.setStatus(OUTBOX_STATUS_FAILED);
                outboxCommandRepository.save(outbox);
                orchestrator.markCommandFailedForOutbox(outbox.getOrderId(), outbox.getCommandType());
                outboxMaxAttemptsExceededCounter.increment();
                outboxPublishFailureCounter.increment();

                // Trigger saga timeout/compensation for the stuck saga
                // This ensures the saga doesn't remain in a pending state forever
                try {
                    orchestrator.handleTimeout(outbox.getOrderId());
                    log.info("Triggered saga timeout for order {} due to outbox max attempts exceeded", outbox.getOrderId());
                } catch (Exception e) {
                    log.error("Failed to trigger saga timeout for order {}: {}", outbox.getOrderId(), e.getMessage(), e);
                }
                continue;
            }

            processOutboxCommand(outbox);
        }
    }

    /**
     * Processes a single outbox command synchronously to ensure atomicity.
     * The Kafka send is done synchronously to ensure the message is delivered
     * before we update the outbox status and release the lock.
     */
    private void processOutboxCommand(OutboxCommand outbox) {
        try {
            outboxPublishAttemptsCounter.increment();
            CompletableFuture<SendResult<String, String>> future = stringKafkaTemplate.send(
                outbox.getTopic(), outbox.getOrderId(), outbox.getPayloadJson());

            // Wait synchronously for the send to complete to avoid dirty read issues
            try {
                future.get(KAFKA_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                // Success - update outbox within the same transaction
                outbox.setStatus(OUTBOX_STATUS_SENT);
                outbox.setLastAttemptAt(LocalDateTime.now());
                outbox.setAttempts(outbox.getAttempts() + 1);
                outboxCommandRepository.save(outbox);
                orchestrator.markCommandSentForOutbox(outbox.getOrderId(), outbox.getCommandType());
                outboxPublishSuccessCounter.increment();
                log.debug("Successfully published outbox command orderId={}, type={}",
                    outbox.getOrderId(), outbox.getCommandType());

            } catch (TimeoutException e) {
                handleSendFailure(outbox, "Kafka send timeout after " + KAFKA_SEND_TIMEOUT_SECONDS + " seconds");
            } catch (ExecutionException e) {
                handleSendFailure(outbox, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                handleSendFailure(outbox, "Kafka send interrupted");
            }
        } catch (Exception e) {
            // Handle synchronous exceptions (e.g., serialization errors)
            handleSendFailure(outbox, e.getMessage());
        }
    }

    private void handleSendFailure(OutboxCommand outbox, String errorMessage) {
        outbox.setLastAttemptAt(LocalDateTime.now());
        outbox.setAttempts(outbox.getAttempts() + 1);
        outboxCommandRepository.save(outbox);
        outboxPublishFailureCounter.increment();
        log.error("Failed to publish outbox command orderId={}, type={}: {}",
            outbox.getOrderId(), outbox.getCommandType(), errorMessage);
    }
}
