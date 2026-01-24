package com.thesis.orchestration.order.scheduler;

import com.thesis.orchestration.order.config.SagaOrchestratorProperties;
import com.thesis.orchestration.order.model.ProcessedCommand;
import com.thesis.orchestration.order.repository.ProcessedCommandRepository;
import com.thesis.orchestration.order.statemachine.OrderSagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Scheduler to retry pending saga commands that were not confirmed as sent.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class PendingCommandRetryScheduler {

    private static final String COMMAND_STATUS_PENDING = "PENDING";

    private final ProcessedCommandRepository processedCommandRepository;
    private final OrderSagaOrchestrator orchestrator;
    private final SagaOrchestratorProperties sagaProperties;

    /**
     * Retry pending commands every 10 seconds.
     */
    @Scheduled(fixedDelayString = "${saga.orchestrator.pending-command-retry-interval:10s}")
    public void retryPendingCommands() {
        Duration retryDelay = sagaProperties.getPendingCommandRetryDelay();
        LocalDateTime cutoff = LocalDateTime.now().minus(retryDelay);
        List<ProcessedCommand> pendingCommands = processedCommandRepository.findByStatus(COMMAND_STATUS_PENDING);

        for (ProcessedCommand command : pendingCommands) {
            if (command.getProcessedAt() != null && command.getProcessedAt().isAfter(cutoff)) {
                continue;
            }
            log.warn("Retrying pending command: orderId={}, type={}, lastAttempt={}",
                    command.getOrderId(), command.getCommandType(), command.getProcessedAt());
            orchestrator.retryPendingCommand(command.getOrderId(), command.getCommandType());
        }
    }
}
