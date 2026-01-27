package com.thesis.orchestration.order.scheduler;

import com.thesis.orchestration.order.config.SagaOrchestratorProperties;
import com.thesis.orchestration.order.model.SagaInstance;
import com.thesis.orchestration.order.repository.SagaInstanceRepository;
import com.thesis.orchestration.order.statemachine.OrderSagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

/**
 * Scheduler for detecting and handling stale/timed-out sagas.
 * Triggers compensation for sagas that are stuck in pending states.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SagaTimeoutScheduler {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final OrderSagaOrchestrator orchestrator;
    private final SagaOrchestratorProperties sagaProperties;

    /**
     * Check for stale sagas every 10 seconds.
     */
    @Scheduled(fixedDelayString = "${saga.orchestrator.saga-timeout-check-interval:10s}")
    public void checkStaleSagas() {
        Duration timeout = sagaProperties.getSagaTimeout();
        Instant cutoff = Instant.now().minus(timeout);
        LocalDateTime cutoffLocal = LocalDateTime.ofInstant(cutoff, ZoneId.systemDefault());
        List<String> terminalStates = List.of("COMPLETED", "CANCELLED");

        List<SagaInstance> staleSagas = sagaInstanceRepository
                .findByCurrentStateNotInAndUpdatedAtBefore(terminalStates, cutoffLocal);

        for (SagaInstance saga : staleSagas) {
            log.warn("Saga timeout detected: orderId={}, state={}, lastUpdated={}",
                    saga.getOrderId(), saga.getCurrentState(), saga.getUpdatedAt());
            orchestrator.handleTimeout(saga.getOrderId());
        }
    }
}
