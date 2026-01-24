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
import java.util.List;

/**
 * Scheduler for detecting and handling stale/timed-out sagas.
 * Triggers compensation for sagas that are stuck in pending states.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SagaTimeoutScheduler {

    private static final List<String> PENDING_STATES = List.of(
            "PAYMENT_PENDING",
            "INVENTORY_PENDING",
            "SHIPPING_PENDING",
            "COMPENSATING"
    );

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

        for (String state : PENDING_STATES) {
            List<SagaInstance> staleSagas = sagaInstanceRepository
                    .findByCurrentStateNotInAndUpdatedAtBefore(List.of("COMPLETED", "CANCELLED"), cutoff);

            for (SagaInstance saga : staleSagas) {
                log.warn("Saga timeout detected: orderId={}, state={}, lastUpdated={}",
                        saga.getOrderId(), saga.getCurrentState(), saga.getUpdatedAt());
                orchestrator.handleTimeout(saga.getOrderId());
            }
        }
    }
}
