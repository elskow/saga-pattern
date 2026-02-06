package com.thesis.saga.scheduler;

import com.thesis.saga.config.SagaFrameworkProperties;
import com.thesis.saga.metrics.SagaMetricsRecorder;
import com.thesis.saga.persistence.SagaInstanceEntity;
import com.thesis.saga.persistence.SagaInstanceRepository;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Component
@Slf4j
public class SagaTimeoutScheduler {

    private final SagaInstanceRepository instanceRepository;
    private final SagaFrameworkProperties properties;
    private final SagaMetricsRecorder metricsRecorder;

    private final Map<String, OrchestratorRegistration> orchestrators = new ConcurrentHashMap<>();

    public SagaTimeoutScheduler(
            SagaInstanceRepository instanceRepository,
            SagaFrameworkProperties properties,
            SagaMetricsRecorder metricsRecorder) {
        this.instanceRepository = instanceRepository;
        this.properties = properties;
        this.metricsRecorder = metricsRecorder;
    }

    public void registerOrchestrator(String sagaType, List<String> terminalStates,
                                     Consumer<SagaInstanceEntity> timeoutHandler) {
        orchestrators.put(sagaType, new OrchestratorRegistration(terminalStates, timeoutHandler));
        log.info("Registered orchestrator for saga type: {}", sagaType);
    }

    public void unregisterOrchestrator(String sagaType) {
        orchestrators.remove(sagaType);
        log.info("Unregistered orchestrator for saga type: {}", sagaType);
    }

    @Scheduled(fixedDelayString = "${saga.framework.timeout-check-interval:10000}")
    @SchedulerLock(name = "sagaTimeoutChecker", lockAtLeastFor = "5s", lockAtMostFor = "5m")
    @Transactional
    public void checkStaleSagas() {
        if (!properties.timeoutSchedulerEnabled()) {
            return;
        }

        if (orchestrators.isEmpty()) {
            log.trace("No orchestrators registered, skipping timeout check");
            return;
        }

        LocalDateTime cutoff = LocalDateTime.now().minus(properties.sagaTimeout());
        log.trace("Checking for stale sagas updated before {}", cutoff);

        for (Map.Entry<String, OrchestratorRegistration> entry : orchestrators.entrySet()) {
            String sagaType = entry.getKey();
            OrchestratorRegistration registration = entry.getValue();

            try {
                List<SagaInstanceEntity> staleSagas = instanceRepository.findStaleSagas(
                        sagaType, registration.terminalStates(), cutoff);

                if (!staleSagas.isEmpty()) {
                    log.warn("Found {} stale {} saga(s), triggering timeout handling",
                            staleSagas.size(), sagaType);

                    for (SagaInstanceEntity saga : staleSagas) {
                        try {
                            registration.timeoutHandler().accept(saga);
                            metricsRecorder.recordSagaTimeout(sagaType);
                        } catch (Exception e) {
                            log.error("Error handling timeout for saga {}: {}",
                                    saga.getSagaId(), e.getMessage(), e);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Error checking stale sagas for type {}: {}", sagaType, e.getMessage(), e);
            }
        }
    }

    public List<SagaInstanceEntity> findStaleSagas(String sagaType, List<String> terminalStates) {
        LocalDateTime cutoff = LocalDateTime.now().minus(properties.sagaTimeout());
        return instanceRepository.findStaleSagas(sagaType, terminalStates, cutoff);
    }

    public int getRegisteredOrchestratorCount() {
        return orchestrators.size();
    }

    private record OrchestratorRegistration(
            List<String> terminalStates,
            Consumer<SagaInstanceEntity> timeoutHandler) {}
}
