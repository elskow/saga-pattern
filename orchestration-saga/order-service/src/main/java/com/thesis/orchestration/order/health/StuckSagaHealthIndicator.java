package com.thesis.orchestration.order.health;

import com.thesis.common.metrics.SagaMetrics;
import com.thesis.orchestration.order.config.SagaOrchestratorProperties;
import com.thesis.orchestration.order.repository.SagaInstanceRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class StuckSagaHealthIndicator implements HealthIndicator {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaOrchestratorProperties sagaProperties;
    private final MeterRegistry meterRegistry;
    private Gauge stuckSagasGauge;

    @Override
    public Health health() {
        Instant timeoutCutoff = Instant.now().minus(sagaProperties.getSagaTimeout());
        List<String> terminalStates = List.of(
            "COMPLETED",
            "CANCELLED"
        );

        long stuckCount = sagaInstanceRepository
            .findByCurrentStateNotInAndUpdatedAtBefore(terminalStates, timeoutCutoff)
            .size();

        if (stuckSagasGauge == null) {
            stuckSagasGauge = Gauge.builder("saga.stuck.sagas.count", this, indicator -> {
                Instant cutoff = Instant.now().minus(sagaProperties.getSagaTimeout());
                List<String> states = List.of("COMPLETED", "CANCELLED");
                return (double) sagaInstanceRepository.findByCurrentStateNotInAndUpdatedAtBefore(states, cutoff).size();
            }).tag("service", "orchestration").register(meterRegistry);
        }

        if (stuckCount > 0) {
            return Health.down()
                .withDetail("stuckSagas", stuckCount)
                .withDetail("timeout", sagaProperties.getSagaTimeout())
                .withDetail("cutoffTime", timeoutCutoff)
                .build();
        }
        return Health.up()
                .withDetail("stuckSagas", 0)
                .build();
    }
}
