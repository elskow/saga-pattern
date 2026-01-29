package com.thesis.orchestration.order.health;

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
import java.time.LocalDateTime;
import java.time.ZoneId;
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
        LocalDateTime cutoffLocal = LocalDateTime.ofInstant(timeoutCutoff, ZoneId.systemDefault());
        List<String> terminalStates = List.of(
            "COMPLETED",
            "CANCELLED"
        );

        long stuckCount = sagaInstanceRepository
            .findByCurrentStateNotInAndUpdatedAtBefore(terminalStates, cutoffLocal)
            .size();

        if (stuckSagasGauge == null) {
            stuckSagasGauge = Gauge.builder("saga.stuck.sagas.count", this, indicator -> {
                Instant cutoff = Instant.now().minus(sagaProperties.getSagaTimeout());
                LocalDateTime lambdaCutoff = LocalDateTime.ofInstant(cutoff, ZoneId.systemDefault());
                List<String> states = List.of("COMPLETED", "CANCELLED");
                return sagaInstanceRepository.findByCurrentStateNotInAndUpdatedAtBefore(states, lambdaCutoff).size();
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
