package com.thesis.common.config;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.aop.ObservedAspect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class that enables @Observed annotation support for automatic
 * span creation in distributed tracing.
 * <p>
 * This provides automatic tracing for methods annotated with @Observed,
 * creating spans that show up in Zipkin and can be correlated with metrics.
 */
@Configuration
@ConditionalOnClass(ObservedAspect.class)
public class ObservationConfig {

    /**
     * Creates an ObservedAspect bean that enables @Observed annotation processing.
     * This allows methods annotated with @Observed to automatically create spans
     * and observation records.
     */
    @Bean
    public ObservedAspect observedAspect(ObservationRegistry observationRegistry) {
        return new ObservedAspect(observationRegistry);
    }
}
