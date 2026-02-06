package com.thesis.choreography.shipping;

import com.thesis.choreography.shipping.aot.ShippingRuntimeHints;
import com.thesis.common.retry.MetricsRetryListener;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.thesis.choreography.shipping", "com.thesis.common"})
@EnableScheduling
@EnableRetry
@ImportRuntimeHints(ShippingRuntimeHints.class)
public class ShippingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShippingServiceApplication.class, args);
    }
    
    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    public MetricsRetryListener metricsRetryListener(MeterRegistry meterRegistry) {
        return new MetricsRetryListener(meterRegistry, "shipping-choreography");
    }
}
