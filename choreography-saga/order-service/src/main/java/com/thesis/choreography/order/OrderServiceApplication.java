package com.thesis.choreography.order;

import com.thesis.choreography.order.aot.OrderRuntimeHints;
import com.thesis.common.retry.MetricsRetryListener;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.thesis.choreography.order", "com.thesis.common"})
@EnableScheduling
@EnableRetry
@ImportRuntimeHints(OrderRuntimeHints.class)
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
    
    @Bean
    public MetricsRetryListener metricsRetryListener(MeterRegistry meterRegistry) {
        return new MetricsRetryListener(meterRegistry, "order-choreography");
    }
}
