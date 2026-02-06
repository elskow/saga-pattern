package com.thesis.orchestration.order;

import com.thesis.orchestration.order.aot.OrderRuntimeHints;
import com.thesis.orchestration.order.config.SagaOrchestratorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.thesis.orchestration.order", "com.thesis.common"})
@ImportRuntimeHints(OrderRuntimeHints.class)
@EnableScheduling
@EnableConfigurationProperties(SagaOrchestratorProperties.class)
@EnableJpaRepositories(basePackages = {"com.thesis.orchestration.order.repository", "com.thesis.saga.persistence"})
@EntityScan(basePackages = {"com.thesis.orchestration.order.model", "com.thesis.saga.persistence"})
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
