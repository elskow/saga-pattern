package com.thesis.orchestration.payment;

import com.thesis.orchestration.payment.aot.PaymentRuntimeHints;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.boot.autoconfigure.domain.EntityScan;

@SpringBootApplication(scanBasePackages = {"com.thesis.orchestration.payment", "com.thesis.common"})
@ImportRuntimeHints(PaymentRuntimeHints.class)
@EnableJpaRepositories(basePackages = {"com.thesis.orchestration.payment.repository", "com.thesis.saga.persistence"})
@EntityScan(basePackages = {"com.thesis.orchestration.payment.model", "com.thesis.saga.persistence"})
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
