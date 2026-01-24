package com.thesis.choreography.payment;

import com.thesis.choreography.payment.aot.PaymentRuntimeHints;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.thesis.choreography.payment", "com.thesis.common"})
@EnableScheduling
@EnableRetry
@ImportRuntimeHints(PaymentRuntimeHints.class)
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
