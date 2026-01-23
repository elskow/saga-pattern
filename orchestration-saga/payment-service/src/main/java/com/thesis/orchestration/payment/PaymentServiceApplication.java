package com.thesis.orchestration.payment;

import com.thesis.orchestration.payment.aot.PaymentRuntimeHints;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportRuntimeHints;

@SpringBootApplication(scanBasePackages = {"com.thesis.orchestration.payment", "com.thesis.common"})
@ImportRuntimeHints(PaymentRuntimeHints.class)
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
