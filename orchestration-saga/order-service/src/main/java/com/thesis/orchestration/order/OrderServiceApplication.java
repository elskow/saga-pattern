package com.thesis.orchestration.order;

import com.thesis.orchestration.order.aot.OrderRuntimeHints;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportRuntimeHints;

@SpringBootApplication(scanBasePackages = {"com.thesis.orchestration.order", "com.thesis.common"})
@ImportRuntimeHints(OrderRuntimeHints.class)
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
