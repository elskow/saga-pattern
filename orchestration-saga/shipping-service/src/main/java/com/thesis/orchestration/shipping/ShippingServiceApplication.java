package com.thesis.orchestration.shipping;

import com.thesis.orchestration.shipping.aot.ShippingRuntimeHints;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.boot.autoconfigure.domain.EntityScan;

@SpringBootApplication(scanBasePackages = {"com.thesis.orchestration.shipping", "com.thesis.common"})
@ImportRuntimeHints(ShippingRuntimeHints.class)
@EnableJpaRepositories(basePackages = {"com.thesis.orchestration.shipping.repository", "com.thesis.saga.persistence"})
@EntityScan(basePackages = {"com.thesis.orchestration.shipping.model", "com.thesis.saga.persistence"})
public class ShippingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShippingServiceApplication.class, args);
    }
}
