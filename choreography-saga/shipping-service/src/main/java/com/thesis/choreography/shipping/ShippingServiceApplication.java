package com.thesis.choreography.shipping;

import com.thesis.choreography.shipping.aot.ShippingRuntimeHints;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.thesis.choreography.shipping", "com.thesis.common"})
@EnableScheduling
@ImportRuntimeHints(ShippingRuntimeHints.class)
public class ShippingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShippingServiceApplication.class, args);
    }
}
