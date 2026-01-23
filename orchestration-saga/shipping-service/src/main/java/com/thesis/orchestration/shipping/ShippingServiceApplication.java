package com.thesis.orchestration.shipping;

import com.thesis.orchestration.shipping.aot.ShippingRuntimeHints;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportRuntimeHints;

@SpringBootApplication(scanBasePackages = {"com.thesis.orchestration.shipping", "com.thesis.common"})
@ImportRuntimeHints(ShippingRuntimeHints.class)
public class ShippingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShippingServiceApplication.class, args);
    }
}
