package com.thesis.choreography.inventory;

import com.thesis.choreography.inventory.aot.InventoryRuntimeHints;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.thesis.choreography.inventory", "com.thesis.common"})
@EnableScheduling
@EnableRetry
@ImportRuntimeHints(InventoryRuntimeHints.class)
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
