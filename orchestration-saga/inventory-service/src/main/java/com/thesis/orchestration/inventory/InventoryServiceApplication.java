package com.thesis.orchestration.inventory;

import com.thesis.orchestration.inventory.aot.InventoryRuntimeHints;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportRuntimeHints;

@SpringBootApplication(scanBasePackages = {"com.thesis.orchestration.inventory", "com.thesis.common"})
@ImportRuntimeHints(InventoryRuntimeHints.class)
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
