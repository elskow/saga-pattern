package com.thesis.orchestration.inventory;

import com.thesis.orchestration.inventory.aot.InventoryRuntimeHints;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.boot.autoconfigure.domain.EntityScan;

@SpringBootApplication(scanBasePackages = {"com.thesis.orchestration.inventory", "com.thesis.common"})
@ImportRuntimeHints(InventoryRuntimeHints.class)
@EnableJpaRepositories(basePackages = {"com.thesis.orchestration.inventory.repository", "com.thesis.saga.persistence"})
@EntityScan(basePackages = {"com.thesis.orchestration.inventory.model", "com.thesis.saga.persistence"})
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
