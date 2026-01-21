package com.thesis.orchestration.inventory.config;

import com.thesis.orchestration.inventory.model.ProductEntity;
import com.thesis.orchestration.inventory.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "app.data.init", havingValue = "true", matchIfMissing = true)
public class DataInitializer {

    @Bean
    public CommandLineRunner initializeProducts(ProductRepository productRepository) {
        return args -> {
            if (productRepository.count() == 0) {
                log.info("Initializing sample products...");
                
                productRepository.save(ProductEntity.builder()
                        .productId("PROD-001")
                        .name("Laptop")
                        .quantity(100)
                        .reservedQuantity(0)
                        .build());
                
                productRepository.save(ProductEntity.builder()
                        .productId("PROD-002")
                        .name("Smartphone")
                        .quantity(200)
                        .reservedQuantity(0)
                        .build());
                
                productRepository.save(ProductEntity.builder()
                        .productId("PROD-003")
                        .name("Headphones")
                        .quantity(500)
                        .reservedQuantity(0)
                        .build());
                
                productRepository.save(ProductEntity.builder()
                        .productId("PROD-004")
                        .name("Tablet")
                        .quantity(150)
                        .reservedQuantity(0)
                        .build());
                
                log.info("Sample products initialized successfully");
            }
        };
    }
}
