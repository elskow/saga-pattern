package com.thesis.choreography.inventory.config;

import com.thesis.choreography.inventory.model.Product;
import com.thesis.choreography.inventory.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class DataInitializer {

    @Bean
    public CommandLineRunner initializeProducts(ProductRepository productRepository) {
        return args -> {
            if (productRepository.count() == 0) {
                log.info("Initializing sample products...");
                
                productRepository.save(Product.builder()
                        .productId("PROD-001")
                        .productName("Laptop")
                        .quantityAvailable(100)
                        .quantityReserved(0)
                        .build());
                
                productRepository.save(Product.builder()
                        .productId("PROD-002")
                        .productName("Smartphone")
                        .quantityAvailable(200)
                        .quantityReserved(0)
                        .build());
                
                productRepository.save(Product.builder()
                        .productId("PROD-003")
                        .productName("Headphones")
                        .quantityAvailable(500)
                        .quantityReserved(0)
                        .build());
                
                productRepository.save(Product.builder()
                        .productId("PROD-004")
                        .productName("Tablet")
                        .quantityAvailable(150)
                        .quantityReserved(0)
                        .build());
                
                log.info("Sample products initialized successfully");
            }
        };
    }
}
