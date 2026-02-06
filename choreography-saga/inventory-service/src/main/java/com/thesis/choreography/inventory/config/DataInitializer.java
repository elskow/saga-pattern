package com.thesis.choreography.inventory.config;

import com.thesis.choreography.inventory.model.Product;
import com.thesis.choreography.inventory.repository.ProductRepository;
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

                productRepository.save(Product.builder()
                    .productId("PROD-005")
                    .productName("Monitor")
                    .quantityAvailable(120)
                    .quantityReserved(0)
                    .build());

                productRepository.save(Product.builder()
                    .productId("PROD-006")
                    .productName("Webcam")
                    .quantityAvailable(300)
                    .quantityReserved(0)
                    .build());

                productRepository.save(Product.builder()
                    .productId("PROD-007")
                    .productName("USB Hub")
                    .quantityAvailable(400)
                    .quantityReserved(0)
                    .build());

                productRepository.save(Product.builder()
                    .productId("PROD-008")
                    .productName("Mousepad")
                    .quantityAvailable(500)
                    .quantityReserved(0)
                    .build());

                productRepository.save(Product.builder()
                    .productId("PROD-LOW-001")
                    .productName("Rare Item")
                    .quantityAvailable(10)
                    .quantityReserved(0)
                    .build());

                productRepository.save(Product.builder()
                    .productId("PROD-LOW-002")
                    .productName("Limited Edition")
                    .quantityAvailable(15)
                    .quantityReserved(0)
                    .build());

                productRepository.save(Product.builder()
                    .productId("PROD-PREMIUM-001")
                    .productName("Premium Item")
                    .quantityAvailable(100)
                    .quantityReserved(0)
                    .build());

                log.info("Sample products initialized successfully");
            }
        };
    }
}
