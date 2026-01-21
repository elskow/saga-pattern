package com.thesis.common.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for Kafka topic names.
 * These can be overridden via environment variables:
 * - APP_KAFKA_TOPICS_ORDER_EVENTS
 * - APP_KAFKA_TOPICS_PAYMENT_EVENTS
 * - APP_KAFKA_TOPICS_INVENTORY_EVENTS
 * - APP_KAFKA_TOPICS_SHIPPING_EVENTS
 */
@Configuration
@ConfigurationProperties(prefix = "app.kafka.topics")
@Getter
@Setter
public class KafkaTopicsConfig {
    
    private String orderEvents = "order-events";
    private String paymentEvents = "payment-events";
    private String inventoryEvents = "inventory-events";
    private String shippingEvents = "shipping-events";
}
