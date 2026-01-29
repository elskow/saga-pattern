package com.thesis.common.dto;

/**
 * Kafka topic name constants.
 * These are default values that can be overridden via environment variables:
 * - KAFKA_TOPIC_ORDER_EVENTS
 * - KAFKA_TOPIC_PAYMENT_EVENTS
 * - KAFKA_TOPIC_INVENTORY_EVENTS
 * - KAFKA_TOPIC_SHIPPING_EVENTS
 * <p>
 * For @KafkaListener annotations, use SpEL expressions like:
 *
 * @KafkaListener(topics = "${app.kafka.topics.order-events}")
 */
public class KafkaTopics {
    public static final String ORDER_EVENTS = "order-events";
    public static final String PAYMENT_EVENTS = "payment-events";
    public static final String INVENTORY_EVENTS = "inventory-events";
    public static final String SHIPPING_EVENTS = "shipping-events";

    // SpEL expression keys for use in @KafkaListener annotations
    public static final String ORDER_EVENTS_TOPIC = "${app.kafka.topics.order-events:order-events}";
    public static final String PAYMENT_EVENTS_TOPIC = "${app.kafka.topics.payment-events:payment-events}";
    public static final String INVENTORY_EVENTS_TOPIC = "${app.kafka.topics.inventory-events:inventory-events}";
    public static final String SHIPPING_EVENTS_TOPIC = "${app.kafka.topics.shipping-events:shipping-events}";

    private KafkaTopics() {
    }
}
