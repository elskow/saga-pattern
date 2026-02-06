package com.thesis.common.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "app.kafka.topics")
@Validated
public record KafkaTopicsProperties(
    @NotBlank String orderEvents,
    @NotBlank String paymentEvents,
    @NotBlank String inventoryEvents,
    @NotBlank String shippingEvents,
    @NotBlank String paymentCommands,
    @NotBlank String inventoryCommands,
    @NotBlank String shippingCommands,
    @NotBlank String paymentReplies,
    @NotBlank String inventoryReplies,
    @NotBlank String shippingReplies
) {
    public KafkaTopicsProperties {
        orderEvents = defaultIfBlank(orderEvents, "order-events");
        paymentEvents = defaultIfBlank(paymentEvents, "payment-events");
        inventoryEvents = defaultIfBlank(inventoryEvents, "inventory-events");
        shippingEvents = defaultIfBlank(shippingEvents, "shipping-events");
        paymentCommands = defaultIfBlank(paymentCommands, "orchestration.payment.commands");
        inventoryCommands = defaultIfBlank(inventoryCommands, "orchestration.inventory.commands");
        shippingCommands = defaultIfBlank(shippingCommands, "orchestration.shipping.commands");
        paymentReplies = defaultIfBlank(paymentReplies, "orchestration.payment.replies");
        inventoryReplies = defaultIfBlank(inventoryReplies, "orchestration.inventory.replies");
        shippingReplies = defaultIfBlank(shippingReplies, "orchestration.shipping.replies");
    }

    private static String defaultIfBlank(String value, String defaultValue) {
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    public KafkaTopicsProperties() {
        this(null, null, null, null, null, null, null, null, null, null);
    }

    public static final String ORDER_EVENTS_TOPIC = "${app.kafka.topics.order-events:order-events}";
    public static final String PAYMENT_EVENTS_TOPIC = "${app.kafka.topics.payment-events:payment-events}";
    public static final String INVENTORY_EVENTS_TOPIC = "${app.kafka.topics.inventory-events:inventory-events}";
    public static final String SHIPPING_EVENTS_TOPIC = "${app.kafka.topics.shipping-events:shipping-events}";
    public static final String PAYMENT_COMMANDS_TOPIC = "${app.kafka.topics.payment-commands:orchestration.payment.commands}";
    public static final String INVENTORY_COMMANDS_TOPIC = "${app.kafka.topics.inventory-commands:orchestration.inventory.commands}";
    public static final String SHIPPING_COMMANDS_TOPIC = "${app.kafka.topics.shipping-commands:orchestration.shipping.commands}";
    public static final String PAYMENT_REPLIES_TOPIC = "${app.kafka.topics.payment-replies:orchestration.payment.replies}";
    public static final String INVENTORY_REPLIES_TOPIC = "${app.kafka.topics.inventory-replies:orchestration.inventory.replies}";
    public static final String SHIPPING_REPLIES_TOPIC = "${app.kafka.topics.shipping-replies:orchestration.shipping.replies}";
}
