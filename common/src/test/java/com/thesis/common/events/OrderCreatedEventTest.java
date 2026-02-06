package com.thesis.common.events;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OrderCreatedEventTest {

    @Test
    void shouldCreateOrderCreatedEventWithAllFields() {
        // Given
        String orderId = "ORDER-123";
        String customerId = "CUST-001";
        String shippingAddress = "123 Main St";
        String correlationId = "CORR-001";
        BigDecimal totalAmount = new BigDecimal("199.99");
        Instant createdAt = Instant.now();

        var item = new OrderCreatedEvent.OrderItemEvent(
                "PROD-001",
                "Test Product",
                2,
                new BigDecimal("99.99"));

        // When
        var event = new OrderCreatedEvent(
                orderId,
                customerId,
                shippingAddress,
                correlationId,
                List.of(item),
                totalAmount,
                createdAt);

        // Then
        assertThat(event.orderId()).isEqualTo(orderId);
        assertThat(event.customerId()).isEqualTo(customerId);
        assertThat(event.shippingAddress()).isEqualTo(shippingAddress);
        assertThat(event.correlationId()).isEqualTo(correlationId);
        assertThat(event.totalAmount()).isEqualTo(totalAmount);
        assertThat(event.createdAt()).isEqualTo(createdAt);
        assertThat(event.items()).hasSize(1);
        assertThat(event.items().getFirst().productId()).isEqualTo("PROD-001");
    }

    @Test
    void shouldCreateOrderItemEventWithAllFields() {
        // Given
        String productId = "PROD-001";
        String productName = "Test Product";
        int quantity = 5;
        BigDecimal price = new BigDecimal("49.99");

        // When
        var item = new OrderCreatedEvent.OrderItemEvent(productId, productName, quantity, price);

        // Then
        assertThat(item.productId()).isEqualTo(productId);
        assertThat(item.productName()).isEqualTo(productName);
        assertThat(item.quantity()).isEqualTo(quantity);
        assertThat(item.price()).isEqualTo(price);
    }

    @Test
    void shouldCreateOrderItemEventUsingFactoryMethod() {
        // When
        var item = OrderCreatedEvent.OrderItemEvent.of("PROD-002", "Another Product", 3, new BigDecimal("29.99"));

        // Then
        assertThat(item.productId()).isEqualTo("PROD-002");
        assertThat(item.productName()).isEqualTo("Another Product");
        assertThat(item.quantity()).isEqualTo(3);
        assertThat(item.price()).isEqualTo(new BigDecimal("29.99"));
    }

    @Test
    void shouldHaveEqualsAndHashCode() {
        // Given
        var event1 = new OrderCreatedEvent(
                "ORDER-123", "CUST-001", "123 Main St", "CORR-001",
                List.of(), new BigDecimal("100.00"), null);

        var event2 = new OrderCreatedEvent(
                "ORDER-123", "CUST-001", "123 Main St", "CORR-001",
                List.of(), new BigDecimal("100.00"), null);

        // Then
        assertThat(event1).isEqualTo(event2);
        assertThat(event1.hashCode()).isEqualTo(event2.hashCode());
    }

    @Test
    void shouldHaveToString() {
        // Given
        var event = new OrderCreatedEvent(
                "ORDER-789", "CUST-003", "456 Oak Ave", "CORR-002",
                List.of(), new BigDecimal("50.00"), null);

        // Then
        assertThat(event.toString()).contains("ORDER-789");
        assertThat(event.toString()).contains("CUST-003");
    }
}
