package com.thesis.common.events;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OrderCreatedEventTest {

    @Test
    void shouldBuildOrderCreatedEventWithAllFields() {
        // Given
        String orderId = "ORDER-123";
        String customerId = "CUST-001";
        String shippingAddress = "123 Main St";
        BigDecimal totalAmount = new BigDecimal("199.99");
        Instant createdAt = Instant.now();

        OrderCreatedEvent.OrderItemEvent item = OrderCreatedEvent.OrderItemEvent.builder()
                .productId("PROD-001")
                .productName("Test Product")
                .quantity(2)
                .price(new BigDecimal("99.99"))
                .build();

        List<OrderCreatedEvent.OrderItemEvent> items = Arrays.asList(item);

        // When
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(orderId)
                .customerId(customerId)
                .shippingAddress(shippingAddress)
                .items(items)
                .totalAmount(totalAmount)
                .createdAt(createdAt)
                .build();

        // Then
        assertThat(event.getOrderId()).isEqualTo(orderId);
        assertThat(event.getCustomerId()).isEqualTo(customerId);
        assertThat(event.getShippingAddress()).isEqualTo(shippingAddress);
        assertThat(event.getTotalAmount()).isEqualTo(totalAmount);
        assertThat(event.getCreatedAt()).isEqualTo(createdAt);
        assertThat(event.getItems()).hasSize(1);
        assertThat(event.getItems().get(0).getProductId()).isEqualTo("PROD-001");
    }

    @Test
    void shouldBuildOrderItemEventWithAllFields() {
        // Given
        String productId = "PROD-001";
        String productName = "Test Product";
        int quantity = 5;
        BigDecimal price = new BigDecimal("49.99");

        // When
        OrderCreatedEvent.OrderItemEvent item = OrderCreatedEvent.OrderItemEvent.builder()
                .productId(productId)
                .productName(productName)
                .quantity(quantity)
                .price(price)
                .build();

        // Then
        assertThat(item.getProductId()).isEqualTo(productId);
        assertThat(item.getProductName()).isEqualTo(productName);
        assertThat(item.getQuantity()).isEqualTo(quantity);
        assertThat(item.getPrice()).isEqualTo(price);
    }

    @Test
    void shouldCreateOrderCreatedEventWithNoArgsConstructor() {
        // When
        OrderCreatedEvent event = new OrderCreatedEvent();

        // Then
        assertThat(event.getOrderId()).isNull();
        assertThat(event.getItems()).isNull();
    }

    @Test
    void shouldSupportSetters() {
        // Given
        OrderCreatedEvent event = new OrderCreatedEvent();

        // When
        event.setOrderId("ORDER-456");
        event.setCustomerId("CUST-002");

        // Then
        assertThat(event.getOrderId()).isEqualTo("ORDER-456");
        assertThat(event.getCustomerId()).isEqualTo("CUST-002");
    }

    @Test
    void shouldHaveEqualsAndHashCode() {
        // Given
        OrderCreatedEvent event1 = OrderCreatedEvent.builder()
                .orderId("ORDER-123")
                .customerId("CUST-001")
                .build();

        OrderCreatedEvent event2 = OrderCreatedEvent.builder()
                .orderId("ORDER-123")
                .customerId("CUST-001")
                .build();

        // Then
        assertThat(event1).isEqualTo(event2);
        assertThat(event1.hashCode()).isEqualTo(event2.hashCode());
    }
}
