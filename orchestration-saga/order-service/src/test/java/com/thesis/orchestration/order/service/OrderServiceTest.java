package com.thesis.orchestration.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.exception.OrderNotFoundException;
import com.thesis.orchestration.order.model.OrderEntity;
import com.thesis.orchestration.order.repository.OrderRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    private ObjectMapper objectMapper;
    private MeterRegistry meterRegistry;
    private OrderService orderService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        meterRegistry = new SimpleMeterRegistry();
        orderService = new OrderService(orderRepository, objectMapper, meterRegistry);
    }

    @Test
    void shouldCreateOrderSuccessfully() {
        // Given
        String orderId = "ORDER-123";
        String customerId = "CUST-001";
        BigDecimal totalAmount = new BigDecimal("99.99");
        String shippingAddress = "123 Main Street";
        List<OrderCreatedEvent.OrderItemEvent> items = List.of(
                OrderCreatedEvent.OrderItemEvent.of(
                        "PROD-001",
                        "Test Product",
                        2,
                        new BigDecimal("49.99")
                )
        );

        when(orderRepository.save(any(OrderEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When
        orderService.createOrder(orderId, customerId, totalAmount, shippingAddress, items, "PAY-1", "RES-1", "SHIP-1");

        // Then
        ArgumentCaptor<OrderEntity> orderCaptor = ArgumentCaptor.forClass(OrderEntity.class);
        verify(orderRepository).save(orderCaptor.capture());

        OrderEntity savedOrder = orderCaptor.getValue();
        assertThat(savedOrder.getOrderId()).isEqualTo(orderId);
        assertThat(savedOrder.getCustomerId()).isEqualTo(customerId);
        assertThat(savedOrder.getTotalAmount()).isEqualTo(totalAmount);
        assertThat(savedOrder.getStatus()).isEqualTo(OrderEntity.OrderStatus.CREATED);
    }

    @Test
    void shouldFindOrderById() {
        // Given
        String orderId = "ORDER-123";
        OrderEntity order = OrderEntity.builder()
                .orderId(orderId)
                .customerId("CUST-001")
                .status(OrderEntity.OrderStatus.COMPLETED)
                .build();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        // When
        Optional<OrderEntity> result = orderService.findById(orderId);

        // Then
        assertThat(result).isPresent();
        assertThat(result.get().getOrderId()).isEqualTo(orderId);
    }

    @Test
    void shouldReturnEmptyWhenOrderNotFound() {
        // Given
        String orderId = "NON-EXISTENT";
        when(orderRepository.findById(orderId)).thenReturn(Optional.empty());

        // When
        Optional<OrderEntity> result = orderService.findById(orderId);

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldFindOrdersByCustomerId() {
        // Given
        String customerId = "CUST-001";
        List<OrderEntity> orders = List.of(
                OrderEntity.builder().orderId("ORDER-1").customerId(customerId).build(),
                OrderEntity.builder().orderId("ORDER-2").customerId(customerId).build()
        );

        when(orderRepository.findByCustomerId(customerId)).thenReturn(orders);

        // When
        List<OrderEntity> result = orderService.findByCustomerId(customerId);

        // Then
        assertThat(result).hasSize(2);
    }

    @Test
    void shouldCompleteOrderSuccessfully() {
        // Given
        String orderId = "ORDER-123";
        OrderEntity order = OrderEntity.builder()
                .orderId(orderId)
                .status(OrderEntity.OrderStatus.CREATED)
                .build();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        // When
        orderService.completeOrder(orderId);

        // Then
        verify(orderRepository).save(any(OrderEntity.class));
        assertThat(order.getStatus()).isEqualTo(OrderEntity.OrderStatus.COMPLETED);
        assertThat(order.getCompletedAt()).isNotNull();
    }

    @Test
    void shouldThrowExceptionWhenCompletingNonExistentOrder() {
        // Given
        String orderId = "NON-EXISTENT";
        when(orderRepository.findById(orderId)).thenReturn(Optional.empty());

        // When/Then
        assertThatThrownBy(() -> orderService.completeOrder(orderId))
                .isInstanceOf(OrderNotFoundException.class)
                .hasMessageContaining(orderId);
    }

    @Test
    void shouldFailOrderWithReason() {
        // Given
        String orderId = "ORDER-123";
        String reason = "Payment failed";
        OrderEntity order = OrderEntity.builder()
                .orderId(orderId)
                .status(OrderEntity.OrderStatus.CREATED)
                .build();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        // When
        orderService.failOrder(orderId, reason);

        // Then
        verify(orderRepository).save(any(OrderEntity.class));
        assertThat(order.getStatus()).isEqualTo(OrderEntity.OrderStatus.CANCELLED);
        assertThat(order.getFailureReason()).isEqualTo(reason);
    }

    @Test
    void shouldThrowExceptionWhenFailingNonExistentOrder() {
        // Given
        String orderId = "NON-EXISTENT";
        when(orderRepository.findById(orderId)).thenReturn(Optional.empty());

        // When/Then
        assertThatThrownBy(() -> orderService.failOrder(orderId, "Some reason"))
                .isInstanceOf(OrderNotFoundException.class)
                .hasMessageContaining(orderId);
    }

    @Test
    void shouldFindAllOrders() {
        // Given
        List<OrderEntity> orders = List.of(
                OrderEntity.builder().orderId("ORDER-1").build(),
                OrderEntity.builder().orderId("ORDER-2").build(),
                OrderEntity.builder().orderId("ORDER-3").build()
        );

        when(orderRepository.findAll()).thenReturn(orders);

        // When
        List<OrderEntity> result = orderService.findAll();

        // Then
        assertThat(result).hasSize(3);
    }
}
