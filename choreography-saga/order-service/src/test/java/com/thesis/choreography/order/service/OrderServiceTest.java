package com.thesis.choreography.order.service;

import com.thesis.choreography.order.kafka.OrderEventPublisher;
import com.thesis.choreography.order.model.Order;
import com.thesis.choreography.order.model.OrderItem;
import com.thesis.choreography.order.repository.OrderRepository;
import com.thesis.common.dto.CreateOrderRequest;
import com.thesis.common.dto.OrderResponse;
import com.thesis.common.events.OrderCreatedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
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

    @Mock
    private OrderEventPublisher eventPublisher;

    private MeterRegistry meterRegistry;
    private OrderService orderService;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        orderService = new OrderService(orderRepository, eventPublisher, meterRegistry);
    }

    @Test
    void shouldCreateOrderSuccessfully() {
        // Given
        CreateOrderRequest request = createOrderRequest();
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> {
            Order order = invocation.getArgument(0);
            order.setCreatedAt(Instant.now());
            return order;
        });

        // When
        OrderResponse response = orderService.createOrder(request);

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getOrderId()).isNotNull();
        assertThat(response.getCustomerId()).isEqualTo("CUST-001");
        assertThat(response.getStatus()).isEqualTo("PENDING");

        verify(orderRepository).save(any(Order.class));
        verify(eventPublisher).publishOrderCreated(any(OrderCreatedEvent.class));
    }

    @Test
    void shouldCalculateTotalAmountCorrectly() {
        // Given
        CreateOrderRequest request = createOrderRequest();
        ArgumentCaptor<Order> orderCaptor = ArgumentCaptor.forClass(Order.class);
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When
        orderService.createOrder(request);

        // Then
        verify(orderRepository).save(orderCaptor.capture());
        Order savedOrder = orderCaptor.getValue();
        // 2 items: 2 * 49.99 = 99.98
        assertThat(savedOrder.getTotalAmount()).isEqualByComparingTo(new BigDecimal("99.98"));
    }

    @Test
    void shouldPublishOrderCreatedEvent() {
        // Given
        CreateOrderRequest request = createOrderRequest();
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> invocation.getArgument(0));
        ArgumentCaptor<OrderCreatedEvent> eventCaptor = ArgumentCaptor.forClass(OrderCreatedEvent.class);

        // When
        orderService.createOrder(request);

        // Then
        verify(eventPublisher).publishOrderCreated(eventCaptor.capture());
        OrderCreatedEvent event = eventCaptor.getValue();
        assertThat(event.getCustomerId()).isEqualTo("CUST-001");
        assertThat(event.getShippingAddress()).isEqualTo("123 Main Street");
        assertThat(event.getItems()).hasSize(1);
    }

    @Test
    void shouldUpdateOrderInventory() {
        // Given
        String orderId = "ORDER-123";
        String reservationId = "RES-456";
        Order existingOrder = Order.builder()
                .orderId(orderId)
                .status(Order.OrderStatus.PENDING)
                .build();
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(existingOrder));

        // When
        orderService.updateOrderInventory(orderId, reservationId);

        // Then
        verify(orderRepository).save(any(Order.class));
        assertThat(existingOrder.getReservationId()).isEqualTo(reservationId);
        assertThat(existingOrder.getStatus()).isEqualTo(Order.OrderStatus.INVENTORY_RESERVED);
    }

    @Test
    void shouldUpdateOrderPayment() {
        // Given
        String orderId = "ORDER-123";
        String paymentId = "PAY-456";
        Order existingOrder = Order.builder()
                .orderId(orderId)
                .status(Order.OrderStatus.PENDING)
                .build();
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(existingOrder));

        // When
        orderService.updateOrderPayment(orderId, paymentId);

        // Then
        verify(orderRepository).save(any(Order.class));
        assertThat(existingOrder.getPaymentId()).isEqualTo(paymentId);
        assertThat(existingOrder.getStatus()).isEqualTo(Order.OrderStatus.PAYMENT_COMPLETED);
    }

    @Test
    void shouldCompleteOrderWithShipping() {
        // Given
        String orderId = "ORDER-123";
        String shippingId = "SHIP-789";
        String trackingNumber = "TRACK-001";
        Order existingOrder = Order.builder()
                .orderId(orderId)
                .status(Order.OrderStatus.INVENTORY_RESERVED)
                .build();
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(existingOrder));

        // When
        orderService.completeOrderWithShipping(orderId, shippingId, trackingNumber);

        // Then
        verify(orderRepository).save(any(Order.class));
        assertThat(existingOrder.getShippingId()).isEqualTo(shippingId);
        assertThat(existingOrder.getTrackingNumber()).isEqualTo(trackingNumber);
        assertThat(existingOrder.getStatus()).isEqualTo(Order.OrderStatus.COMPLETED);
    }

    @Test
    void shouldCancelOrderWithReason() {
        // Given
        String orderId = "ORDER-123";
        String reason = "Payment failed";
        Order existingOrder = Order.builder()
                .orderId(orderId)
                .status(Order.OrderStatus.PENDING)
                .build();
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(existingOrder));

        // When
        orderService.cancelOrder(orderId, reason);

        // Then
        verify(orderRepository).save(any(Order.class));
        assertThat(existingOrder.getStatus()).isEqualTo(Order.OrderStatus.CANCELLED);
        assertThat(existingOrder.getFailureReason()).isEqualTo(reason);
    }

    @Test
    void shouldGetOrderById() {
        // Given
        String orderId = "ORDER-123";
        Order order = Order.builder()
                .orderId(orderId)
                .customerId("CUST-001")
                .status(Order.OrderStatus.PENDING)
                .totalAmount(new BigDecimal("99.99"))
                .createdAt(Instant.now())
                .build();
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        // When
        OrderResponse response = orderService.getOrder(orderId);

        // Then
        assertThat(response.getOrderId()).isEqualTo(orderId);
        assertThat(response.getCustomerId()).isEqualTo("CUST-001");
    }

    @Test
    void shouldThrowExceptionWhenOrderNotFound() {
        // Given
        String orderId = "NON-EXISTENT";
        when(orderRepository.findById(orderId)).thenReturn(Optional.empty());

        // When/Then
        assertThatThrownBy(() -> orderService.getOrder(orderId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Order not found");
    }

    @Test
    void shouldGetOrdersByCustomer() {
        // Given
        String customerId = "CUST-001";
        List<Order> orders = Arrays.asList(
                Order.builder().orderId("ORDER-1").customerId(customerId).status(Order.OrderStatus.PENDING).build(),
                Order.builder().orderId("ORDER-2").customerId(customerId).status(Order.OrderStatus.COMPLETED).build()
        );
        when(orderRepository.findByCustomerId(customerId)).thenReturn(orders);

        // When
        List<OrderResponse> responses = orderService.getOrdersByCustomer(customerId);

        // Then
        assertThat(responses).hasSize(2);
    }

    private CreateOrderRequest createOrderRequest() {
        CreateOrderRequest.OrderItemRequest item = new CreateOrderRequest.OrderItemRequest();
        item.setProductId("PROD-001");
        item.setProductName("Test Product");
        item.setQuantity(2);
        item.setPrice(new BigDecimal("49.99"));

        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId("CUST-001");
        request.setShippingAddress("123 Main Street");
        request.setItems(Arrays.asList(item));

        return request;
    }
}
