package com.thesis.choreography.order.service;

import com.thesis.choreography.order.kafka.OrderEventPublisher;
import com.thesis.choreography.order.model.Order;
import com.thesis.choreography.order.model.OrderItem;
import com.thesis.choreography.order.repository.OrderRepository;
import com.thesis.common.dto.CreateOrderRequest;
import com.thesis.common.dto.OrderResponse;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.exception.OrderNotFoundException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventPublisher eventPublisher;
    private final Counter orderCreatedCounter;
    private final Counter orderCompletedCounter;
    private final Counter orderFailedCounter;
    private final Timer orderProcessingTimer;

    public OrderService(OrderRepository orderRepository, 
                        OrderEventPublisher eventPublisher,
                        MeterRegistry meterRegistry) {
        this.orderRepository = orderRepository;
        this.eventPublisher = eventPublisher;
        
        // Metrics
        this.orderCreatedCounter = meterRegistry.counter("orders.created", "service", "choreography");
        this.orderCompletedCounter = meterRegistry.counter("orders.completed", "service", "choreography");
        this.orderFailedCounter = meterRegistry.counter("orders.failed", "service", "choreography");
        this.orderProcessingTimer = meterRegistry.timer("order.processing.time", "service", "choreography");
    }

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        return orderProcessingTimer.record(() -> {
            log.info("Creating order for customer: {}", request.getCustomerId());
            
            String orderId = UUID.randomUUID().toString();
            
            Order order = Order.builder()
                    .orderId(orderId)
                    .customerId(request.getCustomerId())
                    .shippingAddress(request.getShippingAddress())
                    .status(Order.OrderStatus.PENDING)
                    .build();

            // Calculate total and add items
            BigDecimal total = BigDecimal.ZERO;
            for (CreateOrderRequest.OrderItemRequest itemRequest : request.getItems()) {
                OrderItem item = OrderItem.builder()
                        .productId(itemRequest.getProductId())
                        .productName(itemRequest.getProductName())
                        .quantity(itemRequest.getQuantity())
                        .price(itemRequest.getPrice())
                        .build();
                order.addItem(item);
                total = total.add(itemRequest.getPrice().multiply(BigDecimal.valueOf(itemRequest.getQuantity())));
            }
            order.setTotalAmount(total);
            
            Order savedOrder = orderRepository.save(order);
            log.info("Order created with ID: {}", savedOrder.getOrderId());

            // Publish OrderCreatedEvent
            OrderCreatedEvent event = OrderCreatedEvent.builder()
                    .orderId(savedOrder.getOrderId())
                    .customerId(savedOrder.getCustomerId())
                    .shippingAddress(savedOrder.getShippingAddress())
                    .totalAmount(savedOrder.getTotalAmount())
                    .createdAt(Instant.now())
                    .items(savedOrder.getItems().stream()
                            .map(item -> OrderCreatedEvent.OrderItemEvent.builder()
                                    .productId(item.getProductId())
                                    .productName(item.getProductName())
                                    .quantity(item.getQuantity())
                                    .price(item.getPrice())
                                    .build())
                            .collect(Collectors.toList()))
                    .build();

            eventPublisher.publishOrderCreated(event);
            orderCreatedCounter.increment();
            
            return mapToResponse(savedOrder);
        });
    }

    @Transactional
    public void updateOrderStatus(String orderId, Order.OrderStatus status) {
        log.info("Updating order {} status to {}", orderId, status);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setStatus(status);
        orderRepository.save(order);
    }

    @Transactional
    public void updateOrderPayment(String orderId, String paymentId) {
        log.info("Updating order {} with payment ID: {}", orderId, paymentId);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setPaymentId(paymentId);
        order.setStatus(Order.OrderStatus.PAYMENT_COMPLETED);
        orderRepository.save(order);
    }

    @Transactional
    public void updateOrderInventory(String orderId, String reservationId) {
        log.info("Updating order {} with reservation ID: {}", orderId, reservationId);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setReservationId(reservationId);
        order.setStatus(Order.OrderStatus.INVENTORY_RESERVED);
        orderRepository.save(order);
    }

    @Transactional
    public void updateOrderShipping(String orderId, String shippingId, String trackingNumber) {
        log.info("Updating order {} with shipping ID: {} and tracking: {}", orderId, shippingId, trackingNumber);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setShippingId(shippingId);
        order.setTrackingNumber(trackingNumber);
        order.setStatus(Order.OrderStatus.SHIPPING_SCHEDULED);
        orderRepository.save(order);
    }

    @Transactional
    public void completeOrder(String orderId) {
        log.info("Completing order: {}", orderId);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setStatus(Order.OrderStatus.COMPLETED);
        orderRepository.save(order);
        orderCompletedCounter.increment();
    }

    @Transactional
    public void cancelOrder(String orderId, String reason) {
        log.info("Cancelling order {} due to: {}", orderId, reason);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setStatus(Order.OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        orderRepository.save(order);
        orderFailedCounter.increment();
    }

    public OrderResponse getOrder(String orderId) {
        return orderRepository.findById(orderId)
                .map(this::mapToResponse)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    public List<OrderResponse> getOrdersByCustomer(String customerId) {
        return orderRepository.findByCustomerId(customerId).stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    private OrderResponse mapToResponse(Order order) {
        return OrderResponse.builder()
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .status(order.getStatus().name())
                .totalAmount(order.getTotalAmount())
                .createdAt(order.getCreatedAt())
                .updatedAt(order.getUpdatedAt())
                .items(order.getItems().stream()
                        .map(item -> OrderResponse.OrderItemResponse.builder()
                                .productId(item.getProductId())
                                .productName(item.getProductName())
                                .quantity(item.getQuantity())
                                .price(item.getPrice())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }
}
