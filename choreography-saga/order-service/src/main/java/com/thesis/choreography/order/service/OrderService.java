package com.thesis.choreography.order.service;

import com.thesis.choreography.order.kafka.OrderEventPublisher;
import com.thesis.choreography.order.model.Order;
import com.thesis.choreography.order.model.OrderItem;
import com.thesis.choreography.order.repository.OrderRepository;
import com.thesis.common.dto.CreateOrderRequest;
import com.thesis.common.dto.OrderResponse;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.exception.OrderNotFoundException;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
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
    private final Timer sagaTotalDurationTimer;
    private final Counter compensationsTotalCounter;
    private final SagaMetricsHelper metricsHelper;

    public OrderService(OrderRepository orderRepository, 
                        OrderEventPublisher eventPublisher,
                        MeterRegistry meterRegistry) {
        this.orderRepository = orderRepository;
        this.eventPublisher = eventPublisher;
        
        // Metrics
        this.orderCreatedCounter = meterRegistry.counter(SagaMetrics.ORDERS_CREATED, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.orderCompletedCounter = meterRegistry.counter(SagaMetrics.ORDERS_COMPLETED, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.orderFailedCounter = meterRegistry.counter(SagaMetrics.ORDERS_FAILED, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.orderProcessingTimer = meterRegistry.timer(SagaMetrics.ORDER_PROCESSING_TIME, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.sagaTotalDurationTimer = meterRegistry.timer(SagaMetrics.SAGA_TOTAL_DURATION, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.compensationsTotalCounter = meterRegistry.counter(SagaMetrics.COMPENSATIONS_TOTAL, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_CHOREOGRAPHY);
    }

    @Transactional
    @Observed(name = "order.create", contextualName = "create-order")
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
            metricsHelper.recordDbInsert(orderId, SagaMetrics.ENTITY_ORDER);
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
            metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
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
        metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
    }

    @Transactional
    public void updateOrderPayment(String orderId, String paymentId) {
        log.info("Updating order {} with payment ID: {}", orderId, paymentId);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setPaymentId(paymentId);
        order.setStatus(Order.OrderStatus.PAYMENT_COMPLETED);
        orderRepository.save(order);
        metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
    }

    @Transactional
    public void updateOrderInventory(String orderId, String reservationId) {
        log.info("Updating order {} with reservation ID: {}", orderId, reservationId);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setReservationId(reservationId);
        order.setStatus(Order.OrderStatus.INVENTORY_RESERVED);
        orderRepository.save(order);
        metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
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
        metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
    }

    @Transactional
    public void completeOrder(String orderId) {
        log.info("Completing order: {}", orderId);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setStatus(Order.OrderStatus.COMPLETED);
        orderRepository.save(order);
        metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
        metricsHelper.recordSagaSuccess(orderId);
        orderCompletedCounter.increment();
        
        // Record saga duration
        if (order.getCreatedAt() != null) {
            long durationMs = Instant.now().toEpochMilli() - order.getCreatedAt().toEpochMilli();
            sagaTotalDurationTimer.record(java.time.Duration.ofMillis(durationMs));
        }
    }

    @Transactional
    public void cancelOrder(String orderId, String reason) {
        log.info("Cancelling order {} due to: {}", orderId, reason);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setStatus(Order.OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        orderRepository.save(order);
        metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
        metricsHelper.recordSagaFailure(orderId);
        orderFailedCounter.increment();
        compensationsTotalCounter.increment();
        
        // Record saga duration
        if (order.getCreatedAt() != null) {
            long durationMs = Instant.now().toEpochMilli() - order.getCreatedAt().toEpochMilli();
            sagaTotalDurationTimer.record(java.time.Duration.ofMillis(durationMs));
        }
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

    public SagaMetricsHelper getMetricsHelper() {
        return metricsHelper;
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
