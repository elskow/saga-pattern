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
import org.slf4j.MDC;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Order Service handling order lifecycle in choreography-based saga pattern.
 * 
 * <p>Uses Spring Retry's @Retryable to handle optimistic locking conflicts that can occur
 * when multiple Kafka events (payment, inventory, shipping) arrive concurrently and 
 * try to update the same order record.</p>
 */
@Service
@Slf4j
public class OrderService {

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 100;

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
        String correlationId = UUID.randomUUID().toString();
        try {
            MDC.put("correlationId", correlationId);
            return orderProcessingTimer.record(() -> {
                log.info("Creating order for customer: {}", request.getCustomerId());
                
                String orderId = UUID.randomUUID().toString();
                MDC.put("orderId", orderId);
            
                Order order = Order.builder()
                        .orderId(orderId)
                        .customerId(request.getCustomerId())
                        .shippingAddress(request.getShippingAddress())
                        .status(Order.OrderStatus.PENDING)
                        .build();

                // Calculate total and add items with validation
                BigDecimal total = BigDecimal.ZERO;
                for (CreateOrderRequest.OrderItemRequest itemRequest : request.getItems()) {
                    validateOrderItem(itemRequest);
                    
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

                // Build and publish event after transaction commit
                OrderCreatedEvent event = buildOrderCreatedEvent(savedOrder, correlationId);
                publishEventAfterCommit(event, orderId, correlationId);
                
                return mapToResponse(savedOrder);
            });
        } finally {
            MDC.clear();
        }
    }

    /**
     * Updates order with payment information.
     * Uses @Retryable to handle concurrent updates from parallel event processing.
     */
    @Transactional
    @Retryable(
        retryFor = ObjectOptimisticLockingFailureException.class,
        maxAttempts = MAX_RETRY_ATTEMPTS,
        backoff = @Backoff(delay = RETRY_DELAY_MS, multiplier = 2)
    )
    public void updateOrderPayment(String orderId, String paymentId) {
        validateNotBlank(orderId, "Order ID");
        validateNotBlank(paymentId, "Payment ID");
        
        try {
            MDC.put("orderId", orderId);
            log.info("Updating order {} with payment ID: {}", orderId, paymentId);
            
            Order order = findOrderOrThrow(orderId);
            order.setPaymentId(paymentId);
            order.setStatus(Order.OrderStatus.PAYMENT_COMPLETED);
            
            orderRepository.save(order);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
        } finally {
            MDC.clear();
        }
    }

    /**
     * Updates order with inventory reservation information.
     * Uses @Retryable to handle concurrent updates from parallel event processing.
     */
    @Transactional
    @Retryable(
        retryFor = ObjectOptimisticLockingFailureException.class,
        maxAttempts = MAX_RETRY_ATTEMPTS,
        backoff = @Backoff(delay = RETRY_DELAY_MS, multiplier = 2)
    )
    public void updateOrderInventory(String orderId, String reservationId) {
        validateNotBlank(orderId, "Order ID");
        validateNotBlank(reservationId, "Reservation ID");
        
        try {
            MDC.put("orderId", orderId);
            log.info("Updating order {} with reservation ID: {}", orderId, reservationId);
            
            Order order = findOrderOrThrow(orderId);
            order.setReservationId(reservationId);
            order.setStatus(Order.OrderStatus.INVENTORY_RESERVED);
            
            orderRepository.save(order);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
        } finally {
            MDC.clear();
        }
    }

    /**
     * Updates order with shipping information and completes the saga in a single atomic operation.
     * This prevents race conditions when the shipping event arrives while other updates are in progress.
     * Uses @Retryable to handle concurrent updates from parallel event processing.
     */
    @Transactional
    @Retryable(
        retryFor = ObjectOptimisticLockingFailureException.class,
        maxAttempts = MAX_RETRY_ATTEMPTS,
        backoff = @Backoff(delay = RETRY_DELAY_MS, multiplier = 2)
    )
    public void completeOrderWithShipping(String orderId, String shippingId, String trackingNumber) {
        validateNotBlank(orderId, "Order ID");
        validateNotBlank(shippingId, "Shipping ID");
        validateNotBlank(trackingNumber, "Tracking number");
        
        try {
            MDC.put("orderId", orderId);
            log.info("Completing order {} with shipping ID: {} and tracking: {}", orderId, shippingId, trackingNumber);
            
            Order order = findOrderOrThrow(orderId);
            
            // Update shipping info and complete in single operation
            order.setShippingId(shippingId);
            order.setTrackingNumber(trackingNumber);
            order.setStatus(Order.OrderStatus.COMPLETED);
            
            orderRepository.save(order);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
            
            // Record completion metrics
            recordSagaCompletion(order);
            
            log.info("Order {} completed successfully", orderId);
        } finally {
            MDC.clear();
        }
    }

    /**
     * Cancels an order with the given reason.
     * Uses @Retryable to handle concurrent updates from parallel event processing.
     */
    @Transactional
    @Retryable(
        retryFor = ObjectOptimisticLockingFailureException.class,
        maxAttempts = MAX_RETRY_ATTEMPTS,
        backoff = @Backoff(delay = RETRY_DELAY_MS, multiplier = 2)
    )
    public void cancelOrder(String orderId, String reason) {
        validateNotBlank(orderId, "Order ID");
        validateNotBlank(reason, "Cancellation reason");
        
        try {
            MDC.put("orderId", orderId);
            log.info("Cancelling order {} due to: {}", orderId, reason);
            
            Order order = findOrderOrThrow(orderId);
            order.setStatus(Order.OrderStatus.CANCELLED);
            order.setFailureReason(reason);
            
            orderRepository.save(order);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
            
            // Record failure metrics
            recordSagaFailure(order);
        } finally {
            MDC.clear();
        }
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrder(String orderId) {
        validateNotBlank(orderId, "Order ID");
        
        try {
            MDC.put("orderId", orderId);
            return orderRepository.findById(orderId)
                    .map(this::mapToResponse)
                    .orElseThrow(() -> new OrderNotFoundException(orderId));
        } finally {
            MDC.clear();
        }
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> getOrdersByCustomer(String customerId) {
        validateNotBlank(customerId, "Customer ID");
        
        try {
            MDC.put("customerId", customerId);
            return orderRepository.findByCustomerId(customerId).stream()
                    .map(this::mapToResponse)
                    .collect(Collectors.toList());
        } finally {
            MDC.clear();
        }
    }

    public SagaMetricsHelper getMetricsHelper() {
        return metricsHelper;
    }

    // ==================== Private Helper Methods ====================

    private void validateOrderItem(CreateOrderRequest.OrderItemRequest item) {
        if (item.getQuantity() <= 0) {
            throw new IllegalArgumentException(
                    "Item quantity must be positive: " + item.getQuantity() + 
                    " for product: " + item.getProductId());
        }
        if (item.getPrice() == null || item.getPrice().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException(
                    "Item price must be positive: " + item.getPrice() + 
                    " for product: " + item.getProductId());
        }
    }

    private void validateNotBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be null or blank");
        }
    }

    private Order findOrderOrThrow(String orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    private OrderCreatedEvent buildOrderCreatedEvent(Order order, String correlationId) {
        return OrderCreatedEvent.builder()
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .shippingAddress(order.getShippingAddress())
                .totalAmount(order.getTotalAmount())
                .correlationId(correlationId)
                .createdAt(Instant.now())
                .items(order.getItems().stream()
                        .map(item -> OrderCreatedEvent.OrderItemEvent.builder()
                                .productId(item.getProductId())
                                .productName(item.getProductName())
                                .quantity(item.getQuantity())
                                .price(item.getPrice())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }

    private void publishEventAfterCommit(OrderCreatedEvent event, String orderId, String correlationId) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            try {
                                MDC.put("orderId", orderId);
                                MDC.put("correlationId", correlationId);
                                eventPublisher.publishOrderCreated(event);
                                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                                orderCreatedCounter.increment();
                            } catch (Exception e) {
                                log.error("Failed to publish OrderCreatedEvent after commit for order: {}", orderId, e);
                            } finally {
                                MDC.clear();
                            }
                        }
                    }
            );
        } else {
            // No active transaction (e.g., in tests), publish immediately
            eventPublisher.publishOrderCreated(event);
            metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
            orderCreatedCounter.increment();
        }
    }

    private void recordSagaCompletion(Order order) {
        metricsHelper.recordSagaSuccess(order.getOrderId());
        orderCompletedCounter.increment();
        
        if (order.getCreatedAt() != null) {
            Duration duration = Duration.between(order.getCreatedAt(), Instant.now());
            sagaTotalDurationTimer.record(duration);
        }
    }

    private void recordSagaFailure(Order order) {
        metricsHelper.recordSagaFailure(order.getOrderId());
        orderFailedCounter.increment();
        compensationsTotalCounter.increment();
        
        if (order.getCreatedAt() != null) {
            Duration duration = Duration.between(order.getCreatedAt(), Instant.now());
            sagaTotalDurationTimer.record(duration);
        }
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
