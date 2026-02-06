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
import com.thesis.common.util.TransactionHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class OrderService {

    private static final long RETRY_DELAY_MS = 100;

    private final OrderRepository orderRepository;
    private final OrderEventPublisher eventPublisher;
    private final Counter orderCreatedCounter;
    private final Counter orderCompletedCounter;
    private final Counter orderFailedCounter;
    private final Timer orderProcessingTimer;
    private final Timer sagaTotalDurationTimer;
    private final Counter compensationsTotalCounter;
    @Getter
    private final SagaMetricsHelper metricsHelper;

    public OrderService(OrderRepository orderRepository,
                        OrderEventPublisher eventPublisher,
                        MeterRegistry meterRegistry) {
        this.orderRepository = orderRepository;
        this.eventPublisher = eventPublisher;

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
                log.debug("Creating order for customer: {}", request.customerId());

                String orderId = UUID.randomUUID().toString();
                MDC.put("orderId", orderId);

                Order order = Order.builder()
                    .orderId(orderId)
                    .customerId(request.customerId())
                    .shippingAddress(request.shippingAddress())
                    .status(Order.OrderStatus.PENDING)
                    .build();

                request.items().forEach(itemRequest -> {
                    validateOrderItem(itemRequest);
                    order.addItem(OrderItem.builder()
                        .productId(itemRequest.productId())
                        .productName(itemRequest.productName())
                        .quantity(itemRequest.quantity())
                        .price(itemRequest.price())
                        .build());
                });
                order.setTotalAmount(request.items().stream()
                    .map(item -> item.price().multiply(BigDecimal.valueOf(item.quantity())))
                    .reduce(BigDecimal.ZERO, BigDecimal::add));

                Order savedOrder = orderRepository.save(order);
                metricsHelper.recordDbInsert(orderId, SagaMetrics.ENTITY_ORDER);
                log.debug("Order created with ID: {}", savedOrder.getOrderId());

                OrderCreatedEvent event = buildOrderCreatedEvent(savedOrder, correlationId);
                publishEventAfterCommit(event, orderId, correlationId);

                return mapToResponse(savedOrder);
            });
        } finally {
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }

    @Transactional
    @Retryable(
        retryFor = ObjectOptimisticLockingFailureException.class,
        backoff = @Backoff(delay = RETRY_DELAY_MS, multiplier = 2)
    )
    public void updateOrderPayment(String orderId, String paymentId) {
        validateNotBlank(orderId, "Order ID");
        validateNotBlank(paymentId, "Payment ID");

        try {
            MDC.put("orderId", orderId);
            log.debug("Updating order {} with payment ID: {}", orderId, paymentId);

            Order order = findOrderOrThrow(orderId);
            order.setPaymentId(paymentId);
            order.setStatus(Order.OrderStatus.PAYMENT_COMPLETED);

            orderRepository.save(order);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
        } finally {
            MDC.remove("orderId");
        }
    }

    @Transactional
    @Retryable(
        retryFor = ObjectOptimisticLockingFailureException.class,
        backoff = @Backoff(delay = RETRY_DELAY_MS, multiplier = 2)
    )
    public void updateOrderInventory(String orderId, String reservationId) {
        validateNotBlank(orderId, "Order ID");
        validateNotBlank(reservationId, "Reservation ID");

        try {
            MDC.put("orderId", orderId);
            log.debug("Updating order {} with reservation ID: {}", orderId, reservationId);

            Order order = findOrderOrThrow(orderId);
            order.setReservationId(reservationId);
            order.setStatus(Order.OrderStatus.INVENTORY_RESERVED);

            orderRepository.save(order);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
        } finally {
            MDC.remove("orderId");
        }
    }

    @Transactional
    @Retryable(
        retryFor = ObjectOptimisticLockingFailureException.class,
        backoff = @Backoff(delay = RETRY_DELAY_MS, multiplier = 2)
    )
    public void completeOrderWithShipping(String orderId, String shippingId, String trackingNumber) {
        validateNotBlank(orderId, "Order ID");
        validateNotBlank(shippingId, "Shipping ID");
        validateNotBlank(trackingNumber, "Tracking number");

        try {
            MDC.put("orderId", orderId);
            log.debug("Completing order {} with shipping ID: {} and tracking: {}", orderId, shippingId, trackingNumber);

            Order order = findOrderOrThrow(orderId);

            order.setShippingId(shippingId);
            order.setTrackingNumber(trackingNumber);
            order.setStatus(Order.OrderStatus.COMPLETED);

            orderRepository.save(order);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);

            recordSagaCompletion(order);

            log.debug("Order {} completed successfully", orderId);
        } finally {
            MDC.remove("orderId");
        }
    }

    @Transactional
    @Retryable(
        retryFor = ObjectOptimisticLockingFailureException.class,
        backoff = @Backoff(delay = RETRY_DELAY_MS, multiplier = 2)
    )
    public void cancelOrder(String orderId, String reason) {
        validateNotBlank(orderId, "Order ID");
        validateNotBlank(reason, "Cancellation reason");

        try {
            MDC.put("orderId", orderId);
            log.debug("Cancelling order {} due to: {}", orderId, reason);

            Order order = findOrderOrThrow(orderId);
            order.setStatus(Order.OrderStatus.CANCELLED);
            order.setFailureReason(reason);

            orderRepository.save(order);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);

            recordSagaFailure(order);
        } finally {
            MDC.remove("orderId");
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
            MDC.remove("orderId");
        }
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> getOrdersByCustomer(String customerId) {
        validateNotBlank(customerId, "Customer ID");

        try {
            MDC.put("customerId", customerId);
            return orderRepository.findByCustomerId(customerId).stream()
                .map(this::mapToResponse)
                .toList();
        } finally {
            MDC.remove("customerId");
        }
    }

    private void validateOrderItem(CreateOrderRequest.OrderItemRequest item) {
        if (item.quantity() <= 0) {
            throw new IllegalArgumentException(
                "Item quantity must be positive: %d for product: %s"
                    .formatted(item.quantity(), item.productId()));
        }
        if (item.price() == null || item.price().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException(
                "Item price must be positive: %s for product: %s"
                    .formatted(item.price(), item.productId()));
        }
    }

    private void validateNotBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("%s cannot be null or blank".formatted(fieldName));
        }
    }

    private Order findOrderOrThrow(String orderId) {
        return orderRepository.findById(orderId)
            .orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    private OrderCreatedEvent buildOrderCreatedEvent(Order order, String correlationId) {
        return new OrderCreatedEvent(
            order.getOrderId(),
            order.getCustomerId(),
            order.getShippingAddress(),
            correlationId,
            order.getItems().stream()
                .map(item -> new OrderCreatedEvent.OrderItemEvent(
                    item.getProductId(),
                    item.getProductName(),
                    item.getQuantity(),
                    item.getPrice()))
                .toList(),
            order.getTotalAmount(),
            Instant.now());
    }

    private void publishEventAfterCommit(OrderCreatedEvent event, String orderId, String correlationId) {
        Runnable publishAction = () -> {
            eventPublisher.publishOrderCreated(event);
            metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
            orderCreatedCounter.increment();
        };
        TransactionHelper.executeAfterCommit(publishAction, orderId, correlationId);
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
        return new OrderResponse(
            order.getOrderId(),
            order.getCustomerId(),
            order.getStatus().name(),
            order.getItems().stream()
                .map(item -> new OrderResponse.OrderItemResponse(
                    item.getProductId(),
                    item.getProductName(),
                    item.getQuantity(),
                    item.getPrice()))
                .toList(),
            order.getTotalAmount(),
            order.getCreatedAt(),
            order.getUpdatedAt());
    }
}
