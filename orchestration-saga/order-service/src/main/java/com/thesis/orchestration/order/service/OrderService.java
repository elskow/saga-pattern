package com.thesis.orchestration.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.exception.OrderNotFoundException;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.orchestration.order.model.OrderEntity;
import com.thesis.orchestration.order.repository.OrderRepository;
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
import java.util.Optional;

@Service
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final ObjectMapper objectMapper;

    private final Counter orderCreatedCounter;
    private final Counter orderCompletedCounter;
    private final Counter orderFailedCounter;
    private final Counter compensationsTotalCounter;
    private final Timer sagaTotalDurationTimer;
    private final SagaMetricsHelper metricsHelper;

    public OrderService(OrderRepository orderRepository,
                        ObjectMapper objectMapper,
                        MeterRegistry meterRegistry) {
        this.orderRepository = orderRepository;
        this.objectMapper = objectMapper;

        // Initialize metrics
        this.orderCreatedCounter = meterRegistry.counter(SagaMetrics.ORDERS_CREATED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.orderCompletedCounter = meterRegistry.counter(SagaMetrics.ORDERS_COMPLETED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.orderFailedCounter = meterRegistry.counter(SagaMetrics.ORDERS_FAILED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.compensationsTotalCounter = meterRegistry.counter(SagaMetrics.COMPENSATIONS_TOTAL,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.sagaTotalDurationTimer = meterRegistry.timer(SagaMetrics.SAGA_TOTAL_DURATION,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_ORCHESTRATION);
    }


    @Transactional
    @Observed(name = "order.create", contextualName = "create-order")
    public void createOrder(String orderId, String customerId, BigDecimal totalAmount,
                            String shippingAddress, List<OrderCreatedEvent.OrderItemEvent> items,
                            String paymentId, String reservationId, String shipmentId) {
        log.info("Creating order: {} for customer: {}", orderId, customerId);

        String itemsJson;
        try {
            itemsJson = objectMapper.writeValueAsString(items);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize order items for order {}: {}", orderId, e.getMessage());
            itemsJson = "[]";
        }

        OrderEntity order = OrderEntity.builder()
                .orderId(orderId)
                .customerId(customerId)
                .totalAmount(totalAmount)
                .shippingAddress(shippingAddress)
                .itemsJson(itemsJson)
                .status(OrderEntity.OrderStatus.CREATED)
                .paymentId(paymentId)
                .reservationId(reservationId)
                .shipmentId(shipmentId)
                .build();

        orderRepository.save(order);
        metricsHelper.recordDbInsert(orderId, SagaMetrics.ENTITY_ORDER);
        orderCreatedCounter.increment();
    }

    @Transactional(readOnly = true)
    public Optional<OrderEntity> findById(String orderId) {
        return orderRepository.findById(orderId);
    }

    @Transactional(readOnly = true)
    public List<OrderEntity> findByCustomerId(String customerId) {
        return orderRepository.findByCustomerId(customerId);
    }

    @Transactional(readOnly = true)
    public List<OrderEntity> findAll() {
        return orderRepository.findAll();
    }

    @Transactional
    public void failOrder(String orderId, String reason) {
        log.info("Failing order {} with reason: {}", orderId, reason);
        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setStatus(OrderEntity.OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        orderRepository.save(order);
        metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
        metricsHelper.recordSagaFailure(orderId);

        orderFailedCounter.increment();
        compensationsTotalCounter.increment();

        // Record saga duration if we have creation time
        if (order.getCreatedAt() != null) {
            long durationMs = Instant.now().toEpochMilli() - order.getCreatedAt().toEpochMilli();
            sagaTotalDurationTimer.record(java.time.Duration.ofMillis(durationMs));
        }
    }

    @Transactional
    public void completeOrder(String orderId) {
        log.info("Completing order: {}", orderId);
        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setStatus(OrderEntity.OrderStatus.COMPLETED);
        order.setCompletedAt(Instant.now());
        orderRepository.save(order);
        metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_ORDER);
        metricsHelper.recordSagaSuccess(orderId);

        orderCompletedCounter.increment();

        // Record saga duration
        if (order.getCreatedAt() != null) {
            long durationMs = order.getCompletedAt().toEpochMilli() - order.getCreatedAt().toEpochMilli();
            sagaTotalDurationTimer.record(java.time.Duration.ofMillis(durationMs));
        }
    }
}
