package com.thesis.orchestration.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.exception.OrderNotFoundException;
import com.thesis.orchestration.order.model.OrderEntity;
import com.thesis.orchestration.order.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final ObjectMapper objectMapper;


    @Transactional
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
    }

    @Transactional
    public void completeOrder(String orderId) {
        log.info("Completing order: {}", orderId);
        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        order.setStatus(OrderEntity.OrderStatus.COMPLETED);
        order.setCompletedAt(Instant.now());
        orderRepository.save(order);
    }
}
