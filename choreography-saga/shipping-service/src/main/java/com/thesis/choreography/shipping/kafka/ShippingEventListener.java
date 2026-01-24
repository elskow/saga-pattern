package com.thesis.choreography.shipping.kafka;

import com.thesis.choreography.shipping.model.PendingShippingAddress;
import com.thesis.choreography.shipping.repository.PendingShippingAddressRepository;
import com.thesis.choreography.shipping.service.IdempotencyService;
import com.thesis.choreography.shipping.service.ShippingService;
import com.thesis.common.dto.KafkaTopics;
import com.thesis.common.events.*;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

@Component
@RequiredArgsConstructor
@Slf4j
public class ShippingEventListener {

    private final ShippingService shippingService;
    private final PendingShippingAddressRepository pendingShippingAddressRepository;
    private final IdempotencyService idempotencyService;
    private final Validator validator;

    @KafkaListener(topics = KafkaTopics.ORDER_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:shipping-service}")
    @Transactional
    public void handleOrderEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof OrderCreatedEvent event) {
            handleEvent(record, event, "OrderCreatedEvent",
                () -> !idempotencyService.markProcessed("order-created:" + event.getOrderId(), "OrderCreatedEvent"),
                () -> {
                    log.info("Received OrderCreatedEvent for order: {}, storing shipping address", event.getOrderId());
                    PendingShippingAddress pendingAddress = PendingShippingAddress.builder()
                            .orderId(event.getOrderId())
                            .shippingAddress(event.getShippingAddress())
                            .build();
                    pendingShippingAddressRepository.save(pendingAddress);
                    log.info("Stored pending shipping address for order: {}", event.getOrderId());
                });
        }
    }

    @KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:shipping-service}")
    @Transactional
    public void handleInventoryEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof InventoryReservedEvent event) {
            handleEvent(record, event, "InventoryReservedEvent",
                () -> !idempotencyService.markProcessed("inventory-reserved:" + event.getReservationId(), "InventoryReservedEvent"),
                () -> {
                    log.info("Received InventoryReservedEvent for order: {}", event.getOrderId());
                    Optional<PendingShippingAddress> pendingAddress = pendingShippingAddressRepository.findByOrderId(event.getOrderId());
                    if (pendingAddress.isPresent()) {
                        shippingService.scheduleShipping(event, pendingAddress.get().getShippingAddress());
                        pendingShippingAddressRepository.deleteByOrderId(event.getOrderId());
                        log.info("Processed and removed pending shipping address for order: {}", event.getOrderId());
                    } else {
                        log.warn("No pending shipping address found for order: {}", event.getOrderId());
                    }
                });
        }
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:shipping-service}")
    @Transactional
    public void handlePaymentEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof PaymentRefundedEvent event) {
            handleEvent(record, event, "PaymentRefundedEvent",
                () -> !idempotencyService.markProcessed("payment-refunded:" + event.getOrderId(), "PaymentRefundedEvent"),
                () -> {
                    log.info("Received PaymentRefundedEvent, cancelling shipping for order: {}", event.getOrderId());
                    shippingService.cancelShipping(event.getOrderId());
                    pendingShippingAddressRepository.deleteByOrderId(event.getOrderId());
                });
        }
    }

    private <T> void handleEvent(ConsumerRecord<String, Object> record, T event, String eventType,
                                   Supplier<Boolean> idempotencyCheck, Runnable handler) {
        String correlationId = UUID.randomUUID().toString();
        try {
            String orderId = getOrderId(event);
            String eventCorrelationId = getCorrelationId(event, correlationId);

            setupMdc(orderId, eventCorrelationId);

            Set<?> violations = validator.validate(event);
            if (!violations.isEmpty()) {
                log.error("Invalid {} received for order: {}. Violations: {}", eventType, orderId, violations);
                return;
            }

            if (idempotencyCheck.get()) {
                log.info("Skipping duplicate {} for order: {}", eventType, orderId);
                return;
            }

            handler.run();
        } catch (Exception e) {
            log.error("Error processing event {}: {}", eventType, e.getMessage(), e);
            throw e;
        } finally {
            MDC.clear();
        }
    }

    private String getOrderId(Object event) {
        if (event instanceof HasOrderId ho) {
            return ho.getOrderId();
        }
        return "unknown";
    }

    private String getCorrelationId(Object event, String defaultCorrelationId) {
        if (event instanceof HasCorrelationId hc) {
            return hc.getCorrelationId() != null ? hc.getCorrelationId() : defaultCorrelationId;
        }
        return defaultCorrelationId;
    }

    private void setupMdc(String orderId, String correlationId) {
        MDC.put("orderId", orderId != null ? orderId : "unknown");
        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }
    }

    private interface HasOrderId {
        String getOrderId();
    }

    private interface HasCorrelationId {
        String getCorrelationId();
    }
}
