package com.thesis.choreography.inventory.kafka;

import com.thesis.choreography.inventory.model.PendingOrderItem;
import com.thesis.choreography.inventory.repository.PendingOrderItemRepository;
import com.thesis.choreography.inventory.service.IdempotencyService;
import com.thesis.choreography.inventory.service.InventoryService;
import com.thesis.common.dto.KafkaTopics;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.events.PaymentCompletedEvent;
import com.thesis.common.events.PaymentFailedEvent;
import com.thesis.common.events.ShippingFailedEvent;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventListener {

    private final InventoryService inventoryService;
    private final PendingOrderItemRepository pendingOrderItemRepository;
    private final IdempotencyService idempotencyService;
    private final Validator validator;

    @KafkaListener(topics = KafkaTopics.ORDER_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:inventory-service}")
    @Transactional
    public void handleOrderEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof OrderCreatedEvent event) {
            handleEvent(record, event, "OrderCreatedEvent",
                () -> idempotencyService.markProcessed("order-created:" + event.getOrderId(), "OrderCreatedEvent"),
                () -> {
                    log.info("Received OrderCreatedEvent for order: {}", event.getOrderId());
                    List<PendingOrderItem> pendingItems = event.getItems().stream()
                        .map(item -> PendingOrderItem.builder()
                            .orderId(event.getOrderId())
                            .productId(item.getProductId())
                            .quantity(item.getQuantity())
                            .build())
                        .collect(Collectors.toList());
                    pendingOrderItemRepository.saveAll(pendingItems);
                    log.info("Stored {} pending order items for order: {}", pendingItems.size(), event.getOrderId());
                });
        }
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:inventory-service}")
    @Transactional
    public void handlePaymentEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof PaymentCompletedEvent event) {
            handleEvent(record, event, "PaymentCompletedEvent",
                () -> idempotencyService.markProcessed("payment-completed:" + event.getPaymentId(), "PaymentCompletedEvent"),
                () -> {
                    log.info("Received PaymentCompletedEvent for order: {}", event.getOrderId());
                    List<PendingOrderItem> pendingItems = pendingOrderItemRepository.findByOrderId(event.getOrderId());
                    if (!pendingItems.isEmpty()) {
                        List<InventoryService.ItemToReserve> items = pendingItems.stream()
                            .map(item -> new InventoryService.ItemToReserve(item.getProductId(), item.getQuantity()))
                            .collect(Collectors.toList());
                        inventoryService.reserveInventory(event, items);
                        pendingOrderItemRepository.deleteByOrderId(event.getOrderId());
                        log.info("Processed and removed pending items for order: {}", event.getOrderId());
                    } else {
                        log.warn("No pending items found for order: {}", event.getOrderId());
                    }
                });
        } else if (record.value() instanceof PaymentFailedEvent event) {
            handleEvent(record, event, "PaymentFailedEvent",
                () -> idempotencyService.markProcessed("payment-failed:" + event.getOrderId(), "PaymentFailedEvent"),
                () -> {
                    log.info("Received PaymentFailedEvent, cleaning up pending items for order: {}", event.getOrderId());
                    pendingOrderItemRepository.deleteByOrderId(event.getOrderId());
                });
        }
    }

    @KafkaListener(topics = KafkaTopics.SHIPPING_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:inventory-service}")
    @Transactional
    public void handleShippingEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof ShippingFailedEvent event) {
            handleEvent(record, event, "ShippingFailedEvent",
                () -> idempotencyService.markProcessed("shipping-failed:" + event.getOrderId(), "ShippingFailedEvent"),
                () -> {
                    log.info("Received ShippingFailedEvent, releasing inventory for order: {}", event.getOrderId());
                    inventoryService.releaseInventory(event.getOrderId());
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

            // idempotencyCheck returns true if event was marked as new (should process)
            // returns false if event was already processed (should skip)
            if (!idempotencyCheck.get()) {
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
