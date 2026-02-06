package com.thesis.choreography.inventory.kafka;

import com.thesis.choreography.inventory.model.PendingOrderItem;
import com.thesis.choreography.inventory.repository.PendingOrderItemRepository;
import com.thesis.choreography.inventory.service.IdempotencyService;
import com.thesis.choreography.inventory.service.InventoryService;
import com.thesis.common.config.KafkaTopicsProperties;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.events.PaymentCompletedEvent;
import com.thesis.common.events.PaymentFailedEvent;
import com.thesis.common.events.ShippingFailedEvent;
import com.thesis.common.util.MdcUtils;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventListener {

    private final InventoryService inventoryService;
    private final PendingOrderItemRepository pendingOrderItemRepository;
    private final IdempotencyService idempotencyService;
    private final Validator validator;

    @KafkaListener(topics = KafkaTopicsProperties.ORDER_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:inventory-service}")
    @Transactional
    public void handleOrderEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof OrderCreatedEvent event) {
            handleEvent(record, event, "OrderCreatedEvent",
                () -> idempotencyService.tryMarkAsProcessed("order-created:%s".formatted(event.orderId()), "OrderCreatedEvent"),
                () -> {
                    log.debug("Received OrderCreatedEvent for order: {}", event.orderId());
                    List<PendingOrderItem> pendingItems = event.items().stream()
                        .map(item -> PendingOrderItem.builder()
                            .orderId(event.orderId())
                            .productId(item.productId())
                            .quantity(item.quantity())
                            .build())
                        .toList();
                    pendingOrderItemRepository.saveAll(pendingItems);
                    log.debug("Stored {} pending order items for order: {}", pendingItems.size(), event.orderId());
                });
        }
    }

    @KafkaListener(topics = KafkaTopicsProperties.PAYMENT_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:inventory-service}")
    @Transactional
    public void handlePaymentEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof PaymentCompletedEvent event) {
            handleEvent(record, event, "PaymentCompletedEvent",
                () -> idempotencyService.tryMarkAsProcessed("payment-completed:%s".formatted(event.paymentId()), "PaymentCompletedEvent"),
                () -> {
                    log.debug("Received PaymentCompletedEvent for order: {}", event.orderId());
                    List<PendingOrderItem> pendingItems = pendingOrderItemRepository.findByOrderId(event.orderId());
                    if (!pendingItems.isEmpty()) {
                        List<InventoryService.ItemToReserve> items = pendingItems.stream()
                            .map(item -> new InventoryService.ItemToReserve(item.getProductId(), item.getQuantity()))
                            .toList();
                        inventoryService.reserveInventory(event, items);
                        pendingOrderItemRepository.deleteByOrderId(event.orderId());
                        log.debug("Processed and removed pending items for order: {}", event.orderId());
                    } else {
                        log.warn("No pending items found for order: {}", event.orderId());
                    }
                });
        } else if (record.value() instanceof PaymentFailedEvent event) {
            handleEvent(record, event, "PaymentFailedEvent",
                () -> idempotencyService.tryMarkAsProcessed("payment-failed:%s".formatted(event.orderId()), "PaymentFailedEvent"),
                () -> {
                    log.debug("Received PaymentFailedEvent, cleaning up pending items for order: {}", event.orderId());
                    pendingOrderItemRepository.deleteByOrderId(event.orderId());
                });
        }
    }

    @KafkaListener(topics = KafkaTopicsProperties.SHIPPING_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:inventory-service}")
    @Transactional
    public void handleShippingEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof ShippingFailedEvent event) {
            handleEvent(record, event, "ShippingFailedEvent",
                () -> idempotencyService.tryMarkAsProcessed("shipping-failed:%s".formatted(event.orderId()), "ShippingFailedEvent"),
                () -> {
                    log.debug("Received ShippingFailedEvent, releasing inventory for order: {}", event.orderId());
                    inventoryService.releaseInventory(event.orderId());
                });
        }
    }

    private <T> void handleEvent(ConsumerRecord<String, Object> record, T event, String eventType,
                                 Supplier<Boolean> idempotencyCheck, Runnable handler) {
        String correlationId = UUID.randomUUID().toString();
        try {
            String orderId = MdcUtils.getOrderId(event);
            String eventCorrelationId = MdcUtils.getCorrelationId(event, correlationId);

            MdcUtils.setupEventMdc(orderId, eventCorrelationId);

            Set<?> violations = validator.validate(event);
            if (!violations.isEmpty()) {
                log.error("Invalid {} received for order: {}. Violations: {}", eventType, orderId, violations);
                return;
            }

            if (!idempotencyCheck.get()) {
                log.trace("Skipping duplicate {} for order: {}", eventType, orderId);
                return;
            }

            handler.run();
        } catch (Exception e) {
            log.error("Error processing event {}: {}", eventType, e.getMessage(), e);
            throw e;
        } finally {
            MdcUtils.clearEventMdc();
        }
    }
}
