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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventListener {

    private final InventoryService inventoryService;
    private final PendingOrderItemRepository pendingOrderItemRepository;
    private final IdempotencyService idempotencyService;

    @KafkaListener(topics = KafkaTopics.ORDER_EVENTS, groupId = "inventory-service")
    @Transactional
    public void handleOrderEvents(ConsumerRecord<String, Object> record) {
        Object event = record.value();
        if (event instanceof OrderCreatedEvent orderEvent) {
            String eventId = "order-created:" + orderEvent.getOrderId();
            if (!idempotencyService.markProcessed(eventId, "OrderCreatedEvent")) {
                log.info("Skipping duplicate OrderCreatedEvent for order: {}", orderEvent.getOrderId());
                return;
            }
            log.info("Received OrderCreatedEvent for order: {}", orderEvent.getOrderId());
            // Store items in database for later reservation when payment is completed
            List<PendingOrderItem> pendingItems = orderEvent.getItems().stream()
                    .map(item -> PendingOrderItem.builder()
                            .orderId(orderEvent.getOrderId())
                            .productId(item.getProductId())
                            .quantity(item.getQuantity())
                            .build())
                    .collect(Collectors.toList());
            pendingOrderItemRepository.saveAll(pendingItems);
            log.info("Stored {} pending order items for order: {}", pendingItems.size(), orderEvent.getOrderId());
        }
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS, groupId = "inventory-service")
    @Transactional
    public void handlePaymentEvents(ConsumerRecord<String, Object> record) {
        Object event = record.value();
        if (event instanceof PaymentCompletedEvent paymentEvent) {
            String eventId = "payment-completed:" + paymentEvent.getPaymentId();
            if (!idempotencyService.markProcessed(eventId, "PaymentCompletedEvent")) {
                log.info("Skipping duplicate PaymentCompletedEvent for order: {}", paymentEvent.getOrderId());
                return;
            }
            log.info("Received PaymentCompletedEvent for order: {}", paymentEvent.getOrderId());
            List<PendingOrderItem> pendingItems = pendingOrderItemRepository.findByOrderId(paymentEvent.getOrderId());
            if (!pendingItems.isEmpty()) {
                List<InventoryService.ItemToReserve> items = pendingItems.stream()
                        .map(item -> new InventoryService.ItemToReserve(item.getProductId(), item.getQuantity()))
                        .collect(Collectors.toList());
                inventoryService.reserveInventory(paymentEvent, items);
                pendingOrderItemRepository.deleteByOrderId(paymentEvent.getOrderId());
                log.info("Processed and removed pending items for order: {}", paymentEvent.getOrderId());
            } else {
                log.warn("No pending items found for order: {}", paymentEvent.getOrderId());
            }
        } else if (event instanceof PaymentFailedEvent paymentEvent) {
            String eventId = "payment-failed:" + paymentEvent.getOrderId();
            if (!idempotencyService.markProcessed(eventId, "PaymentFailedEvent")) {
                log.info("Skipping duplicate PaymentFailedEvent for order: {}", paymentEvent.getOrderId());
                return;
            }
            log.info("Received PaymentFailedEvent, cleaning up pending items for order: {}", paymentEvent.getOrderId());
            pendingOrderItemRepository.deleteByOrderId(paymentEvent.getOrderId());
        }
    }

    @KafkaListener(topics = KafkaTopics.SHIPPING_EVENTS, groupId = "inventory-service")
    @Transactional
    public void handleShippingEvents(ConsumerRecord<String, Object> record) {
        Object event = record.value();
        if (event instanceof ShippingFailedEvent shippingEvent) {
            String eventId = "shipping-failed:" + shippingEvent.getOrderId();
            if (!idempotencyService.markProcessed(eventId, "ShippingFailedEvent")) {
                log.info("Skipping duplicate ShippingFailedEvent for order: {}", shippingEvent.getOrderId());
                return;
            }
            log.info("Received ShippingFailedEvent, releasing inventory for order: {}", shippingEvent.getOrderId());
            inventoryService.releaseInventory(shippingEvent.getOrderId());
        }
    }
}
