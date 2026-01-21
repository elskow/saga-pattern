package com.thesis.choreography.payment.kafka;

import com.thesis.choreography.payment.service.IdempotencyService;
import com.thesis.choreography.payment.service.PaymentService;
import static com.thesis.common.dto.KafkaTopics.*;
import com.thesis.common.events.InventoryReservationFailedEvent;
import com.thesis.common.events.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventListener {

    private final PaymentService paymentService;
    private final IdempotencyService idempotencyService;

    @KafkaListener(topics = ORDER_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:payment-service}")
    @Transactional
    public void handleOrderEvents(ConsumerRecord<String, Object> record) {
        Object event = record.value();
        log.debug("Received event of type: {}", event.getClass().getName());
        if (event instanceof OrderCreatedEvent orderEvent) {
            String eventId = "order-created:" + orderEvent.getOrderId();
            if (!idempotencyService.markProcessed(eventId, "OrderCreatedEvent")) {
                log.info("Skipping duplicate OrderCreatedEvent for order: {}", orderEvent.getOrderId());
                return;
            }
            log.info("Received OrderCreatedEvent for order: {}", orderEvent.getOrderId());
            paymentService.processPayment(orderEvent);
        } else {
            log.warn("Received unhandled event type: {} - content: {}", event.getClass().getName(), event);
        }
    }

    @KafkaListener(topics = INVENTORY_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:payment-service}")
    @Transactional
    public void handleInventoryEvents(ConsumerRecord<String, Object> record) {
        Object event = record.value();
        if (event instanceof InventoryReservationFailedEvent inventoryEvent) {
            String eventId = "inventory-failed:" + inventoryEvent.getOrderId();
            if (!idempotencyService.markProcessed(eventId, "InventoryReservationFailedEvent")) {
                log.info("Skipping duplicate InventoryReservationFailedEvent for order: {}", 
                        inventoryEvent.getOrderId());
                return;
            }
            log.info("Received InventoryReservationFailedEvent, initiating refund for order: {}", 
                    inventoryEvent.getOrderId());
            paymentService.refundPayment(inventoryEvent.getOrderId());
        }
    }
}
