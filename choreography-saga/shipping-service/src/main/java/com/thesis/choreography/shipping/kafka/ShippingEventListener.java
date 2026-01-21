package com.thesis.choreography.shipping.kafka;

import com.thesis.choreography.shipping.model.PendingShippingAddress;
import com.thesis.choreography.shipping.repository.PendingShippingAddressRepository;
import com.thesis.choreography.shipping.service.IdempotencyService;
import com.thesis.choreography.shipping.service.ShippingService;
import static com.thesis.common.dto.KafkaTopics.*;
import com.thesis.common.events.InventoryReservedEvent;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.events.PaymentRefundedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class ShippingEventListener {

    private final ShippingService shippingService;
    private final PendingShippingAddressRepository pendingShippingAddressRepository;
    private final IdempotencyService idempotencyService;

    @KafkaListener(topics = ORDER_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:shipping-service}")
    @Transactional
    public void handleOrderEvents(ConsumerRecord<String, Object> record) {
        Object event = record.value();
        if (event instanceof OrderCreatedEvent orderEvent) {
            String eventId = "order-created:" + orderEvent.getOrderId();
            if (!idempotencyService.markProcessed(eventId, "OrderCreatedEvent")) {
                log.info("Skipping duplicate OrderCreatedEvent for order: {}", orderEvent.getOrderId());
                return;
            }
            log.info("Received OrderCreatedEvent for order: {}, storing shipping address", orderEvent.getOrderId());
            // Store the shipping address in database for later use
            PendingShippingAddress pendingAddress = PendingShippingAddress.builder()
                    .orderId(orderEvent.getOrderId())
                    .shippingAddress(orderEvent.getShippingAddress())
                    .build();
            pendingShippingAddressRepository.save(pendingAddress);
            log.info("Stored pending shipping address for order: {}", orderEvent.getOrderId());
        }
    }

    @KafkaListener(topics = INVENTORY_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:shipping-service}")
    @Transactional
    public void handleInventoryEvents(ConsumerRecord<String, Object> record) {
        Object event = record.value();
        if (event instanceof InventoryReservedEvent inventoryEvent) {
            String eventId = "inventory-reserved:" + inventoryEvent.getReservationId();
            if (!idempotencyService.markProcessed(eventId, "InventoryReservedEvent")) {
                log.info("Skipping duplicate InventoryReservedEvent for order: {}", inventoryEvent.getOrderId());
                return;
            }
            log.info("Received InventoryReservedEvent for order: {}", inventoryEvent.getOrderId());
            Optional<PendingShippingAddress> pendingAddress = pendingShippingAddressRepository.findByOrderId(inventoryEvent.getOrderId());
            if (pendingAddress.isPresent()) {
                shippingService.scheduleShipping(inventoryEvent, pendingAddress.get().getShippingAddress());
                pendingShippingAddressRepository.deleteByOrderId(inventoryEvent.getOrderId());
                log.info("Processed and removed pending shipping address for order: {}", inventoryEvent.getOrderId());
            } else {
                log.warn("No pending shipping address found for order: {}", inventoryEvent.getOrderId());
            }
        }
    }

    @KafkaListener(topics = PAYMENT_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:shipping-service}")
    @Transactional
    public void handlePaymentEvents(ConsumerRecord<String, Object> record) {
        Object event = record.value();
        if (event instanceof PaymentRefundedEvent paymentEvent) {
            String eventId = "payment-refunded:" + paymentEvent.getOrderId();
            if (!idempotencyService.markProcessed(eventId, "PaymentRefundedEvent")) {
                log.info("Skipping duplicate PaymentRefundedEvent for order: {}", paymentEvent.getOrderId());
                return;
            }
            log.info("Received PaymentRefundedEvent, cancelling shipping for order: {}", paymentEvent.getOrderId());
            shippingService.cancelShipping(paymentEvent.getOrderId());
            pendingShippingAddressRepository.deleteByOrderId(paymentEvent.getOrderId());
        }
    }
}
