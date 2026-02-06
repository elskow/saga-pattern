package com.thesis.choreography.shipping.kafka;

import com.thesis.choreography.shipping.model.PendingShippingAddress;
import com.thesis.choreography.shipping.repository.PendingShippingAddressRepository;
import com.thesis.choreography.shipping.service.IdempotencyService;
import com.thesis.choreography.shipping.service.ShippingService;
import com.thesis.common.config.KafkaTopicsProperties;
import com.thesis.common.events.InventoryReservedEvent;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.events.PaymentRefundedEvent;
import com.thesis.common.util.MdcUtils;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

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

    @KafkaListener(topics = KafkaTopicsProperties.ORDER_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:shipping-service}")
    @Transactional
    public void handleOrderEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof OrderCreatedEvent event) {
            handleEvent(event, "OrderCreatedEvent",
                () -> !idempotencyService.tryMarkAsProcessed("order-created:%s".formatted(event.orderId()), "OrderCreatedEvent"),
                () -> {
                    log.debug("Received OrderCreatedEvent for order: {}, storing shipping address", event.orderId());
                    PendingShippingAddress pendingAddress = PendingShippingAddress.builder()
                        .orderId(event.orderId())
                        .shippingAddress(event.shippingAddress())
                        .build();
                    pendingShippingAddressRepository.save(pendingAddress);
                    log.debug("Stored pending shipping address for order: {}", event.orderId());
                });
        }
    }

    @KafkaListener(topics = KafkaTopicsProperties.INVENTORY_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:shipping-service}")
    @Transactional
    public void handleInventoryEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof InventoryReservedEvent event) {
            handleEvent(event, "InventoryReservedEvent",
                () -> !idempotencyService.tryMarkAsProcessed("inventory-reserved:%s".formatted(event.reservationId()), "InventoryReservedEvent"),
                () -> {
                    log.debug("Received InventoryReservedEvent for order: {}", event.orderId());
                    pendingShippingAddressRepository.findByOrderId(event.orderId())
                        .ifPresentOrElse(
                            address -> {
                                shippingService.scheduleShipping(event, address.getShippingAddress());
                                pendingShippingAddressRepository.deleteByOrderId(event.orderId());
                                log.debug("Processed and removed pending shipping address for order: {}", event.orderId());
                            },
                            () -> log.warn("No pending shipping address found for order: {}", event.orderId())
                        );
                });
        }
    }

    @KafkaListener(topics = KafkaTopicsProperties.PAYMENT_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:shipping-service}")
    @Transactional
    public void handlePaymentEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof PaymentRefundedEvent event) {
            handleEvent(event, "PaymentRefundedEvent",
                () -> !idempotencyService.tryMarkAsProcessed("payment-refunded:%s".formatted(event.orderId()), "PaymentRefundedEvent"),
                () -> {
                    log.debug("Received PaymentRefundedEvent, cancelling shipping for order: {}", event.orderId());
                    shippingService.cancelShipping(event.orderId());
                    pendingShippingAddressRepository.deleteByOrderId(event.orderId());
                });
        }
    }

    private <T> void handleEvent(T event, String eventType,
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

            if (idempotencyCheck.get()) {
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
