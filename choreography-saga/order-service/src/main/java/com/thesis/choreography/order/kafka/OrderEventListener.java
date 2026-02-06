package com.thesis.choreography.order.kafka;

import com.thesis.choreography.order.service.IdempotencyService;
import com.thesis.choreography.order.service.OrderService;
import com.thesis.common.config.KafkaTopicsProperties;
import com.thesis.common.events.*;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.util.MdcUtils;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventListener {

    private final OrderService orderService;
    private final IdempotencyService idempotencyService;
    private final Validator validator;

    @KafkaListener(topics = KafkaTopicsProperties.PAYMENT_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:order-service}")
    @Transactional
    public void handlePaymentEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof PaymentCompletedEvent event) {
            handlePaymentEvent(record, event, "PaymentCompletedEvent",
                () -> !idempotencyService.tryMarkAsProcessed("payment-completed:%s".formatted(event.paymentId()), "PaymentCompletedEvent"),
                () -> {
                    log.debug("Received PaymentCompletedEvent for order: {}", event.orderId());
                    recordMessageReceived(event.orderId(), event.createdAt(), "payment");
                    orderService.updateOrderPayment(event.orderId(), event.paymentId());
                });
        } else if (record.value() instanceof PaymentFailedEvent event) {
            handlePaymentEvent(record, event, "PaymentFailedEvent",
                () -> !idempotencyService.tryMarkAsProcessed("payment-failed:%s".formatted(event.orderId()), "PaymentFailedEvent"),
                () -> {
                    log.debug("Received PaymentFailedEvent for order: {}", event.orderId());
                    recordMessageReceived(event.orderId(), event.createdAt(), "payment");
                    orderService.cancelOrder(event.orderId(), "Payment failed: " + event.reason());
                });
        } else if (record.value() instanceof PaymentRefundedEvent event) {
            handlePaymentEvent(record, event, "PaymentRefundedEvent",
                () -> !idempotencyService.tryMarkAsProcessed("payment-refunded:%s".formatted(event.orderId()), "PaymentRefundedEvent"),
                () -> {
                    log.debug("Received PaymentRefundedEvent for order: {}", event.orderId());
                    recordMessageReceived(event.orderId(), event.createdAt(), "payment");
                });
        }
    }

    @KafkaListener(topics = KafkaTopicsProperties.INVENTORY_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:order-service}")
    @Transactional
    public void handleInventoryEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof InventoryReservedEvent event) {
            handleInventoryEvent(record, event, "InventoryReservedEvent",
                () -> !idempotencyService.tryMarkAsProcessed("inventory-reserved:%s".formatted(event.reservationId()), "InventoryReservedEvent"),
                () -> {
                    log.debug("Received InventoryReservedEvent for order: {}", event.orderId());
                    recordMessageReceived(event.orderId(), event.createdAt(), "inventory");
                    orderService.updateOrderInventory(event.orderId(), event.reservationId());
                });
        } else if (record.value() instanceof InventoryReservationFailedEvent event) {
            handleInventoryEvent(record, event, "InventoryReservationFailedEvent",
                () -> !idempotencyService.tryMarkAsProcessed("inventory-failed:%s".formatted(event.orderId()), "InventoryReservationFailedEvent"),
                () -> {
                    log.debug("Received InventoryReservationFailedEvent for order: {}", event.orderId());
                    recordMessageReceived(event.orderId(), event.createdAt(), "inventory");
                    orderService.cancelOrder(event.orderId(), "Inventory reservation failed: " + event.reason());
                });
        }
    }

    @KafkaListener(topics = KafkaTopicsProperties.SHIPPING_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:order-service}")
    @Transactional
    public void handleShippingEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof ShippingScheduledEvent event) {
            handleShippingEvent(record, event, "ShippingScheduledEvent",
                () -> !idempotencyService.tryMarkAsProcessed("shipping-scheduled:%s".formatted(event.shippingId()), "ShippingScheduledEvent"),
                () -> {
                    log.debug("Received ShippingScheduledEvent for order: {}", event.orderId());
                    recordMessageReceived(event.orderId(), event.createdAt(), "shipping");
                    orderService.completeOrderWithShipping(event.orderId(), event.shippingId(), event.trackingNumber());
                });
        } else if (record.value() instanceof ShippingFailedEvent event) {
            handleShippingEvent(record, event, "ShippingFailedEvent",
                () -> !idempotencyService.tryMarkAsProcessed("shipping-failed:%s".formatted(event.orderId()), "ShippingFailedEvent"),
                () -> {
                    log.debug("Received ShippingFailedEvent for order: {}", event.orderId());
                    recordMessageReceived(event.orderId(), event.createdAt(), "shipping");
                    orderService.cancelOrder(event.orderId(), "Shipping failed: " + event.reason());
                });
        }
    }

    private <T> void handlePaymentEvent(ConsumerRecord<String, Object> record, T event, String eventType,
                                        Supplier<Boolean> idempotencyCheck,
                                        Runnable handler) {
        handleEvent(record, event, eventType, "payment", idempotencyCheck, handler);
    }

    private <T> void handleInventoryEvent(ConsumerRecord<String, Object> record, T event, String eventType,
                                          Supplier<Boolean> idempotencyCheck,
                                          Runnable handler) {
        handleEvent(record, event, eventType, "inventory", idempotencyCheck, handler);
    }

    private <T> void handleShippingEvent(ConsumerRecord<String, Object> record, T event, String eventType,
                                         Supplier<Boolean> idempotencyCheck,
                                         Runnable handler) {
        handleEvent(record, event, eventType, "shipping", idempotencyCheck, handler);
    }

    private <T> void handleEvent(ConsumerRecord<String, Object> record, T event, String eventType,
                                 String fromService, Supplier<Boolean> idempotencyCheck,
                                 Runnable handler) {
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

    private void recordMessageReceived(String orderId, Instant eventCreatedAt, String fromService) {
        orderService.getMetricsHelper().recordMessageReceived(orderId, SagaMetrics.TYPE_EVENT);
        if (eventCreatedAt != null) {
            Duration latency = Duration.between(eventCreatedAt, Instant.now());
            orderService.getMetricsHelper().recordMessageLatency(fromService, "order", latency);
        }
    }
}
