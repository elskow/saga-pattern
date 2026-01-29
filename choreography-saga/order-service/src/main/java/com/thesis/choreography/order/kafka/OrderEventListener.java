package com.thesis.choreography.order.kafka;

import com.thesis.choreography.order.service.IdempotencyService;
import com.thesis.choreography.order.service.OrderService;
import com.thesis.common.dto.KafkaTopics;
import com.thesis.common.events.*;
import com.thesis.common.metrics.SagaMetrics;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
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

    @KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:order-service}")
    @Transactional
    public void handlePaymentEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof PaymentCompletedEvent event) {
            handlePaymentEvent(record, event, "PaymentCompletedEvent",
                () -> !idempotencyService.markProcessed("payment-completed:" + event.getPaymentId(), "PaymentCompletedEvent"),
                () -> {
                    log.info("Received PaymentCompletedEvent for order: {}", event.getOrderId());
                    recordMessageReceived(event.getOrderId(), event.getCreatedAt(), "payment");
                    orderService.updateOrderPayment(event.getOrderId(), event.getPaymentId());
                });
        } else if (record.value() instanceof PaymentFailedEvent event) {
            handlePaymentEvent(record, event, "PaymentFailedEvent",
                () -> !idempotencyService.markProcessed("payment-failed:" + event.getOrderId(), "PaymentFailedEvent"),
                () -> {
                    log.info("Received PaymentFailedEvent for order: {}", event.getOrderId());
                    recordMessageReceived(event.getOrderId(), event.getCreatedAt(), "payment");
                    orderService.cancelOrder(event.getOrderId(), "Payment failed: " + event.getReason());
                });
        } else if (record.value() instanceof PaymentRefundedEvent event) {
            handlePaymentEvent(record, event, "PaymentRefundedEvent",
                () -> !idempotencyService.markProcessed("payment-refunded:" + event.getOrderId(), "PaymentRefundedEvent"),
                () -> {
                    log.info("Received PaymentRefundedEvent for order: {}", event.getOrderId());
                    recordMessageReceived(event.getOrderId(), event.getCreatedAt(), "payment");
                });
        }
    }

    @KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:order-service}")
    @Transactional
    public void handleInventoryEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof InventoryReservedEvent event) {
            handleInventoryEvent(record, event, "InventoryReservedEvent",
                () -> !idempotencyService.markProcessed("inventory-reserved:" + event.getReservationId(), "InventoryReservedEvent"),
                () -> {
                    log.info("Received InventoryReservedEvent for order: {}", event.getOrderId());
                    recordMessageReceived(event.getOrderId(), event.getCreatedAt(), "inventory");
                    orderService.updateOrderInventory(event.getOrderId(), event.getReservationId());
                });
        } else if (record.value() instanceof InventoryReservationFailedEvent event) {
            handleInventoryEvent(record, event, "InventoryReservationFailedEvent",
                () -> !idempotencyService.markProcessed("inventory-failed:" + event.getOrderId(), "InventoryReservationFailedEvent"),
                () -> {
                    log.info("Received InventoryReservationFailedEvent for order: {}", event.getOrderId());
                    recordMessageReceived(event.getOrderId(), event.getCreatedAt(), "inventory");
                    orderService.cancelOrder(event.getOrderId(), "Inventory reservation failed: " + event.getReason());
                });
        }
    }

    @KafkaListener(topics = KafkaTopics.SHIPPING_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:order-service}")
    @Transactional
    public void handleShippingEvents(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof ShippingScheduledEvent event) {
            handleShippingEvent(record, event, "ShippingScheduledEvent",
                () -> !idempotencyService.markProcessed("shipping-scheduled:" + event.getShippingId(), "ShippingScheduledEvent"),
                () -> {
                    log.info("Received ShippingScheduledEvent for order: {}", event.getOrderId());
                    recordMessageReceived(event.getOrderId(), event.getCreatedAt(), "shipping");
                    orderService.completeOrderWithShipping(event.getOrderId(), event.getShippingId(), event.getTrackingNumber());
                });
        } else if (record.value() instanceof ShippingFailedEvent event) {
            handleShippingEvent(record, event, "ShippingFailedEvent",
                () -> !idempotencyService.markProcessed("shipping-failed:" + event.getOrderId(), "ShippingFailedEvent"),
                () -> {
                    log.info("Received ShippingFailedEvent for order: {}", event.getOrderId());
                    recordMessageReceived(event.getOrderId(), event.getCreatedAt(), "shipping");
                    orderService.cancelOrder(event.getOrderId(), "Shipping failed: " + event.getReason());
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
            String orderId = getOrderId(event);
            String eventCorrelationId = getCorrelationId(event, correlationId);

            setupMdc(orderId, eventCorrelationId);

            Set<?> violations = validator.validate(event);
            if (!violations.isEmpty()) {
                log.error("Invalid {} received for order: {}. Violations: {}", eventType, orderId, violations);
                return;
            }

            // idempotencyCheck returns true if event is a duplicate (should skip)
            // The lambda wraps markProcessed with ! so: true = duplicate, false = new
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

    private Instant getCreatedAt(Object event) {
        if (event instanceof HasCreatedAt hc) {
            return hc.getCreatedAt();
        }
        return null;
    }

    private void setupMdc(String orderId, String correlationId) {
        MDC.put("orderId", orderId != null ? orderId : "unknown");
        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }
    }

    private void recordMessageReceived(String orderId, Instant eventCreatedAt, String fromService) {
        orderService.getMetricsHelper().recordMessageReceived(orderId, SagaMetrics.TYPE_EVENT);
        if (eventCreatedAt != null) {
            Duration latency = Duration.between(eventCreatedAt, Instant.now());
            orderService.getMetricsHelper().recordMessageLatency(fromService, "order", latency);
        }
    }

    private interface HasOrderId {
        String getOrderId();
    }

    private interface HasCorrelationId {
        String getCorrelationId();
    }

    private interface HasCreatedAt {
        Instant getCreatedAt();
    }
}
