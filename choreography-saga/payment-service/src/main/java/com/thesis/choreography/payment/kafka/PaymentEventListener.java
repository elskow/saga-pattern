package com.thesis.choreography.payment.kafka;

import com.thesis.choreography.payment.service.IdempotencyService;
import com.thesis.choreography.payment.service.PaymentService;
import com.thesis.common.dto.KafkaTopics;
import com.thesis.common.events.InventoryReservationFailedEvent;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventListener {

    private final PaymentService paymentService;
    private final IdempotencyService idempotencyService;
    private final Validator validator;

    @KafkaListener(topics = KafkaTopics.ORDER_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:payment-service}")
    @Transactional
    public void handleOrderEvents(ConsumerRecord<String, Object> record) {
        handleEvent(record, OrderCreatedEvent.class, (orderEvent, eventId) -> {
            if (!idempotencyService.markProcessed(eventId, "OrderCreatedEvent")) {
                log.info("Skipping duplicate OrderCreatedEvent for order: {}", orderEvent.getOrderId());
                return;
            }
            log.info("Received OrderCreatedEvent for order: {}", orderEvent.getOrderId());
            paymentService.processPayment(orderEvent);
        });
    }

    @KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:payment-service}")
    @Transactional
    public void handleInventoryEvents(ConsumerRecord<String, Object> record) {
        handleEvent(record, InventoryReservationFailedEvent.class, (inventoryEvent, eventId) -> {
            if (!idempotencyService.markProcessed(eventId, "InventoryReservationFailedEvent")) {
                log.info("Skipping duplicate InventoryReservationFailedEvent for order: {}", inventoryEvent.getOrderId());
                return;
            }
            log.info("Received InventoryReservationFailedEvent, initiating refund for order: {}", inventoryEvent.getOrderId());
            paymentService.refundPayment(inventoryEvent.getOrderId());
        });
    }

    private <T> void handleEvent(ConsumerRecord<String, Object> record, Class<T> eventClass, EventHandler<T> handler) {
        String correlationId = UUID.randomUUID().toString();
        Object event = record.value();

        try {
            log.debug("Received event of type: {}", event.getClass().getName());
            if (eventClass.isInstance(event)) {
                T typedEvent = eventClass.cast(event);
                String orderId = getOrderId(typedEvent);
                String eventCorrelationId = getCorrelationId(typedEvent, correlationId);

                setupMdc(orderId, eventCorrelationId);

                Set<?> violations = validator.validate(typedEvent);
                if (!violations.isEmpty()) {
                    log.error("Invalid {} received for order: {}. Violations: {}", 
                            eventClass.getSimpleName(), orderId, violations);
                    return;
                }

                String eventId = getEventIdPrefix(eventClass) + ":" + orderId;
                handler.handle(typedEvent, eventId);
            }
        } catch (RuntimeException e) {
            log.error("Error processing event {}: {}", eventClass.getSimpleName(), e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            log.error("Error processing event {}: {}", eventClass.getSimpleName(), e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            MDC.clear();
        }
    }

    @FunctionalInterface
    private interface EventHandler<T> {
        void handle(T event, String eventId) throws Exception;
    }

    private String getOrderId(Object event) {
        if (event instanceof OrderCreatedEvent oce) {
            return oce.getOrderId();
        } else if (event instanceof InventoryReservationFailedEvent irfe) {
            return irfe.getOrderId();
        }
        return "unknown";
    }

    private String getCorrelationId(Object event, String defaultCorrelationId) {
        if (event instanceof OrderCreatedEvent oce) {
            return oce.getCorrelationId() != null ? oce.getCorrelationId() : defaultCorrelationId;
        } else if (event instanceof InventoryReservationFailedEvent irfe) {
            return irfe.getCorrelationId() != null ? irfe.getCorrelationId() : defaultCorrelationId;
        }
        return defaultCorrelationId;
    }

    private String getEventIdPrefix(Class<?> eventClass) {
        if (eventClass == OrderCreatedEvent.class) {
            return "order-created";
        } else if (eventClass == InventoryReservationFailedEvent.class) {
            return "inventory-failed";
        }
        return eventClass.getSimpleName().toLowerCase();
    }

    private void setupMdc(String orderId, String correlationId) {
        MDC.put("orderId", orderId != null ? orderId : "unknown");
        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }
    }
}
