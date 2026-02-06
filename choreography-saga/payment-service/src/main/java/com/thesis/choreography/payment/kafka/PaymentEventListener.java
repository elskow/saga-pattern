package com.thesis.choreography.payment.kafka;

import com.thesis.choreography.payment.service.IdempotencyService;
import com.thesis.choreography.payment.service.PaymentService;
import com.thesis.common.config.KafkaTopicsProperties;
import com.thesis.common.events.InventoryReservationFailedEvent;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.exception.SagaCommandProcessingException;
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

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventListener {

    private final PaymentService paymentService;
    private final IdempotencyService idempotencyService;
    private final Validator validator;

    @KafkaListener(topics = KafkaTopicsProperties.ORDER_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:payment-service}")
    @Transactional
    public void handleOrderEvents(ConsumerRecord<String, Object> record) {
        handleEvent(record, OrderCreatedEvent.class, (orderEvent, eventId) -> {
            if (!idempotencyService.tryMarkAsProcessed(eventId, "OrderCreatedEvent")) {
                log.trace("Skipping duplicate OrderCreatedEvent for order: {}", orderEvent.orderId());
                return;
            }
            log.debug("Received OrderCreatedEvent for order: {}", orderEvent.orderId());
            paymentService.processPayment(orderEvent);
        });
    }

    @KafkaListener(topics = KafkaTopicsProperties.INVENTORY_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:payment-service}")
    @Transactional
    public void handleInventoryEvents(ConsumerRecord<String, Object> record) {
        handleEvent(record, InventoryReservationFailedEvent.class, (inventoryEvent, eventId) -> {
            if (!idempotencyService.tryMarkAsProcessed(eventId, "InventoryReservationFailedEvent")) {
                log.trace("Skipping duplicate InventoryReservationFailedEvent for order: {}", inventoryEvent.orderId());
                return;
            }
            log.debug("Received InventoryReservationFailedEvent, initiating refund for order: {}", inventoryEvent.orderId());
            paymentService.refundPayment(inventoryEvent.orderId());
        });
    }

    private <T> void handleEvent(ConsumerRecord<String, Object> record, Class<T> eventClass, EventHandler<T> handler) {
        String correlationId = UUID.randomUUID().toString();
        Object event = record.value();

        try {
            log.debug("Received event of type: {}", event.getClass().getName());
            if (eventClass.isInstance(event)) {
                T typedEvent = eventClass.cast(event);
                String orderId = MdcUtils.getOrderId(typedEvent);
                String eventCorrelationId = MdcUtils.getCorrelationId(typedEvent, correlationId);

                MdcUtils.setupEventMdc(orderId, eventCorrelationId);

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
            throw new SagaCommandProcessingException("Event processing failed", e);
        } finally {
            MdcUtils.clearEventMdc();
        }
    }

    private String getEventIdPrefix(Class<?> eventClass) {
        return switch (eventClass.getSimpleName()) {
            case "OrderCreatedEvent" -> "order-created";
            case "InventoryReservationFailedEvent" -> "inventory-failed";
            default -> eventClass.getSimpleName().toLowerCase();
        };
    }

    @FunctionalInterface
    private interface EventHandler<T> {
        void handle(T event, String eventId) throws Exception;
    }
}
