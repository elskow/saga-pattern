package com.thesis.choreography.order.kafka;

import com.thesis.choreography.order.service.IdempotencyService;
import com.thesis.choreography.order.service.OrderService;
import static com.thesis.common.dto.KafkaTopics.*;
import com.thesis.common.events.*;
import com.thesis.common.metrics.SagaMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventListener {

    private final OrderService orderService;
    private final IdempotencyService idempotencyService;

    @KafkaListener(topics = PAYMENT_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:order-service}")
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
            recordMessageReceived(paymentEvent.getOrderId(), paymentEvent.getCreatedAt(), "payment");
            orderService.updateOrderPayment(paymentEvent.getOrderId(), paymentEvent.getPaymentId());
        } else if (event instanceof PaymentFailedEvent paymentEvent) {
            String eventId = "payment-failed:" + paymentEvent.getOrderId();
            if (!idempotencyService.markProcessed(eventId, "PaymentFailedEvent")) {
                log.info("Skipping duplicate PaymentFailedEvent for order: {}", paymentEvent.getOrderId());
                return;
            }
            log.info("Received PaymentFailedEvent for order: {}", paymentEvent.getOrderId());
            recordMessageReceived(paymentEvent.getOrderId(), paymentEvent.getCreatedAt(), "payment");
            orderService.cancelOrder(paymentEvent.getOrderId(), "Payment failed: " + paymentEvent.getReason());
        } else if (event instanceof PaymentRefundedEvent paymentEvent) {
            String eventId = "payment-refunded:" + paymentEvent.getOrderId();
            if (!idempotencyService.markProcessed(eventId, "PaymentRefundedEvent")) {
                log.info("Skipping duplicate PaymentRefundedEvent for order: {}", paymentEvent.getOrderId());
                return;
            }
            log.info("Received PaymentRefundedEvent for order: {}", paymentEvent.getOrderId());
            recordMessageReceived(paymentEvent.getOrderId(), paymentEvent.getCreatedAt(), "payment");
            // Payment was refunded, order should already be cancelled
        }
    }

    @KafkaListener(topics = INVENTORY_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:order-service}")
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
            recordMessageReceived(inventoryEvent.getOrderId(), inventoryEvent.getCreatedAt(), "inventory");
            orderService.updateOrderInventory(inventoryEvent.getOrderId(), inventoryEvent.getReservationId());
        } else if (event instanceof InventoryReservationFailedEvent inventoryEvent) {
            String eventId = "inventory-failed:" + inventoryEvent.getOrderId();
            if (!idempotencyService.markProcessed(eventId, "InventoryReservationFailedEvent")) {
                log.info("Skipping duplicate InventoryReservationFailedEvent for order: {}", inventoryEvent.getOrderId());
                return;
            }
            log.info("Received InventoryReservationFailedEvent for order: {}", inventoryEvent.getOrderId());
            recordMessageReceived(inventoryEvent.getOrderId(), inventoryEvent.getCreatedAt(), "inventory");
            orderService.cancelOrder(inventoryEvent.getOrderId(), "Inventory reservation failed: " + inventoryEvent.getReason());
        }
    }

    @KafkaListener(topics = SHIPPING_EVENTS_TOPIC, groupId = "${app.kafka.consumer.group-id:order-service}")
    @Transactional
    public void handleShippingEvents(ConsumerRecord<String, Object> record) {
        Object event = record.value();
        if (event instanceof ShippingScheduledEvent shippingEvent) {
            String eventId = "shipping-scheduled:" + shippingEvent.getShippingId();
            if (!idempotencyService.markProcessed(eventId, "ShippingScheduledEvent")) {
                log.info("Skipping duplicate ShippingScheduledEvent for order: {}", shippingEvent.getOrderId());
                return;
            }
            log.info("Received ShippingScheduledEvent for order: {}", shippingEvent.getOrderId());
            recordMessageReceived(shippingEvent.getOrderId(), shippingEvent.getCreatedAt(), "shipping");
            orderService.updateOrderShipping(
                    shippingEvent.getOrderId(), 
                    shippingEvent.getShippingId(), 
                    shippingEvent.getTrackingNumber());
            // Mark order as completed
            orderService.completeOrder(shippingEvent.getOrderId());
        } else if (event instanceof ShippingFailedEvent shippingEvent) {
            String eventId = "shipping-failed:" + shippingEvent.getOrderId();
            if (!idempotencyService.markProcessed(eventId, "ShippingFailedEvent")) {
                log.info("Skipping duplicate ShippingFailedEvent for order: {}", shippingEvent.getOrderId());
                return;
            }
            log.info("Received ShippingFailedEvent for order: {}", shippingEvent.getOrderId());
            recordMessageReceived(shippingEvent.getOrderId(), shippingEvent.getCreatedAt(), "shipping");
            orderService.cancelOrder(shippingEvent.getOrderId(), "Shipping failed: " + shippingEvent.getReason());
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
