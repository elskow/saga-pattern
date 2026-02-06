package com.thesis.orchestration.order.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.config.KafkaTopicsProperties;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.exception.SagaCommandProcessingException;
import com.thesis.common.replies.*;
import com.thesis.orchestration.order.dto.CreateOrderRequest;
import com.thesis.orchestration.order.service.OrderService;
import com.thesis.orchestration.order.statemachine.OrderEvents;
import com.thesis.orchestration.order.statemachine.OrderStates;
import com.thesis.saga.config.SagaFrameworkProperties;
import com.thesis.saga.core.SagaDefinition;
import com.thesis.saga.idempotency.IdempotencyService;
import com.thesis.saga.metrics.SagaMetricsRecorder;
import com.thesis.saga.orchestrator.AbstractSagaOrchestrator;
import com.thesis.saga.outbox.OutboxService;
import com.thesis.saga.persistence.SagaInstanceRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;


@Service
@Slf4j
public class OrderSagaOrchestrator
    extends AbstractSagaOrchestrator<OrderStates, OrderEvents, OrderSagaContext> {

    private final OrderService orderService;

    public OrderSagaOrchestrator(
        SagaDefinition<OrderStates, OrderEvents, OrderSagaContext> definition,
        SagaInstanceRepository instanceRepository,
        OutboxService outboxService,
        IdempotencyService idempotencyService,
        ObjectMapper objectMapper,
        SagaMetricsRecorder metricsRecorder,
        SagaFrameworkProperties properties,
        OrderService orderService) {
        super(definition, instanceRepository, outboxService, idempotencyService,
            objectMapper, metricsRecorder, properties);
        this.orderService = orderService;
    }


    @Transactional
    public String createOrder(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        String paymentId = UUID.randomUUID().toString();
        String reservationId = UUID.randomUUID().toString();
        String shipmentId = UUID.randomUUID().toString();

        List<OrderCreatedEvent.OrderItemEvent> items = request.items().stream()
            .map(item -> new OrderCreatedEvent.OrderItemEvent(
                item.productId(),
                item.productName(),
                item.quantity(),
                item.price()))
            .toList();

        OrderSagaContext context = OrderSagaContext.create(
            orderId,
            request.customerId(),
            request.totalAmount(),
            request.shippingAddress(),
            items,
            paymentId,
            reservationId,
            shipmentId);

        startSaga(orderId, context);

        return orderId;
    }

    @KafkaListener(topics = KafkaTopicsProperties.PAYMENT_REPLIES_TOPIC, groupId = "order-orchestrator")
    public void handlePaymentReply(String message) {
        try {
            SagaReply reply = objectMapper.readValue(message, SagaReply.class);
            String orderId = reply.orderId();

            switch (reply) {
                case PaymentCompletedReply r -> processReply(orderId, "PAYMENT_SUCCESS", r, OrderEvents.PAYMENT_SUCCESS, null, true);
                case PaymentFailedReply r -> processReply(orderId, "PAYMENT_FAILED", r, null, OrderEvents.PAYMENT_FAILED, false);
                case PaymentRefundedReply _ -> processCompensationReply(orderId, "PAYMENT_REFUNDED");
                default -> log.warn("Unexpected payment reply type: {}", reply.getClass().getSimpleName());
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("Malformed payment reply JSON, skipping: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Error processing payment reply: {}", e.getMessage(), e);
            throw new SagaCommandProcessingException("Payment reply processing failed", e);
        }
    }

    @KafkaListener(topics = KafkaTopicsProperties.INVENTORY_REPLIES_TOPIC, groupId = "order-orchestrator")
    public void handleInventoryReply(String message) {
        try {
            SagaReply reply = objectMapper.readValue(message, SagaReply.class);
            String orderId = reply.orderId();

            switch (reply) {
                case InventoryReservedReply r -> processReply(orderId, "INVENTORY_RESERVED", r, OrderEvents.INVENTORY_RESERVED, null, true);
                case InventoryFailedReply r -> processReply(orderId, "INVENTORY_FAILED", r, null, OrderEvents.INVENTORY_FAILED, false);
                case InventoryReleasedReply _ -> processCompensationReply(orderId, "INVENTORY_RELEASED");
                default -> log.warn("Unexpected inventory reply type: {}", reply.getClass().getSimpleName());
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("Malformed inventory reply JSON, skipping: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Error processing inventory reply: {}", e.getMessage(), e);
            throw new SagaCommandProcessingException("Inventory reply processing failed", e);
        }
    }

    @KafkaListener(topics = KafkaTopicsProperties.SHIPPING_REPLIES_TOPIC, groupId = "order-orchestrator")
    public void handleShippingReply(String message) {
        try {
            SagaReply reply = objectMapper.readValue(message, SagaReply.class);
            String orderId = reply.orderId();

            switch (reply) {
                case ShippingScheduledReply r -> processReply(orderId, "SHIPPING_SCHEDULED", r, OrderEvents.SHIPPING_SCHEDULED, null, true);
                case ShippingFailedReply r -> processReply(orderId, "SHIPPING_FAILED", r, null, OrderEvents.SHIPPING_FAILED, false);
                case ShippingCancelledReply _ -> processCompensationReply(orderId, "SHIPPING_CANCELLED");
                default -> log.warn("Unexpected shipping reply type: {}", reply.getClass().getSimpleName());
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("Malformed shipping reply JSON, skipping: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Error processing shipping reply: {}", e.getMessage(), e);
            throw new SagaCommandProcessingException("Shipping reply processing failed", e);
        }
    }
    
    @Override
    protected void onSagaStarting(String sagaId, OrderSagaContext context) {
        orderService.createOrder(
            context.getOrderId(),
            context.getCustomerId(),
            context.getTotalAmount(),
            context.getShippingAddress(),
            context.getItems(),
            context.getPaymentId(),
            context.getReservationId(),
            context.getShipmentId());

        log.debug("Order {} created, starting saga", sagaId);
    }

    @Override
    protected void updateContextFromReply(OrderSagaContext context, Object reply, boolean success) {
        switch (reply) {
            case PaymentCompletedReply _ -> context.setPaymentCompleted(true);
            case InventoryReservedReply _ -> context.setInventoryReserved(true);
            case ShippingScheduledReply _ -> context.setShippingScheduled(true);
            case PaymentRefundedReply _ -> context.setPaymentRefunded(true);
            case InventoryReleasedReply _ -> context.setInventoryReleased(true);
            case ShippingCancelledReply _ -> context.setShippingCancelled(true);
            default -> {}
        }
    }

    @Override
    protected void onSagaCompleted(String sagaId, OrderSagaContext context) {
        log.debug("Order {} completed successfully", sagaId);
        orderService.completeOrder(sagaId);
    }

    @Override
    protected void onSagaFailed(String sagaId, OrderSagaContext context, String reason) {
        log.warn("Order {} failed: {}", sagaId, reason);
        orderService.failOrder(sagaId, reason);
    }

    @Override
    protected String getStepNameForReply(String replyType) {
        return switch (replyType) {
            case "PAYMENT_SUCCESS", "PAYMENT_FAILED", "PAYMENT_REFUNDED" -> "Payment";
            case "INVENTORY_RESERVED", "INVENTORY_FAILED", "INVENTORY_RELEASED" -> "Inventory";
            case "SHIPPING_SCHEDULED", "SHIPPING_FAILED", "SHIPPING_CANCELLED" -> "Shipping";
            default -> "Unknown";
        };
    }
}
