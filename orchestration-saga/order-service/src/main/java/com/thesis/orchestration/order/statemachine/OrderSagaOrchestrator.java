package com.thesis.orchestration.order.statemachine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.*;
import com.thesis.common.replies.SagaReply;
import com.thesis.common.replies.PaymentCompletedReply;
import com.thesis.common.replies.PaymentFailedReply;
import com.thesis.common.replies.InventoryReservedReply;
import com.thesis.common.replies.InventoryFailedReply;
import com.thesis.common.replies.ShippingScheduledReply;
import com.thesis.common.replies.ShippingFailedReply;
import com.thesis.orchestration.order.dto.CreateOrderRequest;
import com.thesis.orchestration.order.service.OrderService;
import com.thesis.orchestration.order.statemachine.OrderStateMachineConfig.OrderStateMachineFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.statemachine.StateMachine;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Order Saga Orchestrator using Spring State Machine.
 * Coordinates the distributed transaction across payment, inventory, and shipping services.
 * Uses programmatic state machine creation for GraalVM native image compatibility.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderSagaOrchestrator {

    private final OrderStateMachineFactory stateMachineFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    // Topic names for commands
    private static final String PAYMENT_COMMAND_TOPIC = "orchestration.payment.commands";
    private static final String INVENTORY_COMMAND_TOPIC = "orchestration.inventory.commands";
    private static final String SHIPPING_COMMAND_TOPIC = "orchestration.shipping.commands";

    // Topic names for replies
    private static final String PAYMENT_REPLY_TOPIC = "orchestration.payment.replies";
    private static final String INVENTORY_REPLY_TOPIC = "orchestration.inventory.replies";
    private static final String SHIPPING_REPLY_TOPIC = "orchestration.shipping.replies";

    // Store active state machines by orderId
    private final Map<String, StateMachine<OrderStates, OrderEvents>> stateMachines = new ConcurrentHashMap<>();

    // Store saga data by orderId
    private final Map<String, SagaData> sagaDataMap = new ConcurrentHashMap<>();

    /**
     * Creates an order and starts the saga.
     */
    public String createOrder(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        String paymentId = UUID.randomUUID().toString();
        String reservationId = UUID.randomUUID().toString();
        String shipmentId = UUID.randomUUID().toString();

        // Create and persist order entity
        orderService.createOrder(
                orderId,
                request.getCustomerId(),
                request.getTotalAmount(),
                request.getShippingAddress(),
                request.getItems(),
                paymentId,
                reservationId,
                shipmentId
        );

        // Store saga data
        SagaData sagaData = SagaData.builder()
                .orderId(orderId)
                .customerId(request.getCustomerId())
                .paymentId(paymentId)
                .reservationId(reservationId)
                .shipmentId(shipmentId)
                .totalAmount(request.getTotalAmount())
                .shippingAddress(request.getShippingAddress())
                .items(request.getItems())
                .currentStep(SagaData.SagaStep.PAYMENT)
                .build();
        sagaDataMap.put(orderId, sagaData);

        // Create and start state machine (programmatic approach - AOT compatible)
        StateMachine<OrderStates, OrderEvents> sm = stateMachineFactory.create(orderId);
        sm.startReactively().block();
        stateMachines.put(orderId, sm);

        log.info("Starting saga for order: {}", orderId);

        // Trigger the saga start
        sm.sendEvent(OrderEvents.START_SAGA);

        // Send payment command
        sendPaymentCommand(sagaData);

        return orderId;
    }

    // ========== Command Senders ==========

    private void sendPaymentCommand(SagaData data) {
        log.info("Sending payment command for order: {}", data.getOrderId());

        ProcessPaymentCommand command = ProcessPaymentCommand.builder()
                .paymentId(data.getPaymentId())
                .orderId(data.getOrderId())
                .customerId(data.getCustomerId())
                .amount(data.getTotalAmount())
                .build();

        kafkaTemplate.send(PAYMENT_COMMAND_TOPIC, data.getOrderId(), command);
    }

    private void sendInventoryCommand(SagaData data) {
        log.info("Sending inventory command for order: {}", data.getOrderId());

        ReserveInventoryCommand command = ReserveInventoryCommand.builder()
                .reservationId(data.getReservationId())
                .orderId(data.getOrderId())
                .items(data.getItems())
                .build();

        kafkaTemplate.send(INVENTORY_COMMAND_TOPIC, data.getOrderId(), command);
    }

    private void sendShippingCommand(SagaData data) {
        log.info("Sending shipping command for order: {}", data.getOrderId());

        ScheduleShippingCommand command = ScheduleShippingCommand.builder()
                .shipmentId(data.getShipmentId())
                .orderId(data.getOrderId())
                .shippingAddress(data.getShippingAddress())
                .build();

        kafkaTemplate.send(SHIPPING_COMMAND_TOPIC, data.getOrderId(), command);
    }

    // ========== Compensation Commands ==========

    private void sendRefundPaymentCommand(SagaData data) {
        log.info("Sending refund payment command for order: {}", data.getOrderId());

        RefundPaymentCommand command = RefundPaymentCommand.builder()
                .paymentId(data.getPaymentId())
                .orderId(data.getOrderId())
                .build();

        kafkaTemplate.send(PAYMENT_COMMAND_TOPIC, data.getOrderId(), command);
    }

    private void sendReleaseInventoryCommand(SagaData data) {
        log.info("Sending release inventory command for order: {}", data.getOrderId());

        ReleaseInventoryCommand command = ReleaseInventoryCommand.builder()
                .reservationId(data.getReservationId())
                .orderId(data.getOrderId())
                .build();

        kafkaTemplate.send(INVENTORY_COMMAND_TOPIC, data.getOrderId(), command);
    }

    private void sendCancelShippingCommand(SagaData data) {
        log.info("Sending cancel shipping command for order: {}", data.getOrderId());

        CancelShippingCommand command = CancelShippingCommand.builder()
                .shipmentId(data.getShipmentId())
                .orderId(data.getOrderId())
                .build();

        kafkaTemplate.send(SHIPPING_COMMAND_TOPIC, data.getOrderId(), command);
    }

    // ========== Reply Handlers ==========

    @KafkaListener(topics = PAYMENT_REPLY_TOPIC, groupId = "order-orchestrator")
    public void handlePaymentReply(String message) {
        try {
            SagaReply reply = objectMapper.readValue(message, SagaReply.class);
            if (reply instanceof PaymentCompletedReply successReply) {
                handlePaymentSuccess(successReply);
            } else if (reply instanceof PaymentFailedReply failedReply) {
                handlePaymentFailure(failedReply);
            }
        } catch (Exception e) {
            log.error("Failed to parse payment reply: {}", message, e);
        }
    }

    private void handlePaymentSuccess(PaymentCompletedReply reply) {
        String orderId = reply.getOrderId();
        log.info("Payment completed for order: {}", orderId);

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        if (sm != null && data != null) {
            sm.sendEvent(OrderEvents.PAYMENT_SUCCESS);
            data.setCurrentStep(SagaData.SagaStep.INVENTORY);
            data.setPaymentCompleted(true);
            sendInventoryCommand(data);
        }
    }

    private void handlePaymentFailure(PaymentFailedReply reply) {
        String orderId = reply.getOrderId();
        log.error("Payment failed for order: {}. Reason: {}", orderId, reply.getReason());

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        if (sm != null) {
            sm.sendEvent(OrderEvents.PAYMENT_FAILED);
            orderService.failOrder(orderId, "Payment failed: " + reply.getReason());
            cleanup(orderId);
        }
    }

    @KafkaListener(topics = INVENTORY_REPLY_TOPIC, groupId = "order-orchestrator")
    public void handleInventoryReply(String message) {
        try {
            SagaReply reply = objectMapper.readValue(message, SagaReply.class);
            if (reply instanceof InventoryReservedReply successReply) {
                handleInventorySuccess(successReply);
            } else if (reply instanceof InventoryFailedReply failedReply) {
                handleInventoryFailure(failedReply);
            }
        } catch (Exception e) {
            log.error("Failed to parse inventory reply: {}", message, e);
        }
    }

    private void handleInventorySuccess(InventoryReservedReply reply) {
        String orderId = reply.getOrderId();
        log.info("Inventory reserved for order: {}", orderId);

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        if (sm != null && data != null) {
            sm.sendEvent(OrderEvents.INVENTORY_RESERVED);
            data.setCurrentStep(SagaData.SagaStep.SHIPPING);
            data.setInventoryReserved(true);
            sendShippingCommand(data);
        }
    }

    private void handleInventoryFailure(InventoryFailedReply reply) {
        String orderId = reply.getOrderId();
        log.error("Inventory reservation failed for order: {}. Reason: {}", orderId, reply.getReason());

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        if (sm != null && data != null) {
            sm.sendEvent(OrderEvents.INVENTORY_FAILED);
            // Compensate: refund payment
            if (data.isPaymentCompleted()) {
                sendRefundPaymentCommand(data);
            }
            orderService.failOrder(orderId, "Inventory reservation failed: " + reply.getReason());
            completeCompensation(orderId);
        }
    }

    @KafkaListener(topics = SHIPPING_REPLY_TOPIC, groupId = "order-orchestrator")
    public void handleShippingReply(String message) {
        try {
            SagaReply reply = objectMapper.readValue(message, SagaReply.class);
            if (reply instanceof ShippingScheduledReply successReply) {
                handleShippingSuccess(successReply);
            } else if (reply instanceof ShippingFailedReply failedReply) {
                handleShippingFailure(failedReply);
            }
        } catch (Exception e) {
            log.error("Failed to parse shipping reply: {}", message, e);
        }
    }

    private void handleShippingSuccess(ShippingScheduledReply reply) {
        String orderId = reply.getOrderId();
        log.info("Shipping scheduled for order: {}. Order completed!", orderId);

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        if (sm != null) {
            sm.sendEvent(OrderEvents.SHIPPING_SCHEDULED);
            orderService.completeOrder(orderId);
            cleanup(orderId);
        }
    }

    private void handleShippingFailure(ShippingFailedReply reply) {
        String orderId = reply.getOrderId();
        log.error("Shipping failed for order: {}. Reason: {}", orderId, reply.getReason());

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        if (sm != null && data != null) {
            sm.sendEvent(OrderEvents.SHIPPING_FAILED);
            // Compensate: release inventory and refund payment
            if (data.isInventoryReserved()) {
                sendReleaseInventoryCommand(data);
            }
            if (data.isPaymentCompleted()) {
                sendRefundPaymentCommand(data);
            }
            orderService.failOrder(orderId, "Shipping failed: " + reply.getReason());
            completeCompensation(orderId);
        }
    }

    private void completeCompensation(String orderId) {
        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        if (sm != null) {
            sm.sendEvent(OrderEvents.COMPENSATION_COMPLETE);
        }
        cleanup(orderId);
    }

    private void cleanup(String orderId) {
        StateMachine<OrderStates, OrderEvents> sm = stateMachines.remove(orderId);
        if (sm != null) {
            sm.stop();
        }
        sagaDataMap.remove(orderId);
    }

}
