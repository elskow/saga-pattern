package com.thesis.orchestration.order.statemachine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.*;
import com.thesis.common.enums.CommandType;
import com.thesis.common.enums.OutboxStatus;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.replies.SagaReply;
import com.thesis.common.replies.PaymentCompletedReply;
import com.thesis.common.replies.PaymentFailedReply;
import com.thesis.common.replies.PaymentRefundedReply;
import com.thesis.common.replies.InventoryReservedReply;
import com.thesis.common.replies.InventoryFailedReply;
import com.thesis.common.replies.InventoryReleasedReply;
import com.thesis.common.replies.ShippingScheduledReply;
import com.thesis.common.replies.ShippingFailedReply;
import com.thesis.common.replies.ShippingCancelledReply;
import com.thesis.orchestration.order.dto.CreateOrderRequest;
import com.thesis.orchestration.order.config.SagaOrchestratorProperties;
import com.thesis.orchestration.order.model.OutboxCommand;
import com.thesis.orchestration.order.model.ProcessedCommand;
import com.thesis.orchestration.order.model.SagaInstance;
import com.thesis.orchestration.order.repository.OutboxCommandRepository;
import com.thesis.orchestration.order.repository.ProcessedCommandRepository;
import com.thesis.orchestration.order.repository.SagaInstanceRepository;
import com.thesis.orchestration.order.service.OrderService;
import com.thesis.orchestration.order.statemachine.OrderStateMachineConfig.OrderStateMachineFactory;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.statemachine.StateMachine;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import org.slf4j.MDC;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Order Saga Orchestrator using Spring State Machine.
 * Coordinates the distributed transaction across payment, inventory, and shipping services.
 * Uses programmatic state machine creation for GraalVM native image compatibility.
 */
@Service
@Slf4j
public class OrderSagaOrchestrator {

    private final OrderStateMachineFactory stateMachineFactory;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderService orderService;
    private final ObjectMapper objectMapper;
    private final SagaInstanceRepository sagaInstanceRepository;
    private final ProcessedCommandRepository processedCommandRepository;
    private final OutboxCommandRepository outboxCommandRepository;
    private final SagaOrchestratorProperties sagaProperties;
     private final Counter kafkaCommandSendSuccessCounter;
    private final Counter kafkaCommandSendFailureCounter;
    private final Counter kafkaCommandSendRetryCounter;
    private final Counter commandRetryAttemptsCounter;
    private final Counter commandRetrySkippedCounter;
    private final Counter stateTransitionCounter;

    @Autowired
    public OrderSagaOrchestrator(OrderStateMachineFactory stateMachineFactory,
                                  KafkaTemplate<String, Object> kafkaTemplate,
                                  OrderService orderService,
                                  ObjectMapper objectMapper,
                                  SagaInstanceRepository sagaInstanceRepository,
                                  ProcessedCommandRepository processedCommandRepository,
                                  MeterRegistry meterRegistry,
                                  SagaOrchestratorProperties sagaProperties,
                                  OutboxCommandRepository outboxCommandRepository) {
        this.stateMachineFactory = stateMachineFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.orderService = orderService;
        this.objectMapper = objectMapper;
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.processedCommandRepository = processedCommandRepository;
        this.sagaProperties = sagaProperties;
        this.outboxCommandRepository = outboxCommandRepository;
        this.kafkaCommandSendSuccessCounter = meterRegistry.counter(
                SagaMetrics.SAGA_MESSAGES_TOTAL,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_SENT,
                SagaMetrics.TAG_MESSAGE_TYPE, SagaMetrics.TYPE_COMMAND,
                SagaMetrics.TAG_OUTCOME, SagaMetrics.OUTCOME_SUCCESS
        );
        this.kafkaCommandSendFailureCounter = meterRegistry.counter(
                SagaMetrics.SAGA_MESSAGES_TOTAL,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_SENT,
                SagaMetrics.TAG_MESSAGE_TYPE, SagaMetrics.TYPE_COMMAND,
                SagaMetrics.TAG_OUTCOME, SagaMetrics.OUTCOME_FAILURE
        );
        this.kafkaCommandSendRetryCounter = meterRegistry.counter(
                SagaMetrics.SAGA_MESSAGES_TOTAL,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_SENT,
                SagaMetrics.TAG_MESSAGE_TYPE, SagaMetrics.TYPE_COMMAND,
                SagaMetrics.TAG_OUTCOME, "retry"
        );
        this.commandRetryAttemptsCounter = meterRegistry.counter(
                SagaMetrics.COMMANDS_RETRY_ATTEMPTS,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION
        );
        this.commandRetrySkippedCounter = meterRegistry.counter(
                SagaMetrics.COMMANDS_RETRY_SKIPPED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION
        );
        this.stateTransitionCounter = meterRegistry.counter(
                "saga.state.transitions",
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                "direction", "state_machine"
        );
    }

    // Topic names for commands
    private static final String PAYMENT_COMMAND_TOPIC = "orchestration.payment.commands";
    private static final String INVENTORY_COMMAND_TOPIC = "orchestration.inventory.commands";
    private static final String SHIPPING_COMMAND_TOPIC = "orchestration.shipping.commands";
    private static final String COMMAND_PROCESS_PAYMENT = "PROCESS_PAYMENT";
    private static final String COMMAND_REFUND_PAYMENT = "REFUND_PAYMENT";
    private static final String COMMAND_RESERVE_INVENTORY = "RESERVE_INVENTORY";
    private static final String COMMAND_RELEASE_INVENTORY = "RELEASE_INVENTORY";
    private static final String COMMAND_SCHEDULE_SHIPPING = "SCHEDULE_SHIPPING";
    private static final String COMMAND_CANCEL_SHIPPING = "CANCEL_SHIPPING";
    private static final String COMMAND_STATUS_PENDING = "PENDING";
    private static final String COMMAND_STATUS_SENT = "SENT";
    private static final String COMMAND_STATUS_SKIPPED = "SKIPPED";
    private static final String OUTBOX_STATUS_PENDING = "PENDING";
    private static final String OUTBOX_STATUS_SENT = "SENT";
    private static final String OUTBOX_STATUS_FAILED = "FAILED";

    // Topic names for replies
    private static final String PAYMENT_REPLY_TOPIC = "orchestration.payment.replies";
    private static final String INVENTORY_REPLY_TOPIC = "orchestration.inventory.replies";
    private static final String SHIPPING_REPLY_TOPIC = "orchestration.shipping.replies";

    // Store active state machines by orderId
    private final Map<String, StateMachine<OrderStates, OrderEvents>> stateMachines = new ConcurrentHashMap<>();

    // Store saga data by orderId
    private final Map<String, SagaData> sagaDataMap = new ConcurrentHashMap<>();

    // Store saga locks by orderId for thread safety
    private final Map<String, java.util.concurrent.locks.Lock> sagaLocks = new ConcurrentHashMap<>();

    /**
     * Executes an action with saga-level locking to ensure thread safety.
     * Each saga gets its own lock to prevent concurrent access to the same order.
     */
    private void withSagaLock(String orderId, Runnable action) {
        java.util.concurrent.locks.Lock lock = sagaLocks.computeIfAbsent(orderId, k -> new java.util.concurrent.locks.ReentrantLock());
        try {
            lock.lock();
            action.run();
        } finally {
            lock.unlock();
            if (isTerminalState(orderId)) {
                sagaLocks.remove(orderId);
            }
        }
    }

    /**
     * Checks if a saga is in a terminal state (COMPLETED or CANCELLED).
     */
    private boolean isTerminalState(String orderId) {
        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        return sm != null && (sm.getState().getId() == OrderStates.COMPLETED || sm.getState().getId() == OrderStates.CANCELLED);
    }

    /**
     * Cleans up stale in-memory sagas that haven't been updated recently.
     * This prevents memory leaks from sagas that never reach terminal states.
     */
    @Scheduled(fixedDelayString = "${saga.orchestrator.in-memory-cleanup-interval}")
    public void cleanupStaleInMemorySagas() {
        Instant cutoff = Instant.now().minus(sagaProperties.getInMemorySagaTtl());
        int removed = 0;
        var iterator = stateMachines.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            SagaData data = sagaDataMap.get(entry.getKey());
            if (data == null || data.getLastUpdatedAt() == null) {
                continue;
            }
            if (data.getLastUpdatedAt().isBefore(cutoff)) {
                log.info("Cleaning up stale saga for order: {}", entry.getKey());
                sagaDataMap.remove(entry.getKey());
                sagaLocks.remove(entry.getKey());
                iterator.remove();
                removed++;
            }
        }
        if (removed > 0) {
            log.info("Cleaned up {} stale in-memory sagas", removed);
        }
    }

    /**
     * Recovers all active sagas on application startup.
     * This ensures that sagas can continue processing after a restart.
     */
    @PostConstruct
    public void recoverActiveSagasOnStartup() {
        List<String> terminalStates = List.of(
            OrderStates.COMPLETED.name(),
            OrderStates.CANCELLED.name()
        );
        List<SagaInstance> activeSagas = sagaInstanceRepository
            .findByCurrentStateNotIn(terminalStates);
        
        log.info("Recovering {} active sagas on startup", activeSagas.size());
        int recovered = 0;
        for (SagaInstance instance : activeSagas) {
            if (recoverSagaIfNeeded(instance.getOrderId())) {
                recovered++;
            }
        }
        log.info("Successfully recovered {}/{} active sagas", recovered, activeSagas.size());
    }

    /**
     * Creates an order and starts the saga.
     */
    public String createOrder(CreateOrderRequest request) {
        String correlationId = UUID.randomUUID().toString();
        String orderId = UUID.randomUUID().toString();
        String paymentId = UUID.randomUUID().toString();
        String reservationId = UUID.randomUUID().toString();
        String shipmentId = UUID.randomUUID().toString();
        
        try {
            MDC.put("orderId", orderId);
            MDC.put("correlationId", correlationId);

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
                .createdAt(Instant.now())
                .lastUpdatedAt(Instant.now())
                .currentStep(SagaData.SagaStep.PAYMENT)
                .build();
        sagaDataMap.put(orderId, sagaData);

        // Create and start state machine (programmatic approach - AOT compatible)
        StateMachine<OrderStates, OrderEvents> sm = stateMachineFactory.create(orderId);
        sm.startReactively().block();
        stateMachines.put(orderId, sm);

        log.info("Starting saga for order: {}", orderId);

        // Trigger the saga start
        sendEventSafely(sm, OrderEvents.START_SAGA);

        // Persist saga state
        persistSagaState(orderId, OrderStates.PAYMENT_PENDING.name(), sagaData);

        // Send payment command
        sendPaymentCommand(sagaData);

        return orderId;
        } finally {
            MDC.clear();
        }
    }

    // ========== Command Senders ==========

    private void sendPaymentCommand(SagaData data) {
        String commandType = COMMAND_PROCESS_PAYMENT;
        ProcessPaymentCommand command = ProcessPaymentCommand.builder()
                .commandType(commandType)
                .paymentId(data.getPaymentId())
                .orderId(data.getOrderId())
                .customerId(data.getCustomerId())
                .amount(data.getTotalAmount())
                .build();

        sendCommandIfNotSent(data, commandType, command, PAYMENT_COMMAND_TOPIC, "Payment");
    }

    private void sendInventoryCommand(SagaData data) {
        String commandType = COMMAND_RESERVE_INVENTORY;
        ReserveInventoryCommand command = ReserveInventoryCommand.builder()
                .commandType(commandType)
                .reservationId(data.getReservationId())
                .orderId(data.getOrderId())
                .items(data.getItems())
                .build();

        sendCommandIfNotSent(data, commandType, command, INVENTORY_COMMAND_TOPIC, "Inventory");
    }

    private void sendShippingCommand(SagaData data) {
        String commandType = COMMAND_SCHEDULE_SHIPPING;
        ScheduleShippingCommand command = ScheduleShippingCommand.builder()
                .commandType(commandType)
                .shipmentId(data.getShipmentId())
                .orderId(data.getOrderId())
                .shippingAddress(data.getShippingAddress())
                .build();

        sendCommandIfNotSent(data, commandType, command, SHIPPING_COMMAND_TOPIC, "Shipping");
    }

    // ========== Compensation Commands ==========

    private void sendRefundPaymentCommand(SagaData data) {
        String commandType = COMMAND_REFUND_PAYMENT;
        RefundPaymentCommand command = RefundPaymentCommand.builder()
                .commandType(commandType)
                .paymentId(data.getPaymentId())
                .orderId(data.getOrderId())
                .build();

        sendCommandIfNotSent(data, commandType, command, PAYMENT_COMMAND_TOPIC, "Refund payment");
    }

    private void sendReleaseInventoryCommand(SagaData data) {
        String commandType = COMMAND_RELEASE_INVENTORY;
        ReleaseInventoryCommand command = ReleaseInventoryCommand.builder()
                .commandType(commandType)
                .reservationId(data.getReservationId())
                .orderId(data.getOrderId())
                .build();

        sendCommandIfNotSent(data, commandType, command, INVENTORY_COMMAND_TOPIC, "Release inventory");
    }

    private void sendCancelShippingCommand(SagaData data) {
        String commandType = COMMAND_CANCEL_SHIPPING;
        CancelShippingCommand command = CancelShippingCommand.builder()
                .commandType(commandType)
                .shipmentId(data.getShipmentId())
                .orderId(data.getOrderId())
                .build();

        sendCommandIfNotSent(data, commandType, command, SHIPPING_COMMAND_TOPIC, "Cancel shipping");
    }

    // ========== Reply Handlers ==========

    @KafkaListener(topics = PAYMENT_REPLY_TOPIC, groupId = "order-orchestrator")
    public void handlePaymentReply(String message) {
        String correlationId = UUID.randomUUID().toString();
        try {
            SagaReply reply = objectMapper.readValue(message, SagaReply.class);
            String orderId = reply.getOrderId();
            if (orderId != null) {
                MDC.put("orderId", orderId);
            }
            MDC.put("correlationId", correlationId);
            
            if (reply instanceof PaymentCompletedReply successReply) {
                handlePaymentSuccess(successReply);
            } else if (reply instanceof PaymentFailedReply failedReply) {
                handlePaymentFailure(failedReply);
            } else if (reply instanceof PaymentRefundedReply refundReply) {
                handlePaymentRefunded(refundReply);
            }
        } catch (JsonProcessingException e) {
            log.error("JSON parsing failed for payment reply: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error processing payment reply: {}", e.getMessage(), e);
        } finally {
            MDC.clear();
        }
    }

    private void handlePaymentSuccess(PaymentCompletedReply reply) {
        String orderId = reply.getOrderId();
        if (orderId == null) {
            log.error("Payment reply missing orderId");
            return;
        }
        try {
            MDC.put("orderId", orderId);
            log.info("Payment completed for order: {}", orderId);

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        if (sm == null || data == null) {
            log.warn("State machine or saga data not found for order: {}, attempting recovery", orderId);
            if (recoverSagaIfNeeded(orderId)) {
                sm = stateMachines.get(orderId);
                data = sagaDataMap.get(orderId);
            }
        }

        if (sm != null && data != null) {
            sendEventSafely(sm, OrderEvents.PAYMENT_SUCCESS);
            data.setCurrentStep(SagaData.SagaStep.INVENTORY);
            data.setPaymentCompleted(true);
            touchSaga(data);
            persistSagaState(orderId, OrderStates.INVENTORY_PENDING.name(), data);
            sendInventoryCommand(data);
        } else {
            log.error("Failed to recover saga for order: {}", orderId);
        }
        } finally {
            MDC.clear();
        }
    }

    private void handlePaymentFailure(PaymentFailedReply reply) {
        String orderId = reply.getOrderId();
        if (orderId == null) {
            log.error("Payment failed reply missing orderId");
            return;
        }
        try {
            MDC.put("orderId", orderId);
            log.error("Payment failed for order: {}. Reason: {}", orderId, reply.getReason());

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        if (sm == null) {
            log.warn("State machine not found for failed payment order: {}, attempting recovery", orderId);
            recoverSagaIfNeeded(orderId);
            sm = stateMachines.get(orderId);
        }
        
        if (sm != null) {
            sendEventSafely(sm, OrderEvents.PAYMENT_FAILED);
            orderService.failOrder(orderId, "Payment failed: " + reply.getReason());
            deleteSagaInstance(orderId);
            cleanup(orderId);
        } else {
            log.error("Failed to recover saga for failed payment order: {}", orderId);
            orderService.failOrder(orderId, "Payment failed: " + reply.getReason());
        }
        } finally {
            MDC.clear();
        }
    }

    @KafkaListener(topics = INVENTORY_REPLY_TOPIC, groupId = "order-orchestrator")
    public void handleInventoryReply(String message) {
        String correlationId = UUID.randomUUID().toString();
        try {
            SagaReply reply = objectMapper.readValue(message, SagaReply.class);
            String orderId = reply.getOrderId();
            if (orderId != null) {
                MDC.put("orderId", orderId);
            }
            MDC.put("correlationId", correlationId);
            
            if (reply instanceof InventoryReservedReply successReply) {
                handleInventorySuccess(successReply);
            } else if (reply instanceof InventoryFailedReply failedReply) {
                handleInventoryFailure(failedReply);
            } else if (reply instanceof InventoryReleasedReply releaseReply) {
                handleInventoryReleased(releaseReply);
            }
        } catch (JsonProcessingException e) {
            log.error("JSON parsing failed for inventory reply: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error processing inventory reply: {}", e.getMessage(), e);
        } finally {
            MDC.clear();
        }
    }

    private void handleInventorySuccess(InventoryReservedReply reply) {
        String orderId = reply.getOrderId();
        if (orderId == null) {
            log.error("Inventory reply missing orderId");
            return;
        }
        try {
            MDC.put("orderId", orderId);
            log.info("Inventory reserved for order: {}", orderId);

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        if (sm == null || data == null) {
            log.warn("State machine or saga data not found for order: {}, attempting recovery", orderId);
            if (recoverSagaIfNeeded(orderId)) {
                sm = stateMachines.get(orderId);
                data = sagaDataMap.get(orderId);
            }
        }

        if (sm != null && data != null) {
            sendEventSafely(sm, OrderEvents.INVENTORY_RESERVED);
            data.setCurrentStep(SagaData.SagaStep.SHIPPING);
            data.setInventoryReserved(true);
            touchSaga(data);
            persistSagaState(orderId, OrderStates.SHIPPING_PENDING.name(), data);
            sendShippingCommand(data);
        } else {
            log.error("Failed to recover saga for order: {}", orderId);
        }
        } finally {
            MDC.clear();
        }
    }

    private void handleInventoryFailure(InventoryFailedReply reply) {
        String orderId = reply.getOrderId();
        try {
            MDC.put("orderId", orderId);
            log.error("Inventory reservation failed for order: {}. Reason: {}", orderId, reply.getReason());

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        if (sm != null && data != null) {
            sendEventSafely(sm, OrderEvents.INVENTORY_FAILED);
            data.setCurrentStep(SagaData.SagaStep.COMPENSATING);
            orderService.failOrder(orderId, "Inventory reservation failed: " + reply.getReason());

            // Compensate: refund payment if completed
            int expectedCompensations = 0;
            if (data.isPaymentCompleted()) {
                sendRefundPaymentCommand(data);
                expectedCompensations++;
            }

            data.setExpectedCompensations(expectedCompensations);
            touchSaga(data);
            persistSagaState(orderId, OrderStates.COMPENSATING.name(), data);

            // If no compensations needed, complete immediately
            if (expectedCompensations == 0) {
                completeCompensation(orderId);
            }
        }
        } finally {
            MDC.clear();
        }
    }

    @KafkaListener(topics = SHIPPING_REPLY_TOPIC, groupId = "order-orchestrator")
    public void handleShippingReply(String message) {
        String correlationId = UUID.randomUUID().toString();
        try {
            SagaReply reply = objectMapper.readValue(message, SagaReply.class);
            String orderId = reply.getOrderId();
            if (orderId != null) {
                MDC.put("orderId", orderId);
            }
            MDC.put("correlationId", correlationId);
            
            if (reply instanceof ShippingScheduledReply successReply) {
                handleShippingSuccess(successReply);
            } else if (reply instanceof ShippingFailedReply failedReply) {
                handleShippingFailure(failedReply);
            } else if (reply instanceof ShippingCancelledReply cancelReply) {
                handleShippingCancelled(cancelReply);
            }
        } catch (JsonProcessingException e) {
            log.error("JSON parsing failed for shipping reply: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error processing shipping reply: {}", e.getMessage(), e);
        } finally {
            MDC.clear();
        }
    }

    private void handleShippingSuccess(ShippingScheduledReply reply) {
        String orderId = reply.getOrderId();
        try {
            MDC.put("orderId", orderId);
            log.info("Shipping scheduled for order: {}. Order completed!", orderId);

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        if (sm != null) {
            sendEventSafely(sm, OrderEvents.SHIPPING_SCHEDULED);
            orderService.completeOrder(orderId);
            deleteSagaInstance(orderId);
            cleanup(orderId);
        }
        } finally {
            MDC.clear();
        }
    }

    private void handleShippingFailure(ShippingFailedReply reply) {
        String orderId = reply.getOrderId();
        try {
            MDC.put("orderId", orderId);
            log.error("Shipping failed for order: {}. Reason: {}", orderId, reply.getReason());

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        if (sm != null && data != null) {
            sendEventSafely(sm, OrderEvents.SHIPPING_FAILED);
            data.setCurrentStep(SagaData.SagaStep.COMPENSATING);
            orderService.failOrder(orderId, "Shipping failed: " + reply.getReason());

            // Compensate: release inventory and refund payment
            int expectedCompensations = 0;
            if (data.isInventoryReserved()) {
                sendReleaseInventoryCommand(data);
                expectedCompensations++;
            }
            if (data.isPaymentCompleted()) {
                sendRefundPaymentCommand(data);
                expectedCompensations++;
            }

            data.setExpectedCompensations(expectedCompensations);
            touchSaga(data);
            persistSagaState(orderId, OrderStates.COMPENSATING.name(), data);

            // If no compensations needed, complete immediately
            if (expectedCompensations == 0) {
                completeCompensation(orderId);
            }
        }
        } finally {
            MDC.clear();
        }
    }

    // ========== Compensation Reply Handlers ==========

    private void handlePaymentRefunded(PaymentRefundedReply reply) {
        String orderId = reply.getOrderId();
        try {
            MDC.put("orderId", orderId);
            log.info("Payment refunded for order: {}, success: {}", orderId, reply.isSuccess());

        SagaData data = sagaDataMap.get(orderId);
        if (data != null && data.getCurrentStep() == SagaData.SagaStep.COMPENSATING) {
            data.setPaymentRefunded(true);
            data.setCompletedCompensations(data.getCompletedCompensations() + 1);
            touchSaga(data);
            checkCompensationComplete(orderId, data);
        }
        } finally {
            MDC.clear();
        }
    }

    private void handleInventoryReleased(InventoryReleasedReply reply) {
        String orderId = reply.getOrderId();
        try {
            MDC.put("orderId", orderId);
            log.info("Inventory released for order: {}, success: {}", orderId, reply.isSuccess());

        SagaData data = sagaDataMap.get(orderId);
        if (data != null && data.getCurrentStep() == SagaData.SagaStep.COMPENSATING) {
            data.setInventoryReleased(true);
            data.setCompletedCompensations(data.getCompletedCompensations() + 1);
            touchSaga(data);
            checkCompensationComplete(orderId, data);
        }
        } finally {
            MDC.clear();
        }
    }

    private void handleShippingCancelled(ShippingCancelledReply reply) {
        String orderId = reply.getOrderId();
        try {
            MDC.put("orderId", orderId);
            log.info("Shipping cancelled for order: {}, success: {}", orderId, reply.isSuccess());

        SagaData data = sagaDataMap.get(orderId);
        if (data != null && data.getCurrentStep() == SagaData.SagaStep.COMPENSATING) {
            data.setShippingCancelled(true);
            data.setCompletedCompensations(data.getCompletedCompensations() + 1);
            touchSaga(data);
            checkCompensationComplete(orderId, data);
        }
        } finally {
            MDC.clear();
        }
    }

    private void checkCompensationComplete(String orderId, SagaData data) {
        if (data.getCompletedCompensations() >= data.getExpectedCompensations()) {
            log.info("All compensations completed for order: {} ({}/{})",
                    orderId, data.getCompletedCompensations(), data.getExpectedCompensations());
            completeCompensation(orderId);
        } else {
            log.info("Waiting for more compensations for order: {} ({}/{})",
                    orderId, data.getCompletedCompensations(), data.getExpectedCompensations());
        }
    }

    private void completeCompensation(String orderId) {
        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        if (sm != null) {
            sendEventSafely(sm, OrderEvents.COMPENSATION_COMPLETE);
        }
        deleteSagaInstance(orderId);
        cleanup(orderId);
    }

    private void cleanup(String orderId) {
        StateMachine<OrderStates, OrderEvents> sm = stateMachines.remove(orderId);
        if (sm != null) {
            sm.stop();
        }
        sagaDataMap.remove(orderId);
    }

    // ========== Persistence Methods ==========

    private void persistSagaState(String orderId, String state, SagaData data) {
        try {
            touchSaga(data);
            String sagaDataJson = objectMapper.writeValueAsString(data);
            LocalDateTime now = LocalDateTime.now();

            SagaInstance instance = sagaInstanceRepository.findByOrderId(orderId)
                    .orElse(SagaInstance.builder()
                            .sagaId(UUID.randomUUID().toString())
                            .orderId(orderId)
                            .createdAt(now)
                            .build());

            instance.setCurrentState(state);
            instance.setSagaDataJson(sagaDataJson);
            instance.setUpdatedAt(now);

            sagaInstanceRepository.save(instance);
            log.debug("Persisted saga state for order {}: {}", orderId, state);
        } catch (DataAccessException e) {
            log.error("Database operation failed while persisting saga state for order {}: {}", orderId, e.getMessage());
        } catch (JsonProcessingException e) {
            log.error("JSON serialization failed while persisting saga state for order {}: {}", orderId, e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error persisting saga state for order {}: {}", orderId, e.getMessage(), e);
        }
    }

    private void deleteSagaInstance(String orderId) {
        try {
            sagaInstanceRepository.findByOrderId(orderId)
                    .ifPresent(sagaInstanceRepository::delete);
            log.debug("Deleted saga instance for order: {}", orderId);
        } catch (DataAccessException e) {
            log.error("Database operation failed while deleting saga instance for order {}: {}", orderId, e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error deleting saga instance for order {}: {}", orderId, e.getMessage(), e);
        }
    }

    // ========== Idempotency Methods ==========

    private boolean isCommandAlreadySent(String orderId, String commandType) {
        String commandId = orderId + ":" + commandType;
        return processedCommandRepository.existsByCommandIdAndStatus(commandId, COMMAND_STATUS_SENT);
    }

    private boolean registerCommandAttempt(String orderId, String commandType) {
        String commandId = orderId + ":" + commandType;
        try {
            return processedCommandRepository.findById(commandId)
                    .map(existing -> {
                        if (COMMAND_STATUS_SENT.equals(existing.getStatus())) {
                            return false;
                        }
                        existing.setStatus(COMMAND_STATUS_PENDING);
                        existing.setProcessedAt(LocalDateTime.now());
                        processedCommandRepository.save(existing);
                        return true;
                    })
                    .orElseGet(() -> {
                        ProcessedCommand pending = ProcessedCommand.builder()
                                .commandId(commandId)
                                .orderId(orderId)
                                .commandType(commandType)
                                .status(COMMAND_STATUS_PENDING)
                                .processedAt(LocalDateTime.now())
                                .build();
                        processedCommandRepository.save(pending);
                        return true;
                    });
        } catch (DataAccessException e) {
            log.error("Database operation failed while registering command attempt for order {}, type {}: {}",
                    orderId, commandType, e.getMessage());
            return true;
        } catch (Exception e) {
            log.error("Unexpected error registering command attempt for order {}, type {}: {}",
                    orderId, commandType, e.getMessage(), e);
            return true;
        }
    }

    private void markCommandSent(String orderId, String commandType) {
        markCommandStatus(orderId, commandType, COMMAND_STATUS_SENT);
    }

    private void markCommandStatus(String orderId, String commandType, String status) {
        try {
            String commandId = orderId + ":" + commandType;
            ProcessedCommand command = processedCommandRepository.findById(commandId)
                    .orElse(ProcessedCommand.builder()
                            .commandId(commandId)
                            .orderId(orderId)
                            .commandType(commandType)
                            .build());
            command.setStatus(status);
            command.setProcessedAt(LocalDateTime.now());
            processedCommandRepository.save(command);
        } catch (DataAccessException e) {
            log.error("Database operation failed while marking command status {} for order {}, type {}: {}",
                    status, orderId, commandType, e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error marking command status {} for order {}, type {}: {}",
                    status, orderId, commandType, e.getMessage(), e);
        }
    }

    // ========== Timeout Handling ==========

    /**
     * Handles saga timeout by triggering compensation for any completed steps.
     * Called by SagaTimeoutScheduler when a saga is stuck.
     */
    public void handleTimeout(String orderId) {
        try {
            MDC.put("orderId", orderId);
            log.warn("Handling timeout for order: {}", orderId);

        // Try to get saga data from memory or persistence
        SagaData data = sagaDataMap.get(orderId);
        if (data == null) {
            // Try to recover from persistence
            data = recoverSagaData(orderId);
        }

        if (data == null) {
            log.warn("No saga data found for timed out order: {}", orderId);
            return;
        }

        // Already compensating? Skip
        if (data.getCurrentStep() == SagaData.SagaStep.COMPENSATING) {
            log.info("Order {} already in compensation, skipping timeout handling", orderId);
            return;
        }

        // Trigger compensation
        data.setCurrentStep(SagaData.SagaStep.COMPENSATING);
        orderService.failOrder(orderId, "Saga timeout - compensation triggered");

        int expectedCompensations = 0;
        if (data.isShippingScheduled()) {
            sendCancelShippingCommand(data);
            expectedCompensations++;
        }
        if (data.isInventoryReserved()) {
            sendReleaseInventoryCommand(data);
            expectedCompensations++;
        }
        if (data.isPaymentCompleted()) {
            sendRefundPaymentCommand(data);
            expectedCompensations++;
        }

        data.setExpectedCompensations(expectedCompensations);
        touchSaga(data);
        sagaDataMap.put(orderId, data);
        persistSagaState(orderId, OrderStates.COMPENSATING.name(), data);

        if (expectedCompensations == 0) {
            completeCompensation(orderId);
        }
        } finally {
            MDC.clear();
        }
    }

    private SagaData recoverSagaData(String orderId) {
        try {
            return sagaInstanceRepository.findByOrderId(orderId)
                    .map(instance -> {
                        try {
                            return objectMapper.readValue(instance.getSagaDataJson(), SagaData.class);
                        } catch (JsonProcessingException e) {
                            log.error("JSON deserialization failed for saga data for order {}: {}", orderId, e.getMessage());
                            return null;
                        } catch (Exception e) {
                            log.error("Unexpected error deserializing saga data for order {}: {}", orderId, e.getMessage(), e);
                            return null;
                        }
                    })
                    .orElse(null);
        } catch (DataAccessException e) {
            log.error("Database operation failed while recovering saga data for order {}: {}", orderId, e.getMessage());
            return null;
        } catch (Exception e) {
            log.error("Unexpected error recovering saga data for order {}: {}", orderId, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Recovers saga state machine and data from persistence.
     * Called when saga is not found in memory (e.g., after restart or memory cleanup).
     */
    private boolean recoverSagaIfNeeded(String orderId) {
        try {
            SagaInstance instance = sagaInstanceRepository.findByOrderId(orderId)
                    .orElse(null);
            
            if (instance == null) {
                log.warn("No saga instance found in persistence for order: {}", orderId);
                return false;
            }

            // Recover saga data
            SagaData data = recoverSagaData(orderId);
            if (data == null) {
                log.error("Failed to recover saga data for order: {}", orderId);
                return false;
            }

            // Recreate state machine
            StateMachine<OrderStates, OrderEvents> sm = stateMachineFactory.create(orderId);
            sm.startReactively().block();
            
            // Restore state machine to current state
            try {
                OrderStates targetState = OrderStates.valueOf(instance.getCurrentState());
                // State machine will be in the correct state after transitions
                // We need to send events to bring it to the current state
                restoreStateMachineToState(sm, targetState);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid state {} for order {}: {}", instance.getCurrentState(), orderId, e.getMessage());
            } catch (IllegalStateException e) {
                log.warn("State machine error while restoring state {} for order {}: {}", 
                        instance.getCurrentState(), orderId, e.getMessage());
            } catch (Exception e) {
                log.warn("Unexpected error restoring state machine to state {} for order {}: {}", 
                        instance.getCurrentState(), orderId, e.getMessage(), e);
            }

            // Restore to memory
            stateMachines.put(orderId, sm);
            sagaDataMap.put(orderId, data);
            
            log.info("Successfully recovered saga for order: {}, state: {}", orderId, instance.getCurrentState());
            return true;
        } catch (DataAccessException e) {
            log.error("Database operation failed while recovering saga for order {}: {}", orderId, e.getMessage());
            return false;
        } catch (Exception e) {
            log.error("Unexpected error recovering saga for order {}: {}", orderId, e.getMessage(), e);
            return false;
        }
    }

    private void restoreStateMachineToState(StateMachine<OrderStates, OrderEvents> sm, OrderStates targetState) {
        // State machine starts in CREATED, we need to transition to target state
        // This is a simplified approach - in production you might want more sophisticated state restoration
        if (targetState == OrderStates.PAYMENT_PENDING) {
            sm.sendEvent(OrderEvents.START_SAGA);
        } else if (targetState == OrderStates.INVENTORY_PENDING) {
            sm.sendEvent(OrderEvents.START_SAGA);
            sm.sendEvent(OrderEvents.PAYMENT_SUCCESS);
        } else if (targetState == OrderStates.SHIPPING_PENDING) {
            sm.sendEvent(OrderEvents.START_SAGA);
            sm.sendEvent(OrderEvents.PAYMENT_SUCCESS);
            sm.sendEvent(OrderEvents.INVENTORY_RESERVED);
        } else if (targetState == OrderStates.COMPENSATING) {
            // For compensating state, we don't restore transitions as it's already in error state
            log.debug("Saga in compensating state, not restoring transitions");
        }
    }

    private void sendEventSafely(StateMachine<OrderStates, OrderEvents> sm, OrderEvents event) {
        synchronized (sm) {
            if (sm.getState() != null) {
                OrderStates fromState = sm.getState().getId();
                if (sm.sendEvent(event)) {
                    OrderStates toState = sm.getState().getId();
                    stateTransitionCounter.increment();
                    log.debug("State transition: {} -> {} (event: {})", fromState, toState, event);
                } else {
                    log.warn("State transition failed: {} (event: {})", fromState, event);
                }
            } else {
                log.warn("State machine has no state for event {}", event);
            }
        }
    }

    private void touchSaga(SagaData data) {
        Instant now = Instant.now();
        if (data.getCreatedAt() == null) {
            data.setCreatedAt(now);
        }
        data.setLastUpdatedAt(now);
    }

    /**
     * Sends a command to Kafka with comprehensive error handling.
     * Uses CompletableFuture callback to handle async results and failures.
     * On failure, the command is automatically enqueued to outbox for retry.
     * Note: Commands are sent via outbox pattern, so this method is primarily for direct sends if needed.
     */
    private void sendCommandSafely(String topic, String key, Object command, String orderId, String commandType) {
        try {
            CompletableFuture<?> future = kafkaTemplate.send(topic, key, command);
            
            future.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    handleKafkaSendFailure(topic, key, command, orderId, commandType, throwable);
                } else {
                    kafkaCommandSendSuccessCounter.increment();
                    markCommandSent(orderId, commandType);
                    log.debug("Successfully sent command {} for order {} to topic {}", commandType, orderId, topic);
                }
            });
            
        } catch (Exception e) {
            log.error("Exception while sending command {} for order {} to {}: {}",
                    commandType, orderId, topic, e.getMessage(), e);
            handleKafkaSendFailure(topic, key, command, orderId, commandType, e);
        }
    }
    
    /**
     * Handles Kafka send failures by logging, incrementing metrics, and enqueueing to outbox for retry.
     */
    private void handleKafkaSendFailure(String topic, String key, Object command, 
                                       String orderId, String commandType, Throwable throwable) {
        kafkaCommandSendFailureCounter.increment();
        
        log.error("Failed to send command {} for order {} to topic {}: {}",
                commandType, orderId, topic, throwable.getMessage(), throwable);
        
        // Enqueue to outbox for retry - this ensures the command will be retried
        try {
            enqueueOutboxCommand(orderId, commandType, command, topic);
            log.info("Command {} for order {} enqueued to outbox for retry", commandType, orderId);
        } catch (Exception e) {
            log.error("Failed to enqueue command {} for order {} to outbox: {}",
                    commandType, orderId, e.getMessage(), e);
            // Mark command as failed if we can't even enqueue it
            markCommandStatus(orderId, commandType, COMMAND_STATUS_SKIPPED);
        }
    }

    private void sendCommandIfNotSent(SagaData data,
                                      String commandType,
                                      Object command,
                                      String topic,
                                      String commandLabel) {
        if (!registerCommandAttempt(data.getOrderId(), commandType)) {
            log.info("{} command already sent for order: {}", commandLabel, data.getOrderId());
            return;
        }

        log.info("Sending {} command for order: {}", commandLabel, data.getOrderId());
        enqueueOutboxCommand(data.getOrderId(), commandType, command, topic);
    }

    public void retryPendingCommand(String orderId, String commandType) {
        commandRetryAttemptsCounter.increment();
        SagaData data = sagaDataMap.get(orderId);
        if (data == null) {
            data = recoverSagaData(orderId);
        }

        if (data == null) {
            log.warn("No saga data found for pending command retry: orderId={}, commandType={}", orderId, commandType);
            return;
        }

        if (isSagaTerminal(orderId)) {
            log.info("Skipping pending command retry for terminal saga: orderId={}, commandType={}",
                    orderId, commandType);
            commandRetrySkippedCounter.increment();
            markCommandStatus(orderId, commandType, COMMAND_STATUS_SKIPPED);
            return;
        }

        boolean outboxExists = outboxCommandRepository
                .findByOrderIdAndCommandTypeAndStatus(orderId, commandType, OUTBOX_STATUS_PENDING)
                .isPresent();
        if (outboxExists) {
            log.info("Pending outbox command already exists for orderId={}, commandType={}", orderId, commandType);
            return;
        }

        switch (commandType) {
            case COMMAND_PROCESS_PAYMENT -> sendPaymentCommand(data);
            case COMMAND_REFUND_PAYMENT -> sendRefundPaymentCommand(data);
            case COMMAND_RESERVE_INVENTORY -> sendInventoryCommand(data);
            case COMMAND_RELEASE_INVENTORY -> sendReleaseInventoryCommand(data);
            case COMMAND_SCHEDULE_SHIPPING -> sendShippingCommand(data);
            case COMMAND_CANCEL_SHIPPING -> sendCancelShippingCommand(data);
            default -> log.warn("Unknown command type for retry: {}", commandType);
        }
    }

    private boolean isSagaTerminal(String orderId) {
        return sagaInstanceRepository.findByOrderId(orderId)
                .map(instance -> {
                    String state = instance.getCurrentState();
                    return OrderStates.COMPLETED.name().equals(state)
                            || OrderStates.CANCELLED.name().equals(state);
                })
                .orElse(false);
    }

    public void markCommandSentForOutbox(String orderId, String commandType) {
        markCommandSent(orderId, commandType);
    }

    public void markCommandFailedForOutbox(String orderId, String commandType) {
        markCommandStatus(orderId, commandType, COMMAND_STATUS_SKIPPED);
    }

    private void enqueueOutboxCommand(String orderId, String commandType, Object command, String topic) {
        try {
            String payload = objectMapper.writeValueAsString(command);
            OutboxCommand outbox = OutboxCommand.builder()
                    .outboxId(UUID.randomUUID().toString())
                    .orderId(orderId)
                    .commandType(commandType)
                    .topic(topic)
                    .payloadJson(payload)
                    .status(OUTBOX_STATUS_PENDING)
                    .attempts(0)
                    .createdAt(LocalDateTime.now())
                    .build();
            outboxCommandRepository.save(outbox);
        } catch (JsonProcessingException e) {
            log.error("JSON serialization failed while enqueueing outbox command for order {}, type {}: {}",
                    orderId, commandType, e.getMessage());
            markCommandStatus(orderId, commandType, COMMAND_STATUS_SKIPPED);
        } catch (DataAccessException e) {
            log.error("Database operation failed while enqueueing outbox command for order {}, type {}: {}",
                    orderId, commandType, e.getMessage());
            markCommandStatus(orderId, commandType, COMMAND_STATUS_SKIPPED);
        } catch (Exception e) {
            log.error("Unexpected error enqueueing outbox command for order {}, type {}: {}",
                    orderId, commandType, e.getMessage(), e);
            markCommandStatus(orderId, commandType, COMMAND_STATUS_SKIPPED);
        }
    }

}
