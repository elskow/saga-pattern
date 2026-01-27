package com.thesis.orchestration.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.replies.*;
import com.thesis.orchestration.order.model.OutboxCommand;
import com.thesis.orchestration.order.model.ProcessedReply;
import com.thesis.orchestration.order.model.SagaInstance;
import com.thesis.orchestration.order.repository.OutboxCommandRepository;
import com.thesis.orchestration.order.repository.ProcessedReplyRepository;
import com.thesis.orchestration.order.repository.SagaInstanceRepository;
import com.thesis.orchestration.order.statemachine.OrderEvents;
import com.thesis.orchestration.order.statemachine.OrderStates;
import com.thesis.orchestration.order.statemachine.SagaData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.statemachine.StateMachine;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import com.thesis.common.command.ProcessPaymentCommand;
import com.thesis.common.events.OrderCreatedEvent;

/**
 * Service for processing saga replies with proper @Transactional support.
 * 
 * This service is separated from OrderSagaOrchestrator to fix the Spring AOP proxy
 * self-invocation issue. When @KafkaListener methods call @Transactional methods
 * within the same class, Spring's proxy doesn't intercept the call because it's
 * an internal method invocation.
 * 
 * By extracting the transactional logic to a separate service that is injected,
 * Spring can properly proxy the calls and manage transactions.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ReplyProcessingService {

    private final ProcessedReplyRepository processedReplyRepository;
    private final SagaInstanceRepository sagaInstanceRepository;
    private final OutboxCommandRepository outboxCommandRepository;
    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    // Command constants
    private static final String COMMAND_PROCESS_PAYMENT = "PROCESS_PAYMENT";
    private static final String PAYMENT_COMMAND_TOPIC = "orchestration.payment.commands";
    private static final String OUTBOX_STATUS_PENDING = "PENDING";

    /**
     * Result of processing a reply, containing the next action to take.
     */
    public record ReplyProcessingResult(
            boolean processed,
            boolean sagaRecovered,
            String nextState,
            SagaData updatedSagaData
    ) {
        public static ReplyProcessingResult skipped() {
            return new ReplyProcessingResult(false, false, null, null);
        }

        public static ReplyProcessingResult success(String nextState, SagaData data) {
            return new ReplyProcessingResult(true, false, nextState, data);
        }

        public static ReplyProcessingResult successWithRecovery(String nextState, SagaData data) {
            return new ReplyProcessingResult(true, true, nextState, data);
        }
    }

    // ========== Atomic Idempotency ==========

    /**
     * Atomically tries to mark a reply as processed.
     * Returns true if the reply was already processed (duplicate), false if this is the first time.
     * Uses INSERT...ON CONFLICT DO NOTHING for atomic idempotency.
     */
    @Transactional
    public boolean tryMarkReplyProcessedAtomically(String orderId, String replyType) {
        String replyId = orderId + ":" + replyType;
        try {
            int inserted = processedReplyRepository.insertIfNotExists(
                    replyId, orderId, replyType, LocalDateTime.now());
            if (inserted == 0) {
                log.debug("Reply {} already processed for order {} (atomic check)", replyType, orderId);
                return true; // Already processed
            }
            log.debug("Marked reply {} as processed for order {} (atomic insert)", replyType, orderId);
            return false; // First time processing
        } catch (DataAccessException e) {
            log.error("Database error during atomic idempotency check for order {}, reply {}: {}",
                    orderId, replyType, e.getMessage());
            // On error, fall back to check-then-mark approach
            return processedReplyRepository.existsByReplyId(replyId);
        }
    }

    // ========== Payment Reply Processing ==========

    @Transactional
    public ReplyProcessingResult processPaymentSuccess(
            PaymentCompletedReply reply,
            SagaData sagaData,
            Consumer<SagaData> persistCallback) {
        
        String orderId = reply.getOrderId();
        String replyType = "PAYMENT_SUCCESS";

        // Atomic idempotency check
        if (tryMarkReplyProcessedAtomically(orderId, replyType)) {
            log.info("Reply {} already processed for order {}, skipping", replyType, orderId);
            return ReplyProcessingResult.skipped();
        }

        log.info("Processing payment success for order: {}", orderId);

        if (sagaData == null) {
            log.warn("No saga data for payment success, order: {}", orderId);
            return ReplyProcessingResult.skipped();
        }

        // Update saga state
        sagaData.setCurrentStep(SagaData.SagaStep.INVENTORY);
        sagaData.setPaymentCompleted(true);
        touchSaga(sagaData);

        // Persist state change
        persistSagaState(orderId, OrderStates.INVENTORY_PENDING.name(), sagaData);

        return ReplyProcessingResult.success(OrderStates.INVENTORY_PENDING.name(), sagaData);
    }

    @Transactional
    public ReplyProcessingResult processPaymentFailure(PaymentFailedReply reply) {
        String orderId = reply.getOrderId();
        String replyType = "PAYMENT_FAILED";

        // Atomic idempotency check
        if (tryMarkReplyProcessedAtomically(orderId, replyType)) {
            log.info("Reply {} already processed for order {}, skipping", replyType, orderId);
            return ReplyProcessingResult.skipped();
        }

        log.error("Processing payment failure for order: {}. Reason: {}", orderId, reply.getReason());

        // Fail the order
        orderService.failOrder(orderId, "Payment failed: " + reply.getReason());

        // Delete saga instance (terminal state)
        deleteSagaInstance(orderId);

        return ReplyProcessingResult.success(OrderStates.CANCELLED.name(), null);
    }

    @Transactional
    public ReplyProcessingResult processPaymentRefunded(
            PaymentRefundedReply reply,
            SagaData sagaData,
            ReentrantLock sagaLock) {
        
        String orderId = reply.getOrderId();
        String replyType = "PAYMENT_REFUNDED";

        // Atomic idempotency check
        if (tryMarkReplyProcessedAtomically(orderId, replyType)) {
            log.info("Reply {} already processed for order {}, skipping", replyType, orderId);
            return ReplyProcessingResult.skipped();
        }

        log.info("Processing payment refund for order: {}, success: {}", orderId, reply.isSuccess());

        if (sagaData == null || sagaData.getCurrentStep() != SagaData.SagaStep.COMPENSATING) {
            log.warn("Saga not in compensating state for refund, order: {}", orderId);
            return ReplyProcessingResult.skipped();
        }

        // Note: Lock is already held by caller (processPaymentRefundedWithService)
        // Create a defensive copy with updated values to avoid race conditions
        SagaData updatedData = SagaData.builder()
                .orderId(sagaData.getOrderId())
                .customerId(sagaData.getCustomerId())
                .paymentId(sagaData.getPaymentId())
                .reservationId(sagaData.getReservationId())
                .shipmentId(sagaData.getShipmentId())
                .totalAmount(sagaData.getTotalAmount())
                .shippingAddress(sagaData.getShippingAddress())
                .items(sagaData.getItems())
                .createdAt(sagaData.getCreatedAt())
                .lastUpdatedAt(java.time.Instant.now())
                .currentStep(sagaData.getCurrentStep())
                .paymentCompleted(sagaData.isPaymentCompleted())
                .inventoryReserved(sagaData.isInventoryReserved())
                .shippingScheduled(sagaData.isShippingScheduled())
                .paymentRefunded(true)  // Updated
                .inventoryReleased(sagaData.isInventoryReleased())
                .shippingCancelled(sagaData.isShippingCancelled())
                .expectedCompensations(sagaData.getExpectedCompensations())
                .completedCompensations(sagaData.getCompletedCompensations() + 1)  // Updated
                .build();
        
        return new ReplyProcessingResult(true, false, OrderStates.COMPENSATING.name(), updatedData);
    }

    // ========== Inventory Reply Processing ==========

    @Transactional
    public ReplyProcessingResult processInventorySuccess(
            InventoryReservedReply reply,
            SagaData sagaData) {
        
        String orderId = reply.getOrderId();
        String replyType = "INVENTORY_RESERVED";

        // Atomic idempotency check
        if (tryMarkReplyProcessedAtomically(orderId, replyType)) {
            log.info("Reply {} already processed for order {}, skipping", replyType, orderId);
            return ReplyProcessingResult.skipped();
        }

        log.info("Processing inventory reserved for order: {}", orderId);

        if (sagaData == null) {
            log.warn("No saga data for inventory success, order: {}", orderId);
            return ReplyProcessingResult.skipped();
        }

        // Update saga state
        sagaData.setCurrentStep(SagaData.SagaStep.SHIPPING);
        sagaData.setInventoryReserved(true);
        touchSaga(sagaData);

        // Persist state change
        persistSagaState(orderId, OrderStates.SHIPPING_PENDING.name(), sagaData);

        return ReplyProcessingResult.success(OrderStates.SHIPPING_PENDING.name(), sagaData);
    }

    /**
     * Processes inventory failure atomically:
     * 1. Marks reply as processed (idempotency)
     * 2. Fails the order
     * 3. Persists saga state as COMPENSATING
     * 4. Enqueues compensation commands to outbox
     * 
     * All operations happen in a single transaction to ensure consistency.
     */
    @Transactional
    public ReplyProcessingResult processInventoryFailure(
            InventoryFailedReply reply,
            SagaData sagaData) {
        
        String orderId = reply.getOrderId();
        String replyType = "INVENTORY_FAILED";

        // Atomic idempotency check
        if (tryMarkReplyProcessedAtomically(orderId, replyType)) {
            log.info("Reply {} already processed for order {}, skipping", replyType, orderId);
            return ReplyProcessingResult.skipped();
        }

        log.error("Processing inventory failure for order: {}. Reason: {}", orderId, reply.getReason());

        if (sagaData == null) {
            log.warn("No saga data for inventory failure, order: {}", orderId);
            orderService.failOrder(orderId, "Inventory reservation failed: " + reply.getReason());
            return ReplyProcessingResult.skipped();
        }

        // Update saga state to compensating
        sagaData.setCurrentStep(SagaData.SagaStep.COMPENSATING);
        orderService.failOrder(orderId, "Inventory reservation failed: " + reply.getReason());

        // Calculate expected compensations and enqueue commands atomically
        int expectedCompensations = 0;
        if (sagaData.isPaymentCompleted()) {
            CompensationCommand cmd = createRefundPaymentCompensation(sagaData);
            if (cmd != null) {
                enqueueCompensationCommand(orderId, cmd);
                expectedCompensations++;
            }
        }

        sagaData.setExpectedCompensations(expectedCompensations);
        touchSaga(sagaData);
        persistSagaState(orderId, OrderStates.COMPENSATING.name(), sagaData);

        log.info("Inventory failure handled atomically for order: {}, expectedCompensations: {}", 
                orderId, expectedCompensations);

        return ReplyProcessingResult.success(OrderStates.COMPENSATING.name(), sagaData);
    }

    @Transactional
    public ReplyProcessingResult processInventoryReleased(
            InventoryReleasedReply reply,
            SagaData sagaData,
            ReentrantLock sagaLock) {
        
        String orderId = reply.getOrderId();
        String replyType = "INVENTORY_RELEASED";

        // Atomic idempotency check
        if (tryMarkReplyProcessedAtomically(orderId, replyType)) {
            log.info("Reply {} already processed for order {}, skipping", replyType, orderId);
            return ReplyProcessingResult.skipped();
        }

        log.info("Processing inventory released for order: {}, success: {}", orderId, reply.isSuccess());

        if (sagaData == null || sagaData.getCurrentStep() != SagaData.SagaStep.COMPENSATING) {
            log.warn("Saga not in compensating state for inventory release, order: {}", orderId);
            return ReplyProcessingResult.skipped();
        }

        // Note: Lock is already held by caller (processInventoryReleasedWithService)
        // Create a defensive copy with updated values to avoid race conditions
        SagaData updatedData = SagaData.builder()
                .orderId(sagaData.getOrderId())
                .customerId(sagaData.getCustomerId())
                .paymentId(sagaData.getPaymentId())
                .reservationId(sagaData.getReservationId())
                .shipmentId(sagaData.getShipmentId())
                .totalAmount(sagaData.getTotalAmount())
                .shippingAddress(sagaData.getShippingAddress())
                .items(sagaData.getItems())
                .createdAt(sagaData.getCreatedAt())
                .lastUpdatedAt(java.time.Instant.now())
                .currentStep(sagaData.getCurrentStep())
                .paymentCompleted(sagaData.isPaymentCompleted())
                .inventoryReserved(sagaData.isInventoryReserved())
                .shippingScheduled(sagaData.isShippingScheduled())
                .paymentRefunded(sagaData.isPaymentRefunded())
                .inventoryReleased(true)  // Updated
                .shippingCancelled(sagaData.isShippingCancelled())
                .expectedCompensations(sagaData.getExpectedCompensations())
                .completedCompensations(sagaData.getCompletedCompensations() + 1)  // Updated
                .build();
        
        return new ReplyProcessingResult(true, false, OrderStates.COMPENSATING.name(), updatedData);
    }

    // ========== Shipping Reply Processing ==========

    @Transactional
    public ReplyProcessingResult processShippingSuccess(ShippingScheduledReply reply) {
        String orderId = reply.getOrderId();
        String replyType = "SHIPPING_SCHEDULED";

        // Atomic idempotency check
        if (tryMarkReplyProcessedAtomically(orderId, replyType)) {
            log.info("Reply {} already processed for order {}, skipping", replyType, orderId);
            return ReplyProcessingResult.skipped();
        }

        log.info("Processing shipping scheduled for order: {}. Order completed!", orderId);

        // Complete the order
        orderService.completeOrder(orderId);

        // Delete saga instance (terminal state)
        deleteSagaInstance(orderId);

        return ReplyProcessingResult.success(OrderStates.COMPLETED.name(), null);
    }

    /**
     * Processes shipping failure atomically:
     * 1. Marks reply as processed (idempotency)
     * 2. Fails the order
     * 3. Persists saga state as COMPENSATING
     * 4. Enqueues compensation commands to outbox
     * 
     * All operations happen in a single transaction to ensure consistency.
     */
    @Transactional
    public ReplyProcessingResult processShippingFailure(
            ShippingFailedReply reply,
            SagaData sagaData) {
        
        String orderId = reply.getOrderId();
        String replyType = "SHIPPING_FAILED";

        // Atomic idempotency check
        if (tryMarkReplyProcessedAtomically(orderId, replyType)) {
            log.info("Reply {} already processed for order {}, skipping", replyType, orderId);
            return ReplyProcessingResult.skipped();
        }

        log.error("Processing shipping failure for order: {}. Reason: {}", orderId, reply.getReason());

        if (sagaData == null) {
            log.warn("No saga data for shipping failure, order: {}", orderId);
            orderService.failOrder(orderId, "Shipping failed: " + reply.getReason());
            return ReplyProcessingResult.skipped();
        }

        // Update saga state to compensating
        sagaData.setCurrentStep(SagaData.SagaStep.COMPENSATING);
        orderService.failOrder(orderId, "Shipping failed: " + reply.getReason());

        // Calculate expected compensations and enqueue commands atomically
        int expectedCompensations = 0;
        if (sagaData.isInventoryReserved()) {
            CompensationCommand cmd = createReleaseInventoryCompensation(sagaData);
            if (cmd != null) {
                enqueueCompensationCommand(orderId, cmd);
                expectedCompensations++;
            }
        }
        if (sagaData.isPaymentCompleted()) {
            CompensationCommand cmd = createRefundPaymentCompensation(sagaData);
            if (cmd != null) {
                enqueueCompensationCommand(orderId, cmd);
                expectedCompensations++;
            }
        }

        sagaData.setExpectedCompensations(expectedCompensations);
        touchSaga(sagaData);
        persistSagaState(orderId, OrderStates.COMPENSATING.name(), sagaData);

        log.info("Shipping failure handled atomically for order: {}, expectedCompensations: {}", 
                orderId, expectedCompensations);

        return ReplyProcessingResult.success(OrderStates.COMPENSATING.name(), sagaData);
    }

    @Transactional
    public ReplyProcessingResult processShippingCancelled(
            ShippingCancelledReply reply,
            SagaData sagaData,
            ReentrantLock sagaLock) {
        
        String orderId = reply.getOrderId();
        String replyType = "SHIPPING_CANCELLED";

        // Atomic idempotency check
        if (tryMarkReplyProcessedAtomically(orderId, replyType)) {
            log.info("Reply {} already processed for order {}, skipping", replyType, orderId);
            return ReplyProcessingResult.skipped();
        }

        log.info("Processing shipping cancelled for order: {}, success: {}", orderId, reply.isSuccess());

        if (sagaData == null || sagaData.getCurrentStep() != SagaData.SagaStep.COMPENSATING) {
            log.warn("Saga not in compensating state for shipping cancel, order: {}", orderId);
            return ReplyProcessingResult.skipped();
        }

        // Note: Lock is already held by caller (processShippingCancelledWithService)
        // Create a defensive copy with updated values to avoid race conditions
        SagaData updatedData = SagaData.builder()
                .orderId(sagaData.getOrderId())
                .customerId(sagaData.getCustomerId())
                .paymentId(sagaData.getPaymentId())
                .reservationId(sagaData.getReservationId())
                .shipmentId(sagaData.getShipmentId())
                .totalAmount(sagaData.getTotalAmount())
                .shippingAddress(sagaData.getShippingAddress())
                .items(sagaData.getItems())
                .createdAt(sagaData.getCreatedAt())
                .lastUpdatedAt(java.time.Instant.now())
                .currentStep(sagaData.getCurrentStep())
                .paymentCompleted(sagaData.isPaymentCompleted())
                .inventoryReserved(sagaData.isInventoryReserved())
                .shippingScheduled(sagaData.isShippingScheduled())
                .paymentRefunded(sagaData.isPaymentRefunded())
                .inventoryReleased(sagaData.isInventoryReleased())
                .shippingCancelled(true)  // Updated
                .expectedCompensations(sagaData.getExpectedCompensations())
                .completedCompensations(sagaData.getCompletedCompensations() + 1)  // Updated
                .build();
        
        return new ReplyProcessingResult(true, false, OrderStates.COMPENSATING.name(), updatedData);
    }

    // ========== Helper Methods ==========

    private void touchSaga(SagaData data) {
        java.time.Instant now = java.time.Instant.now();
        if (data.getCreatedAt() == null) {
            data.setCreatedAt(now);
        }
        data.setLastUpdatedAt(now);
    }

    private void persistSagaState(String orderId, String state, SagaData data) {
        try {
            touchSaga(data);
            String sagaDataJson = objectMapper.writeValueAsString(data);
            LocalDateTime now = LocalDateTime.now();

            SagaInstance instance = sagaInstanceRepository.findByOrderId(orderId)
                    .orElse(SagaInstance.builder()
                            .sagaId(java.util.UUID.randomUUID().toString())
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
        }
    }

    /**
     * Public method to persist saga state with proper transaction management.
     * Called from OrderSagaOrchestrator for timeout handling and other scenarios.
     */
    @Transactional
    public void persistSagaStateTransactional(String orderId, String state, SagaData data) {
        persistSagaState(orderId, state, data);
    }

    private void deleteSagaInstance(String orderId) {
        try {
            sagaInstanceRepository.findByOrderId(orderId)
                    .ifPresent(sagaInstanceRepository::delete);
            log.debug("Deleted saga instance for order: {}", orderId);
        } catch (DataAccessException e) {
            log.error("Database operation failed while deleting saga instance for order {}: {}", orderId, e.getMessage());
        }
    }

    /**
     * Recovers saga data from persistence.
     */
    @Transactional(readOnly = true)
    public SagaData recoverSagaData(String orderId) {
        try {
            return sagaInstanceRepository.findByOrderId(orderId)
                    .map(instance -> {
                        try {
                            return objectMapper.readValue(instance.getSagaDataJson(), SagaData.class);
                        } catch (JsonProcessingException e) {
                            log.error("JSON deserialization failed for saga data for order {}: {}", orderId, e.getMessage());
                            return null;
                        }
                    })
                    .orElse(null);
        } catch (DataAccessException e) {
            log.error("Database operation failed while recovering saga data for order {}: {}", orderId, e.getMessage());
            return null;
        }
    }

    // ========== Order Creation (Transaction Boundary) ==========

    /**
     * Creates an order with saga state in a single transaction.
     * This ensures that order creation, saga instance persistence, and outbox command
     * are all committed together or all fail together.
     */
    @Transactional
    public void createOrderWithSagaState(
            String orderId,
            String customerId,
            BigDecimal totalAmount,
            String shippingAddress,
            List<OrderCreatedEvent.OrderItemEvent> items,
            String paymentId,
            String reservationId,
            String shipmentId,
            SagaData sagaData) {

        // 1. Create and persist order entity
        orderService.createOrder(
                orderId,
                customerId,
                totalAmount,
                shippingAddress,
                items,
                paymentId,
                reservationId,
                shipmentId
        );

        // 2. Persist saga state
        persistSagaState(orderId, OrderStates.PAYMENT_PENDING.name(), sagaData);

        // 3. Enqueue payment command to outbox (for reliable delivery)
        enqueuePaymentCommand(sagaData);

        log.info("Created order and saga state for orderId: {}", orderId);
    }

    /**
     * Enqueues the initial payment command to the outbox.
     */
    private void enqueuePaymentCommand(SagaData data) {
        try {
            ProcessPaymentCommand command = ProcessPaymentCommand.builder()
                    .commandType(COMMAND_PROCESS_PAYMENT)
                    .paymentId(data.getPaymentId())
                    .orderId(data.getOrderId())
                    .customerId(data.getCustomerId())
                    .amount(data.getTotalAmount())
                    .build();

            String payload = objectMapper.writeValueAsString(command);
            OutboxCommand outbox = OutboxCommand.builder()
                    .outboxId(UUID.randomUUID().toString())
                    .orderId(data.getOrderId())
                    .commandType(COMMAND_PROCESS_PAYMENT)
                    .topic(PAYMENT_COMMAND_TOPIC)
                    .payloadJson(payload)
                    .status(OUTBOX_STATUS_PENDING)
                    .attempts(0)
                    .createdAt(LocalDateTime.now())
                    .build();
            outboxCommandRepository.save(outbox);
            log.debug("Enqueued payment command to outbox for order: {}", data.getOrderId());
        } catch (JsonProcessingException e) {
            log.error("JSON serialization failed while enqueueing payment command for order {}: {}",
                    data.getOrderId(), e.getMessage());
            throw new RuntimeException("Failed to enqueue payment command", e);
        }
    }

    // ========== Timeout Compensation (Atomic Transaction) ==========

    /**
     * Result of timeout handling containing compensation commands to send.
     */
    public record TimeoutHandlingResult(
            boolean handled,
            SagaData updatedSagaData,
            List<CompensationCommand> compensationCommands
    ) {
        public static TimeoutHandlingResult skipped() {
            return new TimeoutHandlingResult(false, null, List.of());
        }

        public static TimeoutHandlingResult success(SagaData data, List<CompensationCommand> commands) {
            return new TimeoutHandlingResult(true, data, commands);
        }
    }

    /**
     * Represents a compensation command to be sent.
     */
    public record CompensationCommand(
            String commandType,
            String topic,
            Object command
    ) {}

    /**
     * Handles saga timeout by atomically:
     * 1. Failing the order
     * 2. Persisting saga state to COMPENSATING
     * 3. Enqueueing compensation commands to outbox
     * 
     * This ensures all operations succeed or fail together, preventing scenarios
     * where the order is marked as CANCELLED but compensation commands are never sent.
     * 
     * @param orderId the order ID
     * @param sagaData current saga data
     * @return result containing updated saga data and compensation commands
     */
    @Transactional
    public TimeoutHandlingResult handleTimeoutTransactional(String orderId, SagaData sagaData) {
        if (sagaData == null) {
            log.warn("No saga data for timeout handling, order: {}", orderId);
            return TimeoutHandlingResult.skipped();
        }

        // Already compensating? Skip
        if (sagaData.getCurrentStep() == SagaData.SagaStep.COMPENSATING) {
            log.info("Order {} already in compensation, skipping timeout handling", orderId);
            return TimeoutHandlingResult.skipped();
        }

        log.warn("Processing timeout for order: {} in transaction", orderId);

        // 1. Update saga state to COMPENSATING
        sagaData.setCurrentStep(SagaData.SagaStep.COMPENSATING);

        // 2. Fail the order
        orderService.failOrder(orderId, "Saga timeout - compensation triggered");

        // 3. Calculate and create compensation commands
        List<CompensationCommand> compensationCommands = new java.util.ArrayList<>();
        int expectedCompensations = 0;

        if (sagaData.isShippingScheduled()) {
            CompensationCommand cmd = createCancelShippingCompensation(sagaData);
            if (cmd != null) {
                compensationCommands.add(cmd);
                enqueueCompensationCommand(orderId, cmd);
                expectedCompensations++;
            }
        }
        if (sagaData.isInventoryReserved()) {
            CompensationCommand cmd = createReleaseInventoryCompensation(sagaData);
            if (cmd != null) {
                compensationCommands.add(cmd);
                enqueueCompensationCommand(orderId, cmd);
                expectedCompensations++;
            }
        }
        if (sagaData.isPaymentCompleted()) {
            CompensationCommand cmd = createRefundPaymentCompensation(sagaData);
            if (cmd != null) {
                compensationCommands.add(cmd);
                enqueueCompensationCommand(orderId, cmd);
                expectedCompensations++;
            }
        }

        // 4. Update saga data with expected compensations
        sagaData.setExpectedCompensations(expectedCompensations);
        touchSaga(sagaData);

        // 5. Persist saga state
        persistSagaState(orderId, OrderStates.COMPENSATING.name(), sagaData);

        log.info("Timeout handled for order: {}, expected compensations: {}", orderId, expectedCompensations);

        return TimeoutHandlingResult.success(sagaData, compensationCommands);
    }

    private static final String COMMAND_CANCEL_SHIPPING = "CANCEL_SHIPPING";
    private static final String COMMAND_RELEASE_INVENTORY = "RELEASE_INVENTORY";
    private static final String COMMAND_REFUND_PAYMENT = "REFUND_PAYMENT";
    private static final String INVENTORY_COMMAND_TOPIC = "orchestration.inventory.commands";
    private static final String SHIPPING_COMMAND_TOPIC = "orchestration.shipping.commands";

    private CompensationCommand createCancelShippingCompensation(SagaData data) {
        try {
            com.thesis.common.command.CancelShippingCommand command = 
                    com.thesis.common.command.CancelShippingCommand.builder()
                            .commandType(COMMAND_CANCEL_SHIPPING)
                            .shipmentId(data.getShipmentId())
                            .orderId(data.getOrderId())
                            .build();
            return new CompensationCommand(COMMAND_CANCEL_SHIPPING, SHIPPING_COMMAND_TOPIC, command);
        } catch (Exception e) {
            log.error("Failed to create cancel shipping compensation for order {}: {}", 
                    data.getOrderId(), e.getMessage());
            return null;
        }
    }

    private CompensationCommand createReleaseInventoryCompensation(SagaData data) {
        try {
            com.thesis.common.command.ReleaseInventoryCommand command = 
                    com.thesis.common.command.ReleaseInventoryCommand.builder()
                            .commandType(COMMAND_RELEASE_INVENTORY)
                            .reservationId(data.getReservationId())
                            .orderId(data.getOrderId())
                            .build();
            return new CompensationCommand(COMMAND_RELEASE_INVENTORY, INVENTORY_COMMAND_TOPIC, command);
        } catch (Exception e) {
            log.error("Failed to create release inventory compensation for order {}: {}", 
                    data.getOrderId(), e.getMessage());
            return null;
        }
    }

    private CompensationCommand createRefundPaymentCompensation(SagaData data) {
        try {
            com.thesis.common.command.RefundPaymentCommand command = 
                    com.thesis.common.command.RefundPaymentCommand.builder()
                            .commandType(COMMAND_REFUND_PAYMENT)
                            .paymentId(data.getPaymentId())
                            .orderId(data.getOrderId())
                            .build();
            return new CompensationCommand(COMMAND_REFUND_PAYMENT, PAYMENT_COMMAND_TOPIC, command);
        } catch (Exception e) {
            log.error("Failed to create refund payment compensation for order {}: {}", 
                    data.getOrderId(), e.getMessage());
            return null;
        }
    }

    /**
     * Enqueues a compensation command to the outbox for reliable delivery.
     */
    private void enqueueCompensationCommand(String orderId, CompensationCommand cmd) {
        try {
            String payload = objectMapper.writeValueAsString(cmd.command());
            OutboxCommand outbox = OutboxCommand.builder()
                    .outboxId(UUID.randomUUID().toString())
                    .orderId(orderId)
                    .commandType(cmd.commandType())
                    .topic(cmd.topic())
                    .payloadJson(payload)
                    .status(OUTBOX_STATUS_PENDING)
                    .attempts(0)
                    .createdAt(LocalDateTime.now())
                    .build();
            outboxCommandRepository.save(outbox);
            log.debug("Enqueued compensation command {} to outbox for order: {}", cmd.commandType(), orderId);
        } catch (JsonProcessingException e) {
            log.error("JSON serialization failed while enqueueing compensation command {} for order {}: {}",
                    cmd.commandType(), orderId, e.getMessage());
            throw new RuntimeException("Failed to enqueue compensation command: " + cmd.commandType(), e);
        }
    }

    // ========== Generic Outbox Command Enqueueing ==========

    /**
     * Atomically enqueues a command to the outbox for reliable delivery.
     * This method is transactional to ensure consistency.
     * 
     * @param orderId the order ID
     * @param commandType the type of command
     * @param command the command object to serialize
     * @param topic the Kafka topic to send to
     * @return true if enqueued successfully, false otherwise
     */
    @Transactional
    public boolean enqueueOutboxCommandTransactional(String orderId, String commandType, Object command, String topic) {
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
            log.debug("Enqueued outbox command {} for order: {}", commandType, orderId);
            return true;
        } catch (JsonProcessingException e) {
            log.error("JSON serialization failed while enqueueing outbox command {} for order {}: {}",
                    commandType, orderId, e.getMessage());
            return false;
        } catch (DataAccessException e) {
            log.error("Database error while enqueueing outbox command {} for order {}: {}",
                    commandType, orderId, e.getMessage());
            return false;
        }
    }
}
