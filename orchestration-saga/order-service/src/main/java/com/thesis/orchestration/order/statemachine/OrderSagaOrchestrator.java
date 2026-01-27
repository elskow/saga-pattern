package com.thesis.orchestration.order.statemachine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.*;
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
import com.thesis.orchestration.order.model.ProcessedReply;
import com.thesis.orchestration.order.model.SagaInstance;
import com.thesis.orchestration.order.repository.OutboxCommandRepository;
import com.thesis.orchestration.order.repository.ProcessedCommandRepository;
import com.thesis.orchestration.order.repository.ProcessedReplyRepository;
import com.thesis.orchestration.order.repository.SagaInstanceRepository;
import com.thesis.orchestration.order.service.OrderService;
import com.thesis.orchestration.order.service.ReplyProcessingService;
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
import org.springframework.transaction.annotation.Transactional;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Mono;
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
    private final ReplyProcessingService replyProcessingService;
    private final ObjectMapper objectMapper;
    private final SagaInstanceRepository sagaInstanceRepository;
    private final ProcessedCommandRepository processedCommandRepository;
    private final ProcessedReplyRepository processedReplyRepository;
    private final OutboxCommandRepository outboxCommandRepository;
    private final SagaOrchestratorProperties sagaProperties;
     private final Counter kafkaCommandSendSuccessCounter;
    private final Counter kafkaCommandSendFailureCounter;
    private final Counter commandRetryAttemptsCounter;
    private final Counter commandRetrySkippedCounter;
    private final Counter stateTransitionCounter;

    @Autowired
    public OrderSagaOrchestrator(OrderStateMachineFactory stateMachineFactory,
                                  KafkaTemplate<String, Object> kafkaTemplate,
                                  OrderService orderService,
                                  ReplyProcessingService replyProcessingService,
                                  ObjectMapper objectMapper,
                                  SagaInstanceRepository sagaInstanceRepository,
                                  ProcessedCommandRepository processedCommandRepository,
                                  ProcessedReplyRepository processedReplyRepository,
                                  MeterRegistry meterRegistry,
                                  SagaOrchestratorProperties sagaProperties,
                                  OutboxCommandRepository outboxCommandRepository) {
        this.stateMachineFactory = stateMachineFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.orderService = orderService;
        this.replyProcessingService = replyProcessingService;
        this.objectMapper = objectMapper;
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.processedCommandRepository = processedCommandRepository;
        this.processedReplyRepository = processedReplyRepository;
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
        meterRegistry.counter(
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

    // Topic names for replies
    private static final String PAYMENT_REPLY_TOPIC = "orchestration.payment.replies";
    private static final String INVENTORY_REPLY_TOPIC = "orchestration.inventory.replies";
    private static final String SHIPPING_REPLY_TOPIC = "orchestration.shipping.replies";

    // Store active state machines by orderId
    private final Map<String, StateMachine<OrderStates, OrderEvents>> stateMachines = new ConcurrentHashMap<>();

    // Store saga data by orderId
    private final Map<String, SagaData> sagaDataMap = new ConcurrentHashMap<>();

    // Store saga locks by orderId for thread safety when modifying SagaData
    private final Map<String, java.util.concurrent.locks.ReentrantLock> sagaLocks = new ConcurrentHashMap<>();

    /**
     * Gets or creates a lock for the given orderId.
     * Used to ensure thread-safe modifications to SagaData.
     */
    private java.util.concurrent.locks.ReentrantLock getSagaLock(String orderId) {
        return sagaLocks.computeIfAbsent(orderId, k -> new java.util.concurrent.locks.ReentrantLock());
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
                // Stop the state machine to release resources before removal
                StateMachine<OrderStates, OrderEvents> sm = entry.getValue();
                if (sm != null) {
                    try {
                        sm.stopReactively().block();
                    } catch (Exception e) {
                        log.warn("Error stopping state machine for order {}: {}", entry.getKey(), e.getMessage());
                    }
                }
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
     * This method creates the order and persists the saga state in a single transaction.
     * The state machine is created after the transaction commits to ensure consistency.
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

            // Build saga data
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

            // TRANSACTION BOUNDARY: Create order, persist saga state, and enqueue outbox command
            // All these operations must succeed together or fail together
            replyProcessingService.createOrderWithSagaState(
                    orderId,
                    request.getCustomerId(),
                    request.getTotalAmount(),
                    request.getShippingAddress(),
                    request.getItems(),
                    paymentId,
                    reservationId,
                    shipmentId,
                    sagaData
            );

            // POST-TRANSACTION: Create and start state machine (in-memory operation)
            // If this fails, the saga can be recovered from persistence on next reply
            StateMachine<OrderStates, OrderEvents> sm;
            try {
                sm = stateMachineFactory.create(orderId);
                sm.startReactively().block();
                stateMachines.put(orderId, sm);
                sagaDataMap.put(orderId, sagaData);
                
                log.info("Starting saga for order: {}", orderId);
                
                // Trigger the saga start
                sendEventSafely(sm, OrderEvents.START_SAGA);
            } catch (Exception e) {
                log.error("Failed to start state machine for order {}: {}. Saga will be recovered on next event.",
                        orderId, e.getMessage(), e);
                // Don't throw - the order and saga state are persisted, recovery will happen on next reply
            }

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
                processPaymentSuccessWithService(successReply);
            } else if (reply instanceof PaymentFailedReply failedReply) {
                processPaymentFailureWithService(failedReply);
            } else if (reply instanceof PaymentRefundedReply refundReply) {
                processPaymentRefundedWithService(refundReply);
            }
        } catch (JsonProcessingException e) {
            // Malformed message - retrying won't help, log and acknowledge
            log.error("JSON parsing failed for payment reply (message will be acknowledged): {}", e.getMessage());
        } catch (DataAccessException e) {
            // Database error - retrying may help
            log.error("Database error processing payment reply, will retry: {}", e.getMessage());
            throw new RuntimeException("Retryable database error", e);
        } catch (Exception e) {
            // Unexpected error - rethrow to trigger retry
            log.error("Unexpected error processing payment reply, will retry: {}", e.getMessage(), e);
            throw new RuntimeException("Retryable processing error", e);
        } finally {
            MDC.clear();
        }
    }

    /**
     * Processes payment success using the ReplyProcessingService for proper transaction management.
     */
    private void processPaymentSuccessWithService(PaymentCompletedReply reply) {
        String orderId = reply.getOrderId();
        if (orderId == null) {
            log.error("Payment reply missing orderId");
            return;
        }

        // Get or recover state machine and data
        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        if (sm == null || data == null) {
            log.warn("State machine or saga data not found for order: {}, attempting recovery", orderId);
            if (recoverSagaIfNeeded(orderId)) {
                sm = stateMachines.get(orderId);
                data = sagaDataMap.get(orderId);
            }
        }

        if (sm == null || data == null) {
            log.error("Failed to recover saga for order: {}", orderId);
            return;
        }

        // Process in transaction via service
        ReplyProcessingService.ReplyProcessingResult result = 
                replyProcessingService.processPaymentSuccess(reply, data, d -> {});

        if (!result.processed()) {
            return; // Skipped (duplicate or error)
        }

        // Post-transaction actions (state machine events, send commands)
        sendEventSafely(sm, OrderEvents.PAYMENT_SUCCESS);
        sagaDataMap.put(orderId, result.updatedSagaData());
        sendInventoryCommand(result.updatedSagaData());
    }

    /**
     * Processes payment failure using the ReplyProcessingService.
     */
    private void processPaymentFailureWithService(PaymentFailedReply reply) {
        String orderId = reply.getOrderId();
        if (orderId == null) {
            log.error("Payment failed reply missing orderId");
            return;
        }

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        if (sm == null) {
            log.warn("State machine not found for failed payment order: {}, attempting recovery", orderId);
            recoverSagaIfNeeded(orderId);
            sm = stateMachines.get(orderId);
        }

        // Process in transaction via service
        ReplyProcessingService.ReplyProcessingResult result = 
                replyProcessingService.processPaymentFailure(reply);

        if (!result.processed()) {
            return; // Skipped (duplicate or error)
        }

        // Post-transaction actions
        if (sm != null) {
            sendEventSafely(sm, OrderEvents.PAYMENT_FAILED);
        }
        cleanup(orderId);
    }

    /**
     * Processes payment refund using the ReplyProcessingService.
     * Acquires lock before reading saga data to ensure thread safety.
     */
    private void processPaymentRefundedWithService(PaymentRefundedReply reply) {
        String orderId = reply.getOrderId();
        java.util.concurrent.locks.ReentrantLock lock = getSagaLock(orderId);
        
        lock.lock();
        try {
            // Read saga data under lock to ensure consistency
            SagaData data = sagaDataMap.get(orderId);

            // Process in transaction via service (lock is passed for internal use)
            ReplyProcessingService.ReplyProcessingResult result = 
                    replyProcessingService.processPaymentRefunded(reply, data, lock);

            if (!result.processed()) {
                return; // Skipped (duplicate or error)
            }

            // Update local cache and check compensation
            if (result.updatedSagaData() != null) {
                sagaDataMap.put(orderId, result.updatedSagaData());
                checkCompensationComplete(orderId, result.updatedSagaData());
            }
        } finally {
            lock.unlock();
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
                processInventorySuccessWithService(successReply);
            } else if (reply instanceof InventoryFailedReply failedReply) {
                processInventoryFailureWithService(failedReply);
            } else if (reply instanceof InventoryReleasedReply releaseReply) {
                processInventoryReleasedWithService(releaseReply);
            }
        } catch (JsonProcessingException e) {
            // Malformed message - retrying won't help, log and acknowledge
            log.error("JSON parsing failed for inventory reply (message will be acknowledged): {}", e.getMessage());
        } catch (DataAccessException e) {
            // Database error - retrying may help
            log.error("Database error processing inventory reply, will retry: {}", e.getMessage());
            throw new RuntimeException("Retryable database error", e);
        } catch (Exception e) {
            // Unexpected error - rethrow to trigger retry
            log.error("Unexpected error processing inventory reply, will retry: {}", e.getMessage(), e);
            throw new RuntimeException("Retryable processing error", e);
        } finally {
            MDC.clear();
        }
    }

    /**
     * Processes inventory success using the ReplyProcessingService.
     */
    private void processInventorySuccessWithService(InventoryReservedReply reply) {
        String orderId = reply.getOrderId();
        if (orderId == null) {
            log.error("Inventory reply missing orderId");
            return;
        }

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        if (sm == null || data == null) {
            log.warn("State machine or saga data not found for order: {}, attempting recovery", orderId);
            if (recoverSagaIfNeeded(orderId)) {
                sm = stateMachines.get(orderId);
                data = sagaDataMap.get(orderId);
            }
        }

        if (sm == null || data == null) {
            log.error("Failed to recover saga for order: {}", orderId);
            return;
        }

        // Process in transaction via service
        ReplyProcessingService.ReplyProcessingResult result = 
                replyProcessingService.processInventorySuccess(reply, data);

        if (!result.processed()) {
            return;
        }

        // Post-transaction actions
        sendEventSafely(sm, OrderEvents.INVENTORY_RESERVED);
        sagaDataMap.put(orderId, result.updatedSagaData());
        sendShippingCommand(result.updatedSagaData());
    }

    /**
     * Processes inventory failure using the ReplyProcessingService.
     * Compensation commands are now enqueued atomically within the service transaction.
     */
    private void processInventoryFailureWithService(InventoryFailedReply reply) {
        String orderId = reply.getOrderId();
        if (orderId == null) {
            log.error("Inventory failed reply missing orderId");
            return;
        }

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        // Process in transaction via service (compensation commands are enqueued atomically)
        ReplyProcessingService.ReplyProcessingResult result = 
                replyProcessingService.processInventoryFailure(reply, data);

        if (!result.processed()) {
            return;
        }

        // Post-transaction actions (state machine event only - compensation commands already enqueued)
        if (sm != null) {
            sendEventSafely(sm, OrderEvents.INVENTORY_FAILED);
        }

        if (result.updatedSagaData() != null) {
            sagaDataMap.put(orderId, result.updatedSagaData());
            
            // Check if no compensations needed (immediate completion)
            if (result.updatedSagaData().getExpectedCompensations() == 0) {
                completeCompensation(orderId);
            }
        }
    }

    /**
     * Processes inventory released using the ReplyProcessingService.
     * Acquires lock before reading saga data to ensure thread safety.
     */
    private void processInventoryReleasedWithService(InventoryReleasedReply reply) {
        String orderId = reply.getOrderId();
        java.util.concurrent.locks.ReentrantLock lock = getSagaLock(orderId);

        lock.lock();
        try {
            // Read saga data under lock to ensure consistency
            SagaData data = sagaDataMap.get(orderId);

            // Process in transaction via service (lock is passed for internal use)
            ReplyProcessingService.ReplyProcessingResult result = 
                    replyProcessingService.processInventoryReleased(reply, data, lock);

            if (!result.processed()) {
                return;
            }

            // Update local cache and check compensation
            if (result.updatedSagaData() != null) {
                sagaDataMap.put(orderId, result.updatedSagaData());
                checkCompensationComplete(orderId, result.updatedSagaData());
            }
        } finally {
            lock.unlock();
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
                processShippingSuccessWithService(successReply);
            } else if (reply instanceof ShippingFailedReply failedReply) {
                processShippingFailureWithService(failedReply);
            } else if (reply instanceof ShippingCancelledReply cancelReply) {
                processShippingCancelledWithService(cancelReply);
            }
        } catch (JsonProcessingException e) {
            // Malformed message - retrying won't help, log and acknowledge
            log.error("JSON parsing failed for shipping reply (message will be acknowledged): {}", e.getMessage());
        } catch (DataAccessException e) {
            // Database error - retrying may help
            log.error("Database error processing shipping reply, will retry: {}", e.getMessage());
            throw new RuntimeException("Retryable database error", e);
        } catch (Exception e) {
            // Unexpected error - rethrow to trigger retry
            log.error("Unexpected error processing shipping reply, will retry: {}", e.getMessage(), e);
            throw new RuntimeException("Retryable processing error", e);
        } finally {
            MDC.clear();
        }
    }

    /**
     * Processes shipping success using the ReplyProcessingService.
     */
    private void processShippingSuccessWithService(ShippingScheduledReply reply) {
        String orderId = reply.getOrderId();
        if (orderId == null) {
            log.error("Shipping success reply missing orderId");
            return;
        }

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);

        // Process in transaction via service
        ReplyProcessingService.ReplyProcessingResult result = 
                replyProcessingService.processShippingSuccess(reply);

        if (!result.processed()) {
            return;
        }

        // Post-transaction actions
        if (sm != null) {
            sendEventSafely(sm, OrderEvents.SHIPPING_SCHEDULED);
        }
        cleanup(orderId);
    }

    /**
     * Processes shipping failure using the ReplyProcessingService.
     * Compensation commands are now enqueued atomically within the service transaction.
     */
    private void processShippingFailureWithService(ShippingFailedReply reply) {
        String orderId = reply.getOrderId();

        StateMachine<OrderStates, OrderEvents> sm = stateMachines.get(orderId);
        SagaData data = sagaDataMap.get(orderId);

        // Process in transaction via service (compensation commands are enqueued atomically)
        ReplyProcessingService.ReplyProcessingResult result = 
                replyProcessingService.processShippingFailure(reply, data);

        if (!result.processed()) {
            return;
        }

        // Post-transaction actions (state machine event only - compensation commands already enqueued)
        if (sm != null) {
            sendEventSafely(sm, OrderEvents.SHIPPING_FAILED);
        }

        if (result.updatedSagaData() != null) {
            sagaDataMap.put(orderId, result.updatedSagaData());
            
            // Check if no compensations needed (immediate completion)
            if (result.updatedSagaData().getExpectedCompensations() == 0) {
                completeCompensation(orderId);
            }
        }
    }

    /**
     * Processes shipping cancelled using the ReplyProcessingService.
     * Acquires lock before reading saga data to ensure thread safety.
     */
    private void processShippingCancelledWithService(ShippingCancelledReply reply) {
        String orderId = reply.getOrderId();
        java.util.concurrent.locks.ReentrantLock lock = getSagaLock(orderId);

        lock.lock();
        try {
            // Read saga data under lock to ensure consistency
            SagaData data = sagaDataMap.get(orderId);

            // Process in transaction via service (lock is passed for internal use)
            ReplyProcessingService.ReplyProcessingResult result = 
                    replyProcessingService.processShippingCancelled(reply, data, lock);

            if (!result.processed()) {
                return;
            }

            // Update local cache and check compensation
            if (result.updatedSagaData() != null) {
                sagaDataMap.put(orderId, result.updatedSagaData());
                checkCompensationComplete(orderId, result.updatedSagaData());
            }
        } finally {
            lock.unlock();
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
        sagaLocks.remove(orderId);
    }

    // ========== Persistence Methods ==========

    /**
     * Persists saga state to database.
     * Note: This method is NOT transactional when called internally.
     * For proper transaction management, use replyProcessingService.persistSagaStateTransactional().
     * This method is kept for legacy/internal use only.
     * @deprecated Use replyProcessingService.persistSagaStateTransactional() for external calls.
     */
    protected void persistSagaState(String orderId, String state, SagaData data) {
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

    // Reply idempotency is now handled by ReplyProcessingService

    private boolean isCommandAlreadySent(String orderId, String commandType) {
        String commandId = orderId + ":" + commandType;
        return processedCommandRepository.existsByCommandIdAndStatus(commandId, COMMAND_STATUS_SENT);
    }

    /**
     * Atomically registers a command attempt using INSERT...ON CONFLICT pattern.
     * This prevents race conditions where multiple threads could insert duplicate records.
     * 
     * @return true if command should be sent (new record inserted or existing was not SENT)
     *         false if command was already SENT (skip sending)
     */
    private boolean registerCommandAttempt(String orderId, String commandType) {
        String commandId = orderId + ":" + commandType;
        try {
            // First check if command is already SENT
            if (processedCommandRepository.existsByCommandIdAndStatus(commandId, COMMAND_STATUS_SENT)) {
                log.debug("Command {} for order {} already sent, skipping", commandType, orderId);
                return false;
            }

            // Atomic insert - if record already exists (due to race), this returns 0
            int inserted = processedCommandRepository.insertIfNotExists(
                    commandId, orderId, commandType, COMMAND_STATUS_PENDING);

            if (inserted == 0) {
                // Record already existed - check if it's SENT (another thread may have sent it)
                if (processedCommandRepository.existsByCommandIdAndStatus(commandId, COMMAND_STATUS_SENT)) {
                    log.debug("Command {} for order {} already sent (detected after insert attempt), skipping", 
                            commandType, orderId);
                    return false;
                }
                // Record exists but not SENT - we can proceed (another thread registered it as PENDING)
                log.debug("Command {} for order {} already registered as PENDING, proceeding", commandType, orderId);
            }
            return true;
        } catch (DataAccessException e) {
            log.error("Database operation failed while registering command attempt for order {}, type {}: {}",
                    orderId, commandType, e.getMessage());
            // On error, default to allowing the attempt (better to risk duplicate than miss)
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
     * Uses locking to prevent duplicate compensation commands when multiple threads
     * try to handle the same timeout concurrently.
     * 
     * The actual compensation logic is handled atomically in a single transaction
     * via ReplyProcessingService.handleTimeoutTransactional() to ensure order failure
     * and compensation command enqueue happen together or not at all.
     */
    public void handleTimeout(String orderId) {
        // Acquire lock to prevent race condition where multiple timeout handlers
        // could trigger duplicate compensation commands for the same order
        java.util.concurrent.locks.ReentrantLock lock = getSagaLock(orderId);
        lock.lock();
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

            // Already compensating? Skip (double-check under lock)
            if (data.getCurrentStep() == SagaData.SagaStep.COMPENSATING) {
                log.info("Order {} already in compensation, skipping timeout handling", orderId);
                return;
            }

            // Handle timeout atomically in a single transaction
            // This ensures order failure and compensation command enqueue happen together
            ReplyProcessingService.TimeoutHandlingResult result = 
                    replyProcessingService.handleTimeoutTransactional(orderId, data);

            if (!result.handled()) {
                log.debug("Timeout handling skipped for order: {}", orderId);
                return;
            }

            // Update in-memory state
            if (result.updatedSagaData() != null) {
                sagaDataMap.put(orderId, result.updatedSagaData());
            }

            // Complete compensation if no compensations needed
            if (result.updatedSagaData() != null && 
                    result.updatedSagaData().getExpectedCompensations() == 0) {
                completeCompensation(orderId);
            }
        } finally {
            lock.unlock();
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
     * Uses pessimistic locking to prevent multiple instances from recovering the same saga.
     * Also uses in-memory lock to prevent race conditions when multiple threads try to recover the same saga.
     */
    private boolean recoverSagaIfNeeded(String orderId) {
        // First, acquire in-memory lock to prevent race condition where multiple threads
        // could create duplicate state machines for the same orderId
        java.util.concurrent.locks.ReentrantLock lock = getSagaLock(orderId);
        lock.lock();
        try {
            // Double-check if already recovered while waiting for lock
            if (stateMachines.containsKey(orderId) && sagaDataMap.containsKey(orderId)) {
                log.debug("Saga already recovered for order: {} (found after acquiring lock)", orderId);
                return true;
            }

            // Use pessimistic lock to prevent concurrent recovery by multiple instances
            SagaInstance instance = sagaInstanceRepository.findByOrderIdForUpdate(orderId)
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
        } finally {
            lock.unlock();
        }
    }

    private void restoreStateMachineToState(StateMachine<OrderStates, OrderEvents> sm, OrderStates targetState) {
        // State machine starts in CREATED, we need to transition to target state
        // Using reactive API with block() to ensure events are processed synchronously
        if (targetState == OrderStates.PAYMENT_PENDING) {
            sm.sendEvent(Mono.just(MessageBuilder.withPayload(OrderEvents.START_SAGA).build())).blockFirst();
        } else if (targetState == OrderStates.INVENTORY_PENDING) {
            sm.sendEvent(Mono.just(MessageBuilder.withPayload(OrderEvents.START_SAGA).build())).blockFirst();
            sm.sendEvent(Mono.just(MessageBuilder.withPayload(OrderEvents.PAYMENT_SUCCESS).build())).blockFirst();
        } else if (targetState == OrderStates.SHIPPING_PENDING) {
            sm.sendEvent(Mono.just(MessageBuilder.withPayload(OrderEvents.START_SAGA).build())).blockFirst();
            sm.sendEvent(Mono.just(MessageBuilder.withPayload(OrderEvents.PAYMENT_SUCCESS).build())).blockFirst();
            sm.sendEvent(Mono.just(MessageBuilder.withPayload(OrderEvents.INVENTORY_RESERVED).build())).blockFirst();
        } else if (targetState == OrderStates.COMPENSATING) {
            // For compensating state, we transition through failure path
            // Note: PAYMENT_FAILED goes to CANCELLED, not COMPENSATING
            // INVENTORY_FAILED or SHIPPING_FAILED go to COMPENSATING
            sm.sendEvent(Mono.just(MessageBuilder.withPayload(OrderEvents.START_SAGA).build())).blockFirst();
            sm.sendEvent(Mono.just(MessageBuilder.withPayload(OrderEvents.PAYMENT_SUCCESS).build())).blockFirst();
            // Now we're in INVENTORY_PENDING, INVENTORY_FAILED will put us in COMPENSATING
            sm.sendEvent(Mono.just(MessageBuilder.withPayload(OrderEvents.INVENTORY_FAILED).build())).blockFirst();
            log.debug("Saga restored to compensating state");
        } else if (targetState == OrderStates.COMPLETED) {
            // Already terminal, no need to restore
            log.debug("Saga already completed, skipping state restoration");
        } else if (targetState == OrderStates.CANCELLED) {
            // Already terminal, no need to restore
            log.debug("Saga already cancelled, skipping state restoration");
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

    /**
     * Enqueues a command to the outbox for reliable delivery.
     * Uses the ReplyProcessingService for proper transactional handling.
     */
    private void enqueueOutboxCommand(String orderId, String commandType, Object command, String topic) {
        boolean success = replyProcessingService.enqueueOutboxCommandTransactional(
                orderId, commandType, command, topic);
        if (!success) {
            log.warn("Failed to enqueue outbox command {} for order {}, marking as skipped", 
                    commandType, orderId);
            markCommandStatus(orderId, commandType, COMMAND_STATUS_SKIPPED);
        }
    }

}
