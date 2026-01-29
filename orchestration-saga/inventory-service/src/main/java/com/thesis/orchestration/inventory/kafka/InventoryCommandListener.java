package com.thesis.orchestration.inventory.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.ReleaseInventoryCommand;
import com.thesis.common.command.ReserveInventoryCommand;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.common.replies.InventoryFailedReply;
import com.thesis.common.replies.InventoryReleasedReply;
import com.thesis.common.replies.InventoryReservedReply;
import com.thesis.orchestration.inventory.service.InventoryService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Kafka-based command listener for inventory service.
 * Handles inventory reservation and release commands from the saga orchestrator.
 * <p>
 * Uses InventoryService for database operations to ensure proper @Transactional support
 * (avoiding Spring AOP self-invocation issues).
 */
@Component
@Slf4j
public class InventoryCommandListener {

    private static final String COMMAND_TOPIC = "orchestration.inventory.commands";
    private static final String REPLY_TOPIC = "orchestration.inventory.replies";

    private final InventoryService inventoryService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Counter reservationSuccessCounter;
    private final Counter reservationFailedCounter;
    private final Timer reservationTimer;
    private final Counter compensationInventoryCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    private final Counter kafkaReplySendFailureCounter;
    private final SagaMetricsHelper metricsHelper;
    private final Validator validator;

    public InventoryCommandListener(InventoryService inventoryService,
                                    KafkaTemplate<String, Object> kafkaTemplate,
                                    ObjectMapper objectMapper,
                                    MeterRegistry meterRegistry,
                                    Validator validator) {
        this.inventoryService = inventoryService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.reservationSuccessCounter = meterRegistry.counter(SagaMetrics.INVENTORY_RESERVATIONS_SUCCESS,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.reservationFailedCounter = meterRegistry.counter(SagaMetrics.INVENTORY_RESERVATIONS_FAILED,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.reservationTimer = meterRegistry.timer(SagaMetrics.STEP_INVENTORY_DURATION,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.compensationInventoryCounter = meterRegistry.counter(SagaMetrics.COMPENSATIONS_INVENTORY,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.compensationDurationTimer = meterRegistry.timer(SagaMetrics.COMPENSATION_DURATION,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
            SagaMetrics.TAG_STEP, SagaMetrics.STEP_INVENTORY);
        this.sagaStepsExecutedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_EXECUTED,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
            SagaMetrics.TAG_STEP, SagaMetrics.STEP_INVENTORY);
        this.sagaStepsFailedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_FAILED,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
            SagaMetrics.TAG_STEP, SagaMetrics.STEP_INVENTORY);
        this.kafkaReplySendFailureCounter = meterRegistry.counter(
            SagaMetrics.SAGA_MESSAGES_TOTAL,
            SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
            SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_SENT,
            SagaMetrics.TAG_MESSAGE_TYPE, SagaMetrics.TYPE_REPLY,
            SagaMetrics.TAG_OUTCOME, SagaMetrics.OUTCOME_FAILURE
        );
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_ORCHESTRATION);
    }

    @KafkaListener(topics = COMMAND_TOPIC, groupId = "inventory-service")
    public void handleCommand(String message) {
        String correlationId = UUID.randomUUID().toString();
        try {
            JsonNode node = objectMapper.readTree(message);
            String commandType = node.path("commandType").asText(null);
            if (commandType == null) {
                log.warn("Inventory command missing commandType: {}", message);
                return;
            }

            switch (commandType) {
                case "RESERVE_INVENTORY" -> {
                    ReserveInventoryCommand command = objectMapper.readValue(message, ReserveInventoryCommand.class);
                    if (command.getOrderId() != null) {
                        MDC.put("orderId", command.getOrderId());
                    }
                    MDC.put("correlationId", correlationId);
                    String validationError = validateCommandWithReason(command);
                    if (validationError != null) {
                        sendValidationFailureReply(command.getReservationId(), command.getOrderId(), validationError);
                    } else if (command.getItems() == null || command.getItems().isEmpty()) {
                        sendValidationFailureReply(command.getReservationId(), command.getOrderId(), "No items provided");
                    } else {
                        handleReserveInventory(command);
                    }
                }
                case "RELEASE_INVENTORY" -> {
                    ReleaseInventoryCommand command = objectMapper.readValue(message, ReleaseInventoryCommand.class);
                    if (command.getOrderId() != null) {
                        MDC.put("orderId", command.getOrderId());
                    }
                    MDC.put("correlationId", correlationId);
                    String validationError = validateCommandWithReason(command);
                    if (validationError != null) {
                        InventoryReleasedReply reply = InventoryReleasedReply.builder()
                            .reservationId(command.getReservationId())
                            .orderId(command.getOrderId())
                            .success(false)
                            .reason("Validation failed: " + validationError)
                            .build();
                        sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                    } else {
                        handleReleaseInventory(command);
                    }
                }
                default -> log.warn("Unknown inventory command type: {}", commandType);
            }
        } catch (JsonProcessingException e) {
            log.error("JSON parsing failed for inventory command: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error processing inventory command: {}", e.getMessage(), e);
        } finally {
            MDC.clear();
        }
    }

    private void sendValidationFailureReply(String reservationId, String orderId, String validationError) {
        reservationFailedCounter.increment();
        sagaStepsFailedCounter.increment();

        InventoryFailedReply reply = InventoryFailedReply.builder()
            .reservationId(reservationId)
            .orderId(orderId)
            .reason("Validation failed: " + validationError)
            .build();

        sendReplySafely(REPLY_TOPIC, orderId, reply, orderId);
        log.error("Inventory command validation failed for order {}: {}", orderId, validationError);
    }

    @Observed(name = "inventory.reserve", contextualName = "reserve-inventory")
    private void handleReserveInventory(ReserveInventoryCommand command) {
        long startTime = System.nanoTime();
        try {
            metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

            // Delegate to transactional service for database operations
            InventoryService.ReservationResult result = inventoryService.reserveInventory(command);

            if (result.success()) {
                reservationSuccessCounter.increment();
                sagaStepsExecutedCounter.increment();

                InventoryReservedReply reply = InventoryReservedReply.builder()
                    .reservationId(command.getReservationId())
                    .orderId(command.getOrderId())
                    .build();
                sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
            } else {
                reservationFailedCounter.increment();
                sagaStepsFailedCounter.increment();

                InventoryFailedReply reply = InventoryFailedReply.builder()
                    .reservationId(command.getReservationId())
                    .orderId(command.getOrderId())
                    .reason(result.errorMessage())
                    .build();
                sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                log.error("Inventory {} reservation failed: {}", command.getReservationId(), result.errorMessage());
            }
        } catch (Exception e) {
            reservationFailedCounter.increment();
            sagaStepsFailedCounter.increment();

            InventoryFailedReply reply = InventoryFailedReply.builder()
                .reservationId(command.getReservationId())
                .orderId(command.getOrderId())
                .reason("Reservation failed: " + e.getMessage())
                .build();
            sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
            log.error("Unexpected error reserving inventory {}: {}", command.getReservationId(), e.getMessage(), e);
        } finally {
            reservationTimer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }

    @Observed(name = "inventory.release", contextualName = "release-inventory")
    private void handleReleaseInventory(ReleaseInventoryCommand command) {
        long startTime = System.currentTimeMillis();
        metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

        boolean success = false;
        String reason = null;

        try {
            success = inventoryService.releaseInventory(command);
            if (!success) {
                reason = "Reservation not found: " + command.getReservationId();
            }
        } catch (Exception e) {
            reason = "Release failed: " + e.getMessage();
            log.error("Unexpected error releasing inventory {}: {}", command.getReservationId(), e.getMessage(), e);
        }

        InventoryReleasedReply reply = InventoryReleasedReply.builder()
            .reservationId(command.getReservationId())
            .orderId(command.getOrderId())
            .success(success)
            .reason(reason)
            .build();
        sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());

        compensationInventoryCounter.increment();
        compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
    }

    private void sendReplySafely(String topic, String key, Object reply, String orderId) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, reply);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_REPLY);
                    log.debug("Successfully sent reply for order {} to topic {}", orderId, topic);
                } else {
                    kafkaReplySendFailureCounter.increment();
                    log.error("Failed to send reply for order {} to topic {}: {}",
                        orderId, topic, ex.getMessage(), ex);
                }
            });
        } catch (Exception e) {
            kafkaReplySendFailureCounter.increment();
            log.error("Exception while sending reply for order {} to topic {}: {}",
                orderId, topic, e.getMessage(), e);
        }
    }

    private <T> String validateCommandWithReason(T command) {
        Set<ConstraintViolation<T>> violations = validator.validate(command);
        if (!violations.isEmpty()) {
            String errorMessage = violations.stream()
                .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                .reduce((a, b) -> a + ", " + b)
                .orElse("Unknown validation error");
            log.error("Command validation failed: {}", errorMessage);
            return errorMessage;
        }
        return null;
    }
}
