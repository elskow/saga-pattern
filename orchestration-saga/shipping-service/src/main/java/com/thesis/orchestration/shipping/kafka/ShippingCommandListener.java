package com.thesis.orchestration.shipping.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.CancelShippingCommand;
import com.thesis.common.command.ScheduleShippingCommand;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.common.replies.ShippingCancelledReply;
import com.thesis.common.replies.ShippingFailedReply;
import com.thesis.common.replies.ShippingScheduledReply;
import com.thesis.orchestration.shipping.model.ShipmentEntity;
import com.thesis.orchestration.shipping.repository.ShipmentRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Kafka-based command listener for shipping service.
 * Handles shipping scheduling and cancellation commands from the saga orchestrator.
 */
@Component
@Slf4j
public class ShippingCommandListener {

    private static final String COMMAND_TOPIC = "orchestration.shipping.commands";
    private static final String REPLY_TOPIC = "orchestration.shipping.replies";

    private final ShipmentRepository shipmentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Counter shippingSuccessCounter;
    private final Counter shippingFailedCounter;
    private final Timer shippingProcessingTimer;
    private final Counter compensationShippingCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    private final Counter kafkaReplySendFailureCounter;
    private final SagaMetricsHelper metricsHelper;
    private final Validator validator;

    public ShippingCommandListener(ShipmentRepository shipmentRepository,
                                    KafkaTemplate<String, Object> kafkaTemplate,
                                    ObjectMapper objectMapper,
                                    MeterRegistry meterRegistry,
                                    Validator validator) {
        this.shipmentRepository = shipmentRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.shippingSuccessCounter = meterRegistry.counter(SagaMetrics.SHIPPING_SUCCESS,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.shippingFailedCounter = meterRegistry.counter(SagaMetrics.SHIPPING_FAILED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.shippingProcessingTimer = meterRegistry.timer(SagaMetrics.STEP_SHIPPING_DURATION,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.compensationShippingCounter = meterRegistry.counter(SagaMetrics.COMPENSATIONS_SHIPPING,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.compensationDurationTimer = meterRegistry.timer(SagaMetrics.COMPENSATION_DURATION,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_SHIPPING);
        this.sagaStepsExecutedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_EXECUTED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_SHIPPING);
        this.sagaStepsFailedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_FAILED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_SHIPPING);
        this.kafkaReplySendFailureCounter = meterRegistry.counter(
                SagaMetrics.SAGA_MESSAGES_TOTAL,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_SENT,
                SagaMetrics.TAG_MESSAGE_TYPE, SagaMetrics.TYPE_REPLY,
                SagaMetrics.TAG_OUTCOME, SagaMetrics.OUTCOME_FAILURE
        );
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_ORCHESTRATION);
    }

    @KafkaListener(topics = COMMAND_TOPIC, groupId = "shipping-service")
    public void handleCommand(String message) {
        String correlationId = UUID.randomUUID().toString();
        try {
            JsonNode node = objectMapper.readTree(message);
            String commandType = node.path("commandType").asText(null);
            if (commandType == null) {
                log.warn("Shipping command missing commandType: {}", message);
                return;
            }

            switch (commandType) {
                case "SCHEDULE_SHIPPING" -> {
                    ScheduleShippingCommand command = objectMapper.readValue(message, ScheduleShippingCommand.class);
                    if (command.getOrderId() != null) {
                        MDC.put("orderId", command.getOrderId());
                    }
                    MDC.put("correlationId", correlationId);
                    if (validateCommand(command)) {
                        handleScheduleShipping(command);
                    }
                }
                case "CANCEL_SHIPPING" -> {
                    CancelShippingCommand command = objectMapper.readValue(message, CancelShippingCommand.class);
                    if (command.getOrderId() != null) {
                        MDC.put("orderId", command.getOrderId());
                    }
                    MDC.put("correlationId", correlationId);
                    if (validateCommand(command)) {
                        handleCancelShipping(command);
                    }
                }
                default -> log.warn("Unknown shipping command type: {}", commandType);
            }
        } catch (JsonProcessingException e) {
            log.error("JSON parsing failed for shipping command: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error processing shipping command: {}", e.getMessage(), e);
        } finally {
            MDC.clear();
        }
    }

    @Observed(name = "shipping.schedule", contextualName = "schedule-shipping")
    private void handleScheduleShipping(ScheduleShippingCommand command) {
        shippingProcessingTimer.record(() -> {
            log.info("Scheduling shipping {} for order {} to address: {}",
                    command.getShipmentId(), command.getOrderId(), command.getShippingAddress());

            metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

            try {
                String trackingNumber = "TRK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

                ShipmentEntity shipment = ShipmentEntity.builder()
                        .shipmentId(command.getShipmentId())
                        .orderId(command.getOrderId())
                        .shippingAddress(command.getShippingAddress())
                        .trackingNumber(trackingNumber)
                        .status(ShipmentEntity.ShipmentStatus.SCHEDULED)
                        .scheduledAt(Instant.now())
                        .build();
                shipmentRepository.save(shipment);
                metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_SHIPMENT);

                shippingSuccessCounter.increment();
                sagaStepsExecutedCounter.increment();

                ShippingScheduledReply reply = ShippingScheduledReply.builder()
                        .shipmentId(command.getShipmentId())
                        .orderId(command.getOrderId())
                        .build();

                sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                log.info("Shipping {} scheduled successfully with tracking: {}", command.getShipmentId(), trackingNumber);

            } catch (DataAccessException e) {
                ShipmentEntity shipment = ShipmentEntity.builder()
                        .shipmentId(command.getShipmentId())
                        .orderId(command.getOrderId())
                        .shippingAddress(command.getShippingAddress())
                        .status(ShipmentEntity.ShipmentStatus.FAILED)
                        .failureReason("Database operation failed: " + e.getMessage())
                        .build();
                shipmentRepository.save(shipment);
                metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_SHIPMENT);

                shippingFailedCounter.increment();
                sagaStepsFailedCounter.increment();

                ShippingFailedReply reply = ShippingFailedReply.builder()
                        .shipmentId(command.getShipmentId())
                        .orderId(command.getOrderId())
                        .reason("Shipping scheduling failed: " + e.getMessage())
                        .build();

                sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                log.error("Shipping {} scheduling failed due to database error: {}", command.getShipmentId(), e.getMessage());
            } catch (Exception e) {
                ShipmentEntity shipment = ShipmentEntity.builder()
                        .shipmentId(command.getShipmentId())
                        .orderId(command.getOrderId())
                        .shippingAddress(command.getShippingAddress())
                        .status(ShipmentEntity.ShipmentStatus.FAILED)
                        .failureReason(e.getMessage())
                        .build();
                shipmentRepository.save(shipment);
                metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_SHIPMENT);

                shippingFailedCounter.increment();
                sagaStepsFailedCounter.increment();

                ShippingFailedReply reply = ShippingFailedReply.builder()
                        .shipmentId(command.getShipmentId())
                        .orderId(command.getOrderId())
                        .reason("Shipping scheduling failed: " + e.getMessage())
                        .build();

                sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                log.error("Shipping {} scheduling failed: {}", command.getShipmentId(), e.getMessage(), e);
            }
        });
    }

    @Observed(name = "shipping.cancel", contextualName = "cancel-shipping")
    private void handleCancelShipping(CancelShippingCommand command) {
        long startTime = System.currentTimeMillis();
        log.info("Cancelling shipping {} for order {}", command.getShipmentId(), command.getOrderId());

        metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

        boolean success = false;
        String reason = null;

        try {
            var shipmentOpt = shipmentRepository.findById(command.getShipmentId());
            if (shipmentOpt.isPresent()) {
                var shipment = shipmentOpt.get();
                shipment.setStatus(ShipmentEntity.ShipmentStatus.CANCELLED);
                shipment.setCancelledAt(Instant.now());
                shipment.setCancellationReason("Order cancelled - saga compensation");
                shipmentRepository.save(shipment);
                metricsHelper.recordDbUpdate(command.getOrderId(), SagaMetrics.ENTITY_SHIPMENT);
                log.info("Shipping {} cancelled successfully", command.getShipmentId());
                success = true;
            } else {
                reason = "Shipment not found: " + command.getShipmentId();
                log.warn(reason);
            }
        } catch (DataAccessException e) {
            reason = "Database operation failed: " + e.getMessage();
            log.error("Database error while cancelling shipping {}: {}", command.getShipmentId(), e.getMessage());
        } catch (Exception e) {
            reason = "Cancellation failed: " + e.getMessage();
            log.error("Unexpected error cancelling shipping {}: {}", command.getShipmentId(), e.getMessage(), e);
        }

        // Send compensation reply
        ShippingCancelledReply reply = ShippingCancelledReply.builder()
                .shipmentId(command.getShipmentId())
                .orderId(command.getOrderId())
                .success(success)
                .reason(reason)
                .build();
        sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());

        compensationShippingCounter.increment();
        compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
    }

    /**
     * Sends a reply to Kafka with comprehensive error handling.
     * Uses CompletableFuture callback to handle async results and failures.
     */
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
                    // Note: Replies are typically not retried as they are responses to commands
                    // If reply fails, the orchestrator will timeout and handle accordingly
                }
            });
        } catch (Exception e) {
            kafkaReplySendFailureCounter.increment();
            log.error("Exception while sending reply for order {} to topic {}: {}",
                    orderId, topic, e.getMessage(), e);
        }
    }

    private <T> boolean validateCommand(T command) {
        Set<ConstraintViolation<T>> violations = validator.validate(command);
        if (!violations.isEmpty()) {
            log.error("Command validation failed: {}", violations.stream()
                    .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("Unknown validation error"));
            return false;
        }
        return true;
    }
}
