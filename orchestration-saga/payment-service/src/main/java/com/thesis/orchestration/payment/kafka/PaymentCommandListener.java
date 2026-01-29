package com.thesis.orchestration.payment.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.ProcessPaymentCommand;
import com.thesis.common.command.RefundPaymentCommand;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.common.replies.PaymentCompletedReply;
import com.thesis.common.replies.PaymentFailedReply;
import com.thesis.common.replies.PaymentRefundedReply;
import com.thesis.orchestration.payment.model.PaymentEntity;
import com.thesis.orchestration.payment.repository.PaymentRepository;
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

import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Kafka-based command listener for payment service.
 * Handles payment processing and refund commands from the saga orchestrator.
 */
@Component
@Slf4j
public class PaymentCommandListener {

    private static final String COMMAND_TOPIC = "orchestration.payment.commands";
    private static final String REPLY_TOPIC = "orchestration.payment.replies";
    
    /**
     * Maximum payment amount allowed. Orders with total amount >= this value will be rejected.
     * This simulates real-world payment gateway behavior for high-value transactions.
     */
    private static final BigDecimal MAX_PAYMENT_AMOUNT = new BigDecimal("10000.00");

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Counter paymentSuccessCounter;
    private final Counter paymentFailedCounter;
    private final Timer paymentProcessingTimer;
    private final Counter compensationPaymentCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    private final Counter kafkaReplySendFailureCounter;
    private final SagaMetricsHelper metricsHelper;
    private final Validator validator;

    public PaymentCommandListener(PaymentRepository paymentRepository,
                                   KafkaTemplate<String, Object> kafkaTemplate,
                                   ObjectMapper objectMapper,
                                   MeterRegistry meterRegistry,
                                   Validator validator) {
        this.paymentRepository = paymentRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.paymentSuccessCounter = meterRegistry.counter(SagaMetrics.PAYMENTS_SUCCESS,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.paymentFailedCounter = meterRegistry.counter(SagaMetrics.PAYMENTS_FAILED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.paymentProcessingTimer = meterRegistry.timer(SagaMetrics.STEP_PAYMENT_DURATION,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.compensationPaymentCounter = meterRegistry.counter(SagaMetrics.COMPENSATIONS_PAYMENT,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION);
        this.compensationDurationTimer = meterRegistry.timer(SagaMetrics.COMPENSATION_DURATION,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_PAYMENT);
        this.sagaStepsExecutedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_EXECUTED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_PAYMENT);
        this.sagaStepsFailedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_FAILED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_PAYMENT);
        this.kafkaReplySendFailureCounter = meterRegistry.counter(
                SagaMetrics.SAGA_MESSAGES_TOTAL,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_ORCHESTRATION,
                SagaMetrics.TAG_DIRECTION, SagaMetrics.DIRECTION_SENT,
                SagaMetrics.TAG_MESSAGE_TYPE, SagaMetrics.TYPE_REPLY,
                SagaMetrics.TAG_OUTCOME, SagaMetrics.OUTCOME_FAILURE
        );
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_ORCHESTRATION);
    }

    @KafkaListener(topics = COMMAND_TOPIC, groupId = "payment-service")
    public void handleCommand(String message) {
        String correlationId = UUID.randomUUID().toString();
        try {
            JsonNode node = objectMapper.readTree(message);
            String commandType = node.path("commandType").asText(null);
            if (commandType == null) {
                log.warn("Payment command missing commandType: {}", message);
                return;
            }

            switch (commandType) {
                case "PROCESS_PAYMENT" -> {
                    ProcessPaymentCommand command = objectMapper.readValue(message, ProcessPaymentCommand.class);
                    if (command.getOrderId() != null) {
                        MDC.put("orderId", command.getOrderId());
                    }
                    MDC.put("correlationId", correlationId);
                    String validationError = validateCommandWithReason(command);
                    if (validationError != null) {
                        // Send failure reply so orchestrator doesn't wait forever
                        sendValidationFailureReply(command.getPaymentId(), command.getOrderId(), validationError);
                    } else {
                        handleProcessPayment(command);
                    }
                }
                case "REFUND_PAYMENT" -> {
                    RefundPaymentCommand command = objectMapper.readValue(message, RefundPaymentCommand.class);
                    if (command.getOrderId() != null) {
                        MDC.put("orderId", command.getOrderId());
                    }
                    MDC.put("correlationId", correlationId);
                    String validationError = validateCommandWithReason(command);
                    if (validationError != null) {
                        // Send compensation reply with failure so orchestrator doesn't wait forever
                        PaymentRefundedReply reply = PaymentRefundedReply.builder()
                                .paymentId(command.getPaymentId())
                                .orderId(command.getOrderId())
                                .success(false)
                                .reason("Validation failed: " + validationError)
                                .build();
                        sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                    } else {
                        handleRefundPayment(command);
                    }
                }
                default -> log.warn("Unknown payment command type: {}", commandType);
            }
        } catch (JsonProcessingException e) {
            log.error("JSON parsing failed for payment command: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error processing payment command: {}", e.getMessage(), e);
        } finally {
            MDC.clear();
        }
    }

    /**
     * Sends a failure reply when validation fails, so orchestrator doesn't wait forever.
     */
    private void sendValidationFailureReply(String paymentId, String orderId, String validationError) {
        paymentFailedCounter.increment();
        sagaStepsFailedCounter.increment();
        
        PaymentFailedReply reply = PaymentFailedReply.builder()
                .paymentId(paymentId)
                .orderId(orderId)
                .reason("Validation failed: " + validationError)
                .build();
        
        sendReplySafely(REPLY_TOPIC, orderId, reply, orderId);
        log.error("Payment command validation failed for order {}: {}", orderId, validationError);
    }

    @Transactional
    @Observed(name = "payment.process", contextualName = "process-payment")
    protected void handleProcessPayment(ProcessPaymentCommand command) {
        paymentProcessingTimer.record(() -> {
            log.info("Processing payment {} for order {}, amount: {}",
                    command.getPaymentId(), command.getOrderId(), command.getAmount());

            metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

            // Reject high-value payments (simulates real-world payment gateway limits)
            if (command.getAmount() != null && command.getAmount().compareTo(MAX_PAYMENT_AMOUNT) >= 0) {
                log.warn("Payment rejected for order {}: amount {} exceeds maximum limit {}", 
                    command.getOrderId(), command.getAmount(), MAX_PAYMENT_AMOUNT);
                
                PaymentEntity payment = PaymentEntity.builder()
                        .paymentId(command.getPaymentId())
                        .orderId(command.getOrderId())
                        .customerId(command.getCustomerId())
                        .amount(command.getAmount())
                        .status(PaymentEntity.PaymentStatus.FAILED)
                        .failureReason("Payment amount " + command.getAmount() + 
                            " exceeds maximum allowed limit of " + MAX_PAYMENT_AMOUNT)
                        .build();
                paymentRepository.save(payment);
                metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_PAYMENT);
                
                paymentFailedCounter.increment();
                sagaStepsFailedCounter.increment();
                
                PaymentFailedReply reply = PaymentFailedReply.builder()
                        .paymentId(command.getPaymentId())
                        .orderId(command.getOrderId())
                        .reason("Payment amount exceeds maximum allowed limit of " + MAX_PAYMENT_AMOUNT)
                        .build();
                sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                return;
            }

            // Idempotency check: if payment already exists and is completed/pending, send success reply
            var existingPayment = paymentRepository.findById(command.getPaymentId());
            if (existingPayment.isPresent()) {
                PaymentEntity existing = existingPayment.get();
                if (existing.getStatus() == PaymentEntity.PaymentStatus.COMPLETED ||
                    existing.getStatus() == PaymentEntity.PaymentStatus.PENDING) {
                    log.info("Payment {} already exists with status {}, sending idempotent success reply",
                            command.getPaymentId(), existing.getStatus());
                    PaymentCompletedReply reply = PaymentCompletedReply.builder()
                            .paymentId(command.getPaymentId())
                            .orderId(command.getOrderId())
                            .build();
                    sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                    return;
                } else if (existing.getStatus() == PaymentEntity.PaymentStatus.FAILED) {
                    log.info("Payment {} already exists with FAILED status, sending idempotent failure reply",
                            command.getPaymentId());
                    PaymentFailedReply reply = PaymentFailedReply.builder()
                            .paymentId(command.getPaymentId())
                            .orderId(command.getOrderId())
                            .reason("Payment previously failed: " + existing.getFailureReason())
                            .build();
                    sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                    return;
                }
                // REFUNDED status - treat as already processed, send failure
                log.info("Payment {} already exists with REFUNDED status, sending idempotent failure reply",
                        command.getPaymentId());
                PaymentFailedReply reply = PaymentFailedReply.builder()
                        .paymentId(command.getPaymentId())
                        .orderId(command.getOrderId())
                        .reason("Payment was already refunded")
                        .build();
                sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                return;
            }

            PaymentEntity payment = PaymentEntity.builder()
                    .paymentId(command.getPaymentId())
                    .orderId(command.getOrderId())
                    .customerId(command.getCustomerId())
                    .amount(command.getAmount())
                    .status(PaymentEntity.PaymentStatus.PENDING)
                    .build();

            try {
                payment.setStatus(PaymentEntity.PaymentStatus.COMPLETED);
                payment.setProcessedAt(Instant.now());
                paymentRepository.save(payment);
                metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_PAYMENT);

                paymentSuccessCounter.increment();
                sagaStepsExecutedCounter.increment();

                PaymentCompletedReply reply = PaymentCompletedReply.builder()
                        .paymentId(command.getPaymentId())
                        .orderId(command.getOrderId())
                        .build();

                sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                log.info("Payment {} completed successfully", command.getPaymentId());

            } catch (DataAccessException e) {
                payment.setStatus(PaymentEntity.PaymentStatus.FAILED);
                payment.setFailureReason("Database operation failed: " + e.getMessage());
                paymentRepository.save(payment);
                metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_PAYMENT);

                paymentFailedCounter.increment();
                sagaStepsFailedCounter.increment();

                PaymentFailedReply reply = PaymentFailedReply.builder()
                        .paymentId(command.getPaymentId())
                        .orderId(command.getOrderId())
                        .reason("Payment processing failed: " + e.getMessage())
                        .build();

                sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                log.error("Payment {} failed due to database error: {}", command.getPaymentId(), e.getMessage());
            } catch (Exception e) {
                payment.setStatus(PaymentEntity.PaymentStatus.FAILED);
                payment.setFailureReason(e.getMessage());
                paymentRepository.save(payment);
                metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_PAYMENT);

                paymentFailedCounter.increment();
                sagaStepsFailedCounter.increment();

                PaymentFailedReply reply = PaymentFailedReply.builder()
                        .paymentId(command.getPaymentId())
                        .orderId(command.getOrderId())
                        .reason("Payment processing failed: " + e.getMessage())
                        .build();

                sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                log.error("Payment {} failed: {}", command.getPaymentId(), e.getMessage(), e);
            }
        });
    }

    @Transactional
    @Observed(name = "payment.refund", contextualName = "refund-payment")
    protected void handleRefundPayment(RefundPaymentCommand command) {
        long startTime = System.currentTimeMillis();
        log.info("Refunding payment {} for order {}", command.getPaymentId(), command.getOrderId());

        metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

        // Idempotency check: if payment already refunded, send idempotent success reply
        var existingPayment = paymentRepository.findById(command.getPaymentId());
        if (existingPayment.isPresent()) {
            PaymentEntity existing = existingPayment.get();
            if (existing.getStatus() == PaymentEntity.PaymentStatus.REFUNDED) {
                log.info("Payment {} already refunded, sending idempotent success reply", command.getPaymentId());
                PaymentRefundedReply reply = PaymentRefundedReply.builder()
                        .paymentId(command.getPaymentId())
                        .orderId(command.getOrderId())
                        .success(true)
                        .reason("Already refunded: " + existing.getRefundReason())
                        .build();
                sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());
                compensationPaymentCounter.increment();
                compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
                return;
            }
        }

        boolean success = false;
        String reason = null;

        try {
            var paymentOpt = paymentRepository.findById(command.getPaymentId());
            if (paymentOpt.isPresent()) {
                var payment = paymentOpt.get();
                payment.setStatus(PaymentEntity.PaymentStatus.REFUNDED);
                payment.setRefundedAt(Instant.now());
                payment.setRefundReason("Order cancelled - saga compensation");
                paymentRepository.save(payment);
                metricsHelper.recordDbUpdate(command.getOrderId(), SagaMetrics.ENTITY_PAYMENT);
                log.info("Payment {} refunded successfully", command.getPaymentId());
                success = true;
            } else {
                // Payment not found - treat as success (nothing to refund)
                log.info("Payment {} not found, treating refund as success", command.getPaymentId());
                success = true;
                reason = "Payment not found (nothing to refund): " + command.getPaymentId();
            }
        } catch (DataAccessException e) {
            reason = "Database operation failed: " + e.getMessage();
            log.error("Database error while refunding payment {}: {}", command.getPaymentId(), e.getMessage());
        } catch (Exception e) {
            reason = "Refund failed: " + e.getMessage();
            log.error("Unexpected error refunding payment {}: {}", command.getPaymentId(), e.getMessage(), e);
        }

        // Send compensation reply
        PaymentRefundedReply reply = PaymentRefundedReply.builder()
                .paymentId(command.getPaymentId())
                .orderId(command.getOrderId())
                .success(success)
                .reason(reason)
                .build();
        sendReplySafely(REPLY_TOPIC, command.getOrderId(), reply, command.getOrderId());

        compensationPaymentCounter.increment();
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

    /**
     * Validates a command and returns the error message if validation fails, or null if valid.
     */
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
