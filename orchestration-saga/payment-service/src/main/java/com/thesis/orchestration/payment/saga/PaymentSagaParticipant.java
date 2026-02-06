package com.thesis.orchestration.payment.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.ProcessPaymentCommand;
import com.thesis.common.command.RefundPaymentCommand;
import com.thesis.common.config.PaymentProperties;
import com.thesis.common.replies.PaymentCompletedReply;
import com.thesis.common.replies.PaymentFailedReply;
import com.thesis.common.replies.PaymentRefundedReply;
import com.thesis.orchestration.payment.model.PaymentEntity;
import com.thesis.orchestration.payment.repository.PaymentRepository;
import com.thesis.saga.metrics.SagaMetricsRecorder;
import com.thesis.saga.participant.AbstractSagaParticipant;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;

@Component
@Slf4j
public class PaymentSagaParticipant extends AbstractSagaParticipant {

    private static final String COMMAND_TOPIC = "orchestration.payment.commands";
    private static final String REPLY_TOPIC = "orchestration.payment.replies";

    private final PaymentRepository paymentRepository;
    private final BigDecimal maxPaymentAmount;

    public PaymentSagaParticipant(
            KafkaTemplate<String, Object> kafkaTemplate,
            ObjectMapper objectMapper,
            Validator validator,
            SagaMetricsRecorder metricsRecorder,
            PaymentRepository paymentRepository,
            PaymentProperties paymentProperties) {
        super(kafkaTemplate, objectMapper, validator, metricsRecorder, "payment-service", REPLY_TOPIC);
        this.paymentRepository = paymentRepository;
        this.maxPaymentAmount = paymentProperties.maxPaymentAmount();

        registerHandler("PROCESS_PAYMENT", ProcessPaymentCommand.class, this::processPayment);
        registerHandler("REFUND_PAYMENT", RefundPaymentCommand.class, this::refundPayment);
    }

    @KafkaListener(topics = COMMAND_TOPIC, groupId = "payment-service")
    public void handleCommand(String message) {
        super.handleCommand(message);
    }

    @Transactional
    protected Object processPayment(ProcessPaymentCommand command) {
        log.debug("Processing payment {} for order {}, amount: {}",
                command.paymentId(), command.orderId(), command.amount());

        if (command.amount() != null && command.amount().compareTo(maxPaymentAmount) >= 0) {
            log.warn("Payment rejected: amount {} exceeds maximum limit {}",
                    command.amount(), maxPaymentAmount);

            savePayment(command, PaymentEntity.PaymentStatus.FAILED,
                    "Payment amount exceeds maximum allowed limit of %s".formatted(maxPaymentAmount));

            return PaymentFailedReply.of(
                    command.paymentId(),
                    command.orderId(),
                    "Payment amount exceeds maximum allowed limit of %s".formatted(maxPaymentAmount));
        }

        var existingReplyOpt = paymentRepository.findById(command.paymentId())
            .map(existing -> {
                log.debug("Payment {} already exists with status {}", command.paymentId(), existing.getStatus());
                return switch (existing.getStatus()) {
                    case COMPLETED, PENDING -> (Object) PaymentCompletedReply.of(command.paymentId(), command.orderId());
                    case FAILED -> PaymentFailedReply.of(command.paymentId(), command.orderId(),
                            "Payment previously failed: %s".formatted(existing.getFailureReason()));
                    case REFUNDED -> PaymentFailedReply.of(command.paymentId(), command.orderId(),
                            "Payment was already refunded");
                };
            });
        if (existingReplyOpt.isPresent()) return existingReplyOpt.get();

        PaymentEntity payment = PaymentEntity.builder()
                .paymentId(command.paymentId())
                .orderId(command.orderId())
                .customerId(command.customerId())
                .amount(command.amount())
                .status(PaymentEntity.PaymentStatus.COMPLETED)
                .processedAt(Instant.now())
                .build();

        paymentRepository.save(payment);
        log.debug("Payment {} completed successfully", command.paymentId());

        return PaymentCompletedReply.of(command.paymentId(), command.orderId());
    }

    @Transactional
    protected Object refundPayment(RefundPaymentCommand command) {
        log.debug("Refunding payment {} for order {}", command.paymentId(), command.orderId());

        return paymentRepository.findById(command.paymentId())
            .map(payment -> {
                if (payment.getStatus() == PaymentEntity.PaymentStatus.REFUNDED) {
                    log.debug("Payment {} already refunded", command.paymentId());
                    return buildRefundReply(command, "Already refunded");
                }
                payment.setStatus(PaymentEntity.PaymentStatus.REFUNDED);
                payment.setRefundedAt(Instant.now());
                payment.setRefundReason("Saga compensation");
                paymentRepository.save(payment);
                log.debug("Payment {} refunded successfully", command.paymentId());
                return buildRefundReply(command, "Refund completed");
            })
            .orElseGet(() -> {
                log.warn("Payment {} not found for refund", command.paymentId());
                return buildRefundReply(command, "Payment not found (nothing to refund)");
            });
    }

    private PaymentRefundedReply buildRefundReply(RefundPaymentCommand cmd, String reason) {
        return PaymentRefundedReply.success(cmd.paymentId(), cmd.orderId());
    }

    private void savePayment(ProcessPaymentCommand command, PaymentEntity.PaymentStatus status, String failureReason) {
        PaymentEntity payment = PaymentEntity.builder()
                .paymentId(command.paymentId())
                .orderId(command.orderId())
                .customerId(command.customerId())
                .amount(command.amount())
                .status(status)
                .failureReason(failureReason)
                .build();
        paymentRepository.save(payment);
    }

    @Override
    protected Object createValidationFailureReply(Object command, String commandType, String error) {
        return createFailureReply(command, "Validation failed: %s".formatted(error));
    }

    @Override
    protected Object createErrorReply(Object command, String commandType, String error) {
        return createFailureReply(command, "Processing error: %s".formatted(error));
    }

    private Object createFailureReply(Object command, String reason) {
        return switch (command) {
            case ProcessPaymentCommand cmd -> PaymentFailedReply.of(cmd.paymentId(), cmd.orderId(), reason);
            case RefundPaymentCommand cmd -> PaymentRefundedReply.failure(cmd.paymentId(), cmd.orderId(), reason);
            default -> null;
        };
    }
}
