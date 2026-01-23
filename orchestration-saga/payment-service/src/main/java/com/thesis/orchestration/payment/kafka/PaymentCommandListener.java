package com.thesis.orchestration.payment.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.command.ProcessPaymentCommand;
import com.thesis.common.command.RefundPaymentCommand;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.common.replies.PaymentCompletedReply;
import com.thesis.common.replies.PaymentFailedReply;
import com.thesis.orchestration.payment.model.PaymentEntity;
import com.thesis.orchestration.payment.repository.PaymentRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Kafka-based command listener for payment service.
 * Handles payment processing and refund commands from the saga orchestrator.
 */
@Component
@Slf4j
public class PaymentCommandListener {

    private static final String COMMAND_TOPIC = "orchestration.payment.commands";
    private static final String REPLY_TOPIC = "orchestration.payment.replies";

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
    private final SagaMetricsHelper metricsHelper;

    public PaymentCommandListener(PaymentRepository paymentRepository,
                                   KafkaTemplate<String, Object> kafkaTemplate,
                                   ObjectMapper objectMapper,
                                   MeterRegistry meterRegistry) {
        this.paymentRepository = paymentRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
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
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_ORCHESTRATION);
    }

    @KafkaListener(topics = COMMAND_TOPIC, groupId = "payment-service")
    public void handleCommand(String message) {
        try {
            // Try to parse as ProcessPaymentCommand first
            if (message.contains("amount") && message.contains("customerId")) {
                ProcessPaymentCommand command = objectMapper.readValue(message, ProcessPaymentCommand.class);
                handleProcessPayment(command);
                return;
            }

            // Try to parse as RefundPaymentCommand
            if (message.contains("paymentId") && message.contains("orderId")) {
                RefundPaymentCommand command = objectMapper.readValue(message, RefundPaymentCommand.class);
                handleRefundPayment(command);
            }
        } catch (Exception e) {
            log.error("Failed to process command: {}", message, e);
        }
    }

    @Observed(name = "payment.process", contextualName = "process-payment")
    private void handleProcessPayment(ProcessPaymentCommand command) {
        paymentProcessingTimer.record(() -> {
            log.info("Processing payment {} for order {}, amount: {}",
                    command.getPaymentId(), command.getOrderId(), command.getAmount());

            metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

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

                kafkaTemplate.send(REPLY_TOPIC, command.getOrderId(), reply);
                metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
                log.info("Payment {} completed successfully", command.getPaymentId());

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

                kafkaTemplate.send(REPLY_TOPIC, command.getOrderId(), reply);
                metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
                log.error("Payment {} failed: {}", command.getPaymentId(), e.getMessage());
            }
        });
    }

    @Observed(name = "payment.refund", contextualName = "refund-payment")
    private void handleRefundPayment(RefundPaymentCommand command) {
        long startTime = System.currentTimeMillis();
        log.info("Refunding payment {} for order {}", command.getPaymentId(), command.getOrderId());

        metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

        paymentRepository.findById(command.getPaymentId()).ifPresent(payment -> {
            payment.setStatus(PaymentEntity.PaymentStatus.REFUNDED);
            payment.setRefundedAt(Instant.now());
            payment.setRefundReason("Order cancelled - saga compensation");
            paymentRepository.save(payment);
            metricsHelper.recordDbUpdate(command.getOrderId(), SagaMetrics.ENTITY_PAYMENT);
            log.info("Payment {} refunded successfully", command.getPaymentId());
        });

        compensationPaymentCounter.increment();
        compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
        metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
    }
}
