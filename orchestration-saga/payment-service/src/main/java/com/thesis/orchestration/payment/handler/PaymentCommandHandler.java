package com.thesis.orchestration.payment.handler;

import com.thesis.common.command.ProcessPaymentCommand;
import com.thesis.common.command.RefundPaymentCommand;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.common.replies.PaymentCompletedReply;
import com.thesis.common.replies.PaymentFailedReply;
import com.thesis.orchestration.payment.model.PaymentEntity;
import com.thesis.orchestration.payment.repository.PaymentRepository;
import io.eventuate.tram.commands.consumer.CommandHandlers;
import io.eventuate.tram.commands.consumer.CommandMessage;
import io.eventuate.tram.messaging.common.Message;
import io.eventuate.tram.sagas.participant.SagaCommandHandlersBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;

import static io.eventuate.tram.commands.consumer.CommandHandlerReplyBuilder.withFailure;
import static io.eventuate.tram.commands.consumer.CommandHandlerReplyBuilder.withSuccess;

@Component
@Slf4j
public class PaymentCommandHandler {

    private final PaymentRepository paymentRepository;
    private final Counter paymentSuccessCounter;
    private final Counter paymentFailedCounter;
    private final Timer paymentProcessingTimer;
    private final Counter compensationPaymentCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    private final SagaMetricsHelper metricsHelper;

    public PaymentCommandHandler(PaymentRepository paymentRepository, MeterRegistry meterRegistry) {
        this.paymentRepository = paymentRepository;
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

    public CommandHandlers commandHandlers() {
        return SagaCommandHandlersBuilder
            .fromChannel("payment-service")
            .onMessage(ProcessPaymentCommand.class, this::handleProcessPayment)
            .onMessage(RefundPaymentCommand.class, this::handleRefundPayment)
            .build();
    }

    @Observed(name = "payment.process", contextualName = "process-payment")
    private Message handleProcessPayment(CommandMessage<ProcessPaymentCommand> cm) {
        return paymentProcessingTimer.record(() -> {
            ProcessPaymentCommand command = cm.getCommand();
            log.info("Processing payment {} for order {}, amount: {}",
                command.getPaymentId(), command.getOrderId(), command.getAmount());

            // Record command received
            metricsHelper.recordMessageReceived(command.getOrderId(), SagaMetrics.TYPE_COMMAND);

            // Create payment entity
            PaymentEntity payment = PaymentEntity.builder()
                .paymentId(command.getPaymentId())
                .orderId(command.getOrderId())
                .customerId(command.getCustomerId())
                .amount(command.getAmount())
                .status(PaymentEntity.PaymentStatus.PENDING)
                .build();

            try {
                // Process payment (in real app, this would call payment gateway)
                payment.setStatus(PaymentEntity.PaymentStatus.COMPLETED);
                payment.setProcessedAt(Instant.now());
                paymentRepository.save(payment);
                metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_PAYMENT);

                paymentSuccessCounter.increment();
                sagaStepsExecutedCounter.increment();
                metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
                log.info("Payment {} completed successfully", command.getPaymentId());
                return withSuccess(PaymentCompletedReply.builder()
                    .paymentId(command.getPaymentId())
                    .orderId(command.getOrderId())
                    .build());
            } catch (Exception e) {
                payment.setStatus(PaymentEntity.PaymentStatus.FAILED);
                payment.setFailureReason(e.getMessage());
                paymentRepository.save(payment);
                metricsHelper.recordDbInsert(command.getOrderId(), SagaMetrics.ENTITY_PAYMENT);

                paymentFailedCounter.increment();
                sagaStepsFailedCounter.increment();
                metricsHelper.recordMessageSent(command.getOrderId(), SagaMetrics.TYPE_REPLY);
                log.error("Payment {} failed: {}", command.getPaymentId(), e.getMessage());
                return withFailure(PaymentFailedReply.builder()
                    .paymentId(command.getPaymentId())
                    .orderId(command.getOrderId())
                    .reason("Payment processing failed: " + e.getMessage())
                    .build());
            }
        });
    }

    @Observed(name = "payment.refund", contextualName = "refund-payment")
    private Message handleRefundPayment(CommandMessage<RefundPaymentCommand> cm) {
        long startTime = System.currentTimeMillis();
        RefundPaymentCommand command = cm.getCommand();
        log.info("Refunding payment {} for order {}", command.getPaymentId(), command.getOrderId());

        // Record command received
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
        
        return withSuccess();
    }

    public SagaMetricsHelper getMetricsHelper() {
        return metricsHelper;
    }
}
