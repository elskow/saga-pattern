package com.thesis.orchestration.payment.handler;

import com.thesis.common.command.ProcessPaymentCommand;
import com.thesis.common.command.RefundPaymentCommand;
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

    public PaymentCommandHandler(PaymentRepository paymentRepository, MeterRegistry meterRegistry) {
        this.paymentRepository = paymentRepository;
        this.paymentSuccessCounter = meterRegistry.counter("payments.success", "service", "orchestration");
        this.paymentFailedCounter = meterRegistry.counter("payments.failed", "service", "orchestration");
        this.paymentProcessingTimer = meterRegistry.timer("payment.processing.time", "service", "orchestration");
    }

    public CommandHandlers commandHandlers() {
        return SagaCommandHandlersBuilder
            .fromChannel("payment-service")
            .onMessage(ProcessPaymentCommand.class, this::handleProcessPayment)
            .onMessage(RefundPaymentCommand.class, this::handleRefundPayment)
            .build();
    }

    private Message handleProcessPayment(CommandMessage<ProcessPaymentCommand> cm) {
        return paymentProcessingTimer.record(() -> {
            ProcessPaymentCommand command = cm.getCommand();
            log.info("Processing payment {} for order {}, amount: {}",
                command.getPaymentId(), command.getOrderId(), command.getAmount());

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

                paymentSuccessCounter.increment();
                log.info("Payment {} completed successfully", command.getPaymentId());
                return withSuccess(PaymentCompletedReply.builder()
                    .paymentId(command.getPaymentId())
                    .orderId(command.getOrderId())
                    .build());
            } catch (Exception e) {
                payment.setStatus(PaymentEntity.PaymentStatus.FAILED);
                payment.setFailureReason(e.getMessage());
                paymentRepository.save(payment);

                paymentFailedCounter.increment();
                log.error("Payment {} failed: {}", command.getPaymentId(), e.getMessage());
                return withFailure(PaymentFailedReply.builder()
                    .paymentId(command.getPaymentId())
                    .orderId(command.getOrderId())
                    .reason("Payment processing failed: " + e.getMessage())
                    .build());
            }
        });
    }

    private Message handleRefundPayment(CommandMessage<RefundPaymentCommand> cm) {
        RefundPaymentCommand command = cm.getCommand();
        log.info("Refunding payment {} for order {}", command.getPaymentId(), command.getOrderId());

        paymentRepository.findById(command.getPaymentId()).ifPresent(payment -> {
            payment.setStatus(PaymentEntity.PaymentStatus.REFUNDED);
            payment.setRefundedAt(Instant.now());
            payment.setRefundReason("Order cancelled - saga compensation");
            paymentRepository.save(payment);
            log.info("Payment {} refunded successfully", command.getPaymentId());
        });

        return withSuccess();
    }
}
