package com.thesis.choreography.payment.service;

import com.thesis.choreography.payment.kafka.PaymentEventPublisher;
import com.thesis.choreography.payment.model.Payment;
import com.thesis.choreography.payment.repository.PaymentRepository;
import com.thesis.common.exception.PaymentNotFoundException;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.events.PaymentCompletedEvent;
import com.thesis.common.events.PaymentFailedEvent;
import com.thesis.common.events.PaymentRefundedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@Slf4j
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentEventPublisher eventPublisher;
    private final Counter paymentSuccessCounter;
    private final Counter paymentFailedCounter;
    private final Timer paymentProcessingTimer;

    public PaymentService(PaymentRepository paymentRepository,
                          PaymentEventPublisher eventPublisher,
                          MeterRegistry meterRegistry) {
        this.paymentRepository = paymentRepository;
        this.eventPublisher = eventPublisher;
        
        this.paymentSuccessCounter = meterRegistry.counter("payments.success", "service", "choreography");
        this.paymentFailedCounter = meterRegistry.counter("payments.failed", "service", "choreography");
        this.paymentProcessingTimer = meterRegistry.timer("payment.processing.time", "service", "choreography");
    }

    @Transactional
    public void processPayment(OrderCreatedEvent orderEvent) {
        paymentProcessingTimer.record(() -> {
            log.info("Processing payment for order: {}", orderEvent.getOrderId());
            
            String paymentId = UUID.randomUUID().toString();
            
            Payment payment = Payment.builder()
                    .paymentId(paymentId)
                    .orderId(orderEvent.getOrderId())
                    .amount(orderEvent.getTotalAmount())
                    .status(Payment.PaymentStatus.PENDING)
                    .build();
            
            paymentRepository.save(payment);

            try {
                // Process payment
                String transactionId = "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
                payment.setTransactionId(transactionId);
                payment.setStatus(Payment.PaymentStatus.COMPLETED);
                paymentRepository.save(payment);

                PaymentCompletedEvent completedEvent = PaymentCompletedEvent.builder()
                        .paymentId(paymentId)
                        .orderId(orderEvent.getOrderId())
                        .amount(orderEvent.getTotalAmount())
                        .transactionId(transactionId)
                        .completedAt(Instant.now())
                        .build();

                eventPublisher.publishPaymentCompleted(completedEvent);
                paymentSuccessCounter.increment();
                log.info("Payment completed for order: {}", orderEvent.getOrderId());
            } catch (Exception e) {
                payment.setStatus(Payment.PaymentStatus.FAILED);
                payment.setFailureReason(e.getMessage());
                paymentRepository.save(payment);

                PaymentFailedEvent failedEvent = PaymentFailedEvent.builder()
                        .paymentId(paymentId)
                        .orderId(orderEvent.getOrderId())
                        .reason(e.getMessage())
                        .failedAt(Instant.now())
                        .build();

                eventPublisher.publishPaymentFailed(failedEvent);
                paymentFailedCounter.increment();
                log.error("Payment failed for order: {}", orderEvent.getOrderId(), e);
            }
        });
    }

    @Transactional
    public void refundPayment(String orderId) {
        log.info("Processing refund for order: {}", orderId);
        
        Payment payment = paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new PaymentNotFoundException(orderId));
        
        if (payment.getStatus() == Payment.PaymentStatus.COMPLETED) {
            payment.setStatus(Payment.PaymentStatus.REFUNDED);
            paymentRepository.save(payment);

            PaymentRefundedEvent refundedEvent = PaymentRefundedEvent.builder()
                    .paymentId(payment.getPaymentId())
                    .orderId(orderId)
                    .refundAmount(payment.getAmount())
                    .refundedAt(Instant.now())
                    .build();

            eventPublisher.publishPaymentRefunded(refundedEvent);
            log.info("Payment refunded for order: {}", orderId);
        }
    }
}
