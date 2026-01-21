package com.thesis.choreography.payment.service;

import com.thesis.choreography.payment.kafka.PaymentEventPublisher;
import com.thesis.choreography.payment.model.Payment;
import com.thesis.choreography.payment.repository.PaymentRepository;
import com.thesis.common.exception.PaymentNotFoundException;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.events.PaymentCompletedEvent;
import com.thesis.common.events.PaymentFailedEvent;
import com.thesis.common.events.PaymentRefundedEvent;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
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
    private final Timer stepPaymentDurationTimer;
    private final Counter compensationPaymentCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    private final SagaMetricsHelper metricsHelper;

    public PaymentService(PaymentRepository paymentRepository,
                          PaymentEventPublisher eventPublisher,
                          MeterRegistry meterRegistry) {
        this.paymentRepository = paymentRepository;
        this.eventPublisher = eventPublisher;
        
        this.paymentSuccessCounter = meterRegistry.counter(SagaMetrics.PAYMENTS_SUCCESS, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.paymentFailedCounter = meterRegistry.counter(SagaMetrics.PAYMENTS_FAILED, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.paymentProcessingTimer = meterRegistry.timer(SagaMetrics.PAYMENT_PROCESSING_TIME, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.stepPaymentDurationTimer = meterRegistry.timer(SagaMetrics.STEP_PAYMENT_DURATION, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.compensationPaymentCounter = meterRegistry.counter(SagaMetrics.COMPENSATIONS_PAYMENT, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY);
        this.compensationDurationTimer = meterRegistry.timer(SagaMetrics.COMPENSATION_DURATION, 
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_PAYMENT);
        this.sagaStepsExecutedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_EXECUTED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_PAYMENT);
        this.sagaStepsFailedCounter = meterRegistry.counter(SagaMetrics.SAGA_STEPS_FAILED,
                SagaMetrics.TAG_SERVICE, SagaMetrics.SERVICE_CHOREOGRAPHY,
                SagaMetrics.TAG_STEP, SagaMetrics.STEP_PAYMENT);
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, SagaMetrics.SERVICE_CHOREOGRAPHY);
    }

    @Transactional
    @Observed(name = "payment.process", contextualName = "process-payment")
    public void processPayment(OrderCreatedEvent orderEvent) {
        stepPaymentDurationTimer.record(() -> {
            log.info("Processing payment for order: {}", orderEvent.getOrderId());
            
            // Record received message and latency
            metricsHelper.recordMessageReceived(orderEvent.getOrderId(), SagaMetrics.TYPE_EVENT);
            if (orderEvent.getCreatedAt() != null) {
                Duration latency = Duration.between(orderEvent.getCreatedAt(), Instant.now());
                metricsHelper.recordMessageLatency("order", "payment", latency);
            }
            
            String paymentId = UUID.randomUUID().toString();
            
            Payment payment = Payment.builder()
                    .paymentId(paymentId)
                    .orderId(orderEvent.getOrderId())
                    .amount(orderEvent.getTotalAmount())
                    .status(Payment.PaymentStatus.PENDING)
                    .build();
            
            paymentRepository.save(payment);
            metricsHelper.recordDbInsert(orderEvent.getOrderId(), SagaMetrics.ENTITY_PAYMENT);

            try {
                // Process payment
                String transactionId = "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
                payment.setTransactionId(transactionId);
                payment.setStatus(Payment.PaymentStatus.COMPLETED);
                paymentRepository.save(payment);
                metricsHelper.recordDbUpdate(orderEvent.getOrderId(), SagaMetrics.ENTITY_PAYMENT);

                PaymentCompletedEvent completedEvent = PaymentCompletedEvent.builder()
                        .paymentId(paymentId)
                        .orderId(orderEvent.getOrderId())
                        .amount(orderEvent.getTotalAmount())
                        .transactionId(transactionId)
                        .completedAt(Instant.now())
                        .createdAt(Instant.now())
                        .build();

                eventPublisher.publishPaymentCompleted(completedEvent);
                metricsHelper.recordMessageSent(orderEvent.getOrderId(), SagaMetrics.TYPE_EVENT);
                paymentSuccessCounter.increment();
                sagaStepsExecutedCounter.increment();
                log.info("Payment completed for order: {}", orderEvent.getOrderId());
            } catch (Exception e) {
                payment.setStatus(Payment.PaymentStatus.FAILED);
                payment.setFailureReason(e.getMessage());
                paymentRepository.save(payment);
                metricsHelper.recordDbUpdate(orderEvent.getOrderId(), SagaMetrics.ENTITY_PAYMENT);

                PaymentFailedEvent failedEvent = PaymentFailedEvent.builder()
                        .paymentId(paymentId)
                        .orderId(orderEvent.getOrderId())
                        .reason(e.getMessage())
                        .failedAt(Instant.now())
                        .createdAt(Instant.now())
                        .build();

                eventPublisher.publishPaymentFailed(failedEvent);
                metricsHelper.recordMessageSent(orderEvent.getOrderId(), SagaMetrics.TYPE_EVENT);
                paymentFailedCounter.increment();
                sagaStepsFailedCounter.increment();
                log.error("Payment failed for order: {}", orderEvent.getOrderId(), e);
            }
        });
    }

    @Transactional
    @Observed(name = "payment.refund", contextualName = "refund-payment")
    public void refundPayment(String orderId) {
        long startTime = System.currentTimeMillis();
        log.info("Processing refund for order: {}", orderId);
        
        Payment payment = paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new PaymentNotFoundException(orderId));
        
        if (payment.getStatus() == Payment.PaymentStatus.COMPLETED) {
            payment.setStatus(Payment.PaymentStatus.REFUNDED);
            paymentRepository.save(payment);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_PAYMENT);

            PaymentRefundedEvent refundedEvent = PaymentRefundedEvent.builder()
                    .paymentId(payment.getPaymentId())
                    .orderId(orderId)
                    .refundAmount(payment.getAmount())
                    .refundedAt(Instant.now())
                    .createdAt(Instant.now())
                    .build();

            eventPublisher.publishPaymentRefunded(refundedEvent);
            metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
            
            compensationPaymentCounter.increment();
            compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
            
            log.info("Payment refunded for order: {}", orderId);
        }
    }

    public SagaMetricsHelper getMetricsHelper() {
        return metricsHelper;
    }
}
