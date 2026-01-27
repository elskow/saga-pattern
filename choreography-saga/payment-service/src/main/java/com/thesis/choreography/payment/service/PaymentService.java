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
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

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
        // Input validation
        if (orderEvent == null) {
            throw new IllegalArgumentException("OrderCreatedEvent cannot be null");
        }
        if (orderEvent.getOrderId() == null || orderEvent.getOrderId().isBlank()) {
            throw new IllegalArgumentException("Order ID cannot be null or blank");
        }
        if (orderEvent.getTotalAmount() == null) {
            throw new IllegalArgumentException("Total amount cannot be null");
        }
        
        String orderId = orderEvent.getOrderId();
        try {
            MDC.put("orderId", orderId);
            stepPaymentDurationTimer.record(() -> {
                log.info("Processing payment for order: {}", orderId);
                
                // Record received message and latency
                metricsHelper.recordMessageReceived(orderId, SagaMetrics.TYPE_EVENT);
                if (orderEvent.getCreatedAt() != null) {
                    Duration latency = Duration.between(orderEvent.getCreatedAt(), Instant.now());
                    metricsHelper.recordMessageLatency("order", "payment", latency);
                }
                
                String paymentId = UUID.randomUUID().toString();
                
                Payment payment = Payment.builder()
                        .paymentId(paymentId)
                        .orderId(orderId)
                        .customerId(orderEvent.getCustomerId())
                        .amount(orderEvent.getTotalAmount())
                        .status(Payment.PaymentStatus.PENDING)
                        .build();
                
                try {
                    paymentRepository.save(payment);
                    metricsHelper.recordDbInsert(orderId, SagaMetrics.ENTITY_PAYMENT);
                } catch (DataAccessException e) {
                    log.error("Database error while saving payment for order: {}", orderId, e);
                    throw e;
                }

                try {
                    // Process payment
                    String transactionId = "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
                    payment.setTransactionId(transactionId);
                    payment.setStatus(Payment.PaymentStatus.COMPLETED);
                    paymentRepository.save(payment);
                    metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_PAYMENT);

                    String correlationId = MDC.get("correlationId");
                    if (correlationId == null || correlationId.isBlank()) {
                        correlationId = orderEvent.getCorrelationId() != null ? 
                                orderEvent.getCorrelationId() : UUID.randomUUID().toString();
                        MDC.put("correlationId", correlationId);
                    }
                    
                    PaymentCompletedEvent completedEvent = PaymentCompletedEvent.builder()
                            .paymentId(paymentId)
                            .orderId(orderId)
                            .amount(orderEvent.getTotalAmount())
                            .transactionId(transactionId)
                            .completedAt(Instant.now())
                            .correlationId(correlationId)
                            .createdAt(Instant.now())
                            .build();

                    // Publish event after transaction commit
                    final String finalCorrelationId = correlationId;
                    if (TransactionSynchronizationManager.isSynchronizationActive()) {
                        TransactionSynchronizationManager.registerSynchronization(
                                new TransactionSynchronization() {
                                    @Override
                                    public void afterCommit() {
                                        try {
                                            MDC.put("orderId", orderId);
                                            MDC.put("correlationId", finalCorrelationId);
                                            eventPublisher.publishPaymentCompleted(completedEvent);
                                            metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                                            paymentSuccessCounter.increment();
                                            sagaStepsExecutedCounter.increment();
                                            log.info("Payment completed for order: {}", orderId);
                                        } catch (Exception e) {
                                            log.error("Failed to publish PaymentCompletedEvent after commit for order: {}", orderId, e);
                                        } finally {
                                            MDC.clear();
                                        }
                                    }
                                }
                        );
                    } else {
                        eventPublisher.publishPaymentCompleted(completedEvent);
                        metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                        paymentSuccessCounter.increment();
                        sagaStepsExecutedCounter.increment();
                        log.info("Payment completed for order: {}", orderId);
                    }
                } catch (IllegalArgumentException e) {
                    log.error("Invalid argument while processing payment for order: {}", orderId, e);
                    handlePaymentFailure(payment, paymentId, orderId, e);
                } catch (DataAccessException e) {
                    log.error("Database error while processing payment for order: {}", orderId, e);
                    handlePaymentFailure(payment, paymentId, orderId, e);
                } catch (Exception e) {
                    log.error("Unexpected error while processing payment for order: {}", orderId, e);
                    handlePaymentFailure(payment, paymentId, orderId, e);
                }
            });
        } finally {
            MDC.clear();
        }
    }
    
    private void handlePaymentFailure(Payment payment, String paymentId, String orderId, Exception e) {
        try {
            payment.setStatus(Payment.PaymentStatus.FAILED);
            payment.setFailureReason(e.getMessage());
            paymentRepository.save(payment);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_PAYMENT);

            String correlationId = MDC.get("correlationId");
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = UUID.randomUUID().toString();
                MDC.put("correlationId", correlationId);
            }
            
            PaymentFailedEvent failedEvent = PaymentFailedEvent.builder()
                    .paymentId(paymentId)
                    .orderId(orderId)
                    .reason(e.getMessage())
                    .failedAt(Instant.now())
                    .correlationId(correlationId)
                    .createdAt(Instant.now())
                    .build();

            // Publish event after transaction commit
            final String finalCorrelationId = correlationId;
            if (TransactionSynchronizationManager.isSynchronizationActive()) {
                TransactionSynchronizationManager.registerSynchronization(
                        new TransactionSynchronization() {
                            @Override
                            public void afterCommit() {
                                try {
                                    MDC.put("orderId", orderId);
                                    MDC.put("correlationId", finalCorrelationId);
                                    eventPublisher.publishPaymentFailed(failedEvent);
                                    metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                                    paymentFailedCounter.increment();
                                    sagaStepsFailedCounter.increment();
                                } catch (Exception ex) {
                                    log.error("Failed to publish PaymentFailedEvent after commit for order: {}", orderId, ex);
                                } finally {
                                    MDC.clear();
                                }
                            }
                        }
                );
            } else {
                eventPublisher.publishPaymentFailed(failedEvent);
                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                paymentFailedCounter.increment();
                sagaStepsFailedCounter.increment();
            }
        } catch (Exception ex) {
            log.error("Error while handling payment failure for order: {}", orderId, ex);
        }
    }

    @Transactional
    @Observed(name = "payment.refund", contextualName = "refund-payment")
    public void refundPayment(String orderId) {
        // Input validation
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("Order ID cannot be null or blank");
        }
        
        long startTime = System.currentTimeMillis();
        try {
            MDC.put("orderId", orderId);
            log.info("Processing refund for order: {}", orderId);
            
            Payment payment;
            try {
                payment = paymentRepository.findByOrderId(orderId)
                        .orElseThrow(() -> new PaymentNotFoundException(orderId));
            } catch (DataAccessException e) {
                log.error("Database error while finding payment for order: {}", orderId, e);
                throw e;
            }
            
            if (payment.getStatus() == Payment.PaymentStatus.COMPLETED) {
                try {
                    payment.setStatus(Payment.PaymentStatus.REFUNDED);
                    paymentRepository.save(payment);
                    metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_PAYMENT);
                } catch (DataAccessException e) {
                    log.error("Database error while saving refunded payment for order: {}", orderId, e);
                    throw e;
                }

            String correlationId = MDC.get("correlationId");
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = UUID.randomUUID().toString();
                MDC.put("correlationId", correlationId);
            }
            
            PaymentRefundedEvent refundedEvent = PaymentRefundedEvent.builder()
                    .paymentId(payment.getPaymentId())
                    .orderId(orderId)
                    .refundAmount(payment.getAmount())
                    .refundedAt(Instant.now())
                    .correlationId(correlationId)
                    .createdAt(Instant.now())
                    .build();

            // Publish event after transaction commit
            final String finalCorrelationId = correlationId;
            if (TransactionSynchronizationManager.isSynchronizationActive()) {
                TransactionSynchronizationManager.registerSynchronization(
                        new TransactionSynchronization() {
                            @Override
                            public void afterCommit() {
                                try {
                                    MDC.put("orderId", orderId);
                                    MDC.put("correlationId", finalCorrelationId);
                                    eventPublisher.publishPaymentRefunded(refundedEvent);
                                    metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                                } catch (Exception e) {
                                    log.error("Failed to publish PaymentRefundedEvent after commit for order: {}", orderId, e);
                                } finally {
                                    MDC.clear();
                                }
                            }
                        }
                );
            } else {
                eventPublisher.publishPaymentRefunded(refundedEvent);
                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
            }

                compensationPaymentCounter.increment();
                compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));
                
                log.info("Payment refunded for order: {}", orderId);
            }
        } finally {
            MDC.clear();
        }
    }

    public SagaMetricsHelper getMetricsHelper() {
        return metricsHelper;
    }
}
