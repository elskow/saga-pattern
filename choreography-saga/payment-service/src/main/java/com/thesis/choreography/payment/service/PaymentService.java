package com.thesis.choreography.payment.service;

import com.thesis.choreography.payment.kafka.PaymentEventPublisher;
import com.thesis.choreography.payment.model.Payment;
import com.thesis.choreography.payment.repository.PaymentRepository;
import com.thesis.common.config.PaymentProperties;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.events.PaymentCompletedEvent;
import com.thesis.common.events.PaymentFailedEvent;
import com.thesis.common.events.PaymentRefundedEvent;
import com.thesis.common.exception.PaymentNotFoundException;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import com.thesis.common.util.CorrelationIdResolver;
import com.thesis.common.util.TransactionHelper;
import com.thesis.common.util.Validators;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.annotation.Observed;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentEventPublisher eventPublisher;
    private final PaymentProperties paymentProperties;
    private final Counter paymentSuccessCounter;
    private final Counter paymentFailedCounter;
    private final Timer paymentProcessingTimer;
    private final Timer stepPaymentDurationTimer;
    private final Counter compensationPaymentCounter;
    private final Timer compensationDurationTimer;
    private final Counter sagaStepsExecutedCounter;
    private final Counter sagaStepsFailedCounter;
    @Getter
    private final SagaMetricsHelper metricsHelper;

    public PaymentService(PaymentRepository paymentRepository,
                          PaymentEventPublisher eventPublisher,
                          PaymentProperties paymentProperties,
                          MeterRegistry meterRegistry) {
        this.paymentRepository = paymentRepository;
        this.eventPublisher = eventPublisher;
        this.paymentProperties = paymentProperties;

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
        validateOrderEvent(orderEvent);
        String orderId = orderEvent.orderId();
        try {
            MDC.put("orderId", orderId);

            if (exceedsMaximumAmount(orderEvent)) {
                rejectHighValuePayment(orderEvent, orderId);
                return;
            }

            stepPaymentDurationTimer.record(() -> executePaymentProcessing(orderEvent, orderId));
        } finally {
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }

    private boolean exceedsMaximumAmount(OrderCreatedEvent orderEvent) {
        return orderEvent.totalAmount().compareTo(paymentProperties.maxPaymentAmount()) >= 0;
    }

    private void rejectHighValuePayment(OrderCreatedEvent orderEvent, String orderId) {
        log.warn("Payment rejected for order {}: amount {} exceeds maximum limit {}",
            orderId, orderEvent.totalAmount(), paymentProperties.maxPaymentAmount());

        String paymentId = UUID.randomUUID().toString();
        Payment payment = Payment.builder()
            .paymentId(paymentId)
            .orderId(orderId)
            .customerId(orderEvent.customerId())
            .amount(orderEvent.totalAmount())
            .status(Payment.PaymentStatus.FAILED)
            .failureReason("Payment amount %s exceeds maximum allowed limit of %s".formatted(
                orderEvent.totalAmount(), paymentProperties.maxPaymentAmount()))
            .build();
        paymentRepository.save(payment);

        handlePaymentFailure(payment, paymentId, orderId,
            new IllegalArgumentException("Payment amount exceeds maximum allowed limit"));
    }

    private void executePaymentProcessing(OrderCreatedEvent orderEvent, String orderId) {
        log.debug("Processing payment for order: {}", orderId);

        recordMessageMetrics(orderEvent, orderId);

        String paymentId = UUID.randomUUID().toString();
        Payment payment = createPendingPayment(paymentId, orderId, orderEvent);

        try {
            completePayment(payment, paymentId, orderId, orderEvent);
        } catch (Exception e) {
            log.error("Error processing payment for order {}: {}", orderId, e.getMessage(), e);
            handlePaymentFailure(payment, paymentId, orderId, e);
        }
    }

    private void recordMessageMetrics(OrderCreatedEvent orderEvent, String orderId) {
        metricsHelper.recordMessageReceived(orderId, SagaMetrics.TYPE_EVENT);
        if (orderEvent.createdAt() != null) {
            Duration latency = Duration.between(orderEvent.createdAt(), Instant.now());
            metricsHelper.recordMessageLatency("order", "payment", latency);
        }
    }

    private Payment createPendingPayment(String paymentId, String orderId, OrderCreatedEvent orderEvent) {
        Payment payment = Payment.builder()
            .paymentId(paymentId)
            .orderId(orderId)
            .customerId(orderEvent.customerId())
            .amount(orderEvent.totalAmount())
            .status(Payment.PaymentStatus.PENDING)
            .build();

        try {
            paymentRepository.save(payment);
            metricsHelper.recordDbInsert(orderId, SagaMetrics.ENTITY_PAYMENT);
        } catch (DataAccessException e) {
            log.error("Database error while saving payment for order: {}", orderId, e);
            throw e;
        }
        return payment;
    }

    private void completePayment(Payment payment, String paymentId, String orderId, OrderCreatedEvent orderEvent) {
        payment.setTransactionId(generateTransactionId());
        payment.setStatus(Payment.PaymentStatus.COMPLETED);
        paymentRepository.save(payment);
        metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_PAYMENT);

        String correlationId = CorrelationIdResolver.resolve(orderEvent.correlationId());
        MDC.put("correlationId", correlationId);

        PaymentCompletedEvent completedEvent = PaymentCompletedEvent.of(
            paymentId,
            orderId,
            orderEvent.totalAmount(),
            payment.getTransactionId(),
            Instant.now(),
            correlationId
        );

        TransactionHelper.executeAfterCommit(() -> {
            eventPublisher.publishPaymentCompleted(completedEvent);
            metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
            paymentSuccessCounter.increment();
            sagaStepsExecutedCounter.increment();
            log.debug("Payment completed for order: {}", orderId);
        }, orderId, correlationId);
    }

    private void handlePaymentFailure(Payment payment, String paymentId, String orderId, Exception e) {
        try {
            payment.setStatus(Payment.PaymentStatus.FAILED);
            payment.setFailureReason(e.getMessage());
            paymentRepository.save(payment);
            metricsHelper.recordDbUpdate(orderId, SagaMetrics.ENTITY_PAYMENT);

            String correlationId = CorrelationIdResolver.resolve(null);
            MDC.put("correlationId", correlationId);

            PaymentFailedEvent failedEvent = PaymentFailedEvent.of(
                paymentId,
                orderId,
                e.getMessage(),
                Instant.now(),
                correlationId
            );

            TransactionHelper.executeAfterCommit(() -> {
                eventPublisher.publishPaymentFailed(failedEvent);
                metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                paymentFailedCounter.increment();
                sagaStepsFailedCounter.increment();
            }, orderId, correlationId);
        } catch (Exception ex) {
            log.error("Error while handling payment failure for order: {}", orderId, ex);
        }
    }

    @Transactional
    @Observed(name = "payment.refund", contextualName = "refund-payment")
    public void refundPayment(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("Order ID cannot be null or blank");
        }

        long startTime = System.currentTimeMillis();
        try {
            MDC.put("orderId", orderId);
            log.debug("Processing refund for order: {}", orderId);

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

                String correlationId = CorrelationIdResolver.resolve(null);
                MDC.put("correlationId", correlationId);

                PaymentRefundedEvent refundedEvent = PaymentRefundedEvent.of(
                    payment.getPaymentId(),
                    orderId,
                    payment.getAmount(),
                    Instant.now(),
                    correlationId
                );

                TransactionHelper.executeAfterCommit(() -> {
                    eventPublisher.publishPaymentRefunded(refundedEvent);
                    metricsHelper.recordMessageSent(orderId, SagaMetrics.TYPE_EVENT);
                }, orderId, correlationId);

                compensationPaymentCounter.increment();
                compensationDurationTimer.record(java.time.Duration.ofMillis(System.currentTimeMillis() - startTime));

                log.debug("Payment refunded for order: {}", orderId);
            } else {
                log.debug("Payment refund skipped for order: {} - current status: {}", orderId, payment.getStatus());
            }
        } finally {
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }

    private void validateOrderEvent(OrderCreatedEvent event) {
        Validators.requireNonNull(event, "OrderCreatedEvent");
        Validators.requireNonBlank(event.orderId(), "Order ID");
        Validators.requireNonNull(event.totalAmount(), "Total amount");
    }

    private String generateTransactionId() {
        return "%s%s".formatted(
            paymentProperties.transactionIdPrefix(),
            UUID.randomUUID().toString().substring(0, 8).toUpperCase()
        );
    }
}
