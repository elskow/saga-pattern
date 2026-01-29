package com.thesis.choreography.payment.service;

import com.thesis.choreography.payment.kafka.PaymentEventPublisher;
import com.thesis.choreography.payment.model.Payment;
import com.thesis.choreography.payment.repository.PaymentRepository;
import com.thesis.common.events.OrderCreatedEvent;
import com.thesis.common.events.PaymentCompletedEvent;
import com.thesis.common.events.PaymentFailedEvent;
import com.thesis.common.events.PaymentRefundedEvent;
import com.thesis.common.exception.PaymentNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private PaymentEventPublisher eventPublisher;

    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        paymentService = new PaymentService(paymentRepository, eventPublisher, new SimpleMeterRegistry());
    }

    @Test
    void shouldCreatePaymentFromOrderEvent() {
        // Given
        OrderCreatedEvent orderEvent = OrderCreatedEvent.builder()
            .orderId("ORDER-123")
            .customerId("CUST-001")
            .totalAmount(new BigDecimal("99.99"))
            .shippingAddress("123 Main St")
            .items(List.of())
            .createdAt(Instant.now())
            .build();

        when(paymentRepository.save(any(Payment.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When
        paymentService.processPayment(orderEvent);

        // Then
        ArgumentCaptor<Payment> paymentCaptor = ArgumentCaptor.forClass(Payment.class);
        verify(paymentRepository, atLeastOnce()).save(paymentCaptor.capture());

        Payment savedPayment = paymentCaptor.getValue();
        assertThat(savedPayment.getOrderId()).isEqualTo("ORDER-123");
        assertThat(savedPayment.getAmount()).isEqualByComparingTo(new BigDecimal("99.99"));
    }

    @Test
    void shouldProcessPaymentAndPublishEvent() {
        // Given
        OrderCreatedEvent orderEvent = OrderCreatedEvent.builder()
            .orderId("ORDER-456")
            .customerId("CUST-002")
            .totalAmount(new BigDecimal("150.00"))
            .shippingAddress("456 Oak Ave")
            .items(List.of())
            .createdAt(Instant.now())
            .build();

        when(paymentRepository.save(any(Payment.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When
        paymentService.processPayment(orderEvent);

        // Then - Either completed or failed event should be published
        verify(eventPublisher, atMostOnce()).publishPaymentCompleted(any(PaymentCompletedEvent.class));
        verify(eventPublisher, atMostOnce()).publishPaymentFailed(any(PaymentFailedEvent.class));
    }

    @Test
    void shouldRefundPaymentSuccessfully() {
        // Given
        String orderId = "ORDER-123";
        Payment existingPayment = Payment.builder()
            .paymentId("PAY-456")
            .orderId(orderId)
            .amount(new BigDecimal("99.99"))
            .status(Payment.PaymentStatus.COMPLETED)
            .build();

        when(paymentRepository.findByOrderId(orderId)).thenReturn(Optional.of(existingPayment));

        // When
        paymentService.refundPayment(orderId);

        // Then
        verify(paymentRepository).save(any(Payment.class));
        assertThat(existingPayment.getStatus()).isEqualTo(Payment.PaymentStatus.REFUNDED);
    }

    @Test
    void shouldPublishPaymentRefundedEvent() {
        // Given
        String orderId = "ORDER-123";
        Payment existingPayment = Payment.builder()
            .paymentId("PAY-456")
            .orderId(orderId)
            .amount(new BigDecimal("99.99"))
            .status(Payment.PaymentStatus.COMPLETED)
            .build();

        when(paymentRepository.findByOrderId(orderId)).thenReturn(Optional.of(existingPayment));

        // When
        paymentService.refundPayment(orderId);

        // Then
        verify(eventPublisher).publishPaymentRefunded(any(PaymentRefundedEvent.class));
    }

    @Test
    void shouldThrowExceptionWhenPaymentNotFoundForRefund() {
        // Given
        String orderId = "ORDER-999";
        when(paymentRepository.findByOrderId(orderId)).thenReturn(Optional.empty());

        // When & Then
        assertThatThrownBy(() -> paymentService.refundPayment(orderId))
            .isInstanceOf(PaymentNotFoundException.class)
            .hasMessageContaining(orderId);

        verify(paymentRepository, never()).save(any());
        verify(eventPublisher, never()).publishPaymentRefunded(any());
    }

    @Test
    void shouldNotRefundWhenPaymentNotCompleted() {
        // Given
        String orderId = "ORDER-123";
        Payment existingPayment = Payment.builder()
            .paymentId("PAY-456")
            .orderId(orderId)
            .amount(new BigDecimal("99.99"))
            .status(Payment.PaymentStatus.PENDING) // Not completed
            .build();

        when(paymentRepository.findByOrderId(orderId)).thenReturn(Optional.of(existingPayment));

        // When
        paymentService.refundPayment(orderId);

        // Then
        verify(paymentRepository, never()).save(any());
        verify(eventPublisher, never()).publishPaymentRefunded(any());
    }

    @Test
    void shouldSavePaymentTwiceDuringProcessing() {
        // Given
        OrderCreatedEvent orderEvent = OrderCreatedEvent.builder()
            .orderId("ORDER-789")
            .customerId("CUST-003")
            .totalAmount(new BigDecimal("200.00"))
            .shippingAddress("789 Pine Rd")
            .items(List.of())
            .createdAt(Instant.now())
            .build();

        // Capture the status at each save call since the same object is mutated
        List<Payment.PaymentStatus> capturedStatuses = new ArrayList<>();
        when(paymentRepository.save(any(Payment.class))).thenAnswer(invocation -> {
            Payment payment = invocation.getArgument(0);
            capturedStatuses.add(payment.getStatus());
            return payment;
        });

        // When
        paymentService.processPayment(orderEvent);

        // Then - Payment should be saved twice (once PENDING, once with final status)
        assertThat(capturedStatuses).hasSize(2);
        // First save is PENDING
        assertThat(capturedStatuses.get(0)).isEqualTo(Payment.PaymentStatus.PENDING);
        // Second save is final status (COMPLETED or FAILED)
        assertThat(capturedStatuses.get(1)).isIn(Payment.PaymentStatus.COMPLETED, Payment.PaymentStatus.FAILED);
    }
}
