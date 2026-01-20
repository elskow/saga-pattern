package com.thesis.common.command;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessPaymentCommandTest {

    @Test
    void shouldBuildProcessPaymentCommandWithAllFields() {
        // Given
        String paymentId = "PAY-123";
        String orderId = "ORDER-456";
        String customerId = "CUST-001";
        BigDecimal amount = new BigDecimal("199.99");

        // When
        ProcessPaymentCommand command = ProcessPaymentCommand.builder()
                .paymentId(paymentId)
                .orderId(orderId)
                .customerId(customerId)
                .amount(amount)
                .build();

        // Then
        assertThat(command.getPaymentId()).isEqualTo(paymentId);
        assertThat(command.getOrderId()).isEqualTo(orderId);
        assertThat(command.getCustomerId()).isEqualTo(customerId);
        assertThat(command.getAmount()).isEqualTo(amount);
    }

    @Test
    void shouldCreateProcessPaymentCommandWithNoArgsConstructor() {
        // When
        ProcessPaymentCommand command = new ProcessPaymentCommand();

        // Then
        assertThat(command.getPaymentId()).isNull();
        assertThat(command.getOrderId()).isNull();
        assertThat(command.getCustomerId()).isNull();
        assertThat(command.getAmount()).isNull();
    }

    @Test
    void shouldSupportSetters() {
        // Given
        ProcessPaymentCommand command = new ProcessPaymentCommand();

        // When
        command.setPaymentId("PAY-789");
        command.setOrderId("ORDER-123");
        command.setCustomerId("CUST-001");
        command.setAmount(new BigDecimal("50.00"));

        // Then
        assertThat(command.getPaymentId()).isEqualTo("PAY-789");
        assertThat(command.getOrderId()).isEqualTo("ORDER-123");
        assertThat(command.getCustomerId()).isEqualTo("CUST-001");
        assertThat(command.getAmount()).isEqualTo(new BigDecimal("50.00"));
    }

    @Test
    void shouldHaveEqualsAndHashCode() {
        // Given
        ProcessPaymentCommand command1 = ProcessPaymentCommand.builder()
                .paymentId("PAY-123")
                .orderId("ORDER-456")
                .customerId("CUST-001")
                .amount(new BigDecimal("100.00"))
                .build();

        ProcessPaymentCommand command2 = ProcessPaymentCommand.builder()
                .paymentId("PAY-123")
                .orderId("ORDER-456")
                .customerId("CUST-001")
                .amount(new BigDecimal("100.00"))
                .build();

        // Then
        assertThat(command1).isEqualTo(command2);
        assertThat(command1.hashCode()).isEqualTo(command2.hashCode());
    }

    @Test
    void shouldHaveToString() {
        // Given
        ProcessPaymentCommand command = ProcessPaymentCommand.builder()
                .paymentId("PAY-123")
                .orderId("ORDER-456")
                .build();

        // Then
        assertThat(command.toString()).contains("PAY-123");
        assertThat(command.toString()).contains("ORDER-456");
    }
}
