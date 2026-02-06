package com.thesis.common.command;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessPaymentCommandTest {

    @Test
    void shouldCreateProcessPaymentCommandWithAllFields() {
        // Given
        String commandType = "PROCESS_PAYMENT";
        String paymentId = "PAY-123";
        String orderId = "ORDER-456";
        String customerId = "CUST-001";
        BigDecimal amount = new BigDecimal("199.99");

        // When
        ProcessPaymentCommand command = new ProcessPaymentCommand(
                commandType, paymentId, orderId, customerId, amount);

        // Then
        assertThat(command.commandType()).isEqualTo(commandType);
        assertThat(command.paymentId()).isEqualTo(paymentId);
        assertThat(command.orderId()).isEqualTo(orderId);
        assertThat(command.customerId()).isEqualTo(customerId);
        assertThat(command.amount()).isEqualTo(amount);
    }

    @Test
    void shouldCreateProcessPaymentCommandUsingFactoryMethod() {
        // Given
        String commandType = "PROCESS_PAYMENT";
        String paymentId = "PAY-789";
        String orderId = "ORDER-123";
        String customerId = "CUST-001";
        BigDecimal amount = new BigDecimal("50.00");

        // When
        ProcessPaymentCommand command = ProcessPaymentCommand.of(
                commandType, paymentId, orderId, customerId, amount);

        // Then
        assertThat(command.commandType()).isEqualTo(commandType);
        assertThat(command.paymentId()).isEqualTo(paymentId);
        assertThat(command.orderId()).isEqualTo(orderId);
        assertThat(command.customerId()).isEqualTo(customerId);
        assertThat(command.amount()).isEqualTo(amount);
    }

    @Test
    void shouldHaveEqualsAndHashCode() {
        // Given
        ProcessPaymentCommand command1 = new ProcessPaymentCommand(
                "PROCESS_PAYMENT", "PAY-123", "ORDER-456", "CUST-001", new BigDecimal("100.00"));

        ProcessPaymentCommand command2 = new ProcessPaymentCommand(
                "PROCESS_PAYMENT", "PAY-123", "ORDER-456", "CUST-001", new BigDecimal("100.00"));

        // Then
        assertThat(command1).isEqualTo(command2);
        assertThat(command1.hashCode()).isEqualTo(command2.hashCode());
    }

    @Test
    void shouldHaveToString() {
        // Given
        ProcessPaymentCommand command = new ProcessPaymentCommand(
                "PROCESS_PAYMENT", "PAY-123", "ORDER-456", "CUST-001", new BigDecimal("100.00"));

        // Then
        assertThat(command.toString()).contains("PAY-123");
        assertThat(command.toString()).contains("ORDER-456");
    }
}
