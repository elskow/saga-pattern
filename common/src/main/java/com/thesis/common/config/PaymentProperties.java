package com.thesis.common.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;

@ConfigurationProperties(prefix = "app.payment")
@Validated
public record PaymentProperties(
    @NotBlank String transactionIdPrefix,
    BigDecimal maxPaymentAmount
) {
    public PaymentProperties {
        if (transactionIdPrefix == null || transactionIdPrefix.isBlank()) transactionIdPrefix = "TXN-";
        if (maxPaymentAmount == null || maxPaymentAmount.compareTo(BigDecimal.ZERO) <= 0) {
            maxPaymentAmount = new BigDecimal("10000.00");
        }
    }

    public PaymentProperties() {
        this("TXN-", new BigDecimal("10000.00"));
    }
}
