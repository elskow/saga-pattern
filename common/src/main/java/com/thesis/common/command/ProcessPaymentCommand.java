package com.thesis.common.command;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessPaymentCommand {
    @NotBlank(message = "commandType must not be blank")
    private String commandType;
    
    @NotNull(message = "paymentId must not be null")
    private String paymentId;
    
    @NotNull(message = "orderId must not be null")
    private String orderId;
    
    @NotNull(message = "customerId must not be null")
    private String customerId;
    
    @NotNull(message = "amount must not be null")
    @Positive(message = "amount must be positive")
    private BigDecimal amount;
}
