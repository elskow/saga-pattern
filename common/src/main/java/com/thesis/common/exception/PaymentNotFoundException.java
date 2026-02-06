package com.thesis.common.exception;

public class PaymentNotFoundException extends ResourceNotFoundException {

    public PaymentNotFoundException(String paymentId) {
        super("Payment", paymentId);
    }

    public PaymentNotFoundException(String paymentId, Throwable cause) {
        super("Payment", paymentId, cause);
    }
}
