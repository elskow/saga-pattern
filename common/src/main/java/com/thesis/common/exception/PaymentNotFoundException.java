package com.thesis.common.exception;

/**
 * Exception thrown when a payment cannot be found.
 */
public class PaymentNotFoundException extends ResourceNotFoundException {

    public PaymentNotFoundException(String paymentId) {
        super("Payment", paymentId);
    }

    public PaymentNotFoundException(String paymentId, Throwable cause) {
        super("Payment", paymentId, cause);
    }
}
