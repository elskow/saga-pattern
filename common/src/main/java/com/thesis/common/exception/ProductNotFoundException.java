package com.thesis.common.exception;

/**
 * Exception thrown when a product cannot be found.
 */
public class ProductNotFoundException extends ResourceNotFoundException {

    public ProductNotFoundException(String productId) {
        super("Product", productId);
    }

    public ProductNotFoundException(String productId, Throwable cause) {
        super("Product", productId, cause);
    }
}
