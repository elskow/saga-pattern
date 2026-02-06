package com.thesis.common.exception;

public class ProductNotFoundException extends ResourceNotFoundException {

    public ProductNotFoundException(String productId) {
        super("Product", productId);
    }

    public ProductNotFoundException(String productId, Throwable cause) {
        super("Product", productId, cause);
    }
}
