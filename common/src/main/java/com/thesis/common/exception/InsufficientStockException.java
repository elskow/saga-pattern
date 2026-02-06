package com.thesis.common.exception;

import lombok.Getter;

@Getter
public class InsufficientStockException extends RuntimeException {

    private final String productId;
    private final int requestedQuantity;
    private final int availableQuantity;

    public InsufficientStockException(String productId, int requestedQuantity, int availableQuantity) {
        super("Insufficient stock for product %s: requested %d, available %d"
            .formatted(productId, requestedQuantity, availableQuantity));
        this.productId = productId;
        this.requestedQuantity = requestedQuantity;
        this.availableQuantity = availableQuantity;
    }

}
