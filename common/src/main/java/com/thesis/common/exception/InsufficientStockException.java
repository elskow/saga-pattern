package com.thesis.common.exception;

import lombok.Getter;

/**
 * Exception thrown when there is insufficient stock to fulfill a reservation.
 */
@Getter
public class InsufficientStockException extends RuntimeException {

    private final String productId;
    private final int requestedQuantity;
    private final int availableQuantity;

    public InsufficientStockException(String productId, int requestedQuantity, int availableQuantity) {
        super(String.format("Insufficient stock for product %s: requested %d, available %d",
            productId, requestedQuantity, availableQuantity));
        this.productId = productId;
        this.requestedQuantity = requestedQuantity;
        this.availableQuantity = availableQuantity;
    }

}
