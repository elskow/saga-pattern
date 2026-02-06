package com.thesis.common.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderResponse(
    String orderId,
    String customerId,
    String status,
    List<OrderItemResponse> items,
    BigDecimal totalAmount,
    Instant createdAt,
    Instant updatedAt
) {

    public record OrderItemResponse(
        String productId,
        String productName,
        int quantity,
        BigDecimal price
    ) {
        public static OrderItemResponse of(String productId, String productName, int quantity, BigDecimal price) {
            return new OrderItemResponse(productId, productName, quantity, price);
        }
    }

    public static OrderResponse of(String orderId, String customerId, String status,
                                   List<OrderItemResponse> items, BigDecimal totalAmount,
                                   Instant createdAt, Instant updatedAt) {
        return new OrderResponse(orderId, customerId, status, items, totalAmount, createdAt, updatedAt);
    }
}
