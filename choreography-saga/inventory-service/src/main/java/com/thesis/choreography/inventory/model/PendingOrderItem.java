package com.thesis.choreography.inventory.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Entity to store pending order items while waiting for payment completion.
 * This replaces the in-memory ConcurrentHashMap to ensure data persistence
 * across service restarts and for reliable saga execution during load testing.
 */
@Entity
@Table(name = "pending_order_items", indexes = {
    @Index(name = "idx_pending_order_items_order_id", columnList = "order_id"),
    @Index(name = "idx_pending_order_items_created_at", columnList = "created_at")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PendingOrderItem {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    @Column(name = "created_at")
    private Instant createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
    }
}
