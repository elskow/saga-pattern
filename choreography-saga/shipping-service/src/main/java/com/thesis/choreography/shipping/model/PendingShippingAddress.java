package com.thesis.choreography.shipping.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Entity to store pending shipping addresses while waiting for inventory reservation.
 * This replaces the in-memory ConcurrentHashMap to ensure data persistence
 * across service restarts and for reliable saga execution during load testing.
 */
@Entity
@Table(name = "pending_shipping_addresses", indexes = {
    @Index(name = "idx_pending_shipping_addresses_order_id", columnList = "order_id"),
    @Index(name = "idx_pending_shipping_addresses_created_at", columnList = "created_at")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PendingShippingAddress {

    @Id
    @Column(name = "order_id")
    private String orderId;

    @Column(name = "shipping_address", nullable = false)
    private String shippingAddress;

    @Column(name = "created_at")
    private Instant createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
    }
}
