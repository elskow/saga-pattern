package com.thesis.choreography.inventory.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "inventory_reservations", indexes = {
    @Index(name = "idx_reservations_order_id", columnList = "order_id"),
    @Index(name = "idx_reservations_status", columnList = "status")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReservation {
    
    @Id
    @Column(name = "reservation_id")
    private String reservationId;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private ReservationStatus status;

    @Column(name = "created_at")
    private Instant createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
    }

    public enum ReservationStatus {
        RESERVED,
        RELEASED,
        CONFIRMED
    }
}
