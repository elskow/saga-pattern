package com.thesis.orchestration.inventory.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "inventory_reservations", indexes = {
        @Index(name = "idx_reservations_order_id", columnList = "orderId"),
        @Index(name = "idx_reservations_status", columnList = "status")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReservationEntity {

    @Id
    private String reservationId;

    @Column(nullable = false)
    private String orderId;

    @Column(length = 2000)
    private String itemsJson;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ReservationStatus status;

    private String failureReason;
    private String releaseReason;

    private Instant createdAt;
    private Instant reservedAt;
    private Instant releasedAt;

    public enum ReservationStatus {
        PENDING, RESERVED, RELEASED, FAILED
    }

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
    }
}
