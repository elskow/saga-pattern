package com.thesis.orchestration.inventory.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "inventory_reservations", indexes = {
    @Index(name = "idx_reservations_status", columnList = "status")
}, uniqueConstraints = {
    @UniqueConstraint(name = "uk_reservations_order_id", columnNames = "orderId")
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

    @Version
    private Long version;

    private Instant createdAt;
    private Instant reservedAt;
    private Instant releasedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
    }

    public enum ReservationStatus {
        PENDING, RESERVED, RELEASED, FAILED
    }
}
