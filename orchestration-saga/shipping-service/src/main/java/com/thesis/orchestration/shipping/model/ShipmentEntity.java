package com.thesis.orchestration.shipping.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "shipments")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShipmentEntity {

    @Id
    private String shipmentId;

    @Version
    private Long version;

    @Column(nullable = false)
    private String orderId;

    @Column(nullable = false)
    private String shippingAddress;

    private String trackingNumber;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ShipmentStatus status;

    private String failureReason;
    private String cancellationReason;

    private Instant createdAt;
    private Instant scheduledAt;
    private Instant cancelledAt;

    public enum ShipmentStatus {
        PENDING, SCHEDULED, CANCELLED, FAILED
    }

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
    }
}
