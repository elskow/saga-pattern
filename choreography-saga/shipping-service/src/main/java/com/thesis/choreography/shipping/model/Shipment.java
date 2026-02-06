package com.thesis.choreography.shipping.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "shipments", indexes = {
    @Index(name = "idx_shipment_order_id", columnList = "order_id"),
    @Index(name = "idx_shipment_status", columnList = "status")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Shipment {

    @Id
    @Column(name = "shipping_id")
    private String shippingId;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "tracking_number")
    private String trackingNumber;

    @Column(name = "shipping_address")
    private String shippingAddress;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private ShippingStatus status;

    @Column(name = "estimated_delivery")
    private Instant estimatedDelivery;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
        updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }

    public boolean isCancellable() {
        return status == ShippingStatus.SCHEDULED || status == ShippingStatus.PENDING;
    }

    public enum ShippingStatus {
        PENDING,
        SCHEDULED,
        IN_TRANSIT,
        DELIVERED,
        CANCELLED,
        FAILED
    }
}
