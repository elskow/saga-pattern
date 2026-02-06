package com.thesis.choreography.inventory.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "products")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Product {

    @Id
    @Column(name = "product_id")
    private String productId;

    @Column(name = "product_name")
    private String productName;

    @Column(name = "quantity_available")
    private int quantityAvailable;

    @Column(name = "quantity_reserved")
    private int quantityReserved;

    @Column(name = "updated_at")
    private Instant updatedAt;

    @PrePersist
    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }

    public boolean canReserve(int quantity) {
        return quantityAvailable >= quantity;
    }

    public void reserve(int quantity) {
        if (!canReserve(quantity)) {
            throw new IllegalStateException("Insufficient stock for product: %s".formatted(productId));
        }
        quantityAvailable -= quantity;
        quantityReserved += quantity;
    }

    public void release(int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("Release quantity must be positive");
        }
        if (quantity > quantityReserved) {
            throw new IllegalStateException("Cannot release more than reserved for product: %s".formatted(productId));
        }
        quantityReserved -= quantity;
        quantityAvailable += quantity;
    }
}
