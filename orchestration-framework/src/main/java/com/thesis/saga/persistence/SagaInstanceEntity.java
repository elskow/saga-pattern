package com.thesis.saga.persistence;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "saga_instances", indexes = {
    @Index(name = "idx_saga_instances_saga_id", columnList = "sagaId"),
    @Index(name = "idx_saga_instances_saga_type", columnList = "sagaType"),
    @Index(name = "idx_saga_instances_current_state", columnList = "currentState"),
    @Index(name = "idx_saga_instances_updated_at", columnList = "updatedAt"),
    @Index(name = "idx_saga_instances_type_state", columnList = "sagaType, currentState")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaInstanceEntity {

    @Id
    private String id;

    @Column(nullable = false)
    private String sagaId;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(nullable = false, length = 100)
    private String sagaType;

    @Column(nullable = false, length = 50)
    private String currentState;

    @Column(columnDefinition = "TEXT")
    private String contextJson;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @Version
    private Long version;

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
        if (updatedAt == null) {
            updatedAt = LocalDateTime.now();
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
