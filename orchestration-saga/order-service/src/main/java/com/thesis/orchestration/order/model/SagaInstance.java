package com.thesis.orchestration.order.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Entity for persisting saga instance state.
 * Enables saga recovery after orchestrator restart.
 */
@Entity
@Table(name = "saga_instances", indexes = {
    @Index(name = "idx_saga_instances_order_id", columnList = "orderId"),
    @Index(name = "idx_saga_instances_current_state", columnList = "currentState"),
    @Index(name = "idx_saga_instances_updated_at", columnList = "updatedAt")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaInstance {

    @Id
    private String sagaId;

    @Column(nullable = false)
    private String orderId;

    @Column(nullable = false)
    private String currentState;

    @Column(columnDefinition = "TEXT")
    private String sagaDataJson;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    /**
     * Version field for optimistic locking.
     * Prevents concurrent updates from overwriting each other.
     */
    @Version
    private Long version;
}
