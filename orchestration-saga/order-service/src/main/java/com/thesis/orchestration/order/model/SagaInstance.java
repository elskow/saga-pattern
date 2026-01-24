package com.thesis.orchestration.order.model;

import java.time.LocalDateTime;
import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Entity for persisting saga instance state.
 * Enables saga recovery after orchestrator restart.
 */
@Entity
@Table(name = "saga_instances")
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
}
