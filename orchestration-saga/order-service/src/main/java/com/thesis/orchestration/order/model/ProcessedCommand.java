package com.thesis.orchestration.order.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Entity for tracking processed commands for idempotency.
 * Prevents duplicate command processing.
 */
@Entity
@Table(name = "processed_commands")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedCommand {

    @Id
    private String commandId;

    @Column(nullable = false)
    private String orderId;

    @Column(nullable = false)
    private String commandType;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false)
    private LocalDateTime processedAt;
}
