package com.thesis.orchestration.order.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Outbox entry for reliable command delivery.
 */
@Entity
@Table(name = "outbox_commands", indexes = {
        @Index(name = "idx_outbox_commands_status_created", columnList = "status, createdAt"),
        @Index(name = "idx_outbox_commands_order_id", columnList = "orderId")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxCommand {

    @Id
    private String outboxId;

    @Column(nullable = false)
    private String orderId;

    @Column(nullable = false)
    private String commandType;

    @Column(nullable = false)
    private String topic;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String payloadJson;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false)
    private int attempts;

    private LocalDateTime lastAttemptAt;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    /**
     * Version field for optimistic locking.
     * Prevents concurrent updates from overwriting each other.
     */
    @Version
    private Long version;
}
