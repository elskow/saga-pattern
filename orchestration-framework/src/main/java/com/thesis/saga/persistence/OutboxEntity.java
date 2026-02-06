package com.thesis.saga.persistence;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "saga_outbox", indexes = {
    @Index(name = "idx_saga_outbox_status_created", columnList = "status, createdAt"),
    @Index(name = "idx_saga_outbox_saga_id", columnList = "sagaId"),
    @Index(name = "idx_saga_outbox_saga_type", columnList = "sagaType"),
    @Index(name = "idx_saga_outbox_status_last_attempt", columnList = "status, lastAttemptAt")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEntity {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_SENT = "SENT";
    public static final String STATUS_FAILED = "FAILED";

    @Id
    private String id;

    @Column(nullable = false)
    private String sagaId;

    @Column(nullable = false, length = 100)
    private String sagaType;

    @Column(nullable = false, length = 100)
    private String commandType;

    @Column(nullable = false)
    private String topic;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String payloadJson;

    @Column(nullable = false, length = 20)
    private String status;

    @Column(nullable = false)
    private int attempts;

    @Column(length = 1000)
    private String lastError;

    private LocalDateTime lastAttemptAt;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    @Version
    private Long version;

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
        if (status == null) {
            status = STATUS_PENDING;
        }
    }

    public void recordAttempt() {
        this.attempts++;
        this.lastAttemptAt = LocalDateTime.now();
    }

    public void markSent() {
        this.status = STATUS_SENT;
        this.lastAttemptAt = LocalDateTime.now();
    }

    public void markFailed(String error) {
        this.status = STATUS_FAILED;
        this.lastError = error != null && error.length() > 1000 ? error.substring(0, 1000) : error;
        this.lastAttemptAt = LocalDateTime.now();
    }
}
