package com.thesis.choreography.payment.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Entity to track processed events for idempotency.
 * Prevents duplicate processing of Kafka messages.
 */
@Entity
@Table(name = "processed_events", indexes = {
    @Index(name = "idx_processed_events_event_id", columnList = "event_id", unique = true)
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    @PrePersist
    protected void onCreate() {
        processedAt = Instant.now();
    }
}
