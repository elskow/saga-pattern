package com.thesis.saga.persistence;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "saga_processed_messages", indexes = {
    @Index(name = "idx_processed_msg_saga_id", columnList = "sagaId"),
    @Index(name = "idx_processed_msg_type", columnList = "messageType"),
    @Index(name = "idx_processed_msg_processed_at", columnList = "processedAt")
}, uniqueConstraints = {
    @UniqueConstraint(name = "uk_processed_msg_saga_type", columnNames = {"sagaId", "messageType", "messageId"})
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedMessageEntity {

    public static final String TYPE_COMMAND = "COMMAND";
    public static final String TYPE_REPLY = "REPLY";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String sagaId;

    @Column(nullable = false, length = 100)
    private String sagaType;

    @Column(nullable = false, length = 20)
    private String messageType;

    @Column(nullable = false, length = 100)
    private String messageId;

    @Column(nullable = false)
    private LocalDateTime processedAt;

    @PrePersist
    protected void onCreate() {
        if (processedAt == null) {
            processedAt = LocalDateTime.now();
        }
    }

    public static ProcessedMessageEntity forCommand(String sagaId, String sagaType, String commandType) {
        return ProcessedMessageEntity.builder()
                .sagaId(sagaId)
                .sagaType(sagaType)
                .messageType(TYPE_COMMAND)
                .messageId(commandType)
                .processedAt(LocalDateTime.now())
                .build();
    }

    public static ProcessedMessageEntity forReply(String sagaId, String sagaType, String replyType) {
        return ProcessedMessageEntity.builder()
                .sagaId(sagaId)
                .sagaType(sagaType)
                .messageType(TYPE_REPLY)
                .messageId(replyType)
                .processedAt(LocalDateTime.now())
                .build();
    }
}
