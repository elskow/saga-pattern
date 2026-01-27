package com.thesis.orchestration.order.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Entity for tracking processed replies for idempotency.
 * Prevents duplicate reply processing when Kafka redelivers messages.
 * 
 * The replyId is constructed as: orderId + ":" + replyType
 * For example: "order-123:PAYMENT_SUCCESS" or "order-123:INVENTORY_RESERVED"
 */
@Entity
@Table(name = "processed_replies", indexes = {
    @Index(name = "idx_processed_replies_order_id", columnList = "orderId")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedReply {

    /**
     * Unique identifier for the reply, constructed as orderId:replyType
     */
    @Id
    private String replyId;

    @Column(nullable = false)
    private String orderId;

    /**
     * Type of reply (e.g., PAYMENT_SUCCESS, PAYMENT_FAILED, INVENTORY_RESERVED, etc.)
     */
    @Column(nullable = false)
    private String replyType;

    /**
     * Timestamp when the reply was processed
     */
    @Column(nullable = false)
    private LocalDateTime processedAt;
}
