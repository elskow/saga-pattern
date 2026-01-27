package com.thesis.orchestration.order.repository;

import com.thesis.orchestration.order.model.ProcessedReply;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

/**
 * Repository for reply idempotency tracking.
 * Prevents duplicate reply processing when Kafka redelivers messages.
 */
@Repository
public interface ProcessedReplyRepository extends JpaRepository<ProcessedReply, String> {

    /**
     * Check if a reply has already been processed.
     * 
     * @param replyId the unique reply identifier (format: orderId:replyType)
     * @return true if the reply has already been processed
     */
    boolean existsByReplyId(String replyId);

    /**
     * Atomically insert a processed reply if it doesn't exist.
     * Uses INSERT...ON CONFLICT DO NOTHING for atomic idempotency.
     * 
     * @param replyId the unique reply identifier
     * @param orderId the order identifier
     * @param replyType the type of reply
     * @param processedAt the timestamp when processed
     * @return 1 if inserted (new reply), 0 if already exists (duplicate)
     */
    @Modifying
    @Query(value = "INSERT INTO processed_replies (reply_id, order_id, reply_type, processed_at) " +
                   "VALUES (:replyId, :orderId, :replyType, :processedAt) " +
                   "ON CONFLICT (reply_id) DO NOTHING",
           nativeQuery = true)
    int insertIfNotExists(@Param("replyId") String replyId,
                          @Param("orderId") String orderId,
                          @Param("replyType") String replyType,
                          @Param("processedAt") LocalDateTime processedAt);

    /**
     * Delete old processed replies before the given timestamp.
     * Used by cleanup scheduler to prevent unbounded table growth.
     * 
     * @param before the cutoff timestamp
     * @return the number of deleted records
     */
    long deleteByProcessedAtBefore(LocalDateTime before);
}
