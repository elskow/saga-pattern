package com.thesis.saga.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessageEntity, Long> {

    boolean existsBySagaIdAndSagaTypeAndMessageTypeAndMessageId(
            String sagaId, String sagaType, String messageType, String messageId);

    @Modifying
    @Query(value = "INSERT INTO saga_processed_messages (saga_id, saga_type, message_type, message_id, processed_at) " +
           "VALUES (:sagaId, :sagaType, :messageType, :messageId, :processedAt) " +
           "ON CONFLICT (saga_id, message_type, message_id) DO NOTHING",
           nativeQuery = true)
    int insertIfNotExists(
            @Param("sagaId") String sagaId,
            @Param("sagaType") String sagaType,
            @Param("messageType") String messageType,
            @Param("messageId") String messageId,
            @Param("processedAt") LocalDateTime processedAt);

    @Modifying
    @Query("DELETE FROM ProcessedMessageEntity p WHERE p.sagaId = :sagaId AND p.sagaType = :sagaType")
    int deleteBySagaIdAndSagaType(@Param("sagaId") String sagaId, @Param("sagaType") String sagaType);

    @Modifying
    @Query("DELETE FROM ProcessedMessageEntity p WHERE p.processedAt < :cutoff")
    int deleteOldEntries(@Param("cutoff") LocalDateTime cutoff);

    long countBySagaIdAndSagaType(String sagaId, String sagaType);
}
