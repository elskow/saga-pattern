package com.thesis.saga.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import jakarta.persistence.LockModeType;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEntity, String> {

    @Query(value = "SELECT * FROM saga_outbox o WHERE o.status = :status " +
           "AND (o.last_attempt_at IS NULL OR o.last_attempt_at < :cutoff) " +
           "ORDER BY o.created_at ASC LIMIT :limit FOR UPDATE SKIP LOCKED",
           nativeQuery = true)
    List<OutboxEntity> findPendingWithLock(
            @Param("status") String status,
            @Param("cutoff") LocalDateTime cutoff,
            @Param("limit") int limit);

    @Query("SELECT o FROM OutboxEntity o WHERE o.status = :status " +
           "AND (o.lastAttemptAt IS NULL OR o.lastAttemptAt < :cutoff) " +
           "ORDER BY o.createdAt ASC")
    List<OutboxEntity> findPending(
            @Param("status") String status,
            @Param("cutoff") LocalDateTime cutoff);

    List<OutboxEntity> findBySagaIdAndSagaTypeOrderByCreatedAtAsc(String sagaId, String sagaType);

    @Modifying
    @Query("UPDATE OutboxEntity o SET o.status = :newStatus, o.attempts = o.attempts + 1, " +
           "o.lastAttemptAt = :attemptAt WHERE o.id = :id")
    int updateStatus(@Param("id") String id, @Param("newStatus") String newStatus,
                     @Param("attemptAt") LocalDateTime attemptAt);

    @Modifying
    @Query("UPDATE OutboxEntity o SET o.status = 'SENT', o.lastAttemptAt = :attemptAt WHERE o.id = :id")
    int markSent(@Param("id") String id, @Param("attemptAt") LocalDateTime attemptAt);

    @Modifying
    @Query("UPDATE OutboxEntity o SET o.status = 'FAILED', o.attempts = o.attempts + 1, " +
           "o.lastAttemptAt = :attemptAt, o.lastError = :error WHERE o.id = :id")
    int markFailed(@Param("id") String id, @Param("attemptAt") LocalDateTime attemptAt, @Param("error") String error);

    @Modifying
    @Query("DELETE FROM OutboxEntity o WHERE o.status = 'SENT' AND o.lastAttemptAt < :cutoff")
    int deleteOldSent(@Param("cutoff") LocalDateTime cutoff);

    @Modifying
    @Query("DELETE FROM OutboxEntity o WHERE o.sagaId = :sagaId AND o.sagaType = :sagaType AND o.status = 'SENT'")
    int deleteSentBySaga(@Param("sagaId") String sagaId, @Param("sagaType") String sagaType);

    @Query("SELECT COUNT(o) FROM OutboxEntity o WHERE o.status = 'PENDING'")
    long countPending();

    @Query("SELECT COUNT(o) FROM OutboxEntity o WHERE o.status = :status AND o.sagaType = :sagaType")
    long countByStatusAndSagaType(@Param("status") String status, @Param("sagaType") String sagaType);
}
