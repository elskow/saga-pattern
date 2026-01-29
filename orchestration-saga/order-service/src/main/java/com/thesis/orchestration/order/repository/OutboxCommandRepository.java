package com.thesis.orchestration.order.repository;

import com.thesis.orchestration.order.model.OutboxCommand;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface OutboxCommandRepository extends JpaRepository<OutboxCommand, String> {
    List<OutboxCommand> findByStatus(String status);

    List<OutboxCommand> findByStatusAndLastAttemptAtBefore(String status, LocalDateTime before);

    Optional<OutboxCommand> findByOrderIdAndCommandTypeAndStatus(String orderId, String commandType, String status);

    long deleteByStatusInAndCreatedAtBefore(List<String> statuses, LocalDateTime before);

    /**
     * Finds pending outbox commands with pessimistic write lock to prevent concurrent processing.
     * Uses SKIP LOCKED to allow other instances to process different commands.
     */
    @Query(value = "SELECT * FROM outbox_commands o WHERE o.status = :status " +
        "AND (o.last_attempt_at IS NULL OR o.last_attempt_at < :cutoff) " +
        "FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<OutboxCommand> findPendingWithLock(@Param("status") String status, @Param("cutoff") LocalDateTime cutoff);

    /**
     * Finds a single outbox command by ID with pessimistic lock for safe update.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT o FROM OutboxCommand o WHERE o.outboxId = :id")
    Optional<OutboxCommand> findByIdWithLock(@Param("id") String id);
}
