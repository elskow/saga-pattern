package com.thesis.orchestration.order.repository;

import com.thesis.orchestration.order.model.ProcessedCommand;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repository for idempotency tracking.
 */
@Repository
public interface ProcessedCommandRepository extends JpaRepository<ProcessedCommand, String> {

    /**
     * Check if a command has already been processed.
     */
    boolean existsByCommandIdAndStatus(String commandId, String status);

    /**
     * Find commands by status for retry handling.
     */
    List<ProcessedCommand> findByStatus(String status);

    /**
     * Atomically inserts a command record if it doesn't exist, or does nothing if it already exists.
     * Uses PostgreSQL INSERT...ON CONFLICT DO NOTHING for true atomicity.
     * Returns the number of rows affected (1 if inserted, 0 if already existed).
     *
     * @param commandId   unique command identifier
     * @param orderId     order identifier
     * @param commandType type of command
     * @param status      initial status (e.g., PENDING)
     * @return 1 if inserted, 0 if already existed
     */
    @Modifying
    @Query(value = "INSERT INTO processed_commands (command_id, order_id, command_type, status, processed_at) " +
        "VALUES (:commandId, :orderId, :commandType, :status, NOW()) " +
        "ON CONFLICT (command_id) DO NOTHING",
        nativeQuery = true)
    int insertIfNotExists(@Param("commandId") String commandId,
                          @Param("orderId") String orderId,
                          @Param("commandType") String commandType,
                          @Param("status") String status);
}
