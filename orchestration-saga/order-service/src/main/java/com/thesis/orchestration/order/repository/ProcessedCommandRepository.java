package com.thesis.orchestration.order.repository;

import com.thesis.orchestration.order.model.ProcessedCommand;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

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
    java.util.List<ProcessedCommand> findByStatus(String status);
}
