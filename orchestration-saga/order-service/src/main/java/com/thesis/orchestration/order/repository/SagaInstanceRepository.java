package com.thesis.orchestration.order.repository;

import com.thesis.orchestration.order.model.SagaInstance;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository for saga instance persistence.
 */
@Repository
public interface SagaInstanceRepository extends JpaRepository<SagaInstance, String> {

    /**
     * Find saga instance by order ID.
     */
    Optional<SagaInstance> findByOrderId(String orderId);

    /**
     * Find stale sagas in a specific state that haven't been updated within the timeout period.
     */
    List<SagaInstance> findByCurrentStateAndUpdatedAtBefore(String state, LocalDateTime before);

    /**
     * Find all active sagas (not completed or cancelled).
     */
    List<SagaInstance> findByCurrentStateNotIn(List<String> terminalStates);

    /**
     * Find sagas that are stuck (not in terminal state and haven't been updated within timeout).
     */
    List<SagaInstance> findByCurrentStateNotInAndUpdatedAtBefore(List<String> terminalStates, Instant before);
}
