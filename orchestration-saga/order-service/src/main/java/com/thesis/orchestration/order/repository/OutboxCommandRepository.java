package com.thesis.orchestration.order.repository;

import com.thesis.orchestration.order.model.OutboxCommand;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OutboxCommandRepository extends JpaRepository<OutboxCommand, String> {
    List<OutboxCommand> findByStatus(String status);

    List<OutboxCommand> findByStatusAndLastAttemptAtBefore(String status, LocalDateTime before);

    java.util.Optional<OutboxCommand> findByOrderIdAndCommandTypeAndStatus(String orderId, String commandType, String status);

    long deleteByStatusInAndCreatedAtBefore(List<String> statuses, LocalDateTime before);
}
