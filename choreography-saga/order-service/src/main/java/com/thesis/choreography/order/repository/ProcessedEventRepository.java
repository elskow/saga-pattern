package com.thesis.choreography.order.repository;

import com.thesis.choreography.order.model.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Repository;

import java.time.Instant;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {
    boolean existsByEventId(String eventId);
    
    @Modifying
    int deleteByProcessedAtBefore(Instant cutoff);
}
