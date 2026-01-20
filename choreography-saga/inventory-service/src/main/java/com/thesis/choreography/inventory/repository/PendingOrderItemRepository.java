package com.thesis.choreography.inventory.repository;

import com.thesis.choreography.inventory.model.PendingOrderItem;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PendingOrderItemRepository extends JpaRepository<PendingOrderItem, Long> {
    
    List<PendingOrderItem> findByOrderId(String orderId);
    
    void deleteByOrderId(String orderId);
}
