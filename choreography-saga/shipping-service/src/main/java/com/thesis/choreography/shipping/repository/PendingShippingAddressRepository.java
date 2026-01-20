package com.thesis.choreography.shipping.repository;

import com.thesis.choreography.shipping.model.PendingShippingAddress;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PendingShippingAddressRepository extends JpaRepository<PendingShippingAddress, String> {
    
    Optional<PendingShippingAddress> findByOrderId(String orderId);
    
    void deleteByOrderId(String orderId);
}
