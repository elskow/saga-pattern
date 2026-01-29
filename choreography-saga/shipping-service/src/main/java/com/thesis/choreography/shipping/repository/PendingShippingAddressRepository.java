package com.thesis.choreography.shipping.repository;

import com.thesis.choreography.shipping.model.PendingShippingAddress;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;

@Repository
public interface PendingShippingAddressRepository extends JpaRepository<PendingShippingAddress, String> {

    Optional<PendingShippingAddress> findByOrderId(String orderId);

    void deleteByOrderId(String orderId);

    @Modifying
    @Query("DELETE FROM PendingShippingAddress p WHERE p.createdAt < :cutoff")
    int deleteByCreatedAtBefore(@Param("cutoff") Instant cutoff);
}
