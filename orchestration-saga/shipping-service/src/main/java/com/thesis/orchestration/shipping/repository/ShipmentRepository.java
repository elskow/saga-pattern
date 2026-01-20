package com.thesis.orchestration.shipping.repository;

import com.thesis.orchestration.shipping.model.ShipmentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ShipmentRepository extends JpaRepository<ShipmentEntity, String> {
    
    Optional<ShipmentEntity> findByOrderId(String orderId);
    
    List<ShipmentEntity> findByStatus(ShipmentEntity.ShipmentStatus status);
}
