package com.thesis.orchestration.inventory.repository;

import com.thesis.orchestration.inventory.model.ReservationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ReservationRepository extends JpaRepository<ReservationEntity, String> {

    Optional<ReservationEntity> findByOrderId(String orderId);

    List<ReservationEntity> findByStatus(ReservationEntity.ReservationStatus status);
}
