package com.thesis.orchestration.payment.repository;

import com.thesis.orchestration.payment.model.PaymentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PaymentRepository extends JpaRepository<PaymentEntity, String> {
    
    Optional<PaymentEntity> findByOrderId(String orderId);
    
    List<PaymentEntity> findByCustomerId(String customerId);
    
    List<PaymentEntity> findByStatus(PaymentEntity.PaymentStatus status);
}
