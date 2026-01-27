package com.thesis.orchestration.inventory.repository;

import com.thesis.orchestration.inventory.model.ProductEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProductRepository extends JpaRepository<ProductEntity, String> {
    List<ProductEntity> findAllByProductIdIn(List<String> productIds);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM ProductEntity p WHERE p.productId IN :productIds")
    List<ProductEntity> findAllByIdInForUpdate(@Param("productIds") List<String> productIds);
}
