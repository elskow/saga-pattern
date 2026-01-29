package com.thesis.choreography.inventory.repository;

import com.thesis.choreography.inventory.model.Product;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ProductRepository extends JpaRepository<Product, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Product> findByProductId(String productId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM Product p WHERE p.productId IN :productIds")
    List<Product> findAllByProductIdIn(@Param("productIds") List<String> productIds);
}
