package com.thesis.saga.persistence;

import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface SagaInstanceRepository extends JpaRepository<SagaInstanceEntity, String> {

    @Cacheable(value = "sagaInstances", key = "#sagaId + ':' + #sagaType", unless = "#result == null")
    Optional<SagaInstanceEntity> findBySagaIdAndSagaType(String sagaId, String sagaType);

    @Query("SELECT s FROM SagaInstanceEntity s WHERE s.sagaType = :sagaType AND s.currentState NOT IN :terminalStates")
    List<SagaInstanceEntity> findActiveSagas(
            @Param("sagaType") String sagaType,
            @Param("terminalStates") List<String> terminalStates);

    @Query("SELECT s FROM SagaInstanceEntity s WHERE s.sagaType = :sagaType " +
           "AND s.currentState NOT IN :terminalStates AND s.updatedAt < :cutoff")
    List<SagaInstanceEntity> findStaleSagas(
            @Param("sagaType") String sagaType,
            @Param("terminalStates") List<String> terminalStates,
            @Param("cutoff") LocalDateTime cutoff);

    @Modifying
    @CacheEvict(value = "sagaInstances", key = "#sagaId + ':' + #sagaType")
    @Query("UPDATE SagaInstanceEntity s SET s.currentState = :newState, s.contextJson = :contextJson, " +
           "s.updatedAt = :updatedAt WHERE s.sagaId = :sagaId AND s.sagaType = :sagaType")
    int updateState(
            @Param("sagaId") String sagaId,
            @Param("sagaType") String sagaType,
            @Param("newState") String newState,
            @Param("contextJson") String contextJson,
            @Param("updatedAt") LocalDateTime updatedAt);

    @Modifying
    @CacheEvict(value = "sagaInstances", key = "#sagaId + ':' + #sagaType")
    @Query("DELETE FROM SagaInstanceEntity s WHERE s.sagaId = :sagaId AND s.sagaType = :sagaType")
    @org.springframework.transaction.annotation.Transactional
    int deleteBySagaIdAndSagaType(@Param("sagaId") String sagaId, @Param("sagaType") String sagaType);

    boolean existsBySagaIdAndSagaType(String sagaId, String sagaType);

    @Query("SELECT COUNT(s) FROM SagaInstanceEntity s WHERE s.sagaType = :sagaType AND s.currentState NOT IN :terminalStates")
    long countActiveSagas(@Param("sagaType") String sagaType, @Param("terminalStates") List<String> terminalStates);

    @Modifying
    @Query("DELETE FROM SagaInstanceEntity s WHERE s.sagaType = :sagaType " +
           "AND s.currentState IN :terminalStates AND s.updatedAt < :cutoff")
    int deleteOldCompletedSagas(
            @Param("sagaType") String sagaType,
            @Param("terminalStates") List<String> terminalStates,
            @Param("cutoff") LocalDateTime cutoff);
}
