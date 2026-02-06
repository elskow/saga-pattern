package com.thesis.saga.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.exception.SagaSerializationException;
import com.thesis.saga.persistence.OutboxEntity;
import com.thesis.saga.persistence.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class OutboxService {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional(propagation = Propagation.MANDATORY)
    public String enqueue(String sagaId, String sagaType, String commandType, String topic, Object command) {
        try {
            String payloadJson = objectMapper.writeValueAsString(command);
            String outboxId = UUID.randomUUID().toString();

            OutboxEntity entity = OutboxEntity.builder()
                    .id(outboxId)
                    .sagaId(sagaId)
                    .sagaType(sagaType)
                    .commandType(commandType)
                    .topic(topic)
                    .payloadJson(payloadJson)
                    .status(OutboxEntity.STATUS_PENDING)
                    .attempts(0)
                    .createdAt(LocalDateTime.now())
                    .build();

            outboxRepository.save(entity);

            log.trace("Enqueued command {} for saga {} to topic {}", commandType, sagaId, topic);
            return outboxId;

        } catch (JsonProcessingException e) {
            log.error("Failed to serialize command {} for saga {}: {}", commandType, sagaId, e.getMessage());
            throw new SagaSerializationException("Failed to serialize command payload", e);
        }
    }

    @Transactional
    public void markSent(String outboxId) {
        int updated = outboxRepository.markSent(outboxId, LocalDateTime.now());
        if (updated == 0) log.warn("No outbox entry found to mark as sent: {}", outboxId);
    }

    @Transactional
    public void markFailed(String outboxId, String error) {
        String truncatedError = error != null && error.length() > 1000 ? error.substring(0, 1000) : error;
        int updated = outboxRepository.markFailed(outboxId, LocalDateTime.now(), truncatedError);
        if (updated == 0) log.warn("No outbox entry found to mark as failed: {}", outboxId);
    }

    @Transactional
    public void cleanupForSaga(String sagaId, String sagaType) {
        int deleted = outboxRepository.deleteSentBySaga(sagaId, sagaType);
        log.trace("Cleaned up {} outbox entries for saga {}", deleted, sagaId);
    }

    @Transactional
    public int cleanupOldSent(LocalDateTime olderThan) {
        int deleted = outboxRepository.deleteOldSent(olderThan);
        if (deleted > 0) {
            log.debug("Cleaned up {} old outbox entries", deleted);
        }
        return deleted;
    }

    public long getPendingCount() {
        return outboxRepository.countPending();
    }
}
