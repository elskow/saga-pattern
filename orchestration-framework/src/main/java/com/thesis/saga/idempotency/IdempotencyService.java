package com.thesis.saga.idempotency;

import com.thesis.saga.persistence.ProcessedMessageEntity;
import com.thesis.saga.persistence.ProcessedMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@Slf4j
@RequiredArgsConstructor
public class IdempotencyService {

    private final ProcessedMessageRepository processedMessageRepository;

    public boolean isReplyProcessed(String sagaId, String sagaType, String replyType) {
        return processedMessageRepository.existsBySagaIdAndSagaTypeAndMessageTypeAndMessageId(
                sagaId, sagaType, ProcessedMessageEntity.TYPE_REPLY, replyType);
    }

    public boolean isCommandSent(String sagaId, String sagaType, String commandType) {
        return processedMessageRepository.existsBySagaIdAndSagaTypeAndMessageTypeAndMessageId(
                sagaId, sagaType, ProcessedMessageEntity.TYPE_COMMAND, commandType);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public boolean tryMarkReplyProcessed(String sagaId, String sagaType, String replyType) {
        int inserted = processedMessageRepository.insertIfNotExists(
                sagaId, sagaType, ProcessedMessageEntity.TYPE_REPLY, replyType, LocalDateTime.now());

        if (inserted == 0) {
            log.trace("Reply {} already processed for saga {} (duplicate)", replyType, sagaId);
            return false;
        }

        log.trace("Marked reply {} as processed for saga {}", replyType, sagaId);
        return true;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public boolean tryMarkCommandSent(String sagaId, String sagaType, String commandType) {
        int inserted = processedMessageRepository.insertIfNotExists(
                sagaId, sagaType, ProcessedMessageEntity.TYPE_COMMAND, commandType, LocalDateTime.now());

        if (inserted == 0) {
            log.trace("Command {} already sent for saga {} (duplicate)", commandType, sagaId);
            return false;
        }

        log.trace("Marked command {} as sent for saga {}", commandType, sagaId);
        return true;
    }

    @Transactional
    public void cleanupForSaga(String sagaId, String sagaType) {
        int deleted = processedMessageRepository.deleteBySagaIdAndSagaType(sagaId, sagaType);
        log.trace("Cleaned up {} processed message records for saga {}", deleted, sagaId);
    }

    @Transactional
    public int cleanupOldEntries(LocalDateTime olderThan) {
        int deleted = processedMessageRepository.deleteOldEntries(olderThan);
        if (deleted > 0) {
            log.debug("Cleaned up {} old processed message records", deleted);
        }
        return deleted;
    }
}
