package com.thesis.common.util;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Slf4j
public final class TransactionHelper {

    private TransactionHelper() {}

    public static void executeAfterCommit(Runnable action, String orderId, String correlationId) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                new AfterCommitSynchronization(() -> executeWithMdc(action, orderId, correlationId))
            );
        } else {
            action.run();
        }
    }

    public static void executeAfterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                new AfterCommitSynchronization(action)
            );
        } else {
            action.run();
        }
    }

    public static void executeWithMdc(Runnable action, String orderId, String correlationId) {
        try {
            if (orderId != null) MDC.put("orderId", orderId);
            if (correlationId != null) MDC.put("correlationId", correlationId);
            action.run();
        } catch (Exception e) {
            log.error("Failed to execute action for order: {}", orderId, e);
            throw e;
        } finally {
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }

    private record AfterCommitSynchronization(Runnable action) implements TransactionSynchronization {
        @Override
        public void afterCommit() {
            action.run();
        }
    }
}
