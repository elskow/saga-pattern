package com.thesis.common.retry;

import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;

/**
 * Custom RetryListener that tracks retry metrics for @Retryable methods.
 * <p>
 * This listener records:
 * - saga.retry.attempts: Count of retry attempts (with method and reason tags)
 * - saga.retry.exhausted: Count of retries that exhausted all attempts
 * <p>
 * Usage:
 * Register this as a bean and Spring Retry will automatically use it.
 */
@Slf4j
public class MetricsRetryListener implements RetryListener {

    private final MeterRegistry meterRegistry;
    private final String serviceName;

    public MetricsRetryListener(MeterRegistry meterRegistry, String serviceName) {
        this.meterRegistry = meterRegistry;
        this.serviceName = serviceName;
    }

    /**
     * Called after an error occurs during a retry operation.
     * Records metrics for each retry attempt.
     */
    @Override
    public <T, E extends Throwable> void onError(RetryContext context,
                                                 RetryCallback<T, E> callback, Throwable throwable) {
        String methodName = extractMethodName(context);
        String reason = classifyException(throwable);
        int retryCount = context.getRetryCount();

        log.debug("Retry attempt {} for method {} due to {}: {}",
            retryCount, methodName, reason, throwable.getMessage());

        meterRegistry.counter(SagaMetrics.RETRY_ATTEMPTS,
            SagaMetrics.TAG_SERVICE, serviceName,
            SagaMetrics.TAG_METHOD, methodName,
            SagaMetrics.TAG_RETRY_REASON, reason
        ).increment();
    }

    /**
     * Called when the retry operation closes (either success or exhausted).
     * Records exhausted metrics if all retries failed.
     */
    @Override
    public <T, E extends Throwable> void close(RetryContext context,
                                               RetryCallback<T, E> callback, Throwable throwable) {
        // If throwable is not null at close, it means all retries were exhausted
        if (throwable != null && context.getRetryCount() > 0) {
            String methodName = extractMethodName(context);
            String reason = classifyException(throwable);

            log.warn("Retry exhausted for method {} after {} attempts due to {}: {}",
                methodName, context.getRetryCount(), reason, throwable.getMessage());

            meterRegistry.counter(SagaMetrics.RETRY_EXHAUSTED,
                SagaMetrics.TAG_SERVICE, serviceName,
                SagaMetrics.TAG_METHOD, methodName,
                SagaMetrics.TAG_RETRY_REASON, reason
            ).increment();
        }
    }

    /**
     * Called before the first attempt. Can be used for logging or initialization.
     */
    @Override
    public <T, E extends Throwable> boolean open(RetryContext context,
                                                 RetryCallback<T, E> callback) {
        // Return true to proceed with retry
        return true;
    }

    /**
     * Extracts method name from retry context.
     * The context.name attribute is set by Spring Retry from the @Retryable method.
     */
    private String extractMethodName(RetryContext context) {
        Object name = context.getAttribute(RetryContext.NAME);
        if (name != null) {
            String fullName = name.toString();
            // Extract just the method name from fully qualified name
            int lastDot = fullName.lastIndexOf('.');
            if (lastDot > 0) {
                return fullName.substring(lastDot + 1);
            }
            return fullName;
        }
        return "unknown";
    }

    /**
     * Classifies exception into a reason tag for metrics.
     * This helps distinguish between different failure types.
     */
    private String classifyException(Throwable throwable) {
        if (throwable == null) {
            return SagaMetrics.RETRY_REASON_TRANSIENT_ERROR;
        }

        // Check exception class name for common patterns
        String className = throwable.getClass().getSimpleName();
        String fullClassName = throwable.getClass().getName();

        // Check for optimistic locking exceptions (Spring Data, Hibernate, JPA)
        if (className.contains("OptimisticLock") ||
            className.contains("StaleObject") ||
            fullClassName.contains("ObjectOptimisticLockingFailureException")) {
            return SagaMetrics.RETRY_REASON_OPTIMISTIC_LOCK;
        }

        // Check for Kafka-related exceptions
        if (className.contains("Kafka") || className.contains("Producer") || className.contains("Consumer")) {
            return SagaMetrics.RETRY_REASON_KAFKA_ERROR;
        }

        // Check cause
        Throwable cause = throwable.getCause();
        if (cause != null && cause != throwable) {
            return classifyException(cause);
        }

        return SagaMetrics.RETRY_REASON_TRANSIENT_ERROR;
    }
}
