package com.thesis.common.retry;

import com.thesis.common.metrics.SagaMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;

@Slf4j
public class MetricsRetryListener implements RetryListener {

    private final MeterRegistry meterRegistry;
    private final String serviceName;

    public MetricsRetryListener(MeterRegistry meterRegistry, String serviceName) {
        this.meterRegistry = meterRegistry;
        this.serviceName = serviceName;
    }

    @Override
    public <T, E extends Throwable> void onError(RetryContext context,
                                                 RetryCallback<T, E> callback, Throwable throwable) {
        String methodName = extractMethodName(context);
        String reason = classifyException(throwable);
        int retryCount = context.getRetryCount();

        log.warn("Retry attempt {} for method {} due to {}: {}",
            retryCount, methodName, reason, throwable.getMessage());

        meterRegistry.counter(SagaMetrics.RETRY_ATTEMPTS,
            SagaMetrics.TAG_SERVICE, serviceName,
            SagaMetrics.TAG_METHOD, methodName,
            SagaMetrics.TAG_RETRY_REASON, reason
        ).increment();
    }

    @Override
    public <T, E extends Throwable> void close(RetryContext context,
                                               RetryCallback<T, E> callback, Throwable throwable) {
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

    @Override
    public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
        return true;
    }

    private String extractMethodName(RetryContext context) {
        Object name = context.getAttribute(RetryContext.NAME);
        if (name != null) {
            String fullName = name.toString();
            int lastDot = fullName.lastIndexOf('.');
            return lastDot > 0 ? fullName.substring(lastDot + 1) : fullName;
        }
        return "unknown";
    }

    private String classifyException(Throwable throwable) {
        if (throwable == null) {
            return SagaMetrics.RETRY_REASON_TRANSIENT_ERROR;
        }

        String className = throwable.getClass().getSimpleName();
        String fullClassName = throwable.getClass().getName();

        if (className.contains("OptimisticLock") ||
            className.contains("StaleObject") ||
            fullClassName.contains("ObjectOptimisticLockingFailureException")) {
            return SagaMetrics.RETRY_REASON_OPTIMISTIC_LOCK;
        }

        if (className.contains("Kafka") || className.contains("Producer") || className.contains("Consumer")) {
            return SagaMetrics.RETRY_REASON_KAFKA_ERROR;
        }

        Throwable cause = throwable.getCause();
        if (cause != null && cause != throwable) {
            return classifyException(cause);
        }

        return SagaMetrics.RETRY_REASON_TRANSIENT_ERROR;
    }
}
