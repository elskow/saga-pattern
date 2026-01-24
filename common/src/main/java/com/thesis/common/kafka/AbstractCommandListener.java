package com.thesis.common.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.metrics.SagaMetrics;
import com.thesis.common.metrics.SagaMetricsHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

@Slf4j
public abstract class AbstractCommandListener<T> {

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final Validator validator;
    private final String serviceName;
    private final SagaMetricsHelper metricsHelper;
    private final Map<String, Counter> replySuccessCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> replyFailureCounters = new ConcurrentHashMap<>();

    protected AbstractCommandListener(ObjectMapper objectMapper,
                                    KafkaTemplate<String, Object> kafkaTemplate,
                                    MeterRegistry meterRegistry,
                                    Validator validator,
                                    String serviceName) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.validator = validator;
        this.serviceName = serviceName;
        this.metricsHelper = new SagaMetricsHelper(meterRegistry, serviceName);
    }

    protected abstract String getCommandTopic();

    protected abstract String getReplyTopic();

    protected abstract Map<String, Class<? extends T>> getCommandTypeMappings();

    protected abstract String getStepName();

    protected abstract void processCommand(T command);

    protected abstract String getOrderId(T command);

    protected abstract String getCorrelationId(T command);

    public void handleCommand(String message) {
        String correlationId = UUID.randomUUID().toString();
        try {
            JsonNode node = objectMapper.readTree(message);
            String commandType = node.path("commandType").asText(null);

            if (commandType == null) {
                log.warn("{} command missing commandType: {}", getServiceName(), message);
                return;
            }

            Class<? extends T> commandClass = getCommandTypeMappings().get(commandType);
            if (commandClass == null) {
                log.warn("Unknown {} command type: {}", getServiceName(), commandType);
                return;
            }

            T command = objectMapper.readValue(message, commandClass);
            String orderId = getOrderId(command);

            if (orderId != null) {
                MDC.put("orderId", orderId);
            }
            MDC.put("correlationId", correlationId);

            if (validateCommand(command)) {
                metricsHelper.recordMessageReceived(orderId, SagaMetrics.TYPE_COMMAND);
                processCommand(command);
            }
        } catch (JsonProcessingException e) {
            log.error("JSON parsing failed for {} command: {}", getServiceName(), e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error processing {} command: {}", getServiceName(), e.getMessage(), e);
        } finally {
            MDC.clear();
        }
    }

    protected boolean validateCommand(T command) {
        Set<ConstraintViolation<T>> violations = validator.validate(command);
        if (!violations.isEmpty()) {
            log.error("Invalid {} command. Violations: {}", getServiceName(), violations);
            return false;
        }
        return true;
    }

    protected void sendReply(String message, String orderId, String replyType) {
        try {
            MDC.put("orderId", orderId != null ? orderId : "unknown");
            kafkaTemplate.send(getReplyTopic(), orderId, message)
                .thenAccept(result -> getReplySuccessCounter(replyType).increment())
                .exceptionally(ex -> {
                    log.error("Failed to send {} reply for order {}: {}", replyType, orderId, ex.getMessage(), ex);
                    getReplyFailureCounter(replyType).increment();
                    return null;
                });
        } catch (Exception e) {
            log.error("Exception sending {} reply for order {}: {}", replyType, orderId, e.getMessage(), e);
            getReplyFailureCounter(replyType).increment();
        } finally {
            MDC.clear();
        }
    }

    protected String getServiceName() {
        return serviceName;
    }

    protected Counter getReplySuccessCounter(String replyType) {
        return replySuccessCounters.computeIfAbsent(replyType, type ->
            meterRegistry.counter(
                SagaMetrics.SAGA_MESSAGES_TOTAL,
                "service", serviceName,
                "direction", SagaMetrics.DIRECTION_SENT,
                "messageType", SagaMetrics.TYPE_REPLY,
                "outcome", SagaMetrics.OUTCOME_SUCCESS,
                "replyType", type
            )
        );
    }

    protected Counter getReplyFailureCounter(String replyType) {
        return replyFailureCounters.computeIfAbsent(replyType, type ->
            meterRegistry.counter(
                SagaMetrics.SAGA_MESSAGES_TOTAL,
                "service", serviceName,
                "direction", SagaMetrics.DIRECTION_SENT,
                "messageType", SagaMetrics.TYPE_REPLY,
                "outcome", SagaMetrics.OUTCOME_FAILURE,
                "replyType", type
            )
        );
    }
}
