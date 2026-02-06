package com.thesis.saga.participant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.exception.SagaCommandProcessingException;
import com.thesis.saga.metrics.SagaMetricsRecorder;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractSagaParticipant {

    protected final KafkaTemplate<String, Object> kafkaTemplate;
    protected final ObjectMapper objectMapper;
    protected final Validator validator;
    protected final SagaMetricsRecorder metricsRecorder;
    protected final String serviceName;
    protected final String replyTopic;

    private final Map<String, HandlerEntry<?>> handlers = new ConcurrentHashMap<>();
    private volatile boolean validatorWarningLogged = false;

    protected AbstractSagaParticipant(
            KafkaTemplate<String, Object> kafkaTemplate,
            ObjectMapper objectMapper,
            Validator validator,
            SagaMetricsRecorder metricsRecorder,
            String serviceName,
            String replyTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.metricsRecorder = metricsRecorder;
        this.serviceName = serviceName;
        this.replyTopic = replyTopic;
    }

    protected <C> void registerHandler(String commandType, Class<C> commandClass,
                                       CommandHandler<C, ?> handler) {
        handlers.put(commandType, new HandlerEntry<>(commandClass, handler));
        log.trace("Registered handler for command type: {}", commandType);
    }

    protected void handleCommand(String message) {
        String correlationId = UUID.randomUUID().toString();
        String sagaId = null;
        String commandType = null;

        try {
            JsonNode node = objectMapper.readTree(message);
            commandType = node.path("commandType").asText(null);
            sagaId = extractSagaId(node);

            if (sagaId != null) {
                MDC.put("sagaId", sagaId);
            }
            MDC.put("correlationId", correlationId);
            MDC.put("service", serviceName);

            if (commandType == null) {
                log.warn("{} received command without commandType: {}", serviceName, truncate(message));
                return;
            }

            HandlerEntry<?> entry = handlers.get(commandType);
            if (entry == null) {
                log.warn("{} received unknown command type: {}", serviceName, commandType);
                return;
            }

            processCommand(message, commandType, sagaId, entry);

        } catch (JsonProcessingException e) {
            log.error("{} failed to parse command JSON: {}", serviceName, e.getMessage());
        } catch (Exception e) {
            log.error("{} unexpected error processing command: {}", serviceName, e.getMessage(), e);
            throw new SagaCommandProcessingException("Command processing failed", e);
        } finally {
            MDC.remove("sagaId");
            MDC.remove("commandType");
        }
    }

    @SuppressWarnings("unchecked")
    private <C> void processCommand(String message, String commandType, String sagaId,
                                    HandlerEntry<C> entry) throws JsonProcessingException {
        C command = objectMapper.readValue(message, entry.commandClass());

        String validationError = validateCommand(command);
        if (validationError != null) {
            log.warn("{} command {} validation failed: {}", serviceName, commandType, validationError);
            Object failureReply = createValidationFailureReply(command, commandType, validationError);
            sendReply(sagaId, failureReply);
            metricsRecorder.recordReplyReceived(serviceName, commandType, false);
            return;
        }

        log.trace("{} processing command {}", serviceName, commandType);

        try {
            Object reply = entry.handler().handle(command);
            sendReply(sagaId, reply);
            metricsRecorder.recordReplyReceived(serviceName, commandType, true);

            log.trace("{} successfully processed command {}", serviceName, commandType);

        } catch (Exception e) {
            log.error("{} error processing command {}: {}", serviceName, commandType, e.getMessage(), e);
            Object errorReply = createErrorReply(command, commandType, e.getMessage());
            sendReply(sagaId, errorReply);
            metricsRecorder.recordReplyReceived(serviceName, commandType, false);
        }
    }

    protected String extractSagaId(JsonNode node) {
        if (node.has("orderId")) {
            return node.path("orderId").asText(null);
        }
        if (node.has("sagaId")) {
            return node.path("sagaId").asText(null);
        }
        return null;
    }

    protected <C> String validateCommand(C command) {
        if (validator == null) {
            if (!validatorWarningLogged) {
                validatorWarningLogged = true;
                log.warn("{} has no validator configured - skipping command validation. " +
                        "Consider adding spring-boot-starter-validation dependency.", serviceName);
            }
            return null;
        }

        Set<ConstraintViolation<C>> violations = validator.validate(command);
        if (violations.isEmpty()) {
            return null;
        }

        return violations.stream()
                .map(v -> "%s: %s".formatted(v.getPropertyPath(), v.getMessage()))
                .reduce((a, b) -> "%s; %s".formatted(a, b))
                .orElse("Validation failed");
    }

    protected void sendReply(String sagaId, Object reply) {
        try {
            kafkaTemplate.send(replyTopic, sagaId, reply)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("{} failed to send reply for saga {}: {}",
                                    serviceName, sagaId, ex.getMessage());
                        } else {
                            log.trace("{} sent reply for saga {} to {}",
                                    serviceName, sagaId, replyTopic);
                        }
                    });
        } catch (Exception e) {
            log.error("{} exception sending reply for saga {}: {}",
                    serviceName, sagaId, e.getMessage());
            throw new SagaCommandProcessingException("Failed to send reply", e);
        }
    }

    protected abstract Object createValidationFailureReply(Object command, String commandType, String error);

    protected abstract Object createErrorReply(Object command, String commandType, String error);

    private String truncate(String s) {
        return s != null && s.length() > 200 ? s.substring(0, 200) + "..." : s;
    }

    @SuppressWarnings("rawtypes")
    private record HandlerEntry<C>(Class<C> commandClass, CommandHandler handler) {}
}
