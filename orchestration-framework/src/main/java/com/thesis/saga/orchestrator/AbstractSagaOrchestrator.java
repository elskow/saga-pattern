package com.thesis.saga.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.common.exception.SagaSerializationException;
import com.thesis.saga.config.SagaFrameworkProperties;
import com.thesis.saga.core.SagaContext;
import com.thesis.saga.core.SagaDefinition;
import com.thesis.saga.core.SagaStep;
import com.thesis.saga.idempotency.IdempotencyService;
import com.thesis.saga.metrics.SagaMetricsRecorder;
import com.thesis.saga.outbox.OutboxService;
import com.thesis.saga.persistence.SagaInstanceEntity;
import com.thesis.saga.persistence.SagaInstanceRepository;
import com.thesis.saga.scheduler.SagaTimeoutScheduler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public abstract class AbstractSagaOrchestrator<S extends Enum<S>, E extends Enum<E>, C extends SagaContext> {

    protected final SagaDefinition<S, E, C> definition;
    protected final SagaInstanceRepository instanceRepository;
    protected final OutboxService outboxService;
    protected final IdempotencyService idempotencyService;
    protected final ObjectMapper objectMapper;
    protected final SagaMetricsRecorder metricsRecorder;
    protected final SagaFrameworkProperties properties;
    protected final SagaStateMachineFactory<S, E, C> stateMachineFactory;

    private final Map<String, SagaStateMachine<S, E>> stateMachines = new ConcurrentHashMap<>();
    private final Map<String, C> contextMap = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> sagaLocks = new ConcurrentHashMap<>();
    private final Map<String, Instant> sagaStartTimes = new ConcurrentHashMap<>();
    private final Map<String, Instant> stepStartTimes = new ConcurrentHashMap<>();

    private SagaTimeoutScheduler timeoutScheduler;

    protected AbstractSagaOrchestrator(
            SagaDefinition<S, E, C> definition,
            SagaInstanceRepository instanceRepository,
            OutboxService outboxService,
            IdempotencyService idempotencyService,
            ObjectMapper objectMapper,
            SagaMetricsRecorder metricsRecorder,
            SagaFrameworkProperties properties) {
        this.definition = definition;
        this.instanceRepository = instanceRepository;
        this.outboxService = outboxService;
        this.idempotencyService = idempotencyService;
        this.objectMapper = objectMapper;
        this.metricsRecorder = metricsRecorder;
        this.properties = properties;
        this.stateMachineFactory = new SagaStateMachineFactory<>(definition);
    }

    @Autowired(required = false)
    public void setTimeoutScheduler(SagaTimeoutScheduler timeoutScheduler) {
        this.timeoutScheduler = timeoutScheduler;
    }

    @Transactional
    public void startSaga(String sagaId, C context) {
        ReentrantLock lock = getSagaLock(sagaId);
        lock.lock();
        try {
            MDC.put("sagaId", sagaId);
            MDC.put("sagaType", definition.getSagaType());

            log.debug("Starting saga {} of type {}", sagaId, definition.getSagaType());

            onSagaStarting(sagaId, context);

            SagaStep<S, E, C> firstStep = definition.getSteps().getFirst();
            S firstStepState = firstStep.getState();

            persistSagaInstance(sagaId, firstStepState.name(), context);
            stepStartTimes.put(sagaId, Instant.now());
            enqueueStepCommand(sagaId, firstStep, context);

            SagaStateMachine<S, E> sm = stateMachineFactory.create(sagaId);
            sm.start();
            sm.sendEvent(definition.getStartEvent());

            stateMachines.put(sagaId, sm);
            contextMap.put(sagaId, context);
            sagaStartTimes.put(sagaId, Instant.now());

            metricsRecorder.recordSagaStarted(definition.getSagaType());
            log.debug("Saga {} started, awaiting {} reply", sagaId, firstStep.getName());

        } finally {
            MDC.remove("sagaId");
            MDC.remove("sagaType");
            lock.unlock();
        }
    }

    @Transactional
    public void processReply(String sagaId, String replyType, Object reply,
                             E successEvent, E failureEvent, boolean isSuccess) {
        ReentrantLock lock = getSagaLock(sagaId);
        lock.lock();
        try {
            MDC.put("sagaId", sagaId);
            MDC.put("sagaType", definition.getSagaType());

            if (!idempotencyService.tryMarkReplyProcessed(sagaId, definition.getSagaType(), replyType)) {
                log.trace("Reply {} already processed for saga {} (duplicate)", replyType, sagaId);
                metricsRecorder.recordDuplicateReply(definition.getSagaType(), replyType);
                return;
            }

            SagaStateMachine<S, E> sm = getOrRecoverStateMachine(sagaId);
            C context = getOrRecoverContext(sagaId);

            if (sm == null || context == null) {
                log.error("Cannot process reply, saga {} not found or recovery failed", sagaId);
                return;
            }

            S currentState = sm.getCurrentState();
            log.trace("Processing {} reply {} for saga {} in state {}",
                    isSuccess ? "success" : "failure", replyType, sagaId, currentState);

            updateContextFromReply(context, reply, isSuccess);
            context.touch();

            E event = isSuccess ? successEvent : failureEvent;
            sm.sendEvent(event);

            S newState = sm.getCurrentState();

            persistSagaState(sagaId, newState.name(), context);
            contextMap.put(sagaId, context);

            String stepName = getStepNameForReply(replyType);
            recordStepDuration(sagaId, stepName);

            if (definition.isTerminalState(newState)) {
                handleTerminalState(sagaId, sm, context, isSuccess);
            } else if (newState.equals(definition.getCompensatingState())) {
                handleCompensationStart(sagaId, context);
            } else {
                handleNextStep(sagaId, newState, context);
            }

            metricsRecorder.recordReplyReceived(definition.getSagaType(),
                    stepName, isSuccess);

        } finally {
            MDC.remove("sagaId");
            MDC.remove("sagaType");
            lock.unlock();
        }
    }

    @Transactional
    public void processCompensationReply(String sagaId, String replyType) {
        ReentrantLock lock = getSagaLock(sagaId);
        lock.lock();
        try {
            MDC.put("sagaId", sagaId);
            MDC.put("sagaType", definition.getSagaType());

            if (!idempotencyService.tryMarkReplyProcessed(sagaId, definition.getSagaType(), replyType)) {
                log.trace("Compensation reply {} already processed for saga {}", replyType, sagaId);
                return;
            }

            C context = getOrRecoverContext(sagaId);
            if (context == null) {
                log.error("Cannot process compensation reply, context not found for saga {}", sagaId);
                return;
            }

            context.incrementCompletedCompensations();
            context.touch();

            log.trace("Saga {} compensation progress: {}/{}",
                    sagaId, context.getCompletedCompensations(), context.getExpectedCompensations());

            if (context.isCompensationComplete()) {
                completeCompensation(sagaId, context);
            } else {
                persistSagaState(sagaId, definition.getCompensatingState().name(), context);
                contextMap.put(sagaId, context);
            }

            metricsRecorder.recordCompensationReplyReceived(definition.getSagaType(),
                    getStepNameForReply(replyType));

        } finally {
            MDC.remove("sagaId");
            MDC.remove("sagaType");
            lock.unlock();
        }
    }

    protected abstract void onSagaStarting(String sagaId, C context);

    protected abstract void updateContextFromReply(C context, Object reply, boolean success);

    protected abstract void onSagaCompleted(String sagaId, C context);

    protected abstract void onSagaFailed(String sagaId, C context, String reason);

    protected abstract String getStepNameForReply(String replyType);

    private void handleTerminalState(String sagaId, SagaStateMachine<S, E> sm, C context, boolean success) {
        if (success) {
            log.debug("Saga {} completed successfully", sagaId);
            onSagaCompleted(sagaId, context);
        } else {
            log.debug("Saga {} terminated with failure (no compensation needed)", sagaId);
            onSagaFailed(sagaId, context, "First step failed");
        }

        recordSagaDuration(sagaId);  
        cleanupSaga(sagaId);
        metricsRecorder.recordSagaCompleted(definition.getSagaType(), success);
    }

    private void handleCompensationStart(String sagaId, C context) {
        log.debug("Starting compensation for saga {}", sagaId);
        metricsRecorder.recordCompensationStarted(definition.getSagaType());

        onSagaFailed(sagaId, context, "Step failed, compensating");

        List<SagaStep<S, E, C>> compensationSteps = definition.getCompensationSteps(context);
        int expectedCompensations = compensationSteps.size();

        log.trace("Saga {} needs {} compensation steps", sagaId, expectedCompensations);
        context.setExpectedCompensations(expectedCompensations);

        if (expectedCompensations == 0) {
            completeCompensation(sagaId, context);
            return;
        }

        for (SagaStep<S, E, C> step : compensationSteps) {
            enqueueCompensationCommand(sagaId, step, context);
        }

        persistSagaState(sagaId, definition.getCompensatingState().name(), context);
        contextMap.put(sagaId, context);
    }

    private void handleNextStep(String sagaId, S currentState, C context) {
        definition.getStepForState(currentState)
            .ifPresentOrElse(
                step -> {
                    log.trace("Saga {} advancing to step {}", sagaId, step.getName());
                    enqueueStepCommand(sagaId, step, context);
                },
                () -> log.warn("No step found for state {} in saga {}", currentState, sagaId)
            );
    }

    private void completeCompensation(String sagaId, C context) {
        log.debug("Compensation complete for saga {}", sagaId);

        Optional.ofNullable(stateMachines.get(sagaId))
            .ifPresent(sm -> sm.sendEvent(definition.getCompensationCompleteEvent()));

        recordSagaDuration(sagaId);
        cleanupSaga(sagaId);
        metricsRecorder.recordCompensationCompleted(definition.getSagaType());
        metricsRecorder.recordSagaCompleted(definition.getSagaType(), false);
    }

    private void recordStepDuration(String sagaId, String stepName) {
        Optional.ofNullable(stepStartTimes.get(sagaId))
            .map(startTime -> Duration.between(startTime, Instant.now()))
            .ifPresent(duration -> metricsRecorder.recordStepDuration(definition.getSagaType(), stepName, duration));
        stepStartTimes.put(sagaId, Instant.now());
    }

    private void enqueueStepCommand(String sagaId, SagaStep<S, E, C> step, C context) {
        if (!idempotencyService.tryMarkCommandSent(sagaId, definition.getSagaType(), step.getCommandType())) {
            log.trace("Command {} already sent for saga {}", step.getCommandType(), sagaId);
            metricsRecorder.recordDuplicateCommand(definition.getSagaType(), step.getCommandType());
            return;
        }

        Object command = step.getCommandFactory().apply(context);
        outboxService.enqueue(sagaId, definition.getSagaType(),
                step.getCommandType(), step.getCommandTopic(), command);

        log.trace("Enqueued command {} for saga {} to topic {}",
                step.getCommandType(), sagaId, step.getCommandTopic());
        metricsRecorder.recordCommandSent(definition.getSagaType(), step.getName());

    }

    private void enqueueCompensationCommand(String sagaId, SagaStep<S, E, C> step, C context) {
        step.getCompensationCommandType().ifPresent(compensationType -> {
            if (!idempotencyService.tryMarkCommandSent(sagaId, definition.getSagaType(), compensationType)) {
                log.trace("Compensation command {} already sent for saga {}", compensationType, sagaId);
                return;
            }

            step.getCompensationCommandFactory()
                .map(f -> f.apply(context))
                .ifPresent(command -> {
                    String topic = step.getCompensationTopic().orElse(step.getCommandTopic());
                    outboxService.enqueue(sagaId, definition.getSagaType(), compensationType, topic, command);

                    log.trace("Enqueued compensation command {} for saga {}", compensationType, sagaId);
                    metricsRecorder.recordCompensationCommandSent(definition.getSagaType(), step.getName());
                });
        });
    }

    private void persistSagaInstance(String sagaId, String state, C context) {
        try {
            String contextJson = objectMapper.writeValueAsString(context);
            SagaInstanceEntity entity = SagaInstanceEntity.builder()
                    .id("%s:%s".formatted(sagaId, definition.getSagaType()))
                    .orderId(context.getSagaId())
                    .sagaId(sagaId)
                    .sagaType(definition.getSagaType())
                    .currentState(state)
                    .contextJson(contextJson)
                    .createdAt(LocalDateTime.now())
                    .updatedAt(LocalDateTime.now())
                    .build();
            instanceRepository.save(entity);
        } catch (JsonProcessingException e) {
            throw new SagaSerializationException("Failed to serialize context", e);
        }
    }

    private void persistSagaState(String sagaId, String state, C context) {
        try {
            String contextJson = objectMapper.writeValueAsString(context);
            instanceRepository.updateState(sagaId, definition.getSagaType(),
                    state, contextJson, LocalDateTime.now());
        } catch (JsonProcessingException e) {
            throw new SagaSerializationException("Failed to serialize context", e);
        }
    }

    @PostConstruct
    public void initialize() {
        if (timeoutScheduler != null) {
            List<String> terminalStateNames = definition.getTerminalStates().stream()
                    .map(Enum::name)
                    .toList();

            timeoutScheduler.registerOrchestrator(
                    definition.getSagaType(),
                    terminalStateNames,
                    instance -> handleTimeout(instance.getSagaId()));

            log.info("Registered {} with timeout scheduler", definition.getSagaType());
        }

        recoverActiveSagas();
    }

    @PreDestroy
    public void shutdown() {
        if (timeoutScheduler != null) {
            timeoutScheduler.unregisterOrchestrator(definition.getSagaType());
            log.info("Unregistered {} from timeout scheduler", definition.getSagaType());
        }

        for (SagaStateMachine<S, E> sm : stateMachines.values()) {
            try {
                sm.stop();
            } catch (Exception e) {
                log.warn("Error stopping state machine: {}", e.getMessage());
            }
        }
    }

    private void recoverActiveSagas() {
        List<String> terminalStateNames = definition.getTerminalStates().stream()
                .map(Enum::name)
                .toList();

        List<SagaInstanceEntity> activeSagas = instanceRepository.findActiveSagas(
                definition.getSagaType(), terminalStateNames);

        log.info("Recovering {} active sagas of type {}", activeSagas.size(), definition.getSagaType());

        for (SagaInstanceEntity instance : activeSagas) {
            try {
                recoverSaga(instance);
                metricsRecorder.recordSagaRecovered(definition.getSagaType());
            } catch (Exception e) {
                log.error("Failed to recover saga {}: {}", instance.getSagaId(), e.getMessage(), e);
            }
        }
    }

    private void recoverSaga(SagaInstanceEntity instance) {
        String sagaId = instance.getSagaId();
        log.trace("Recovering saga {} in state {}", sagaId, instance.getCurrentState());

        C context = deserializeContext(instance.getContextJson());
        if (context == null) {
            log.error("Failed to deserialize context for saga {}", sagaId);
            return;
        }

        SagaStateMachine<S, E> sm = stateMachineFactory.create(sagaId);
        sm.start();

        S targetState = Enum.valueOf(definition.getStateClass(), instance.getCurrentState());
        sm.restoreToState(targetState);

        stateMachines.put(sagaId, sm);
        contextMap.put(sagaId, context);
        sagaStartTimes.put(sagaId, Instant.now());

        log.debug("Recovered saga {} in state {}", sagaId, targetState);
    }

    private SagaStateMachine<S, E> getOrRecoverStateMachine(String sagaId) {
        return Optional.ofNullable(stateMachines.get(sagaId))
            .orElseGet(() -> {
                instanceRepository.findBySagaIdAndSagaType(sagaId, definition.getSagaType())
                    .ifPresent(this::recoverSaga);
                return stateMachines.get(sagaId);
            });
    }

    private C getOrRecoverContext(String sagaId) {
        return Optional.ofNullable(contextMap.get(sagaId))
            .orElseGet(() -> {
                instanceRepository.findBySagaIdAndSagaType(sagaId, definition.getSagaType())
                    .ifPresent(this::recoverSaga);
                return contextMap.get(sagaId);
            });
    }

    @SuppressWarnings("unchecked")
    private C deserializeContext(String json) {
        try {
            return objectMapper.readValue(json, definition.getContextClass());
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize context: {}", e.getMessage());
            return null;
        }
    }

    private void cleanupSaga(String sagaId) {
        Optional.ofNullable(stateMachines.remove(sagaId))
            .ifPresent(SagaStateMachine::stop);

        contextMap.remove(sagaId);
        sagaStartTimes.remove(sagaId);
        stepStartTimes.remove(sagaId);

        instanceRepository.deleteBySagaIdAndSagaType(sagaId, definition.getSagaType());
        idempotencyService.cleanupForSaga(sagaId, definition.getSagaType());
        outboxService.cleanupForSaga(sagaId, definition.getSagaType());

        sagaLocks.remove(sagaId);
    }

    private void recordSagaDuration(String sagaId) {
        Optional.ofNullable(sagaStartTimes.get(sagaId))
            .map(startTime -> Duration.between(startTime, Instant.now()))
            .ifPresent(duration -> metricsRecorder.recordSagaDuration(definition.getSagaType(), duration));
    }

    public void handleTimeout(String sagaId) {
        ReentrantLock lock = getSagaLock(sagaId);
        lock.lock();
        try {
            MDC.put("sagaId", sagaId);
            log.warn("Handling timeout for saga {}", sagaId);

            C context = getOrRecoverContext(sagaId);
            if (context == null) {
                return;
            }

            SagaStateMachine<S, E> sm = getOrRecoverStateMachine(sagaId);
            if (sm == null) {
                return;
            }

            metricsRecorder.recordSagaTimeout(definition.getSagaType());

            handleCompensationStart(sagaId, context);

        } finally {
            MDC.remove("sagaId");
            MDC.remove("sagaType");
            lock.unlock();
        }
    }

    @Scheduled(fixedDelayString = "${saga.framework.in-memory-cleanup-interval:60000}")
    public void cleanupStaleInMemorySagas() {
        Duration maxAge = properties.inMemoryMaxAge();
        Instant cutoff = Instant.now().minus(maxAge);

        List<String> staleSagaIds = sagaStartTimes.entrySet().stream()
                .filter(e -> e.getValue().isBefore(cutoff))
                .map(Map.Entry::getKey)
                .toList();

        if (staleSagaIds.isEmpty()) {
            return;
        }

        List<String> terminalStateNames = definition.getTerminalStates().stream()
                .map(Enum::name)
                .toList();

        int cleaned = 0;
        for (String sagaId : staleSagaIds) {
            Optional<SagaInstanceEntity> instance = instanceRepository.findBySagaIdAndSagaType(
                    sagaId, definition.getSagaType());

            boolean shouldCleanup = instance.isEmpty() ||
                    terminalStateNames.contains(instance.get().getCurrentState());

            if (shouldCleanup) {
                SagaStateMachine<S, E> sm = stateMachines.remove(sagaId);
                if (sm != null) {
                    try {
                        sm.stop();
                    } catch (Exception e) {
                        log.trace("Error stopping state machine during cleanup: {}", e.getMessage());
                    }
                }
                contextMap.remove(sagaId);
                sagaLocks.remove(sagaId);
                sagaStartTimes.remove(sagaId);
                cleaned++;
            }
        }

        if (cleaned > 0) {
            log.trace("Cleaned up {} stale in-memory sagas, remaining: {}", cleaned, stateMachines.size());
        }

        metricsRecorder.recordInMemorySagaCount(definition.getSagaType(), stateMachines.size());
    }

    private ReentrantLock getSagaLock(String sagaId) {
        return sagaLocks.computeIfAbsent(sagaId, k -> new ReentrantLock());
    }

    public SagaDefinition<S, E, C> getDefinition() {
        return definition;
    }

    public int getActiveSagaCount() {
        return stateMachines.size();
    }
}
