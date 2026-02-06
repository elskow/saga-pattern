package com.thesis.saga.orchestrator;

import lombok.extern.slf4j.Slf4j;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.support.DefaultStateMachineContext;
import reactor.core.publisher.Mono;
import org.springframework.messaging.support.MessageBuilder;

@Slf4j
public class SagaStateMachine<S extends Enum<S>, E extends Enum<E>> {

    private final StateMachine<S, E> stateMachine;
    private final String sagaId;

    public SagaStateMachine(StateMachine<S, E> stateMachine, String sagaId) {
        this.stateMachine = stateMachine;
        this.sagaId = sagaId;
    }

    public StateMachine<S, E> getStateMachine() {
        return stateMachine;
    }

    public S getCurrentState() {
        return stateMachine.getState().getId();
    }

    public void start() {
        stateMachine.startReactively().block();
        log.trace("Started state machine for saga {}, initial state: {}", sagaId, getCurrentState());
    }

    public void stop() {
        stateMachine.stopReactively().block();
        log.trace("Stopped state machine for saga {}", sagaId);
    }

    public boolean sendEvent(E event) {
        S beforeState = getCurrentState();

        try {
            var result = stateMachine.sendEvent(
                    Mono.just(MessageBuilder.withPayload(event).build()))
                    .blockFirst();

            S afterState = getCurrentState();
            boolean success = result != null &&
                    result.getResultType() == org.springframework.statemachine.StateMachineEventResult.ResultType.ACCEPTED;

            if (success) {
                log.trace("Saga {} transition: {} --[{}]--> {}",
                        sagaId, beforeState, event, afterState);
            } else {
                log.warn("Saga {} event {} not accepted in state {}",
                        sagaId, event, beforeState);
            }

            return success;
        } catch (Exception e) {
            log.error("Error sending event {} to saga {}: {}", event, sagaId, e.getMessage());
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public void restoreToState(S targetState) {
        stateMachine.stopReactively().block();
        stateMachine.getStateMachineAccessor()
                .doWithAllRegions(accessor -> accessor.resetStateMachineReactively(
                        new DefaultStateMachineContext<>(targetState, null, null, null))
                        .block());
        stateMachine.startReactively().block();
        log.trace("Restored saga {} state machine to state {}", sagaId, targetState);
    }

    public boolean isInState(S state) {
        return getCurrentState().equals(state);
    }
}
