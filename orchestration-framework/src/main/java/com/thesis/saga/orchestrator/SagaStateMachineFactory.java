package com.thesis.saga.orchestrator;

import com.thesis.common.exception.SagaCommandProcessingException;
import com.thesis.saga.core.SagaContext;
import com.thesis.saga.core.SagaDefinition;
import com.thesis.saga.core.SagaStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.config.StateMachineBuilder;
import org.springframework.statemachine.config.builders.StateMachineStateConfigurer;
import org.springframework.statemachine.config.builders.StateMachineTransitionConfigurer;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor
public class SagaStateMachineFactory<S extends Enum<S>, E extends Enum<E>, C extends SagaContext> {

    private final SagaDefinition<S, E, C> definition;

    public SagaStateMachine<S, E> create(String sagaId) {
        try {
            StateMachine<S, E> sm = buildStateMachine(sagaId);
            return new SagaStateMachine<>(sm, sagaId);
        } catch (Exception e) {
            log.error("Failed to create state machine for saga {}: {}", sagaId, e.getMessage(), e);
            throw new SagaCommandProcessingException("Failed to create state machine", e);
        }
    }

    private StateMachine<S, E> buildStateMachine(String sagaId) throws Exception {
        StateMachineBuilder.Builder<S, E> builder = StateMachineBuilder.builder();

        configureStates(builder);
        configureTransitions(builder);

        StateMachine<S, E> machine = builder.build();
        machine.getExtendedState().getVariables().put("sagaId", sagaId);

        return machine;
    }

    private void configureStates(StateMachineBuilder.Builder<S, E> builder) throws Exception {
        StateMachineStateConfigurer<S, E> stateConfigurer = builder.configureStates();

        Set<S> allStates = collectAllStates();
        Set<S> endStates = new HashSet<>(definition.getTerminalStates());

        stateConfigurer.withStates()
                .initial(definition.getInitialState())
                .states(allStates)
                .end(endStates.iterator().next()); // At least one end state

        log.trace("Configured states for {}: initial={}, states={}, terminal={}",
                definition.getSagaType(), definition.getInitialState(), allStates, endStates);
    }

    private void configureTransitions(StateMachineBuilder.Builder<S, E> builder) throws Exception {
        StateMachineTransitionConfigurer<S, E> transitionConfigurer = builder.configureTransitions();

        S firstStepState = definition.getSteps().getFirst().getState();
        transitionConfigurer.withExternal()
                .source(definition.getInitialState())
                .target(firstStepState)
                .event(definition.getStartEvent());

        for (SagaStep<S, E, C> step : definition.getSteps()) {
            transitionConfigurer.withExternal()
                    .source(step.getState())
                    .target(step.getSuccessTargetState())
                    .event(step.getSuccessEvent());

            transitionConfigurer.withExternal()
                    .source(step.getState())
                    .target(step.getFailureTargetState())
                    .event(step.getFailureEvent());
        }

        S cancelledState = definition.getTerminalStates().size() > 1
                ? definition.getTerminalStates().getLast()
                : definition.getTerminalStates().getFirst();

        transitionConfigurer.withExternal()
                .source(definition.getCompensatingState())
                .target(cancelledState)
                .event(definition.getCompensationCompleteEvent());

        log.trace("Configured transitions for {}", definition.getSagaType());
    }

    private Set<S> collectAllStates() {
        Set<S> states = EnumSet.noneOf(definition.getStateClass());

        states.add(definition.getInitialState());
        states.addAll(definition.getTerminalStates());
        states.add(definition.getCompensatingState());

        for (SagaStep<S, E, C> step : definition.getSteps()) {
            states.add(step.getState());
            states.add(step.getSuccessTargetState());
            states.add(step.getFailureTargetState());
        }

        return states;
    }
}
