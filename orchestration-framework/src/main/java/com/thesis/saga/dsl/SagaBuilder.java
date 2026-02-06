package com.thesis.saga.dsl;

import com.thesis.saga.core.SagaContext;
import com.thesis.saga.core.SagaDefinition;
import com.thesis.saga.core.SagaStep;

import java.util.ArrayList;
import java.util.List;

public class SagaBuilder<S extends Enum<S>, E extends Enum<E>, C extends SagaContext> {

    private String sagaType;
    private Class<S> stateClass;
    private Class<E> eventClass;
    private Class<C> contextClass;
    private S initialState;
    private List<S> terminalStates = new ArrayList<>();
    private S compensatingState;
    private E startEvent;
    private E compensationCompleteEvent;
    private final List<SagaStep<S, E, C>> steps = new ArrayList<>();
    private int stepCounter = 0;

    private SagaBuilder() {
    }

    public static <S extends Enum<S>, E extends Enum<E>, C extends SagaContext>
            SagaBuilder<S, E, C> create(String sagaType) {
        SagaBuilder<S, E, C> builder = new SagaBuilder<>();
        builder.sagaType = sagaType;
        return builder;
    }

    public SagaBuilder<S, E, C> withStateClass(Class<S> stateClass) {
        this.stateClass = stateClass;
        return this;
    }

    public SagaBuilder<S, E, C> withEventClass(Class<E> eventClass) {
        this.eventClass = eventClass;
        return this;
    }

    public SagaBuilder<S, E, C> withContextClass(Class<C> contextClass) {
        this.contextClass = contextClass;
        return this;
    }

    public SagaBuilder<S, E, C> initialState(S state) {
        this.initialState = state;
        return this;
    }

    @SafeVarargs
    public final SagaBuilder<S, E, C> terminalStates(S... states) {
        this.terminalStates = List.of(states);
        return this;
    }

    public SagaBuilder<S, E, C> compensatingState(S state) {
        this.compensatingState = state;
        return this;
    }

    public SagaBuilder<S, E, C> startEvent(E event) {
        this.startEvent = event;
        return this;
    }

    public SagaBuilder<S, E, C> compensationCompleteEvent(E event) {
        this.compensationCompleteEvent = event;
        return this;
    }

    public StepBuilder<S, E, C> step(String stepName) {
        return new StepBuilder<>(this, stepName, stepCounter++);
    }

    void addStep(SagaStep<S, E, C> step) {
        this.steps.add(step);
    }

    public SagaDefinition<S, E, C> build() {
        validate();
        return SagaDefinitionImpl.<S, E, C>builder()
                .sagaType(sagaType)
                .stateClass(stateClass)
                .eventClass(eventClass)
                .contextClass(contextClass)
                .initialState(initialState)
                .terminalStates(List.copyOf(terminalStates))
                .compensatingState(compensatingState)
                .startEvent(startEvent)
                .compensationCompleteEvent(compensationCompleteEvent)
                .steps(List.copyOf(steps))
                .build();
    }

    private void validate() {
        if (sagaType == null || sagaType.isBlank()) {
            throw new IllegalStateException("Saga type is required");
        }
        if (stateClass == null) {
            throw new IllegalStateException("State class is required (use withStateClass())");
        }
        if (eventClass == null) {
            throw new IllegalStateException("Event class is required (use withEventClass())");
        }
        if (contextClass == null) {
            throw new IllegalStateException("Context class is required (use withContextClass())");
        }
        if (initialState == null) {
            throw new IllegalStateException("Initial state is required (use initialState())");
        }
        if (terminalStates.isEmpty()) {
            throw new IllegalStateException("At least one terminal state is required (use terminalStates())");
        }
        if (compensatingState == null) {
            throw new IllegalStateException("Compensating state is required (use compensatingState())");
        }
        if (startEvent == null) {
            throw new IllegalStateException("Start event is required (use startEvent())");
        }
        if (compensationCompleteEvent == null) {
            throw new IllegalStateException("Compensation complete event is required (use compensationCompleteEvent())");
        }
        if (steps.isEmpty()) {
            throw new IllegalStateException("At least one step is required");
        }
    }
}
