package com.thesis.saga.core;

import java.util.List;
import java.util.Optional;

public interface SagaDefinition<S extends Enum<S>, E extends Enum<E>, C extends SagaContext> {

    String getSagaType();

    List<SagaStep<S, E, C>> getSteps();

    S getInitialState();

    List<S> getTerminalStates();

    S getCompensatingState();

    E getStartEvent();

    E getCompensationCompleteEvent();

    Class<S> getStateClass();

    Class<E> getEventClass();

    Class<C> getContextClass();

    default Optional<SagaStep<S, E, C>> getStepForState(S state) {
        return getSteps().stream()
                .filter(step -> step.getState().equals(state))
                .findFirst();
    }

    default Optional<SagaStep<S, E, C>> getStepByName(String name) {
        return getSteps().stream()
                .filter(step -> step.getName().equals(name))
                .findFirst();
    }

    default Optional<SagaStep<S, E, C>> getStepByCommandType(String commandType) {
        return getSteps().stream()
                .filter(step -> step.getCommandType().equals(commandType))
                .findFirst();
    }

    default List<String> getAllCommandTopics() {
        return getSteps().stream()
                .map(SagaStep::getCommandTopic)
                .distinct()
                .toList();
    }

    default boolean isTerminalState(S state) {
        return getTerminalStates().contains(state);
    }

    default List<SagaStep<S, E, C>> getCompensationSteps(C context) {
        return getSteps().stream()
                .filter(step -> step.hasCompensation())
                .filter(step -> step.isCompletedChecker().apply(context))
                .sorted((a, b) -> Integer.compare(b.getOrder(), a.getOrder()))
                .toList();
    }
}
