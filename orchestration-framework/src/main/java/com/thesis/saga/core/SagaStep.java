package com.thesis.saga.core;

import java.util.Optional;
import java.util.function.Function;

public interface SagaStep<S extends Enum<S>, E extends Enum<E>, C extends SagaContext> {

    String getName();

    int getOrder();

    S getState();

    E getSuccessEvent();

    E getFailureEvent();

    S getSuccessTargetState();

    S getFailureTargetState();

    String getCommandType();

    String getCommandTopic();

    Function<C, Object> getCommandFactory();

    Optional<String> getCompensationCommandType();

    Optional<String> getCompensationTopic();

    Optional<Function<C, Object>> getCompensationCommandFactory();

    Function<C, Boolean> isCompletedChecker();

    Optional<Class<?>> getSuccessReplyType();

    Optional<Class<?>> getFailureReplyType();

    Optional<Class<?>> getCompensationReplyType();

    default boolean hasCompensation() {
        return getCompensationCommandType().isPresent();
    }
}
