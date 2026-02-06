package com.thesis.saga.dsl;

import com.thesis.saga.core.SagaContext;
import com.thesis.saga.core.SagaStep;
import lombok.Builder;
import lombok.Getter;

import java.util.Optional;
import java.util.function.Function;

@Getter
@Builder
public class SagaStepImpl<S extends Enum<S>, E extends Enum<E>, C extends SagaContext>
        implements SagaStep<S, E, C> {

    private final String name;
    private final int order;
    private final S state;
    private final E successEvent;
    private final E failureEvent;
    private final S successTargetState;
    private final S failureTargetState;
    private final String commandType;
    private final String commandTopic;
    private final Function<C, Object> commandFactory;
    private final String compensationCommandType;
    private final String compensationTopic;
    private final Function<C, Object> compensationCommandFactory;
    private final Function<C, Boolean> completedChecker;
    private final Class<?> successReplyType;
    private final Class<?> failureReplyType;
    private final Class<?> compensationReplyType;

    @Override
    public Optional<String> getCompensationCommandType() {
        return Optional.ofNullable(compensationCommandType);
    }

    @Override
    public Optional<String> getCompensationTopic() {
        return Optional.ofNullable(compensationTopic);
    }

    @Override
    public Optional<Function<C, Object>> getCompensationCommandFactory() {
        return Optional.ofNullable(compensationCommandFactory);
    }

    @Override
    public Function<C, Boolean> isCompletedChecker() {
        return completedChecker != null ? completedChecker : ctx -> true;
    }

    @Override
    public Optional<Class<?>> getSuccessReplyType() {
        return Optional.ofNullable(successReplyType);
    }

    @Override
    public Optional<Class<?>> getFailureReplyType() {
        return Optional.ofNullable(failureReplyType);
    }

    @Override
    public Optional<Class<?>> getCompensationReplyType() {
        return Optional.ofNullable(compensationReplyType);
    }
}
