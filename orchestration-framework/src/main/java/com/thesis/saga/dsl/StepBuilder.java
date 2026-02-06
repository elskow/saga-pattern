package com.thesis.saga.dsl;

import com.thesis.saga.core.SagaContext;
import com.thesis.saga.core.SagaStep;

import java.util.function.Function;

public class StepBuilder<S extends Enum<S>, E extends Enum<E>, C extends SagaContext> {

    private final SagaBuilder<S, E, C> parent;
    private final String stepName;
    private final int order;

    private S state;
    private String commandType;
    private String commandTopic;
    private Function<C, Object> commandFactory;
    private E successEvent;
    private S successTargetState;
    private E failureEvent;
    private S failureTargetState;
    private String compensationCommandType;
    private String compensationTopic;
    private Function<C, Object> compensationFactory;
    private Function<C, Boolean> completedChecker;
    private Class<?> successReplyType;
    private Class<?> failureReplyType;
    private Class<?> compensationReplyType;

    StepBuilder(SagaBuilder<S, E, C> parent, String stepName, int order) {
        this.parent = parent;
        this.stepName = stepName;
        this.order = order;
    }

    public StepBuilder<S, E, C> onState(S state) {
        this.state = state;
        return this;
    }

    public StepBuilder<S, E, C> sendCommand(String commandType, String topic) {
        this.commandType = commandType;
        this.commandTopic = topic;
        return this;
    }

    public StepBuilder<S, E, C> withPayload(Function<C, Object> factory) {
        this.commandFactory = factory;
        return this;
    }

    public StepBuilder<S, E, C> onSuccess(E event, S targetState) {
        this.successEvent = event;
        this.successTargetState = targetState;
        return this;
    }

    public StepBuilder<S, E, C> onFailure(E event, S targetState) {
        this.failureEvent = event;
        this.failureTargetState = targetState;
        return this;
    }

    public StepBuilder<S, E, C> withCompensation(String commandType, String topic) {
        this.compensationCommandType = commandType;
        this.compensationTopic = topic;
        return this;
    }

    public StepBuilder<S, E, C> compensationPayload(Function<C, Object> factory) {
        this.compensationFactory = factory;
        return this;
    }

    public StepBuilder<S, E, C> isCompleted(Function<C, Boolean> checker) {
        this.completedChecker = checker;
        return this;
    }

    public StepBuilder<S, E, C> successReplyType(Class<?> replyType) {
        this.successReplyType = replyType;
        return this;
    }

    public StepBuilder<S, E, C> failureReplyType(Class<?> replyType) {
        this.failureReplyType = replyType;
        return this;
    }

    public StepBuilder<S, E, C> compensationReplyType(Class<?> replyType) {
        this.compensationReplyType = replyType;
        return this;
    }

    public SagaBuilder<S, E, C> endStep() {
        validate();
        SagaStep<S, E, C> step = SagaStepImpl.<S, E, C>builder()
                .name(stepName)
                .order(order)
                .state(state)
                .successEvent(successEvent)
                .failureEvent(failureEvent)
                .successTargetState(successTargetState)
                .failureTargetState(failureTargetState)
                .commandType(commandType)
                .commandTopic(commandTopic)
                .commandFactory(commandFactory)
                .compensationCommandType(compensationCommandType)
                .compensationTopic(compensationTopic)
                .compensationCommandFactory(compensationFactory)
                .completedChecker(completedChecker)
                .successReplyType(successReplyType)
                .failureReplyType(failureReplyType)
                .compensationReplyType(compensationReplyType)
                .build();
        parent.addStep(step);
        return parent;
    }

    private void validate() {
        if (stepName == null || stepName.isBlank()) {
            throw new IllegalStateException("Step name is required");
        }
        if (state == null) {
            throw new IllegalStateException("Step state is required (use onState())");
        }
        if (commandType == null || commandTopic == null) {
            throw new IllegalStateException("Command type and topic are required (use sendCommand())");
        }
        if (commandFactory == null) {
            throw new IllegalStateException("Command payload factory is required (use withPayload())");
        }
        if (successEvent == null || successTargetState == null) {
            throw new IllegalStateException("Success outcome is required (use onSuccess())");
        }
        if (failureEvent == null || failureTargetState == null) {
            throw new IllegalStateException("Failure outcome is required (use onFailure())");
        }
        if (compensationCommandType != null && compensationFactory == null) {
            throw new IllegalStateException("Compensation payload factory is required when compensation is defined");
        }
    }
}
