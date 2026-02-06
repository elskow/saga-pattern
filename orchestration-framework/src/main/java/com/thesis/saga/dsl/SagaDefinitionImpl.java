package com.thesis.saga.dsl;

import com.thesis.saga.core.SagaContext;
import com.thesis.saga.core.SagaDefinition;
import com.thesis.saga.core.SagaStep;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class SagaDefinitionImpl<S extends Enum<S>, E extends Enum<E>, C extends SagaContext>
        implements SagaDefinition<S, E, C> {

    private final String sagaType;
    private final Class<S> stateClass;
    private final Class<E> eventClass;
    private final Class<C> contextClass;
    private final S initialState;
    private final List<S> terminalStates;
    private final S compensatingState;
    private final E startEvent;
    private final E compensationCompleteEvent;
    private final List<SagaStep<S, E, C>> steps;

    @Override
    public String toString() {
        return "SagaDefinition[type=%s, steps=%d, states=%s]"
                .formatted(sagaType, steps.size(), stateClass.getSimpleName());
    }
}
