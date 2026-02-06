package com.thesis.saga.aot;

import com.thesis.saga.core.AbstractSagaContext;
import com.thesis.saga.core.SagaContext;
import com.thesis.saga.core.SagaDefinition;
import com.thesis.saga.core.SagaStep;
import com.thesis.saga.dsl.SagaBuilder;
import com.thesis.saga.dsl.SagaDefinitionImpl;
import com.thesis.saga.dsl.SagaStepImpl;
import com.thesis.saga.dsl.StepBuilder;
import com.thesis.saga.orchestrator.AbstractSagaOrchestrator;
import com.thesis.saga.orchestrator.SagaStateMachine;
import com.thesis.saga.orchestrator.SagaStateMachineFactory;
import com.thesis.saga.participant.AbstractSagaParticipant;
import com.thesis.saga.participant.CommandHandler;
import com.thesis.saga.persistence.OutboxEntity;
import com.thesis.saga.persistence.ProcessedMessageEntity;
import com.thesis.saga.persistence.SagaInstanceEntity;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;

import java.util.List;

public class SagaFrameworkRuntimeHints implements RuntimeHintsRegistrar {

    private static final MemberCategory[] REFLECTION_CATEGORIES = {
            MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
            MemberCategory.INVOKE_DECLARED_METHODS,
            MemberCategory.DECLARED_FIELDS
    };

    private static final MemberCategory[] SERIALIZATION_CATEGORIES = {
            MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
            MemberCategory.INVOKE_DECLARED_METHODS,
            MemberCategory.INVOKE_PUBLIC_METHODS,
            MemberCategory.DECLARED_FIELDS,
            MemberCategory.PUBLIC_FIELDS
    };

    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
        registerReflection(hints, List.of(
                SagaContext.class,
                AbstractSagaContext.class,
                SagaStep.class,
                SagaDefinition.class
        ));

        registerReflection(hints, List.of(
                SagaBuilder.class,
                StepBuilder.class,
                SagaDefinitionImpl.class,
                SagaStepImpl.class
        ));

        registerReflection(hints, List.of(
                AbstractSagaOrchestrator.class,
                SagaStateMachine.class,
                SagaStateMachineFactory.class
        ));

        registerReflection(hints, List.of(
                AbstractSagaParticipant.class,
                CommandHandler.class
        ));

        registerSerialization(hints, List.of(
                SagaInstanceEntity.class,
                OutboxEntity.class,
                ProcessedMessageEntity.class
        ));

        hints.reflection().registerType(
                TypeReference.of("java.util.function.Function"),
                REFLECTION_CATEGORIES);

        hints.resources().registerPattern("META-INF/spring/*.factories");
    }

    private void registerReflection(RuntimeHints hints, List<Class<?>> classes) {
        for (Class<?> clazz : classes) {
            hints.reflection().registerType(clazz, REFLECTION_CATEGORIES);
        }
    }

    private void registerSerialization(RuntimeHints hints, List<Class<?>> classes) {
        for (Class<?> clazz : classes) {
            hints.reflection().registerType(clazz, SERIALIZATION_CATEGORIES);
            if (java.io.Serializable.class.isAssignableFrom(clazz)) {
                @SuppressWarnings("unchecked")
                Class<? extends java.io.Serializable> serializableClass =
                        (Class<? extends java.io.Serializable>) clazz;
                hints.serialization().registerType(serializableClass);
            }
        }
    }
}
