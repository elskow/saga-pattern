package com.thesis.orchestration.order.aot;

import com.thesis.common.aot.CommonRuntimeHints;
import com.thesis.orchestration.order.exception.GlobalExceptionHandler;
import com.thesis.orchestration.order.model.OrderEntity;
import com.thesis.orchestration.order.model.ProcessedCommand;
import com.thesis.orchestration.order.model.SagaInstance;
import com.thesis.orchestration.order.scheduler.SagaTimeoutScheduler;
import com.thesis.orchestration.order.statemachine.*;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

/**
 * GraalVM Native Image runtime hints for orchestration order-service.
 * Registers reflection hints for JPA entities, state machine enums, and related classes.
 * <p>
 * Note: The programmatic StateMachineBuilder approach used in OrderStateMachineConfig
 * is AOT-compatible and doesn't require extensive framework hints.
 */
public class OrderRuntimeHints implements RuntimeHintsRegistrar {

    private static final MemberCategory[] ENTITY_CATEGORIES = {
        MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
        MemberCategory.INVOKE_DECLARED_METHODS,
        MemberCategory.INVOKE_PUBLIC_METHODS,
        MemberCategory.DECLARED_FIELDS,
        MemberCategory.PUBLIC_FIELDS
    };

    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
        // Register common module hints
        new CommonRuntimeHints().registerHints(hints, classLoader);

        // JPA Entities
        hints.reflection().registerType(OrderEntity.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(OrderEntity.OrderStatus.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(SagaInstance.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(ProcessedCommand.class, ENTITY_CATEGORIES);

        // State Machine enums and application classes
        hints.reflection().registerType(OrderStates.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(OrderEvents.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(OrderSagaOrchestrator.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(OrderStateMachineConfig.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(OrderStateMachineConfig.OrderStateMachineFactory.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(SagaData.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(SagaData.SagaStep.class, ENTITY_CATEGORIES);

        // Controller and Scheduler
        hints.reflection().registerType(GlobalExceptionHandler.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(SagaTimeoutScheduler.class, ENTITY_CATEGORIES);
    }
}
