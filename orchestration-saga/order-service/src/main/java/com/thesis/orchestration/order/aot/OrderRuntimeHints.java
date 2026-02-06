package com.thesis.orchestration.order.aot;

import com.thesis.common.aot.CommonRuntimeHints;
import com.thesis.orchestration.order.exception.GlobalExceptionHandler;
import com.thesis.orchestration.order.model.OrderEntity;
import com.thesis.orchestration.order.saga.OrderSagaContext;
import com.thesis.orchestration.order.saga.OrderSagaDefinition;
import com.thesis.orchestration.order.saga.OrderSagaOrchestrator;
import com.thesis.orchestration.order.statemachine.OrderEvents;
import com.thesis.orchestration.order.statemachine.OrderStates;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

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
        new CommonRuntimeHints().registerHints(hints, classLoader);

        hints.reflection().registerType(OrderEntity.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(OrderEntity.OrderStatus.class, ENTITY_CATEGORIES);

        hints.reflection().registerType(OrderStates.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(OrderEvents.class, ENTITY_CATEGORIES);

        hints.reflection().registerType(OrderSagaOrchestrator.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(OrderSagaContext.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(OrderSagaDefinition.class, ENTITY_CATEGORIES);

        hints.reflection().registerType(GlobalExceptionHandler.class, ENTITY_CATEGORIES);
    }
}
