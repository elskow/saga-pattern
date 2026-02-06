package com.thesis.choreography.order.aot;

import com.thesis.choreography.order.model.Order;
import com.thesis.choreography.order.model.OrderItem;
import com.thesis.choreography.order.model.ProcessedEvent;
import com.thesis.common.aot.CommonRuntimeHints;
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

        hints.reflection().registerType(Order.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(Order.OrderStatus.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(OrderItem.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(ProcessedEvent.class, ENTITY_CATEGORIES);
    }
}
