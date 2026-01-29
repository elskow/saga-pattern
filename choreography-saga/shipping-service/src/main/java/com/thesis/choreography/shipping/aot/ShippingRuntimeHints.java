package com.thesis.choreography.shipping.aot;

import com.thesis.choreography.shipping.model.PendingShippingAddress;
import com.thesis.choreography.shipping.model.ProcessedEvent;
import com.thesis.choreography.shipping.model.Shipment;
import com.thesis.common.aot.CommonRuntimeHints;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

/**
 * GraalVM Native Image runtime hints for choreography shipping-service.
 * Registers reflection hints for JPA entities and related classes.
 */
public class ShippingRuntimeHints implements RuntimeHintsRegistrar {

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
        hints.reflection().registerType(Shipment.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(Shipment.ShippingStatus.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(PendingShippingAddress.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(ProcessedEvent.class, ENTITY_CATEGORIES);
    }
}
