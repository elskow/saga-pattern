package com.thesis.orchestration.inventory.aot;

import com.thesis.common.aot.CommonRuntimeHints;
import com.thesis.orchestration.inventory.model.ProductEntity;
import com.thesis.orchestration.inventory.model.ReservationEntity;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

/**
 * GraalVM Native Image runtime hints for orchestration inventory-service.
 * Registers reflection hints for JPA entities and related classes.
 */
public class InventoryRuntimeHints implements RuntimeHintsRegistrar {

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
        hints.reflection().registerType(ProductEntity.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(ReservationEntity.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(ReservationEntity.ReservationStatus.class, ENTITY_CATEGORIES);
    }
}
