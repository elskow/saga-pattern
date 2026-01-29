package com.thesis.choreography.inventory.aot;

import com.thesis.choreography.inventory.model.InventoryReservation;
import com.thesis.choreography.inventory.model.PendingOrderItem;
import com.thesis.choreography.inventory.model.ProcessedEvent;
import com.thesis.choreography.inventory.model.Product;
import com.thesis.common.aot.CommonRuntimeHints;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

/**
 * GraalVM Native Image runtime hints for choreography inventory-service.
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
        hints.reflection().registerType(Product.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(InventoryReservation.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(InventoryReservation.ReservationStatus.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(PendingOrderItem.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(ProcessedEvent.class, ENTITY_CATEGORIES);
    }
}
