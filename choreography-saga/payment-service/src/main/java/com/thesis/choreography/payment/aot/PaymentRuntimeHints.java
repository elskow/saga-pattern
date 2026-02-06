package com.thesis.choreography.payment.aot;

import com.thesis.choreography.payment.model.Payment;
import com.thesis.choreography.payment.model.ProcessedEvent;
import com.thesis.common.aot.CommonRuntimeHints;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

public class PaymentRuntimeHints implements RuntimeHintsRegistrar {

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

        hints.reflection().registerType(Payment.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(Payment.PaymentStatus.class, ENTITY_CATEGORIES);
        hints.reflection().registerType(ProcessedEvent.class, ENTITY_CATEGORIES);
    }
}
